package inject_test

import (
	"context"
	"testing"

	"github.com/tiny-systems/common-module/components/inject"
	"github.com/tiny-systems/common-module/internal/testharness"
)

func TestBasicFlow(t *testing.T) {
	h := testharness.New((&inject.Component{}).Instance())
	ctx := context.Background()

	h.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"labelSelector": "app=nginx", "namespace": "production"},
	})
	h.Handle(ctx, "message", inject.Message{Context: "tick-1"})

	outs := h.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	if o.Context != "tick-1" {
		t.Errorf("context: got %v, want 'tick-1'", o.Context)
	}
	cfg := o.Config.(map[string]any)
	if cfg["labelSelector"] != "app=nginx" {
		t.Errorf("labelSelector: got %v, want 'app=nginx'", cfg["labelSelector"])
	}
	if cfg["namespace"] != "production" {
		t.Errorf("namespace: got %v, want 'production'", cfg["namespace"])
	}
}

func TestMessageBeforeConfig(t *testing.T) {
	h := testharness.New((&inject.Component{}).Instance())
	ctx := context.Background()

	h.Handle(ctx, "message", inject.Message{Context: "early"})

	outs := h.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	if o.Config != nil {
		t.Errorf("expected nil config before any config sent, got %v", o.Config)
	}
}

func TestPodRestart(t *testing.T) {
	ctx := context.Background()
	pod1 := testharness.New((&inject.Component{}).Instance())

	// Pod 1 receives config
	pod1.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"ns": "prod", "label": "app=api"},
	})

	// What the config landed under is the State layer's business. What
	// matters is that a new instance can still find it — the assertion below,
	// which is the reason any of this is persisted at all.

	// Pod 2: fresh instance with pod1's metadata
	pod2 := pod1.NewPod()
	pod2.Reconcile(ctx)

	pod2.Handle(ctx, "message", inject.Message{Context: "from-cron"})

	outs := pod2.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("pod2: expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	if o.Config == nil {
		t.Fatal("pod2: config is nil after reconcile restore")
	}
	cfg := o.Config.(map[string]any)
	if cfg["ns"] != "prod" {
		t.Errorf("pod2: ns: got %v, want 'prod'", cfg["ns"])
	}
	if cfg["label"] != "app=api" {
		t.Errorf("pod2: label: got %v, want 'app=api'", cfg["label"])
	}
}

func TestStaleReconcileDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	// Fresh config via port
	h.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"version": "v2"},
	})

	// Stale reconcile arrives with old metadata
	stale := h.NewPod()
	stale.Metadata["inject-config"] = `{"version":"v1"}`

	// Feed stale metadata to the SAME component (not the new pod)
	// This simulates reconcile arriving after config port already set settingsFromPort=true
	h.Metadata = stale.Metadata
	h.Reconcile(ctx)

	h.Handle(ctx, "message", inject.Message{Context: "test"})

	outs := h.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	cfg := o.Config.(map[string]any)
	if cfg["version"] != "v2" {
		t.Errorf("stale reconcile overwrote fresh config: got %v, want 'v2'", cfg["version"])
	}
}

func TestConfigUpdate(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	h.Handle(ctx, "config", inject.Config{Data: map[string]any{"v": 1}})
	h.Handle(ctx, "message", inject.Message{Context: "a"})

	h.Handle(ctx, "config", inject.Config{Data: map[string]any{"v": 2}})
	h.Handle(ctx, "message", inject.Message{Context: "b"})

	outs := h.PortOutputs("output")
	if len(outs) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(outs))
	}

	o1 := outs[0].(inject.Output)
	// JSON numbers deserialize as float64 in Go
	if v, ok := o1.Config.(map[string]any)["v"]; !ok || v != 1 {
		t.Errorf("first output: v=%v, want 1", v)
	}

	o2 := outs[1].(inject.Output)
	if v, ok := o2.Config.(map[string]any)["v"]; !ok || v != 2 {
		t.Errorf("second output: v=%v, want 2", v)
	}
}

func TestConfigSurvivesTheInstanceThatReceivedIt(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	h.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"key": "value"},
	})

	// The runtime recreates a component instance whenever a node reconciles,
	// which zeroes every in-memory field. A config that lives only in the
	// instance is gone at that moment, and the next message goes out with
	// nothing attached — silently, since nil is a legal config.
	restarted := h.NewPod()
	restarted.Reconcile(ctx)
	restarted.Handle(ctx, "message", inject.Message{Context: "after restart"})

	outs := restarted.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	config, ok := outs[0].(inject.Output).Config.(map[string]any)
	if !ok {
		t.Fatalf("config came back as %T, want the map that was stored", outs[0].(inject.Output).Config)
	}
	if config["key"] != "value" {
		t.Fatalf("config = %v, want the value the first instance received", config)
	}
}

func TestMultipleMessagesShareConfig(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	h.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"env": "staging"},
	})

	for i := 0; i < 5; i++ {
		h.Handle(ctx, "message", inject.Message{Context: i})
	}

	outs := h.PortOutputs("output")
	if len(outs) != 5 {
		t.Fatalf("expected 5 outputs, got %d", len(outs))
	}

	for i, out := range outs {
		o := out.(inject.Output)
		if o.Config == nil {
			t.Errorf("output %d: config is nil", i)
			continue
		}
		cfg := o.Config.(map[string]any)
		if cfg["env"] != "staging" {
			t.Errorf("output %d: env=%v, want 'staging'", i, cfg["env"])
		}
	}
}

func TestPodRestartMultipleRounds(t *testing.T) {
	ctx := context.Background()

	// Pod 1: set config
	pod1 := testharness.New((&inject.Component{}).Instance())
	pod1.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"round": 1},
	})

	// Pod 2: restore, update config
	pod2 := pod1.NewPod()
	pod2.Reconcile(ctx)
	pod2.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"round": 2},
	})

	// Pod 3: restore from pod2's metadata
	pod3 := pod2.NewPod()
	pod3.Reconcile(ctx)
	pod3.Handle(ctx, "message", inject.Message{Context: "final"})

	outs := pod3.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	cfg := o.Config.(map[string]any)
	// JSON round-trip turns int to float64
	if cfg["round"] != float64(2) {
		t.Errorf("pod3: round=%v, want 2 (from pod2's update)", cfg["round"])
	}
}

func TestConfigRequired_MessageBeforeConfig(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	// Enable config required
	h.Handle(ctx, "_settings", inject.Settings{ConfigRequired: true})

	// Message arrives without config
	h.Handle(ctx, "message", inject.Message{Context: "tick"})

	// Should go to error port, not output
	outs := h.PortOutputs("output")
	if len(outs) != 0 {
		t.Fatalf("expected 0 outputs, got %d — message should not pass without config", len(outs))
	}

	errs := h.PortOutputs("error")
	if len(errs) != 1 {
		t.Fatalf("expected 1 error, got %d", len(errs))
	}
	e := errs[0].(inject.ErrorOutput)
	if e.Context != "tick" {
		t.Errorf("error context: got %v, want 'tick'", e.Context)
	}
	if e.Error != "config not set" {
		t.Errorf("error message: got %q, want 'config not set'", e.Error)
	}
}

func TestConfigRequired_MessageAfterConfig(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	h.Handle(ctx, "_settings", inject.Settings{ConfigRequired: true})
	h.Handle(ctx, "config", inject.Config{Data: map[string]any{"env": "prod"}})
	h.Handle(ctx, "message", inject.Message{Context: "tick"})

	// Should go to output, not error
	errs := h.PortOutputs("error")
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors, got %d", len(errs))
	}

	outs := h.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	if o.Config == nil {
		t.Fatal("config is nil")
	}
}

func TestConfigRequired_ErrorPortVisibility(t *testing.T) {
	h := testharness.New((&inject.Component{}).Instance())
	ctx := context.Background()

	// Default: no error port
	ports := h.Ports()
	for _, p := range ports {
		if p.Name == "error" {
			t.Fatal("error port should not be visible when configRequired is false")
		}
	}

	// Enable config required
	h.Handle(ctx, "_settings", inject.Settings{ConfigRequired: true})

	ports = h.Ports()
	found := false
	for _, p := range ports {
		if p.Name == "error" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("error port should be visible when configRequired is true")
	}
}

func TestConfigRequired_PodRestart(t *testing.T) {
	ctx := context.Background()
	pod1 := testharness.New((&inject.Component{}).Instance())

	pod1.Handle(ctx, "_settings", inject.Settings{ConfigRequired: true})
	pod1.Handle(ctx, "config", inject.Config{
		Data: map[string]any{"label": "app=web"},
	})

	// Pod restart, config restored from metadata
	pod2 := pod1.NewPod()
	pod2.Handle(ctx, "_settings", inject.Settings{ConfigRequired: true})
	pod2.Reconcile(ctx)

	pod2.Handle(ctx, "message", inject.Message{Context: "cron-tick"})

	errs := pod2.PortOutputs("error")
	if len(errs) != 0 {
		t.Fatalf("expected 0 errors after config restore, got %d", len(errs))
	}

	outs := pod2.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	if o.Config == nil {
		t.Fatal("config is nil after pod restart — metadata restore failed")
	}
}

func TestConfigNotRequired_NilConfigPassesThrough(t *testing.T) {
	ctx := context.Background()
	h := testharness.New((&inject.Component{}).Instance())

	// Default settings: configRequired=false
	h.Handle(ctx, "message", inject.Message{Context: "tick"})

	// Should still go to output with nil config (backward compatible)
	outs := h.PortOutputs("output")
	if len(outs) != 1 {
		t.Fatalf("expected 1 output, got %d", len(outs))
	}
	o := outs[0].(inject.Output)
	if o.Config != nil {
		t.Errorf("expected nil config in backward-compatible mode, got %v", o.Config)
	}
}
