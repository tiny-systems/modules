package form

import (
	"context"
	"testing"
	"time"

	"github.com/tiny-systems/modules/common-module/internal/testharness"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/pkg/utils"
)

func leaderCtx() context.Context {
	return utils.WithLeader(context.Background(), true)
}

func newForm(t *testing.T, settings *Settings) (*testharness.Harness, *Component) {
	t.Helper()
	comp, ok := (&Component{}).Instance().(*Component)
	if !ok {
		t.Fatal("Instance() did not return *Component")
	}
	h := testharness.New(comp)
	if settings != nil {
		if r := h.Handle(context.Background(), v1alpha1.SettingsPort, *settings); r.Err() != nil {
			t.Fatalf("settings failed: %v", r.Err())
		}
	}
	return h, comp
}

func waitOutputs(t *testing.T, h *testharness.Harness, port string, n int) []any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		outs := h.PortOutputs(port)
		if len(outs) >= n {
			return outs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d outputs on %q, have %d", n, port, len(outs))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Submit emits the filled values on Out.
func TestSubmitEmitsValuesOnOut(t *testing.T) {
	h, _ := newForm(t, &Settings{Context: map[string]any{"apiKey": ""}})

	filled := map[string]any{"apiKey": "sk-test"}
	if r := h.Handle(leaderCtx(), v1alpha1.ControlPort, Control{Context: filled, Submit: true}); r.Err() != nil {
		t.Fatalf("control failed: %v", r.Err())
	}
	outs := waitOutputs(t, h, OutPort, 1)
	got, ok := outs[0].(map[string]any)
	if !ok {
		t.Fatalf("out payload is %T, want map", outs[0])
	}
	if got["apiKey"] != "sk-test" {
		t.Fatalf("out payload = %v, want submitted values", got)
	}
}

// Submit without values falls back to the configured settings context.
func TestSubmitFallsBackToSettingsContext(t *testing.T) {
	h, _ := newForm(t, &Settings{Context: map[string]any{"apiKey": "from-settings"}})

	if r := h.Handle(leaderCtx(), v1alpha1.ControlPort, Control{Submit: true}); r.Err() != nil {
		t.Fatalf("control failed: %v", r.Err())
	}
	outs := waitOutputs(t, h, OutPort, 1)
	got, _ := outs[0].(map[string]any)
	if got["apiKey"] != "from-settings" {
		t.Fatalf("out payload = %v, want settings context", outs[0])
	}
}

// A non-submit control write (keystroke sync) must not fire the flow.
func TestNonSubmitControlDoesNotEmit(t *testing.T) {
	h, _ := newForm(t, &Settings{})

	if r := h.Handle(leaderCtx(), v1alpha1.ControlPort, Control{Context: map[string]any{"apiKey": "typing"}}); r.Err() != nil {
		t.Fatalf("control failed: %v", r.Err())
	}
	time.Sleep(50 * time.Millisecond)
	if outs := h.PortOutputs(OutPort); len(outs) != 0 {
		t.Fatalf("expected no Out emit without Submit, got %d", len(outs))
	}
}

// The flow's Result persists and rides the control data — the widget shows
// the outcome inside the form card, and it survives an instance restart.
func TestResultPersistsAndPublishes(t *testing.T) {
	h, c := newForm(t, &Settings{Context: map[string]any{"apiKey": ""}})

	if r := h.Handle(context.Background(), ResultPort, ResultMessage{Text: "saved ••••1234 ✓"}); r.Err() != nil {
		t.Fatalf("result failed: %v", r.Err())
	}

	ctrl := c.control(context.Background())
	if ctrl.Result != "saved ••••1234 ✓" {
		t.Fatalf("control result = %q, want the flow's text", ctrl.Result)
	}
	if outs := h.PortOutputs(v1alpha1.ControlPort); len(outs) == 0 {
		t.Fatal("expected a control publish after Result")
	}
}

// Prefill puts the flow's values back INTO the fields — the masked saved
// secret shows as a filled password field, not as a sentence about one.
func TestResultPrefillFillsForm(t *testing.T) {
	h, c := newForm(t, &Settings{Context: map[string]any{"apiKey": ""}})

	msg := ResultMessage{Text: "saved ••••1234 ✓", Prefill: map[string]any{"apiKey": "••••1234"}}
	if r := h.Handle(context.Background(), ResultPort, msg); r.Err() != nil {
		t.Fatalf("result failed: %v", r.Err())
	}

	ctrl := c.control(context.Background())
	got, ok := ctrl.Context.(map[string]any)
	if !ok {
		t.Fatalf("control context is %T, want map", ctrl.Context)
	}
	if got["apiKey"] != "••••1234" {
		t.Fatalf("control context = %v, want the prefilled masked secret", got)
	}
	if ctrl.Result != "saved ••••1234 ✓" {
		t.Fatalf("control result = %q, want the flow's text", ctrl.Result)
	}
}

// A result belongs to the submission that produced it: leaving the previous
// one on screen while a new submission runs reads as if this one already
// succeeded.
func TestSubmitClearsPreviousResult(t *testing.T) {
	h, c := newForm(t, &Settings{Context: map[string]any{"apiKey": ""}})

	if r := h.Handle(context.Background(), ResultPort, ResultMessage{Text: "saved ••••1234 ✓"}); r.Err() != nil {
		t.Fatalf("result failed: %v", r.Err())
	}
	if got := c.control(context.Background()).Result; got == "" {
		t.Fatal("precondition: expected a stored result")
	}

	if r := h.Handle(leaderCtx(), v1alpha1.ControlPort, Control{Context: map[string]any{"apiKey": "sk-new"}, Submit: true}); r.Err() != nil {
		t.Fatalf("control failed: %v", r.Err())
	}
	if got := c.control(context.Background()).Result; got != "" {
		t.Fatalf("control result = %q, want it cleared while the new submission runs", got)
	}
	waitOutputs(t, h, OutPort, 1)
}
