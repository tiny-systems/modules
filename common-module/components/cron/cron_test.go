package cron_test

import (
	"context"
	"testing"
	"time"

	"github.com/tiny-systems/common-module/components/cron"
	"github.com/tiny-systems/common-module/internal/testharness"
)

const wait = 300 * time.Millisecond

func newCron() *testharness.Harness {
	return testharness.New((&cron.Component{}).Instance())
}

// startCron starts cron as leader and registers t.Cleanup to stop it.
func startCron(t *testing.T, h *testharness.Harness, schedule string, cronCtx any) {
	t.Helper()
	h.HandleAsLeader(context.Background(), "_control", cron.ControlStopped{
		Start:    true,
		Schedule: schedule,
		Context:  cronCtx,
	})
	time.Sleep(wait)
	t.Cleanup(func() {
		h.HandleAsLeader(context.Background(), "_control", cron.ControlRunning{Stop: true})
		time.Sleep(wait)
	})
}

func getControlPort(h *testharness.Harness) any {
	for _, p := range h.Ports() {
		if p.Name == "_control" {
			return p.Configuration
		}
	}
	return nil
}

func TestStartAndStop(t *testing.T) {
	h := newCron()
	ctx := context.Background()

	h.HandleAsLeader(ctx, "_control", cron.ControlStopped{
		Start:    true,
		Schedule: "*/1 * * * *",
		Context:  "test-payload",
	})
	time.Sleep(wait)

	if h.Metadata["cron-running"] != "true" {
		t.Fatalf("expected cron-running=true, got %q", h.Metadata["cron-running"])
	}
	if h.Metadata["cron-schedule"] != "*/1 * * * *" {
		t.Errorf("expected schedule stored, got %q", h.Metadata["cron-schedule"])
	}

	// Stop
	h.HandleAsLeader(ctx, "_control", cron.ControlRunning{Stop: true})
	time.Sleep(wait)

	if _, ok := h.Metadata["cron-running"]; ok {
		t.Error("cron-running should be cleared after stop")
	}
	if _, ok := h.Metadata["cron-schedule"]; ok {
		t.Error("cron-schedule should be cleared after stop")
	}
	if _, ok := h.Metadata["cron-context"]; ok {
		t.Error("cron-context should be cleared after stop")
	}
	if _, ok := h.Metadata["cron-error"]; ok {
		t.Error("cron-error should be cleared after stop")
	}
}

func TestInvalidSchedule(t *testing.T) {
	h := newCron()
	ctx := context.Background()

	h.HandleAsLeader(ctx, "_control", cron.ControlStopped{
		Start:    true,
		Schedule: "not-a-cron",
		Context:  "test",
	})

	if h.Metadata["cron-error"] == "" {
		t.Fatal("expected error in metadata for invalid schedule")
	}
	if _, ok := h.Metadata["cron-running"]; ok {
		t.Error("should not be running with invalid schedule")
	}
}

func TestErrorClearedOnValidStart(t *testing.T) {
	h := newCron()
	ctx := context.Background()

	// Invalid → error set
	h.HandleAsLeader(ctx, "_control", cron.ControlStopped{
		Start:    true,
		Schedule: "invalid",
	})
	if h.Metadata["cron-error"] == "" {
		t.Fatal("expected error after invalid schedule")
	}

	// Valid start → error cleared
	startCron(t, h, "*/1 * * * *", "test")

	if _, ok := h.Metadata["cron-error"]; ok {
		t.Error("error should be cleared after valid start")
	}
}

func TestControlRequiresLeader(t *testing.T) {
	h := newCron()
	ctx := context.Background()

	// Non-leader start (Handle, not HandleAsLeader)
	h.Handle(ctx, "_control", cron.ControlStopped{
		Start:    true,
		Schedule: "*/1 * * * *",
		Context:  "test",
	})
	time.Sleep(wait)

	if _, ok := h.Metadata["cron-running"]; ok {
		t.Error("cron should not start for non-leader")
	}
}

func TestSettingsDeliveryNotRunning(t *testing.T) {
	h := newCron()
	ctx := context.Background()

	h.Handle(ctx, "_settings", cron.Settings{
		Schedule: "*/5 * * * *",
		Context:  map[string]any{"key": "value"},
	})

	// Not running → no metadata
	if _, ok := h.Metadata["cron-running"]; ok {
		t.Error("should not set metadata when not running")
	}

	// Settings stored internally (visible via Ports)
	ctrl, ok := getControlPort(h).(cron.ControlStopped)
	if !ok {
		t.Fatal("expected ControlStopped when not running")
	}
	if ctrl.Schedule != "*/5 * * * *" {
		t.Errorf("control schedule: got %q, want */5 * * * *", ctrl.Schedule)
	}
}

func TestSettingsUpdatesMetadataWhileRunning(t *testing.T) {
	h := newCron()
	ctx := context.Background()

	startCron(t, h, "*/1 * * * *", "original")

	// Update settings while running
	h.Handle(ctx, "_settings", cron.Settings{
		Schedule: "*/2 * * * *",
		Context:  "updated",
	})

	if h.Metadata["cron-schedule"] != "*/2 * * * *" {
		t.Errorf("schedule not updated in metadata: got %q", h.Metadata["cron-schedule"])
	}
}

func TestPodRestartResumesAsLeader(t *testing.T) {
	ctx := context.Background()
	pod1 := newCron()

	startCron(t, pod1, "*/1 * * * *", "restart-ctx")

	if pod1.Metadata["cron-running"] != "true" {
		t.Fatal("pod1 should be running")
	}

	// Simulate crash: new pod with pod1's metadata
	pod2 := pod1.NewPod()
	pod2.ReconcileAsLeader(ctx)
	time.Sleep(wait)
	t.Cleanup(func() {
		pod2.HandleAsLeader(ctx, "_control", cron.ControlRunning{Stop: true})
		time.Sleep(wait)
	})

	ctrl, ok := getControlPort(pod2).(cron.ControlRunning)
	if !ok {
		t.Fatal("expected ControlRunning after restart")
	}
	if ctrl.Status != "Running" {
		t.Errorf("pod2 status: got %q, want Running", ctrl.Status)
	}
	if ctrl.Schedule != "*/1 * * * *" {
		t.Errorf("pod2 schedule: got %q, want */1 * * * *", ctrl.Schedule)
	}
}

func TestPodRestartDoesNotResumeAsNonLeader(t *testing.T) {
	ctx := context.Background()
	pod1 := newCron()

	startCron(t, pod1, "*/1 * * * *", "ctx")

	// New pod, reconcile as non-leader
	pod2 := pod1.NewPod()
	pod2.Reconcile(ctx)
	time.Sleep(wait)

	ctrl, ok := getControlPort(pod2).(cron.ControlStopped)
	if !ok {
		t.Fatal("expected ControlStopped for non-leader pod")
	}
	if ctrl.Status == "Running" {
		t.Error("non-leader pod should not resume cron")
	}
}

func TestStaleReconcileDoesNotOverwriteFreshSettings(t *testing.T) {
	ctx := context.Background()
	h := newCron()

	// Fresh settings via port
	h.Handle(ctx, "_settings", cron.Settings{
		Schedule: "*/5 * * * *",
		Context:  "fresh",
	})

	// Reconcile with stale metadata
	h.Metadata["cron-schedule"] = "*/10 * * * *"
	h.Metadata["cron-context"] = `"stale"`
	h.Reconcile(ctx)

	ctrl, ok := getControlPort(h).(cron.ControlStopped)
	if !ok {
		t.Fatal("expected ControlStopped")
	}
	if ctrl.Schedule != "*/5 * * * *" {
		t.Errorf("stale reconcile overwrote schedule: got %q, want */5 * * * *", ctrl.Schedule)
	}
}

func TestPortsShowStartWhenStopped(t *testing.T) {
	h := newCron()

	ctrl, ok := getControlPort(h).(cron.ControlStopped)
	if !ok {
		t.Fatal("expected ControlStopped when not running")
	}
	if !ctrl.Start {
		t.Error("should show Start when stopped")
	}
	if ctrl.Status != "Not running" {
		t.Errorf("status: got %q, want 'Not running'", ctrl.Status)
	}
}

func TestPortsShowStopWhenRunning(t *testing.T) {
	h := newCron()
	startCron(t, h, "*/1 * * * *", "test")

	ctrl, ok := getControlPort(h).(cron.ControlRunning)
	if !ok {
		t.Fatal("expected ControlRunning when running")
	}
	if !ctrl.Stop {
		t.Error("should show Stop when running")
	}
	if ctrl.Status != "Running" {
		t.Errorf("status: got %q, want 'Running'", ctrl.Status)
	}
	if ctrl.NextRun == "" {
		t.Error("nextRun should be set when running")
	}
}

func TestStopWhileNotRunning(t *testing.T) {
	h := newCron()
	ctx := context.Background()

	result := h.HandleAsLeader(ctx, "_control", cron.ControlRunning{Stop: true})
	if err := result.Err(); err != nil {
		t.Errorf("stop while not running returned: %v", err)
	}
}

func TestMetadataContextPersistence(t *testing.T) {
	h := newCron()

	startCron(t, h, "*/1 * * * *", map[string]any{"hello": "world"})

	raw := h.Metadata["cron-context"]
	if raw == "" || raw == "null" {
		t.Fatalf("context not persisted: %q", raw)
	}
	if raw != `{"hello":"world"}` {
		t.Errorf("context: got %q, want {\"hello\":\"world\"}", raw)
	}
}

func TestPodRestartRestoresContext(t *testing.T) {
	ctx := context.Background()
	pod1 := newCron()

	startCron(t, pod1, "*/1 * * * *", map[string]any{"env": "prod"})

	// Pod restart
	pod2 := pod1.NewPod()
	pod2.ReconcileAsLeader(ctx)
	time.Sleep(wait)
	t.Cleanup(func() {
		pod2.HandleAsLeader(ctx, "_control", cron.ControlRunning{Stop: true})
		time.Sleep(wait)
	})

	ctrl, ok := getControlPort(pod2).(cron.ControlRunning)
	if !ok {
		t.Fatal("expected ControlRunning after restart")
	}
	ctxMap, ok := ctrl.Context.(map[string]any)
	if !ok {
		t.Fatalf("context not a map: %T", ctrl.Context)
	}
	if ctxMap["env"] != "prod" {
		t.Errorf("context env: got %v, want prod", ctxMap["env"])
	}
}

func TestSettingsFromPortGuard(t *testing.T) {
	ctx := context.Background()
	h := newCron()

	// Settings via port sets guard
	h.Handle(ctx, "_settings", cron.Settings{
		Schedule: "*/3 * * * *",
		Context:  "from-settings",
	})

	// Start overwrites settings but also sets guard
	startCron(t, h, "*/1 * * * *", "from-control")

	// Reconcile should not overwrite (settingsFromPort=true)
	h.Reconcile(ctx)

	ctrl, ok := getControlPort(h).(cron.ControlRunning)
	if !ok {
		t.Fatal("expected ControlRunning")
	}
	if ctrl.Schedule != "*/1 * * * *" {
		t.Errorf("reconcile overwrote schedule: got %q, want */1 * * * *", ctrl.Schedule)
	}
}
