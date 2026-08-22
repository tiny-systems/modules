package cron

import (
	"testing"

	"github.com/tiny-systems/common-module/internal/testharness"
)

// The trap this closes: a user pastes a credential into a running cron's
// widget and presses the button. The message that arrives while running
// carries the context but not the Start flag — the port's schema differs by
// state — so it used to be dropped in silence, and the next scheduled run
// still used the old context.
func TestRunningCronAppliesAFreshContext(t *testing.T) {
	c, ok := (&Component{}).Instance().(*Component)
	if !ok {
		t.Fatal("Instance() did not return *Component")
	}
	testharness.New(c)

	if err := c.handleControl(ControlStopped{Schedule: "*/5 * * * *", Context: map[string]interface{}{"apiKey": ""}}); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = c.stop() }()

	if err := c.handleControl(ControlRunning{Context: map[string]interface{}{"apiKey": "supplied-later"}}); err != nil {
		t.Fatalf("apply while running: %v", err)
	}

	c.mu.Lock()
	got, _ := c.settings.Context.(map[string]interface{})
	c.mu.Unlock()
	if got == nil || got["apiKey"] != "supplied-later" {
		t.Fatalf("context = %v, want the value supplied while running", got)
	}
}

// A redelivery of the same values must not restart the schedule.
func TestRunningCronIgnoresAnUnchangedContext(t *testing.T) {
	c, _ := (&Component{}).Instance().(*Component)
	testharness.New(c)

	same := map[string]interface{}{"apiKey": "k"}
	if err := c.handleControl(ControlStopped{Schedule: "*/5 * * * *", Context: same}); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = c.stop() }()

	if !c.contextMatches(map[string]interface{}{"apiKey": "k"}) {
		t.Fatal("an identical context should compare equal, or every redelivery restarts the cron")
	}
}
