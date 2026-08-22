package collect

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/tiny-systems/modules/common-module/internal/testharness"
	"github.com/tiny-systems/module/api/v1alpha1"
)

// newCollect returns a harness plus the concrete component so tests can
// drive the clock seam without sleeping.
func newCollect(t *testing.T, settings *Settings) (*testharness.Harness, *Component) {
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

func send(t *testing.T, h *testharness.Harness, in InMessage) error {
	t.Helper()
	return h.Handle(context.Background(), ItemPort, in).Err()
}

func mustSend(t *testing.T, h *testharness.Harness, in InMessage) {
	t.Helper()
	if err := send(t, h, in); err != nil {
		t.Fatalf("send %+v failed: %v", in, err)
	}
}

func responses(h *testharness.Harness) []OutMessage {
	var out []OutMessage
	for _, o := range h.PortOutputs(ResponsePort) {
		out = append(out, o.(OutMessage))
	}
	return out
}

func errorsOut(h *testharness.Harness) []ErrorMessage {
	var out []ErrorMessage
	for _, o := range h.PortOutputs(ErrorPort) {
		out = append(out, o.(ErrorMessage))
	}
	return out
}

// stateKeys counts leftover _state/ entries in harness metadata — completed
// and expired groups must not leak buffers.
func stateKeys(h *testharness.Harness) int {
	n := 0
	for k := range h.Metadata {
		if strings.HasPrefix(k, "_state/") {
			n++
		}
	}
	return n
}

func item(group string, idx, total int, payload any, ctx any) InMessage {
	return InMessage{Context: ctx, GroupKey: group, Index: idx, Total: total, Item: payload}
}

func TestAssembly(t *testing.T) {
	tests := []struct {
		name          string
		sends         []InMessage
		wantResponses []OutMessage
	}{
		{
			name: "in-order assembly",
			sends: []InMessage{
				item("g1", 0, 3, "a", "c0"),
				item("g1", 1, 3, "b", "c1"),
				item("g1", 2, 3, "c", "c2"),
			},
			wantResponses: []OutMessage{
				{Context: "c2", Items: []any{"a", "b", "c"}},
			},
		},
		{
			name: "out-of-order assembly ordered by index",
			sends: []InMessage{
				item("g1", 2, 3, "c", "c2"),
				item("g1", 0, 3, "a", "c0"),
				item("g1", 1, 3, "b", "c1"),
			},
			wantResponses: []OutMessage{
				// context from the LAST arriving item, items by index
				{Context: "c1", Items: []any{"a", "b", "c"}},
			},
		},
		{
			name: "duplicate index overwrites, does not double-count",
			sends: []InMessage{
				item("g1", 0, 2, "a", "c0"),
				item("g1", 0, 2, "a-newer", "c0b"),
				item("g1", 1, 2, "b", "c1"),
			},
			wantResponses: []OutMessage{
				{Context: "c1", Items: []any{"a-newer", "b"}},
			},
		},
		{
			name: "two interleaved groups",
			sends: []InMessage{
				item("g1", 0, 2, "1a", "x0"),
				item("g2", 0, 2, "2a", "y0"),
				item("g1", 1, 2, "1b", "x1"),
				item("g2", 1, 2, "2b", "y1"),
			},
			wantResponses: []OutMessage{
				{Context: "x1", Items: []any{"1a", "1b"}},
				{Context: "y1", Items: []any{"2a", "2b"}},
			},
		},
		{
			name: "total 1 completes immediately",
			sends: []InMessage{
				item("solo", 0, 1, "only", "c"),
			},
			wantResponses: []OutMessage{
				{Context: "c", Items: []any{"only"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newCollect(t, nil)
			for _, in := range tt.sends {
				mustSend(t, h, in)
			}
			got := responses(h)
			if !reflect.DeepEqual(got, tt.wantResponses) {
				t.Errorf("responses:\n got  %+v\n want %+v", got, tt.wantResponses)
			}
			if n := stateKeys(h); n != 0 {
				t.Errorf("completed groups must not leak state, %d keys left: %+v", n, h.Metadata)
			}
		})
	}
}

func TestPartialGroupPersistsInState(t *testing.T) {
	h, _ := newCollect(t, nil)
	mustSend(t, h, item("g1", 0, 3, "a", "c0"))

	if n := stateKeys(h); n != 1 {
		t.Fatalf("expected 1 buffered group in state, got %d: %+v", n, h.Metadata)
	}
	if len(responses(h)) != 0 {
		t.Fatal("incomplete group must not emit a response")
	}
}

func TestTimeoutExpiryOnTraffic(t *testing.T) {
	h, comp := newCollect(t, &Settings{EnableErrorPort: true, TimeoutSeconds: 10, MaxGroups: 100})

	base := time.Now()
	comp.now = func() time.Time { return base }

	mustSend(t, h, item("late", 0, 3, "a", "run-a"))
	mustSend(t, h, item("late", 1, 3, "b", "run-b"))

	// Time passes; traffic on a DIFFERENT group triggers the sweep.
	comp.now = func() time.Time { return base.Add(11 * time.Second) }
	mustSend(t, h, item("fresh", 0, 1, "only", "fresh-ctx"))

	errs := errorsOut(h)
	if len(errs) != 1 {
		t.Fatalf("expected 1 timeout error, got %d: %+v", len(errs), errs)
	}
	e := errs[0]
	if e.GroupKey != "late" || e.Received != 2 || e.Total != 3 {
		t.Errorf("error fields: %+v", e)
	}
	if e.Error != "collect timeout: got 2 of 3" {
		t.Errorf("error text: %q", e.Error)
	}
	if e.Context != "run-b" {
		t.Errorf("error context should be the last arriving item's, got %v", e.Context)
	}

	// The fresh group still completed.
	if got := responses(h); len(got) != 1 || got[0].Context != "fresh-ctx" {
		t.Errorf("fresh group response: %+v", got)
	}
	if n := stateKeys(h); n != 0 {
		t.Errorf("expired group must be deleted from state, %d keys left", n)
	}
}

func TestStragglerCompletesExpiredGroup(t *testing.T) {
	// A late item that COMPLETES its group is a success, not a timeout.
	h, comp := newCollect(t, &Settings{EnableErrorPort: true, TimeoutSeconds: 10, MaxGroups: 100})

	base := time.Now()
	comp.now = func() time.Time { return base }
	mustSend(t, h, item("g", 0, 2, "a", "c0"))

	comp.now = func() time.Time { return base.Add(11 * time.Second) }
	mustSend(t, h, item("g", 1, 2, "b", "c1"))

	if errs := errorsOut(h); len(errs) != 0 {
		t.Fatalf("late completion must not error: %+v", errs)
	}
	got := responses(h)
	want := []OutMessage{{Context: "c1", Items: []any{"a", "b"}}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("responses: got %+v, want %+v", got, want)
	}
}

func TestStragglerCannotSaveGroupFailsImmediately(t *testing.T) {
	// A late item that does NOT complete its group fails the group now,
	// counting the straggler, instead of waiting for more traffic.
	h, comp := newCollect(t, &Settings{EnableErrorPort: true, TimeoutSeconds: 10, MaxGroups: 100})

	base := time.Now()
	comp.now = func() time.Time { return base }
	mustSend(t, h, item("g", 0, 3, "a", "c0"))

	comp.now = func() time.Time { return base.Add(11 * time.Second) }
	mustSend(t, h, item("g", 1, 3, "b", "c1"))

	errs := errorsOut(h)
	if len(errs) != 1 {
		t.Fatalf("expected 1 timeout error, got %d", len(errs))
	}
	if errs[0].Received != 2 || errs[0].Total != 3 || errs[0].Context != "c1" {
		t.Errorf("error fields: %+v", errs[0])
	}
	if n := stateKeys(h); n != 0 {
		t.Errorf("expired group must be deleted from state, %d keys left", n)
	}
}

func TestTimeoutWithoutErrorPortFailsTheMessage(t *testing.T) {
	h, comp := newCollect(t, &Settings{EnableErrorPort: false, TimeoutSeconds: 10, MaxGroups: 100})

	base := time.Now()
	comp.now = func() time.Time { return base }
	mustSend(t, h, item("g", 0, 3, "a", "c0"))

	comp.now = func() time.Time { return base.Add(11 * time.Second) }
	err := send(t, h, item("g", 1, 3, "b", "c1"))
	if err == nil {
		t.Fatal("expected the straggler to fail when the error port is disabled")
	}
	if !strings.Contains(err.Error(), "collect timeout: got 2 of 3") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMaxGroupsGuard(t *testing.T) {
	h, _ := newCollect(t, &Settings{TimeoutSeconds: 300, MaxGroups: 2})

	mustSend(t, h, item("g1", 0, 2, "a", nil))
	mustSend(t, h, item("g2", 0, 2, "a", nil))

	err := send(t, h, item("g3", 0, 2, "a", nil))
	if err == nil {
		t.Fatal("expected error when max groups is reached")
	}
	if !strings.Contains(err.Error(), "too many in-flight groups") {
		t.Errorf("unexpected error: %v", err)
	}

	// An item for an EXISTING group must still be accepted.
	mustSend(t, h, item("g1", 1, 2, "b", nil))
	if len(responses(h)) != 1 {
		t.Errorf("existing group should complete despite the guard")
	}
}

func TestExpiredGroupsFreeMaxGroupsSlots(t *testing.T) {
	h, comp := newCollect(t, &Settings{EnableErrorPort: true, TimeoutSeconds: 10, MaxGroups: 1})

	base := time.Now()
	comp.now = func() time.Time { return base }
	mustSend(t, h, item("old", 0, 2, "a", nil))

	// Slot is taken until "old" expires; then a new group fits.
	comp.now = func() time.Time { return base.Add(11 * time.Second) }
	mustSend(t, h, item("new", 0, 2, "a", nil))

	if errs := errorsOut(h); len(errs) != 1 || errs[0].GroupKey != "old" {
		t.Errorf("expected old group to expire: %+v", errs)
	}
}

func TestValidation(t *testing.T) {
	tests := []struct {
		name    string
		in      InMessage
		wantErr string
	}{
		{"missing group key", item("", 0, 2, "a", nil), "groupKey is required"},
		{"total below one", item("g", 0, 0, "a", nil), "total must be >= 1"},
		{"index negative", item("g", -1, 2, "a", nil), "out of range"},
		{"index beyond total", item("g", 2, 2, "a", nil), "out of range"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newCollect(t, nil)
			err := send(t, h, tt.in)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestTotalMismatchFails(t *testing.T) {
	h, _ := newCollect(t, nil)
	mustSend(t, h, item("g", 0, 2, "a", nil))
	err := send(t, h, item("g", 1, 3, "b", nil))
	if err == nil {
		t.Fatal("expected total mismatch to fail")
	}
	if !strings.Contains(err.Error(), "total mismatch") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestStateTooLargeMessage(t *testing.T) {
	// The harness backs State with the real MetadataState guard, so a single
	// oversized item trips the ~900KB cap on Set.
	h, _ := newCollect(t, nil)
	big := strings.Repeat("x", 900*1024)
	err := send(t, h, item("g", 0, 2, big, nil))
	if err == nil {
		t.Fatal("expected oversized item to fail")
	}
	if !strings.Contains(err.Error(), "state budget exhausted") {
		t.Errorf("error should name the state budget, got: %v", err)
	}
	if !strings.Contains(err.Error(), "900KB") {
		t.Errorf("error should name the ~900KB cap, got: %v", err)
	}
}

func TestPodRestartKeepsPartialGroup(t *testing.T) {
	pod1, _ := newCollect(t, nil)
	mustSend(t, pod1, item("g", 0, 2, "a", "c0"))

	pod2 := pod1.NewPod()
	if err := pod2.Handle(context.Background(), ItemPort, item("g", 1, 2, "b", "c1")).Err(); err != nil {
		t.Fatalf("send on new pod failed: %v", err)
	}

	got := pod2.PortOutputs(ResponsePort)
	if len(got) != 1 {
		t.Fatalf("expected the restarted pod to complete the group, got %d responses", len(got))
	}
	out := got[0].(OutMessage)
	if !reflect.DeepEqual(out.Items, []any{"a", "b"}) {
		t.Errorf("items after restart: %+v", out.Items)
	}
}

func TestErrorPortHiddenByDefault(t *testing.T) {
	h, _ := newCollect(t, nil)
	for _, p := range h.Ports() {
		if p.Name == ErrorPort {
			t.Fatal("error port should not be visible unless enabled")
		}
	}
}
