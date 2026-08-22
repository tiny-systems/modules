package arrayfilter

import (
	"context"
	"testing"

	"github.com/tiny-systems/module/module"
)

func run(t *testing.T, in Request, settings Settings) (string, interface{}, error) {
	t.Helper()
	c, ok := (&Component{}).Instance().(*Component)
	if !ok {
		t.Fatal("Instance() did not return *Component")
	}
	if err := c.OnSettings(context.Background(), settings); err != nil {
		t.Fatalf("settings: %v", err)
	}

	var gotPort string
	var gotMsg interface{}
	res := c.Handle(context.Background(), func(_ context.Context, port string, msg interface{}) module.Result {
		gotPort, gotMsg = port, msg
		return module.Result{}
	}, RequestPort, in)
	return gotPort, gotMsg, res.Err()
}

func pods() []any {
	return []any{
		map[string]any{"name": "api-1", "hasProblem": false, "restarts": float64(0)},
		map[string]any{"name": "api-2", "hasProblem": true, "restarts": float64(7)},
		map[string]any{"name": "web-1", "hasProblem": true, "restarts": float64(1)},
	}
}

// The filter a flow wrote in JavaScript, twice, because nothing else could.
func TestKeepsMatchingItems(t *testing.T) {
	port, msg, err := run(t, Request{Array: pods(), Query: "$.hasProblem"}, Settings{})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if port != ResultPort {
		t.Fatalf("emitted on %q", port)
	}
	out := msg.(Result)
	if out.Count != 2 || out.Total != 3 {
		t.Fatalf("count=%d total=%d, want 2 of 3", out.Count, out.Total)
	}
	// Order is the caller's; reordering silently would break anything that
	// pairs the result against another list.
	if out.Items[0].(map[string]any)["name"] != "api-2" {
		t.Errorf("first kept item = %v, want api-2 — original order must hold", out.Items[0])
	}
}

func TestComparisonsAndNumbers(t *testing.T) {
	_, msg, err := run(t, Request{Array: pods(), Query: "$.restarts > 3"}, Settings{})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if got := msg.(Result).Count; got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
}

// Nothing matching is an ordinary answer, not an error — a flow branches on
// count, and an empty result is how "all healthy" is expressed.
func TestNoMatchesIsAnAnswer(t *testing.T) {
	_, msg, err := run(t, Request{Array: pods(), Query: "$.name == 'nope'"}, Settings{})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	out := msg.(Result)
	if out.Count != 0 || out.Total != 3 {
		t.Fatalf("count=%d total=%d, want 0 of 3", out.Count, out.Total)
	}
	if out.Items == nil {
		t.Error("items should be an empty array, not null — a downstream map over null is a different failure")
	}
}

// A mistyped field is the likeliest mistake, and the expensive version of
// getting it wrong is silence: everything filtered away, no error, an agent
// concluding the cluster is healthy.
func TestUnevaluableItemFailsLoudlyByDefault(t *testing.T) {
	_, _, err := run(t, Request{
		Array: []any{map[string]any{"name": "api-1"}},
		Query: "$.hasPrblem",
	}, Settings{})
	if err == nil {
		t.Fatal("a predicate that cannot be evaluated was treated as 'no match'")
	}
}

// A ragged array is a real shape, so the behaviour is available — just not the
// default, and only when the author asks for it.
func TestSkipUnevaluableWhenAsked(t *testing.T) {
	_, msg, err := run(t, Request{
		Array: []any{
			map[string]any{"name": "api-1", "hasProblem": true},
			map[string]any{"name": "api-2"},
		},
		Query: "$.hasProblem",
	}, Settings{SkipUnevaluable: true})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	out := msg.(Result)
	if out.Count != 1 || out.Total != 2 {
		t.Fatalf("count=%d total=%d, want 1 of 2", out.Count, out.Total)
	}
}

// A predicate is a yes-or-no question. Treating a number or a string as truthy
// would keep items for a reason nobody wrote down.
func TestNonBooleanPredicateIsRefused(t *testing.T) {
	_, _, err := run(t, Request{Array: pods(), Query: "$.name"}, Settings{})
	if err == nil {
		t.Fatal("a predicate returning a string was accepted as a filter")
	}
}

func TestEmptyQueryIsRefused(t *testing.T) {
	if _, _, err := run(t, Request{Array: pods()}, Settings{}); err == nil {
		t.Fatal("an empty predicate was accepted")
	}
}

func TestEmptyArrayIsFine(t *testing.T) {
	_, msg, err := run(t, Request{Array: []any{}, Query: "$.hasProblem"}, Settings{})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if out := msg.(Result); out.Count != 0 || out.Total != 0 {
		t.Fatalf("count=%d total=%d, want 0 of 0", out.Count, out.Total)
	}
}

func TestContextIsCarried(t *testing.T) {
	_, msg, err := run(t, Request{
		Context: map[string]any{"runId": "r1"},
		Array:   pods(),
		Query:   "$.hasProblem",
	}, Settings{})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	carried, _ := msg.(Result).Context.(map[string]any)
	if carried["runId"] != "r1" {
		t.Fatalf("context = %v, want it carried", msg.(Result).Context)
	}
}

func TestErrorPortRoutesInsteadOfFailing(t *testing.T) {
	port, msg, err := run(t, Request{Array: pods()}, Settings{EnableErrorPort: true})
	if err != nil {
		t.Fatalf("with the error port on, the run must not fail: %v", err)
	}
	if port != ErrorPort {
		t.Fatalf("emitted on %q, want %q", port, ErrorPort)
	}
	if msg.(Error).Error == "" {
		t.Error("the error port carried no message")
	}
}
