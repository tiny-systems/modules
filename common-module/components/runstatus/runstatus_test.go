package runstatus

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/tiny-systems/module/module"
)

// memState is a minimal in-memory State; Scoped returns itself, so seeding
// keys directly simulates the execution scope the component reads.
type memState struct{ m map[string][]byte }

func (s *memState) Get(_ context.Context, k string) ([]byte, bool, error) {
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *memState) Set(_ context.Context, k string, v []byte) error { s.m[k] = v; return nil }
func (s *memState) Delete(_ context.Context, k string) error       { delete(s.m, k); return nil }
func (s *memState) List(_ context.Context, prefix string) ([]string, error) {
	var out []string
	for k := range s.m {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out, nil
}
func (s *memState) Scoped(_, _ string) module.State { return s }

func seed(t *testing.T, s *memState, stepKey string, rec module.StepRecord) {
	t.Helper()
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	s.m[module.StepLedgerKey(stepKey)] = b
}

// capture returns a handler that records the Response emitted on the
// response port.
func capture(resp *Response) module.Handler {
	return func(_ context.Context, port string, data any) module.Result {
		if port == ResponsePort {
			*resp = data.(Response)
		}
		return module.Ok(nil)
	}
}

func run(t *testing.T, s *memState) Response {
	t.Helper()
	c := &Component{}
	c.OnState(s)
	var got Response
	res := c.Handle(context.Background(), capture(&got), RequestPort, Request{RunID: "r1"})
	if res.Err() != nil {
		t.Fatalf("Handle: %v", res.Err())
	}
	return got
}

func TestRunStatus_UnknownRun(t *testing.T) {
	got := run(t, &memState{m: map[string][]byte{}})
	if got.Found || got.Complete || got.Failed {
		t.Fatalf("unknown run must be not-found/incomplete: %+v", got)
	}
}

func TestRunStatus_InProgress(t *testing.T) {
	s := &memState{m: map[string][]byte{}}
	// Entry completed and emitted SK1; SK1 has no record yet → 1 pending.
	seed(t, s, "r1.root", module.StepRecord{
		Node: "a", Status: module.StepStatusDone,
		Emits:       []module.EmitRecord{{StepKey: "SK1", To: "f.m.b:in"}},
		CompletedAt: time.Now(),
	})
	got := run(t, s)
	if !got.Found || got.Complete || got.Failed || got.PendingSteps != 1 || len(got.Steps) != 1 {
		t.Fatalf("in-progress run misreported: %+v", got)
	}
}

func TestRunStatus_Complete(t *testing.T) {
	s := &memState{m: map[string][]byte{}}
	seed(t, s, "r1.root", module.StepRecord{
		Node: "a", Status: module.StepStatusDone,
		Emits: []module.EmitRecord{{StepKey: "SK1"}}, CompletedAt: time.Now(),
	})
	seed(t, s, "SK1", module.StepRecord{
		Node: "b", Status: module.StepStatusDone, CompletedAt: time.Now(),
	})
	got := run(t, s)
	if !got.Found || !got.Complete || got.Failed || got.PendingSteps != 0 || len(got.Steps) != 2 {
		t.Fatalf("complete run misreported: %+v", got)
	}
}

func TestRunStatus_Failed(t *testing.T) {
	s := &memState{m: map[string][]byte{}}
	seed(t, s, "r1.root", module.StepRecord{
		Node: "a", Status: module.StepStatusDone,
		Emits: []module.EmitRecord{{StepKey: "SK1"}}, CompletedAt: time.Now(),
	})
	seed(t, s, "SK1", module.StepRecord{
		Node: "b", Status: module.StepStatusFailed, Error: "boom", CompletedAt: time.Now(),
	})
	got := run(t, s)
	if !got.Found || got.Complete || !got.Failed {
		t.Fatalf("failed run misreported: %+v", got)
	}
	var failedStep *Step
	for i := range got.Steps {
		if got.Steps[i].Status == module.StepStatusFailed {
			failedStep = &got.Steps[i]
		}
	}
	if failedStep == nil || failedStep.Error != "boom" {
		t.Fatalf("failed step must surface its error: %+v", got.Steps)
	}
}
