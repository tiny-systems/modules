package runstatus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "run_status"
	RequestPort   = "request"
	ResponsePort  = "response"
)

type Context any

type Request struct {
	Context Context `json:"context" configurable:"true" title:"Context"`
	RunID   string  `json:"runID" required:"true" title:"Run ID" description:"The run id (returned by Start Run & Reply)"`
}

type Step struct {
	StepKey     string    `json:"stepKey" title:"Step Key"`
	Node        string    `json:"node" title:"Node"`
	Status      string    `json:"status" title:"Status"`
	Error       string    `json:"error,omitempty" title:"Error"`
	CompletedAt time.Time `json:"completedAt" title:"Completed At"`
}

type Response struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	RunID   string  `json:"runID" title:"Run ID"`
	// Found: the run has at least one ledger record. False = unknown run id
	// or the entry hasn't completed its first step yet.
	Found bool `json:"found" title:"Found"`
	// Complete: every emitted hop has a completion record and none failed.
	Complete bool `json:"complete" title:"Complete"`
	// Failed: at least one step recorded a terminal failure.
	Failed bool `json:"failed" title:"Failed"`
	// PendingSteps: hops durably emitted but not yet completed (the frontier).
	PendingSteps int    `json:"pendingSteps" title:"Pending Steps"`
	Steps        []Step `json:"steps,omitempty" title:"Steps"`
}

// Component reads a durable run's progress straight from the step ledger in
// the execution-scoped State — the polling half of the run_start/run_status
// front-door pair.
type Component struct {
	module.Base
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Run Status",
		Info:        "Checks a run's progress by id: whether the run is known (found), complete, or failed, how many steps are still pending, and the per-step details. Pair with Start Run & Reply behind an HTTP request for instant-reply-then-check.",
		Tags:        []string{"run", "status"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}
	if in.RunID == "" {
		return module.Fail(fmt.Errorf("runID is required"))
	}
	if t.State() == nil {
		return module.Fail(fmt.Errorf("state backend unavailable"))
	}

	exec := t.State().Scoped(module.ScopeExecution, in.RunID)
	keys, err := exec.List(ctx, module.StepLedgerPrefix)
	if err != nil {
		return module.Fail(fmt.Errorf("ledger list: %w", err))
	}

	resp := Response{Context: in.Context, RunID: in.RunID}
	records := map[string]module.StepRecord{}
	for _, k := range keys {
		raw, ok, err := exec.Get(ctx, k)
		if err != nil || !ok {
			continue // deleted between List and Get
		}
		var rec module.StepRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			continue
		}
		stepKey := strings.TrimPrefix(k, module.StepLedgerPrefix)
		records[stepKey] = rec
		resp.Steps = append(resp.Steps, Step{
			StepKey:     stepKey,
			Node:        rec.Node,
			Status:      rec.Status,
			Error:       rec.Error,
			CompletedAt: rec.CompletedAt,
		})
		if rec.Status == module.StepStatusFailed {
			resp.Failed = true
		}
	}

	// Frontier: hops that were durably emitted but have no completion record.
	for _, rec := range records {
		for _, e := range rec.Emits {
			if _, seen := records[e.StepKey]; !seen {
				resp.PendingSteps++
			}
		}
	}

	resp.Found = len(records) > 0
	resp.Complete = resp.Found && resp.PendingSteps == 0 && !resp.Failed

	return handler(ctx, ResponsePort, resp)
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          RequestPort,
			Label:         "Request",
			Configuration: Request{},
			Position:      module.Left,
		},
		{
			Name:          ResponsePort,
			Label:         "Response",
			Source:        true,
			Configuration: Response{},
			Position:      module.Right,
		},
	}
}

var _ module.Component = (*Component)(nil)
var _ module.Stateful = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
