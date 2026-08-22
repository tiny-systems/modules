package runstart

import (
	"context"
	"fmt"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "run_start"
	InPort        = "in"
	OutPort       = "out"
	StartedPort   = "started"
)

type Context any

type Request struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Payload to send into the run"`
}

type Started struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	RunID   string  `json:"runID" title:"Run ID" description:"The run's id — check it with Run Status"`
}

// Component is the front door into durable execution. It starts a durable
// run, fires the chain on Out WITHOUT waiting for it (the emit returns as
// soon as the first hop is durably stored), and then emits {runID} on
// Started — which, wired back to an http_server response, gives the HTTP
// caller an immediate handle to poll while the run continues in the
// background, surviving pod restarts and migrating across replicas.
//
// Place it on a CLASSIC (non-durable) node between the trigger and the
// durable chain. Downstream nodes need no labels: every hop emitted under
// the run identity rides the durable path.
// OutMessage keeps the passthrough Context under a `context` key so a downstream
// edge reads $.context.<field> — the mid-chain convention (same as the router,
// pod_logs_get, llm_tools). Was emitting the raw Context value at root.
type OutMessage struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Passthrough — the message, unchanged"`
}

type Component struct{}

func (t *Component) Instance() module.Component {
	return &Component{}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Start Run & Reply",
		Info:        "Starts a run and immediately returns its id, so a caller — like an HTTP request — gets an instant reply while the work keeps going. Send your payload in on In; wire Started back to your reply (e.g. an HTTP Response) to hand back the run id; everything after Out runs as a run you can watch and retry.",
		Tags:        []string{"run", "reply"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {
	if port != InPort {
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}

	// Fire the chain under a fresh run identity: this emit is durable
	// (fire-and-forget) and returns once the hop is stored.
	runCtx, runID := module.BeginRun(ctx)
	if res := handler(runCtx, OutPort, OutMessage{Context: in.Context}); res.Err() != nil {
		return res
	}

	// Synchronous reply on the ORIGINAL context — classic blocking path back
	// to the caller (e.g. http_server's response).
	return handler(ctx, StartedPort, Started{Context: in.Context, RunID: runID})
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          InPort,
			Label:         "In",
			Configuration: Request{},
			Position:      module.Left,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Configuration: new(OutMessage),
			Position:      module.Right,
		},
		{
			Name:          StartedPort,
			Label:         "Started",
			Source:        true,
			Configuration: Started{},
			Position:      module.Bottom,
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
