package delay

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"time"
)

const (
	ComponentName        = "delay"
	OutPort       string = "out"
	InPort        string = "in"
)

type Context any

type Request struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Arbitrary message to be delayed"`
	Delay   int     `json:"delay" required:"true" title:"Delay (ms)"`
}

// OutMessage keeps the passthrough Context under a `context` key so a downstream
// edge reads $.context.<field> — the mid-chain convention (same as the router,
// pod_logs_get, llm_tools). Was emitting the raw Context value at root.
type OutMessage struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Passthrough — the message, unchanged"`
}

type Component struct {
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Delay",
		Info:        "Timed pause. Receives context + delay (ms) on In, sleeps for specified duration (blocking upstream), then emits context on Out. Use for rate limiting or adding pauses between operations.",
		Tags:        []string{"SDK"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {

	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}
	if in.Delay <= 0 {
		return module.Fail(fmt.Errorf("invalid delay"))
	}

	// Sleep, but honor ctx — a cancelled/expired request context (transport
	// deadline, pod shutdown) should abort the wait rather than sleep on.
	timer := time.NewTimer(time.Millisecond * time.Duration(in.Delay))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		return module.Fail(ctx.Err())
	}
	return handler(ctx, OutPort, OutMessage{Context: in.Context})
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:  InPort,
			Label: "In",
			Configuration: Request{
				Delay: 1000,
			},
			Position: module.Left,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Configuration: new(OutMessage),
			Position:      module.Right,
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
