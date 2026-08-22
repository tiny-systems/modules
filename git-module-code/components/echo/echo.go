package echo

import (
	"context"
	"fmt"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "echo"
	InPort        string = "in"
	OutPort       string = "out"
)

type Context any

type InMessage struct {
	Context Context `json:"context" configurable:"true" required:"true" title:"Context" description:"Arbitrary message to be echoed"`
}

// OutMessage keeps the passthrough Context under a `context` key so a downstream
// edge reads $.context.<field> — the mid-chain convention. Was emitting raw
// Context at root.
type OutMessage struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Passthrough — echoed unchanged"`
}

type Component struct {
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Echo",
		Info:        "Sends the same message as it receives",
		Tags:        []string{"Echo", "Demo"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {
	if in, ok := msg.(InMessage); ok {
		return handler(ctx, OutPort, OutMessage{Context: in.Context})
	}
	return module.Fail(fmt.Errorf("invalid message"))
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          InPort,
			Label:         "In",
			Configuration: InMessage{},
			Position:      module.Left,
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
	registry.Register(&Component{})
}
