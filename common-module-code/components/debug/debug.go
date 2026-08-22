package debug

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/redact"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "debug"
	InPort        string = "in"
)

type Context any

type Settings struct {
	Context Context `json:"context" configurable:"true" required:"true" title:"Context" description:"Component message"`
}

// InMessage is the whole message the sink receives — a generic object so debug
// displays EVERYTHING mapped into it (a node's full output), not just a declared
// `context` field. Seeing the data is the entire point of a debug sink.
type InMessage map[string]interface{}

type Control struct {
	Context Context `json:"context" readonly:"true" required:"true" title:"Context"`
}

type Component struct {
	module.Base

	settings Settings
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Debug",
		Info:        "Message sink for inspection. Receives messages on In (no output ports). Displays last received message in Control port. Use as flow endpoint to inspect data or terminate unused branches.",
		Tags:        []string{"SDK"},
	}
}

// OnSettings receives Settings from the SettingsPort.
func (t *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	t.settings = in
	return nil
}

// Handle dispatches the InPort. Updates the displayed Context and triggers
// a reconcile so the Control port re-renders.
func (t *Component) Handle(ctx context.Context, _ module.Handler, port string, msg interface{}) module.Result {
	if port != InPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(InMessage)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message in"))
	}
	// Mask credentials before the message becomes node state. What lands
	// here is displayed on the dashboard, stored in the TinyNode CR, and
	// carried into any export of the project — an agent flow's context
	// holds an API key on every hop, and one reached a public solution
	// export this way. Debug shows you the data, not your secrets.
	redacted, _ := redact.Secrets(map[string]interface{}(in)).(map[string]interface{})
	t.settings.Context = redacted
	t.Emit(ctx, v1alpha1.ReconcilePort, nil)
	return module.Result{}
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
			Name:   v1alpha1.ControlPort,
			Label:  "Control",
			Source: true,
			Configuration: Control{
				Context: t.settings.Context,
			},
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
	}
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
