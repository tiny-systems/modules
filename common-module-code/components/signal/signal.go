package signal

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "signal"
	OutPort       string = "out"
)

type Context any

type Settings struct {
	Context Context `json:"context" required:"true" configurable:"true" title:"Context" description:"Arbitrary message to send"`
}

type Component struct {
	module.Base
	settings Settings
}

// Control is the _control port schema: a single Send button. Signal is a
// fire-and-forget trigger — there is no "running" state and no Reset, because
// the distributed runtime can't stop downstream work by cancelling a context
// (see [[distributed-control-plane]]). To stop something a Signal started, wire
// a Stop-capable component downstream (e.g. http_server's Stop port).
type Control struct {
	// configurable so a custom Context schema authored on the node (e.g. a
	// masked API-key field on a settings form) overlays the widget — without
	// it UpdateWithDefinitions skips the def and the trigger form renders
	// value-derived plain-text fields, losing title/format/writeOnly.
	Context Context `json:"context" required:"true" configurable:"true" title:"Context"`
	Send    bool    `json:"send" format:"button" title:"Send" required:"true"`
}

func (t *Component) Instance() module.Component {
	return &Component{settings: Settings{}}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Signal",
		Info:        "Flow trigger. Click Send to emit the configured context on Out and kick off the flow. Fire-and-forget: it emits once and returns — there is no running state and no Reset (the distributed runtime can't cancel downstream work by context). To stop something the flow started, wire a Stop-capable component downstream (e.g. http_server's Stop port). Use as an entry point.",
		Tags:        []string{"SDK"},
	}
}

// OnSettings receives Settings from the SettingsPort and stores them. Secret
// values are supplied directly by the user through the trigger widget, so there
// is no server-side placeholder resolution.
func (t *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	t.settings = in
	return nil
}

// OnControl handles the Send button. It emits the payload once on Out and
// returns — no state, no Reset. The Out emit is a request/reply to the
// downstream pod; nothing here waits on or can stop it, so it runs detached on
// a background context.
func (t *Component) OnControl(ctx context.Context, msg any) error {
	if !utils.IsLeader(ctx) {
		return nil
	}
	ctrl, ok := msg.(Control)
	if !ok {
		return fmt.Errorf("invalid control msg: expected Control, got %T", msg)
	}
	if !ctrl.Send {
		return nil
	}

	// The Send dialog carries the user's values directly. When Send is fired
	// without a context (a bare {send:true} from the API, not the widget which
	// pre-fills the form), fall back to the configured Settings.Context so the
	// node emits its own configuration instead of a null.
	sendCtx := ctrl.Context
	if sendCtx == nil {
		sendCtx = t.settings.Context
	}

	log.Info().Msg("signal component: send — emitting on Out (fire-and-forget)")
	go t.Emit(context.Background(), OutPort, sendCtx)
	return nil
}

// Handle is unreachable: OutPort is source-only, no business input.
func (t *Component) Handle(_ context.Context, _ module.Handler, port string, _ any) module.Result {
	return module.Fail(fmt.Errorf("signal has no business-port input: got %q", port))
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Position:      module.Right,
			Configuration: new(Context),
		},
		{
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Source:        true,
			Configuration: Control{Context: t.settings.Context},
		},
	}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.ControlHandler  = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
