package display

import (
	"context"
	"fmt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "display"
	InPort        string = "in"

	// stateKeyText persists what was last shown. Kept in module.State
	// (node status metadata) rather than a field, because a field lives
	// only as long as the pod: a restart, a redeploy or a reschedule
	// blanked every panel on the dashboard. A flow-fed panel recovered on
	// its next run, but one written once — a readme, a summary from a
	// slow schedule — was gone for good.
	stateKeyText = "text"
)

// Settings carries the panel's authored text — what it shows before any
// message arrives.
//
// This is what makes a written panel part of the flow rather than a fact
// about one running copy of it. A readme card receives no messages, so
// without it the text lived only in node state: it never appeared in an
// export, and anyone installing the solution got a blank card with nothing
// to ever fill it.
type Settings struct {
	Text string `json:"text" format:"markdown" title:"Text" description:"Shown until a message arrives. Use it for a readme or a placeholder — it ships with the flow, unlike text delivered at runtime."`
}

// InMessage is what to show. A single named field, deliberately: a display
// panel exists to answer one question, and a flow that wants to surface three
// things should say which three rather than pushing its whole state at a
// person.
type InMessage struct {
	Text string `json:"text" required:"true" title:"Text" description:"What to show. Markdown is rendered."`
}

// Control is the dashboard surface: the text, rendered as prose and not
// offered for editing.
//
// format:"markdown" is what makes the editor render it instead of putting it
// in a single-line input, where a paragraph of model output is unreadable and
// pretends to be typeable. readonly says the obvious thing out loud.
type Control struct {
	Text string `json:"text" readonly:"true" format:"markdown" title:"" description:""`
}

type Component struct {
	module.Base
	settings Settings
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Display",
		Info: "Shows one piece of text on the dashboard, rendered as markdown. " +
			"Use it as a flow's answer panel: wire the value a person actually reads into Text " +
			"(e.g. text: \"{{$.outputData.messages[0].content}}\") rather than passing the whole message, " +
			"which renders as a wall of form fields. Has no output ports — it is a sink. " +
			"Enable it as a dashboard widget to give a flow a readable result. " +
			"For a panel of written text — a readme describing what the agent does — wire nothing and set " +
			"settings.text instead: that ships with the flow, whereas text delivered at runtime does not " +
			"survive an export and leaves a blank card for whoever installs it.",
		Tags: []string{"SDK", "dashboard"},
	}
}

func (t *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	t.settings = in
	return nil
}

func (t *Component) Handle(ctx context.Context, _ module.Handler, port string, msg interface{}) module.Result {
	if port != InPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(InMessage)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message in"))
	}
	// Mask credential-shaped values before they become node state: what is
	// stored here is rendered on the dashboard, written to the node resource,
	// and carried into any export of the project.
	// Deliberately stored as given. Redaction works by field NAME, and a
	// display's field is called "text" — so a flow that pipes a secret in
	// here is the thing to fix, not this component. Publishing redacts
	// credential-shaped fields on the way out.
	if st := t.State(); st != nil {
		if err := st.Set(ctx, stateKeyText, []byte(in.Text)); err != nil {
			return module.Fail(fmt.Errorf("persist text: %w", err))
		}
	}
	t.Emit(ctx, v1alpha1.ReconcilePort, nil)
	return module.Result{}
}

// text returns what to show: the last message if one has ever arrived,
// otherwise the authored default.
//
// Runtime wins over authored, and permanently — a panel that has shown a
// real answer must not revert to its placeholder when the pod restarts,
// which is the whole reason the last message is persisted.
func (t *Component) text(ctx context.Context) string {
	if st := t.State(); st != nil {
		if raw, found, err := st.Get(ctx, stateKeyText); err == nil && found {
			return string(raw)
		}
	}
	return t.settings.Text
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
				Text: t.text(context.Background()),
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
	registry.Register(&Component{})
}
