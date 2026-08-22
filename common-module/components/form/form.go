package form

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
	"github.com/rs/zerolog/log"
)

// form is the one-widget request→response surface: a configurable form with a
// Submit button and a result line INSIDE the same card. Submit emits the
// filled values on Out; the flow answers on Result and the widget shows it in
// place. The settings-form pattern (masked API key → validate → store →
// "saved ••••last4 ✓") is the canonical use.
//
// It is the prompt component's form half, reborn: prompt merged into chat for
// conversations, which left "a form that reports its outcome" without a
// component — signal (stateless trigger) plus display (separate panel) was
// the workaround.
const (
	ComponentName = "form"
	OutPort       = "out"
	ResultPort    = "result"

	// stateKeyResult persists the last result line through module.State
	// (node status metadata) so the widget still shows the outcome after a
	// pod restart — a display panel that forgets "key saved ✓" on every
	// deploy reads as broken.
	stateKeyResult = "result"

	// stateKeyPrefill persists values the flow wrote back into the fields.
	// A masked saved secret ("••••NgAA") sitting IN the password field says
	// "a key is set" the way a sentence never will.
	stateKeyPrefill = "prefill"
)

type Context any

type Settings struct {
	Context Context `json:"context" required:"true" configurable:"true" title:"Form" description:"The form's fields. Author a custom schema (masked passwords via writeOnly + format:\"password\") for the fields a person fills."`
}

// Control is the widget: the form, Submit, and the result line the flow wrote
// back — rendered as read-only prose under the button.
type Control struct {
	Context Context `json:"context" required:"true" configurable:"true" title:"Form"`
	Submit  bool    `json:"submit" format:"button" title:"Submit" required:"true"`
	Result  string  `json:"result,omitempty" readonly:"true" format:"markdown" title:"" description:""`
}

// ResultMessage is what the flow writes back after handling a submission.
type ResultMessage struct {
	Text    string  `json:"text" required:"true" title:"Text" description:"Outcome shown inside the form widget, under the button. Markdown is rendered."`
	Prefill Context `json:"prefill,omitempty" title:"Prefill" description:"Optional values written back into the form's fields (e.g. the masked saved secret into a password field). Persisted; shown until the person edits or the flow overwrites."`
}

type Component struct {
	module.Base
	settings Settings
}

func (t *Component) Instance() module.Component {
	return &Component{settings: Settings{}}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Form",
		Info: "A form widget that reports its outcome in place: a person fills the configured fields and presses Submit, " +
			"the values emit on Out, and whatever the flow writes to Result appears under the button — one card, no separate status panel. " +
			"The settings-form pattern is the canonical use: a masked API-key field (author the context schema with writeOnly + format:\"password\") " +
			"→ validate against the real API → store under a handle → Result: \"saved ••••last4 ✓\". " +
			"The result line persists across restarts. For free-form conversation use chat; for a fire-and-forget trigger use signal.",
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

// OnControl handles Submit: emit the filled values once on Out. Same
// fire-and-forget contract as signal — the flow's answer comes back through
// the Result port, not a blocking reply.
func (t *Component) OnControl(ctx context.Context, msg any) error {
	if !utils.IsLeader(ctx) {
		return nil
	}
	ctrl, ok := msg.(Control)
	if !ok {
		return fmt.Errorf("invalid control msg: expected Control, got %T", msg)
	}
	if !ctrl.Submit {
		return nil
	}

	sendCtx := ctrl.Context
	if sendCtx == nil {
		sendCtx = t.settings.Context
	}

	// A result describes the submission that produced it. Leaving the last
	// one on screen while a new submission runs reads as if THIS one already
	// succeeded — a form that says "History deleted" over a submission that
	// deleted nothing. Clear it and republish; the flow's answer fills it in
	// again.
	if st := t.State(); st != nil {
		if err := st.Delete(ctx, stateKeyResult); err != nil {
			log.Warn().Err(err).Msg("form component: could not clear the previous result")
		}
		t.Emit(ctx, v1alpha1.ControlPort, t.control(ctx))
	}

	log.Info().Msg("form component: submit — emitting on Out")
	go t.Emit(context.Background(), OutPort, sendCtx)
	return nil
}

// Handle receives the flow's Result and shows it in the widget. NOT
// leader-gated: results route to any replica and converge through state,
// same rule as chat's say/ask.
func (t *Component) Handle(ctx context.Context, _ module.Handler, port string, msg any) module.Result {
	if port != ResultPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(ResultMessage)
	if !ok {
		return module.Fail(fmt.Errorf("invalid result message"))
	}
	if st := t.State(); st != nil {
		if err := st.Set(ctx, stateKeyResult, []byte(in.Text)); err != nil {
			return module.Fail(fmt.Errorf("persist result: %w", err))
		}
		if in.Prefill != nil {
			raw, err := json.Marshal(in.Prefill)
			if err != nil {
				return module.Fail(fmt.Errorf("marshal prefill: %w", err))
			}
			if err := st.Set(ctx, stateKeyPrefill, raw); err != nil {
				return module.Fail(fmt.Errorf("persist prefill: %w", err))
			}
		}
	}
	t.Emit(ctx, v1alpha1.ControlPort, t.control(ctx))
	return module.Result{}
}

func (t *Component) loadResult(ctx context.Context) string {
	st := t.State()
	if st == nil {
		return ""
	}
	raw, found, err := st.Get(ctx, stateKeyResult)
	if err != nil || !found {
		return ""
	}
	return string(raw)
}

func (t *Component) loadPrefill(ctx context.Context) Context {
	st := t.State()
	if st == nil {
		return nil
	}
	raw, found, err := st.Get(ctx, stateKeyPrefill)
	if err != nil || !found {
		return nil
	}
	var prefill Context
	if err := json.Unmarshal(raw, &prefill); err != nil {
		return nil
	}
	return prefill
}

func (t *Component) control(ctx context.Context) Control {
	// What the flow wrote back wins over the authored blank form: the person
	// should see the state of the world, not the empty template.
	formCtx := t.settings.Context
	if prefill := t.loadPrefill(ctx); prefill != nil {
		formCtx = prefill
	}
	return Control{
		Context: formCtx,
		Result:  t.loadResult(ctx),
	}
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
			Name:          ResultPort,
			Label:         "Result",
			Position:      module.Left,
			Configuration: ResultMessage{},
		},
		{
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Source:        true,
			Configuration: t.control(context.Background()),
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
