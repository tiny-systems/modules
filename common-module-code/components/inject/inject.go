package inject

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "inject"
	ConfigPort    = "config"
	MessagePort   = "message"
	OutputPort    = "output"
	ErrorPort     = "error"
	// stateKeyConfig persists the injected config through module.State (backed
	// by node metadata). Reading it at message time — rather than trusting an
	// in-memory field — is what lets a message handled after the runtime
	// recreated the component instance (which zeroes in-memory state) still
	// carry the stored config instead of nil.
	stateKeyConfig = "config"
)

type Context any
type Data any

type Settings struct {
	ConfigRequired bool `json:"configRequired" title:"Config Required" description:"When enabled, messages arriving before config is set are sent to the error port instead of output"`
}

// Config is stored in metadata and injected into messages
type Config struct {
	Data Data `json:"data" configurable:"true" required:"true" title:"Data" description:"Configuration data to inject into messages"`
}

// Message passes through with config injected
type Message struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Passthrough context for correlation"`
}

// Output contains original context plus injected config
type Output struct {
	Context Context `json:"context" configurable:"true" title:"Context"`
	Config  Data    `json:"config" title:"Config" description:"Injected configuration from metadata"`
}

// ErrorOutput is sent when config is required but not set
type ErrorOutput struct {
	Context Context `json:"context" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements config injection with metadata persistence
type Component struct {
	module.Base

	settings Settings
	config   any // in-memory fast path; State is the source of truth after a recreate
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Inject",
		Info:        "Injects stored configuration into passing messages. Send config once, then every message passing through gets it attached. Config persists across pod restarts via metadata.",
		Tags:        []string{"Data", "Config", "Enrich"},
	}
}

// OnSettings receives Settings from the SettingsPort.
func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settings = in
	return nil
}

// OnReconcile is a no-op: the config is loaded from State at message time, so
// there is no in-memory field to warm from metadata on reconcile.
func (c *Component) OnReconcile(_ context.Context, _ v1alpha1.TinyNode) error {
	return nil
}

// Handle dispatches business ports (Config and Message). System ports go
// through capability methods.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	switch port {
	case ConfigPort:
		return c.handleConfig(ctx, msg)
	case MessagePort:
		return c.handleMessage(ctx, handler, msg)
	}
	return module.Fail(fmt.Errorf("unknown port: %s", port))
}

func (c *Component) handleConfig(ctx context.Context, msg any) module.Result {
	in, ok := msg.(Config)
	if !ok {
		return module.Fail(fmt.Errorf("invalid config"))
	}
	c.config = in.Data
	if st := c.State(); st != nil {
		if b, err := json.Marshal(in.Data); err == nil {
			if err := st.Set(ctx, stateKeyConfig, b); err != nil {
				return module.Fail(err)
			}
		}
	}
	return module.Result{}
}

func (c *Component) handleMessage(ctx context.Context, handler module.Handler, msg any) module.Result {
	in, ok := msg.(Message)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}
	config := c.loadConfig(ctx)
	if c.settings.ConfigRequired && config == nil {
		return handler(ctx, ErrorPort, ErrorOutput{
			Context: in.Context,
			Error:   "config not set",
		})
	}
	return handler(ctx, OutputPort, Output{
		Context: in.Context,
		Config:  config,
	})
}

// loadConfig returns the in-memory config, falling back to persisted State — so
// a message handled after the runtime recreated the component instance (which
// zeroes c.config) still carries the stored config instead of nil.
func (c *Component) loadConfig(ctx context.Context) any {
	if c.config != nil {
		return c.config
	}
	st := c.State()
	if st == nil {
		return nil
	}
	raw, found, err := st.Get(ctx, stateKeyConfig)
	if err != nil || !found {
		return nil
	}
	var config any
	if json.Unmarshal(raw, &config) != nil {
		return nil
	}
	c.config = config
	return config
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.ReconcilePort},
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{
			Name:          ConfigPort,
			Label:         "Config",
			Configuration: Config{},
			Position:      module.Top,
		},
		{
			Name:          MessagePort,
			Label:         "Message",
			Configuration: Message{},
			Position:      module.Left,
		},
		{
			Name:          OutputPort,
			Label:         "Output",
			Source:        true,
			Configuration: Output{},
			Position:      module.Right,
		},
	}
	if c.settings.ConfigRequired {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Configuration: ErrorOutput{},
			Position:      module.Bottom,
		})
	}
	return ports
}

var (
	_ module.Component        = (*Component)(nil)
	_ module.SettingsHandler  = (*Component)(nil)
	_ module.ReconcileHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
