package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/goccy/go-json"
	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "router"
	InPort        = "input"
	DefaultPort   = "default"
)

// RouteName special type which can carry its value and possible options for enum values
type RouteName struct {
	Value   string
	Options []string
}

// MarshalJSON treat like underlying Value string
func (r *RouteName) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.Value)
}

// UnmarshalJSON treat like underlying Value string
func (r *RouteName) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &r.Value)
}

func (r *RouteName) JSONSchema() (jsonschema.Schema, error) {
	name := jsonschema.Schema{}
	name.AddType(jsonschema.String)
	name.WithTitle("Route")
	name.WithDefault(r.Value)
	name.WithExtraPropertiesItem("shared", true)
	enums := make([]interface{}, len(r.Options))
	for k, v := range r.Options {
		enums[k] = v
	}
	name.WithEnum(enums...)
	return name, nil
}

type Condition struct {
	RouteName RouteName `json:"route" title:"Route" required:"true"`
	Condition bool      `json:"condition" required:"true" title:"Condition"`
}

type Settings struct {
	Routes            []string `json:"routes" required:"true" title:"Routes" minItems:"1" uniqueItems:"true"`
	EnableDefaultPort bool     `json:"enableDefaultPort" required:"true" title:"Enable default port"`
}

type Context any

type InMessage struct {
	Context    Context     `json:"context" configurable:"true" required:"true" title:"Context" description:"Arbitrary message to be routed"`
	Conditions []Condition `json:"conditions" required:"true" title:"Conditions" minItems:"1" uniqueItems:"true"`
}

// OutMessage is what each out_<route> port emits. The passthrough Context is
// kept UNDER a `context` key — same shape every other component uses — so a
// downstream edge reads `$.context.<field>` consistently, instead of `$.field`
// off a bare root. (Previously the port emitted the raw Context value at root,
// which meant router branches were the one place you dropped the `.context`
// segment — a silent mismatch that validated green but resolved null.)
type OutMessage struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Passthrough — the routed message, unchanged"`
}

type Component struct {
	settings Settings
}

var defaultRouterSettings = Settings{
	Routes: []string{"A", "B"},
}

func (t *Component) Instance() module.Component {
	return &Component{
		settings: defaultRouterSettings,
	}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Router",
		Info:        "Conditional message router. Configure routes via settings (e.g., routes=[\"POST\", \"OTHER\"]). Output ports are named out_<lowercase(route)> (e.g., out_post, out_other). Input: context (data to forward) + conditions array (each with route name and boolean). Routes context to FIRST condition where condition=true. If NO condition is true: with enableDefaultPort=true, routes to 'default' port (leave unwired to silently drop); with enableDefaultPort=false, returns an error to the caller. Prefer enableDefaultPort=true when unmatched messages should be ignored. Each out_<route> emits the routed message under `context` — a downstream edge reads `$.context.<field>`.",
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

// Handle routes business-port input to the matching output. The router has
// no other system port logic — settings/control/reconcile dispatch live in
// the capability methods.
func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {
	in, ok := msg.(InMessage)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message on port %q", port))
	}

	for _, condition := range in.Conditions {
		if !condition.Condition {
			continue
		}
		if t.hasRoute(condition.RouteName.Value) {
			return handler(ctx, getPortNameFromRoute(condition.RouteName.Value), OutMessage{Context: in.Context})
		}
		break
	}
	if t.settings.EnableDefaultPort {
		return handler(ctx, DefaultPort, OutMessage{Context: in.Context})
	}
	return module.Fail(fmt.Errorf("no matching route: %v", in.Conditions))
}

// Ports drop settings, make it port payload
func (t *Component) Ports() []module.Port {

	val := "A"
	if len(t.settings.Routes) > 0 {
		val = t.settings.Routes[0]
	}

	inMessage := InMessage{
		Conditions: []Condition{{
			RouteName: RouteName{Value: val, Options: t.settings.Routes},
			Condition: true,
		}},
	}

	ports := []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
		{
			Position:      module.Left,
			Name:          InPort,
			Label:         "IN",
			Configuration: inMessage,
		},
	}
	for _, r := range t.settings.Routes {
		ports = append(ports, module.Port{
			Position:      module.Right,
			Name:          getPortNameFromRoute(r),
			Label:         strings.ToTitle(r),
			Source:        true,
			Configuration: new(OutMessage),
		})
	}
	if t.settings.EnableDefaultPort {
		ports = append(ports, module.Port{
			Position:      module.Bottom,
			Name:          DefaultPort,
			Label:         "Default",
			Source:        true,
			Configuration: new(OutMessage),
		})
	}
	return ports
}

func (t *Component) hasRoute(name string) bool {
	for _, r := range t.settings.Routes {
		if strings.EqualFold(r, name) {
			return true
		}
	}
	return false
}

func getPortNameFromRoute(route string) string {
	return fmt.Sprintf("out_%s", strings.ToLower(route))
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ jsonschema.Exposer     = (*RouteName)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
