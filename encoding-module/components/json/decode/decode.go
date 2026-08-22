package decode

import (
	"context"
	"fmt"

	"github.com/goccy/go-json"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "json_decode"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

type Decoded any

type Settings struct {
	EnableErrorPort bool    `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"If error happen, error port will emit an error message"`
	Decoded         Decoded `json:"decoded" configurable:"true" title:"Decoded shape" description:"Schema and example of the decoded JSON. Downstream edges from this node will be validated against this shape."`
}

type Error struct {
	Context Context `json:"context"`
	Error   string  `json:"error"`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary message to be send alongside with decoded message"`
	Encoded string  `json:"encoded" required:"true" title:"Input string" format:"textarea" description:"JSON encoded string"`
}

type Output struct {
	Context Context `json:"context"`
	Decoded Decoded `json:"decoded" configurable:"true"`
}

type Component struct {
	settings Settings
}

func (h *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "JSON Decoder",
		Info: "Parses a JSON string into data the rest of the flow can read. " +
			"SET THE `decoded` SETTING to an example of the JSON you expect — a string has no shape, so without one every downstream edge is unverifiable: an expression like {{$.decoded.user.id}} is accepted when the flow is built and resolves to null at runtime. With an example, the same mistake is caught immediately. Only the shape matters, not the values; one representative object is enough, and for a list one representative element. " +
			"When the payload carries a list of items, wire array_split after this so each item arrives as its own message instead of every downstream node looping. " +
			"Some senders report that they truncated a batch, in a field alongside it. Include that field in the example and check it, or a batch that silently dropped items reads as the complete set.",
		Tags: []string{"json"},
	}
}

// OnSettings stores the component settings.
func (h *Component) OnSettings(_ context.Context, msg any) error {


	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	h.settings = in
	return nil
}

// Handle dispatches the RequestPort. System ports go through capabilities.
func (h *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}


	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid input"))
	}

	var res Decoded

	err := json.Unmarshal([]byte(in.Encoded), &res)
	if err != nil {
		if !h.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return handler(ctx, ErrorPort, Error{
			Context: in.Context,
			Error:   err.Error(),
		})
	}

	return handler(ctx, ResponsePort, Output{
		Context: in.Context,
		Decoded: res,
	})
}

func (h *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          RequestPort,
			Label:         "In",
			Position:      module.Left,
			Configuration: Request{},
		},
		{
			Name:     ResponsePort,
			Position: module.Right,
			Label:    "Out",
			Source:   true,
			Configuration: Output{
				Decoded: h.settings.Decoded,
			},
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: h.settings,
		},
	}
	if !h.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Position:      module.Bottom,
		Name:          ErrorPort,
		Label:         "Error",
		Source:        true,
		Configuration: Error{},
	})
}

func (h *Component) Instance() module.Component {
	return &Component{
		settings: Settings{},
	}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
