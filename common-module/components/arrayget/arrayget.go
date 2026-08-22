package arrayget

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "array_get"
	RequestPort   = "request"
	ResultPort    = "result"
	ErrorPort     = "error"
)

// Context type alias for schema generation
type Context any

// Item type alias for schema generation
type Item any

// Settings configures the component
type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

// Request is the input to get an array element
type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Array   []Item  `json:"array" required:"true" configurable:"true" title:"Array" description:"Array to get element from"`
	Index   int     `json:"index" required:"true" title:"Index" description:"1-based item number"`
}

// Result is the output with the resolved element
type Result struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Item    Item    `json:"item" configurable:"true" title:"Item"`
	Index   int     `json:"index" title:"Index" description:"1-based index of the returned item"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the array element accessor
type Component struct {
	settings     Settings
	settingsLock sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Array Get",
		Info:        "Get an element from an array by 1-based index. Returns the item or an error if index is out of range. Useful for numbered reference patterns where users select items by number from a previously displayed list.",
		Tags:        []string{"SDK", "ARRAY"},
	}
}

// OnSettings receives Settings from the SettingsPort.
func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settingsLock.Lock()
	c.settings = in
	c.settingsLock.Unlock()
	return nil
}

// Handle dispatches business-port messages. System ports are routed via
// the capability interfaces above.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	return c.handleRequest(ctx, handler, in)
}

func (c *Component) handleRequest(ctx context.Context, handler module.Handler, req Request) module.Result {
	if len(req.Array) == 0 {
		return c.handleError(ctx, handler, req, "array is empty — run a list command first")
	}
	if req.Index < 1 {
		return c.handleError(ctx, handler, req, fmt.Sprintf("index must be >= 1, got %d", req.Index))
	}
	if req.Index > len(req.Array) {
		return c.handleError(ctx, handler, req, fmt.Sprintf("item #%d not found — list has %d item(s)", req.Index, len(req.Array)))
	}

	return handler(ctx, ResultPort, Result{
		Context: req.Context,
		Item:    req.Array[req.Index-1],
		Index:   req.Index,
	})
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, req Request, errMsg string) module.Result {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		return handler(ctx, ErrorPort, Error{
			Context: req.Context,
			Error:   errMsg,
		})
	}
	return module.Fail(errors.New(errMsg))
}

func (c *Component) Ports() []module.Port {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	ports := []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: Settings{},
		},
		{
			Name:  RequestPort,
			Label: "Request",
			Configuration: Request{
				Array: []Item{"first", "second", "third"},
				Index: 1,
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Item:  "first",
				Index: 1,
			},
			Position: module.Right,
		},
	}

	if enableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Configuration: Error{},
			Position:      module.Bottom,
		})
	}

	return ports
}

// AgentTool opts array_get into the platform's MCP tool registry.
// Stateless one-shot: agent supplies array + index, receives the
// resolved element. Uses request / result instead of the in / out
// defaults handleRPC assumes.
func (c *Component) AgentTool() module.AgentToolInfo {
	return module.AgentToolInfo{
		Description: "Get an array element by 1-based index. Input: {array, index}; returns {item, index} or an error if the index is out of range.",
		InputPort:   RequestPort,
		OutputPort:  ResultPort,
	}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.AgentTool       = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
