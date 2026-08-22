package redisget

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/tiny-systems/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "redis_get"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port"`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	URL     string  `json:"url" required:"true" minLength:"1" title:"Redis URL" format:"uri"`
	Key     string  `json:"key" required:"true" minLength:"1" title:"Key"`
}

type Response struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Found   bool    `json:"found" title:"Found"`
	Value   string  `json:"value" title:"Value" description:"Empty when key does not exist"`
}

type Component struct {
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Redis Get",
		Info:        "Reads a key from Redis. Emits found=false with empty value when the key does not exist; the error port is reserved for actual Redis failures.",
		Tags:        []string{"Redis", "DB"},
	}
}

// OnSettings stores the component settings.
func (c *Component) OnSettings(_ context.Context, msg any) error {

	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settings = in
	return nil
}

// Handle dispatches the RequestPort. System ports go through capabilities.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}

	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	return c.get(ctx, handler, in)
}

func (c *Component) get(ctx context.Context, handler module.Handler, in Request) module.Result {
	client, err := pool.Redis(in.URL)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	val, err := client.Get(ctx, in.Key).Result()
	if errors.Is(err, redis.Nil) {
		return handler(ctx, ResponsePort, Response{Context: in.Context, Found: false})
	}
	if err != nil {
		// A GET changes nothing, so re-running the whole handler is free —
		// mark whatever the server or the socket could recover from.
		if pool.IsTransientRedis(err) {
			err = module.Retryable(err)
		}
		return c.fail(ctx, handler, in.Context, err)
	}
	return handler(ctx, ResponsePort, Response{Context: in.Context, Found: true, Value: val})
}

func (c *Component) fail(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	if !c.settings.EnableErrorPort {
		// Bubble unchanged so retryability marked at the call site survives.
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, module.NewError(reqCtx, err))
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: RequestPort, Label: "Request", Configuration: Request{}, Position: module.Left},
		{Name: ResponsePort, Label: "Response", Source: true, Configuration: Response{}, Position: module.Right},
	}
	if !c.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name: ErrorPort, Label: "Error", Source: true, Configuration: module.ErrorMessage{}, Position: module.Bottom,
	})
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
