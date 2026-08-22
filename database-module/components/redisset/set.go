package redisset

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/tiny-systems/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "redis_set"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port"`
}

type Request struct {
	Context    Context `json:"context,omitempty" configurable:"true" title:"Context"`
	URL        string  `json:"url" required:"true" minLength:"1" title:"Redis URL" format:"uri"`
	Key        string  `json:"key" required:"true" minLength:"1" title:"Key"`
	Value      string  `json:"value" required:"true" title:"Value" format:"textarea"`
	TTLSeconds int     `json:"ttlSeconds" title:"TTL Seconds" description:"0 means no expiry"`
	NX         bool    `json:"nx" title:"Only If Not Exists" description:"If true, only sets when key does not already exist"`
}

type Response struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Created bool    `json:"created" title:"Created" description:"true if the value was set; false if NX prevented the set"`
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
		Description: "Redis Set",
		Info:        "Sets a key in Redis. Optional TTL via ttlSeconds (0 = no expiry). Optional NX (set only if key does not already exist).",
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
	return c.set(ctx, handler, in)
}

func (c *Component) set(ctx context.Context, handler module.Handler, in Request) module.Result {
	client, err := pool.Redis(in.URL)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	ttl := time.Duration(in.TTLSeconds) * time.Second
	created := true

	if in.NX {
		ok, err := client.SetNX(ctx, in.Key, in.Value, ttl).Result()
		if err != nil {
			// Deliberately not retryable past the dial: SET NX is a first-seen
			// test, and a failure gives no answer on whether the key landed. A
			// retry that finds its own write reports created=false, telling
			// the caller something else owns the key when nothing else does.
			// A dial failure is the exception — the command never reached the
			// server, so no key can have landed.
			if pool.IsNeverSentRedis(err) {
				err = module.Retryable(err)
			}
			return c.fail(ctx, handler, in.Context, err)
		}
		created = ok
	} else {
		_, err := client.Set(ctx, in.Key, in.Value, ttl).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			// A plain SET writes a fixed key and value, so re-running the
			// handler lands the same state — safe to hand to a retry.
			if pool.IsTransientRedis(err) {
				err = module.Retryable(err)
			}
			return c.fail(ctx, handler, in.Context, err)
		}
	}

	return handler(ctx, ResponsePort, Response{Context: in.Context, Created: created})
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
