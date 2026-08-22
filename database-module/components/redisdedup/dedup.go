package redisdedup

import (
	"context"
	"fmt"
	"time"

	"github.com/tiny-systems/modules/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "redis_dedup"
	RequestPort   = "request"
	NewPort       = "out_new"
	SeenPort      = "out_seen"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"Emit Redis errors on a dedicated error port instead of failing the message"`
}

type Request struct {
	Context    Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Message to be forwarded with the result"`
	URL        string  `json:"url" required:"true" minLength:"1" title:"Redis URL" description:"Connection URL: redis://[user:pass@]host:port[/db]" format:"uri"`
	KeyPrefix  string  `json:"keyPrefix" required:"true" minLength:"1" title:"Key Prefix" description:"Namespace for IDs (composite key = keyPrefix:id)"`
	ID         string  `json:"id" required:"true" minLength:"1" title:"ID" description:"The ID to deduplicate against"`
	TTLSeconds int     `json:"ttlSeconds" required:"true" minimum:"1" title:"TTL Seconds" description:"How long Redis remembers this ID before expiring"`
}

type Result struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	ID      string  `json:"id" title:"ID"`
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
		Description: "Redis Dedup",
		Info:        "Atomic 'first seen' check via SET NX EX. Routes new IDs to the New port and duplicates to the Seen port. Redis client is cached per URL across calls.",
		Tags:        []string{"Redis", "Dedup", "DB"},
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
	return c.dedup(ctx, handler, in)
}

func (c *Component) dedup(ctx context.Context, handler module.Handler, in Request) module.Result {
	client, err := pool.Redis(in.URL)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	key := in.KeyPrefix + ":" + in.ID
	ttl := time.Duration(in.TTLSeconds) * time.Second

	created, err := client.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		// A dial failure means the command never reached the server, so no
		// key can have landed — the one case fail's "unknown whether it
		// landed" reasoning does not apply to.
		if pool.IsNeverSentRedis(err) {
			err = module.Retryable(err)
		}
		return c.fail(ctx, handler, in.Context, err)
	}

	if created {
		return handler(ctx, NewPort, Result{Context: in.Context, ID: in.ID})
	}
	return handler(ctx, SeenPort, Result{Context: in.Context, ID: in.ID})
}

// fail hands the failure to the error port, or bubbles it when the port is off.
// Nothing past a successful dial is ever marked retryable, transient causes
// included: SET NX is the whole dedup decision, and a failure leaves it unknown
// whether the key landed. A second attempt that trips over its own key routes a
// genuinely new ID to Seen — silently dropping it — so this is one call that
// must be answered by the flow author, not by a blind re-run. The dial failure
// marked at the call site is the exception: the command never reached the
// server, so no key can have landed.
func (c *Component) fail(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	if !c.settings.EnableErrorPort {
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, module.NewError(reqCtx, err))
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: RequestPort, Label: "Request", Configuration: Request{TTLSeconds: 86400}, Position: module.Left},
		{Name: NewPort, Label: "New", Source: true, Configuration: Result{}, Position: module.Right},
		{Name: SeenPort, Label: "Seen", Source: true, Configuration: Result{}, Position: module.Right},
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
