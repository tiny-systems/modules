package postgresexec

import (
	"context"
	"fmt"

	"github.com/tiny-systems/modules/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/bundle"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "postgres_exec"
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
	DSN     string  `json:"dsn" title:"DSN" description:"Postgres connection string. Leave empty to use the in-cluster pgvector bundle (auto-discovered); set it to target an external database."`
	SQL     string  `json:"sql" required:"true" minLength:"1" title:"SQL" description:"INSERT/UPDATE/DELETE with $1, $2, ... placeholders" format:"textarea"`
	Params  []any   `json:"params,omitempty" title:"Params" description:"Positional parameters for $1, $2, ..."`
}

type Response struct {
	Context      Context `json:"context,omitempty" configurable:"true" title:"Context"`
	RowsAffected int64   `json:"rowsAffected" title:"Rows Affected"`
}

type Component struct {
	module.Base
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Postgres Exec",
		Info:        "Executes INSERT/UPDATE/DELETE against Postgres with positional parameters. Connection pool is cached per DSN across calls.",
		Tags:        []string{"Postgres", "SQL", "DB"},
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
	return c.run(ctx, handler, in)
}

func (c *Component) run(ctx context.Context, handler module.Handler, in Request) module.Result {
	// Empty DSN = zero-config path: the in-cluster pgvector bundle
	// (auto-discovered from env the operator chart injects). Lets a memory
	// flow bootstrap its table with the same zero-config the vector
	// components use, instead of forcing an explicit DSN just to CREATE TABLE.
	dsn := in.DSN
	schema := ""
	if dsn == "" {
		var derr error
		if dsn, derr = bundle.PostgresDSN("pgvector"); derr != nil {
			return c.fail(ctx, handler, in.Context, derr)
		}
		// Isolate the shared bundle per project: CREATE TABLE and every
		// other statement land in this node's identity-derived schema, so a
		// playground trial can't touch another's tables. Not settable by
		// the author; no-op for an explicit external DSN.
		id := c.Identity()
		schema = pool.TenantSchema(id.Namespace, id.ProjectName)
	}
	p, err := pool.PostgresScoped(ctx, dsn, schema)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	tag, err := p.Exec(ctx, in.SQL, in.Params...)
	if err != nil {
		// Only a provably-never-sent failure (dial refused, query bytes never
		// written) is marked — see fail's comment for why nothing else is.
		if pool.IsNeverSentPostgres(err) {
			err = module.Retryable(err)
		}
		return c.fail(ctx, handler, in.Context, err)
	}

	return handler(ctx, ResponsePort, Response{
		Context:      in.Context,
		RowsAffected: tag.RowsAffected(),
	})
}

// fail hands the failure to the error port, or bubbles it when the port is off.
// Almost nothing here is marked retryable, transient causes included: this runs
// arbitrary INSERT/UPDATE/DELETE, and a statement that fails on the way back
// (connection dropped after commit) has still applied. Re-running the handler
// would insert twice. The one exception, marked at the call site, is a failure
// that provably precedes the send (pool.IsNeverSentPostgres) — nothing ran, so
// nothing can have applied. A caller who knows their SQL is idempotent can
// retry the rest deliberately; the component cannot know that for them.
func (c *Component) fail(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	if !c.settings.EnableErrorPort {
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
