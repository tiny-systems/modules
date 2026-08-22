package postgresquery

import (
	"context"
	"fmt"

	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/bundle"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "postgres_query"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

// Row is the user-defined row shape. The user customises the schema in settings
// so downstream edges can navigate $.rows[0].columnName.
type Row map[string]any

func (Row) PrepareJSONSchema(s *jsonschema.Schema) error {
	if len(s.Properties) == 0 {
		s.AdditionalProperties = nil
	}
	return nil
}

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port"`
	Row             Row  `json:"row,omitempty" type:"object" title:"Row" description:"Expected shape of each returned row. Sample values are placeholders." configurable:"true"`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	DSN     string  `json:"dsn" title:"DSN" description:"Postgres connection string. Leave empty to use the in-cluster pgvector bundle (auto-discovered); set it to target an external database."`
	SQL     string  `json:"sql" required:"true" minLength:"1" title:"SQL" description:"SELECT with $1, $2, ... placeholders" format:"textarea"`
	Params  []any   `json:"params,omitempty" title:"Params"`
}

type Response struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Rows    []Row   `json:"rows" title:"Rows"`
	Count   int     `json:"count" title:"Count"`
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
		Description: "Postgres Query",
		Info:        "Runs SELECT against Postgres and returns rows as a list of objects keyed by column name. Configure expected row shape in settings so downstream edges can navigate the result. Connection pool is cached per DSN.",
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
	return c.query(ctx, handler, in)
}

func (c *Component) query(ctx context.Context, handler module.Handler, in Request) module.Result {
	// Empty DSN = zero-config path: the in-cluster pgvector bundle
	// (auto-discovered), same as postgres_exec and the vector components.
	dsn := in.DSN
	schema := ""
	if dsn == "" {
		var derr error
		if dsn, derr = bundle.PostgresDSN("pgvector"); derr != nil {
			return c.fail(ctx, handler, in.Context, derr)
		}
		// Isolate the shared bundle per project: SELECT only sees this
		// node's identity-derived schema, closing the raw-SQL read path that
		// a metadata-tag filter would leave open. No-op for an external DSN.
		id := c.Identity()
		schema = pool.TenantSchema(id.Namespace, id.ProjectName)
	}
	p, err := pool.PostgresScoped(ctx, dsn, schema)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	rows, err := p.Query(ctx, in.SQL, in.Params...)
	if err != nil {
		return c.fail(ctx, handler, in.Context, c.retryable(err))
	}
	defer rows.Close()

	cols := rows.FieldDescriptions()
	out := make([]Row, 0)

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return c.fail(ctx, handler, in.Context, c.retryable(err))
		}
		row := make(Row, len(cols))
		for i, col := range cols {
			row[string(col.Name)] = values[i]
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return c.fail(ctx, handler, in.Context, c.retryable(err))
	}

	return handler(ctx, ResponsePort, Response{
		Context: in.Context,
		Rows:    out,
		Count:   len(out),
	})
}

// retryable marks a SELECT failure the server or the network could recover
// from. Safe here in a way it isn't for postgres_exec: this component only
// reads, so re-running the whole handler cannot double-apply anything. A SQL
// error (bad syntax, unknown column, permission denied) is left alone — the
// same statement would just fail again.
func (c *Component) retryable(err error) error {
	if pool.IsTransientPostgres(err) {
		return module.Retryable(err)
	}
	return err
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
		{
			Name:   ResponsePort,
			Label:  "Response",
			Source: true,
			Configuration: Response{
				Rows: []Row{c.settings.Row},
			},
			Position: module.Right,
		},
	}
	if !c.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name: ErrorPort, Label: "Error", Source: true, Configuration: module.ErrorMessage{}, Position: module.Bottom,
	})
}

var (
	_ module.Component    = (*Component)(nil)
	_ jsonschema.Preparer = (*Row)(nil)
)

func init() {
	registry.Register(&Component{})
}
