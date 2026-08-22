// Package vectorupsert implements vector_upsert — writes an
// embedding row into a pgvector-enabled Postgres table. Pairs with
// vector_search to form the RAG store half of the agent runtime
// (embed → vector_upsert; later, query embed → vector_search →
// llm_chat).
//
// Zero-config by default: on the in-cluster pgvector bundle path (empty
// DSN) the component CREATES the table on first write if it does not
// exist — `id TEXT PRIMARY KEY, embedding VECTOR(<dims>)[, metadata JSONB]`,
// the dimension inferred from the embedding it was handed. So an
// `embed_text → vector_upsert` flow just works with no separate DDL step.
// For an EXPLICIT external DSN the component does NOT touch schema — the
// caller owns that database, so the table (and `CREATE EXTENSION vector;`)
// must already exist. Column names are configurable so an existing schema
// can be reused; the metadata column is optional (blank setting skips it).
//
// The id is likewise optional: leave it empty and the component mints a
// random one, so the common case (embed → store) needs no id-generating
// node in between.
package vectorupsert

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tiny-systems/modules/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/bundle"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "vector_upsert"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

// identifierRegex restricts table and column identifiers so we can
// interpolate them into SQL safely. Matches the conservative subset
// allowed by Postgres unquoted identifiers plus optional schema
// prefix.
var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)

type Context any

type Settings struct {
	EnableErrorPort bool   `json:"enableErrorPort" required:"true" title:"Enable Error Port"`
	Table           string `json:"table" required:"true" minLength:"1" title:"Table" description:"Target table. May be schema-qualified (e.g. agents.memories). Must already exist and have the 'vector' extension enabled."`
	IdColumn        string `json:"idColumn" required:"true" minLength:"1" default:"id" title:"Id Column" description:"Primary key column. Used in ON CONFLICT clause."`
	EmbeddingColumn string `json:"embeddingColumn" required:"true" minLength:"1" default:"embedding" title:"Embedding Column" description:"pgvector column name."`
	MetadataColumn  string `json:"metadataColumn" default:"metadata" title:"Metadata Column" description:"JSONB column for arbitrary side data. Leave blank to skip writing metadata."`
}

type Request struct {
	Context   Context        `json:"context,omitempty" configurable:"true" title:"Context"`
	DSN       string         `json:"dsn" title:"DSN" description:"Postgres connection string. Leave empty to use the in-cluster pgvector bundle (auto-discovered); set it to target an external database."`
	Id        string         `json:"id" title:"Id" description:"Primary key. Leave empty to auto-generate a random id. Existing rows with the same id are overwritten."`
	Embedding []float32      `json:"embedding" required:"true" minItems:"1" title:"Embedding" description:"Dense vector — same length as the column dimension."`
	Metadata  map[string]any `json:"metadata,omitempty" configurable:"true" title:"Metadata" description:"Optional JSONB payload (tags, source, timestamps, etc.). Ignored if Metadata Column is blank."`
}

type Response struct {
	Context      Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Id           string  `json:"id" title:"Id"`
	RowsAffected int64   `json:"rowsAffected" title:"Rows Affected"`
}

type Component struct {
	module.Base
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			IdColumn:        "id",
			EmbeddingColumn: "embedding",
			MetadataColumn:  "metadata",
		},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Vector Upsert",
		Info:        "Writes an embedding row into a pgvector table. INSERT ... ON CONFLICT (id) DO UPDATE — existing rows are overwritten. Zero-config on the in-cluster bundle: leave DSN empty and the table is created automatically on first write (vector dimension inferred from the embedding), and an empty id is auto-generated — so wire embed_text.embedding straight into this node's embedding with NO id-generating node in between. Only an explicit external DSN requires you to pre-create the table.",
		Tags:        []string{"Vectors", "Postgres", "pgvector", "RAG", "DB"},
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settings = in
	return nil
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	return c.upsert(ctx, handler, in)
}

func (c *Component) upsert(ctx context.Context, handler module.Handler, in Request) module.Result {
	idCol := defaultStr(c.settings.IdColumn, "id")
	embCol := defaultStr(c.settings.EmbeddingColumn, "embedding")
	metaCol := c.settings.MetadataColumn

	if err := validateIdentifier("table", c.settings.Table); err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}
	if err := validateIdentifier("id column", idCol); err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}
	if err := validateIdentifier("embedding column", embCol); err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}
	if metaCol != "" {
		if err := validateIdentifier("metadata column", metaCol); err != nil {
			return c.fail(ctx, handler, in.Context, err)
		}
	}

	// Empty DSN = zero-config path: the in-cluster pgvector bundle the
	// module declared (auto-discovered from env the operator chart
	// injects when the bundle is enabled).
	dsn := in.DSN
	schema := ""
	if dsn == "" {
		var derr error
		if dsn, derr = bundle.PostgresDSN("pgvector"); derr != nil {
			return c.fail(ctx, handler, in.Context, derr)
		}
		// Isolate the shared bundle per project: all SQL is confined to a
		// schema derived from this node's injected identity (not settable
		// by the flow author). No-op for an explicit external DSN.
		id := c.Identity()
		schema = pool.TenantSchema(id.Namespace, id.ProjectName)
	}
	p, err := pool.PostgresScoped(ctx, dsn, schema)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	// Zero-config store: create the table on first write when running
	// against the in-cluster bundle. The embedding we were handed fixes the
	// vector dimension, so we never guess. Skipped for an explicit external
	// DSN — that database's schema belongs to the caller. len==0 is left to
	// fall through so the INSERT surfaces the real "empty embedding" error
	// rather than a CREATE with vector(0).
	if in.DSN == "" && len(in.Embedding) > 0 {
		if err := ensureVectorTable(ctx, p, c.settings.Table, idCol, embCol, metaCol, len(in.Embedding)); err != nil {
			// Safe to mark transient failures here, unlike the INSERT below:
			// the row hasn't been written yet and everything up to this point
			// (pool acquire, CREATE ... IF NOT EXISTS) is idempotent, so
			// re-running the whole handler cannot duplicate anything.
			if pool.IsTransientPostgres(err) {
				err = module.Retryable(err)
			}
			return c.fail(ctx, handler, in.Context, err)
		}
	}

	// Auto-generate an id when the caller left it empty, so an embed→store
	// flow needs no id-minting node in between.
	id := in.Id
	if id == "" {
		id = newID()
	}

	vec := formatVector(in.Embedding)

	var sql string
	var args []any

	if metaCol == "" {
		sql = fmt.Sprintf(
			`INSERT INTO %s (%s, %s) VALUES ($1, $2::vector)
			 ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s`,
			c.settings.Table, idCol, embCol,
			idCol, embCol, embCol,
		)
		args = []any{id, vec}
	} else {
		metaBytes, err := json.Marshal(in.Metadata)
		if err != nil {
			return c.fail(ctx, handler, in.Context, fmt.Errorf("encode metadata: %w", err))
		}
		sql = fmt.Sprintf(
			`INSERT INTO %s (%s, %s, %s) VALUES ($1, $2::vector, $3::jsonb)
			 ON CONFLICT (%s) DO UPDATE SET %s = EXCLUDED.%s, %s = EXCLUDED.%s`,
			c.settings.Table, idCol, embCol, metaCol,
			idCol, embCol, embCol, metaCol, metaCol,
		)
		args = []any{id, vec, string(metaBytes)}
	}

	tag, err := p.Exec(ctx, sql, args...)
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
		Id:           id,
		RowsAffected: tag.RowsAffected(),
	})
}

// fail hands the failure to the error port, or bubbles it when the port is off.
// The INSERT is almost never marked retryable, transient causes included. The
// upsert looks idempotent — ON CONFLICT (id) DO UPDATE — but only when the
// caller supplies the id: an empty id is minted fresh per invocation, so
// re-running the handler writes a SECOND row for the same text instead of
// overwriting the first, quietly duplicating a memory. And a write that fails
// on the way back (connection dropped after commit) has still landed. The call
// sites mark the two provably-safe cases: a failure before the INSERT was sent
// (pool.IsNeverSentPostgres), and the idempotent table-ensure step.
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

// ensureVectorTable creates the target table if it doesn't exist, using the
// embedding length as the vector dimension. Idempotent (IF NOT EXISTS), so
// it's a no-op after the first write. Only called on the zero-config bundle
// path; identifiers are already validated by the caller. CREATE EXTENSION is
// best-effort — the bundle image ships pgvector installed, but requesting it
// IF NOT EXISTS costs nothing and makes a bare Postgres work too.
func ensureVectorTable(ctx context.Context, p *pgxpool.Pool, table, idCol, embCol, metaCol string, dims int) error {
	_, _ = p.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector")
	cols := fmt.Sprintf("%s TEXT PRIMARY KEY, %s vector(%d)", idCol, embCol, dims)
	if metaCol != "" {
		cols += fmt.Sprintf(", %s jsonb", metaCol)
	}
	_, err := p.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, cols))
	return err
}

// newID mints a random 128-bit hex id for callers that don't supply one.
func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read effectively never fails; degrade to a constant-prefixed
		// value rather than an empty primary key if it somehow does.
		return "mem-" + hex.EncodeToString(b[:8])
	}
	return hex.EncodeToString(b[:])
}

// formatVector renders []float32 as pgvector's text input format:
// "[v1,v2,v3]". Postgres parses this via the ::vector cast.
func formatVector(v []float32) string {
	if len(v) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.Grow(len(v) * 12)
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

func validateIdentifier(label, ident string) error {
	if !identifierRegex.MatchString(ident) {
		return fmt.Errorf("invalid %s %q: must match [a-zA-Z_][a-zA-Z0-9_]* (optionally schema-qualified)", label, ident)
	}
	return nil
}

func defaultStr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
