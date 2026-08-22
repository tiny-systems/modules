// Package vectorsearch implements vector_search — kNN over a
// pgvector column. The reader half of the RAG store; pairs with
// vector_upsert.
//
// Returns the top-K rows ordered by distance to the query vector,
// along with a similarity score normalised to [0, 1] where 1 is
// identical. Optional JSONB containment filter restricts the search
// to rows whose metadata @> filter.
//
// Distance metric is configurable via settings (cosine default; also
// l2 and inner product). The corresponding pgvector operator is
// chosen automatically (`<=>`, `<->`, `<#>`).
package vectorsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/database-module/components/pool"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/bundle"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "vector_search"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"

	MetricCosine = "cosine"
	MetricL2     = "l2"
	MetricIP     = "ip"

	defaultTopK = 5
)

var identifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)

type Context any

// Metadata is the user-defined shape of the metadata column. The user
// customises this in settings so downstream edges can navigate
// $.results[0].metadata.fieldName.
type Metadata map[string]any

func (Metadata) PrepareJSONSchema(s *jsonschema.Schema) error {
	if len(s.Properties) == 0 {
		s.AdditionalProperties = nil
	}
	return nil
}

type Settings struct {
	EnableErrorPort bool     `json:"enableErrorPort" required:"true" title:"Enable Error Port"`
	Table           string   `json:"table" required:"true" minLength:"1" title:"Table" description:"Source table. May be schema-qualified (e.g. agents.memories)."`
	IdColumn        string   `json:"idColumn" required:"true" minLength:"1" default:"id" title:"Id Column"`
	EmbeddingColumn string   `json:"embeddingColumn" required:"true" minLength:"1" default:"embedding" title:"Embedding Column"`
	MetadataColumn  string   `json:"metadataColumn" default:"metadata" title:"Metadata Column" description:"JSONB column. Leave blank if the table has no metadata."`
	DistanceMetric  string   `json:"distanceMetric" required:"true" enum:"cosine,l2,ip" default:"cosine" title:"Distance Metric" description:"cosine = pgvector <=> (most common for RAG); l2 = Euclidean <->; ip = negative inner product <#>."`
	Metadata        Metadata `json:"metadata,omitempty" type:"object" title:"Metadata Schema" description:"Expected shape of the metadata column. Sample values are placeholders for schema derivation." configurable:"true"`
}

type Request struct {
	Context        Context        `json:"context,omitempty" configurable:"true" title:"Context"`
	DSN            string         `json:"dsn" title:"DSN" description:"Postgres connection string. Leave empty to use the in-cluster pgvector bundle (auto-discovered); set it to target an external database."`
	Embedding      []float32      `json:"embedding" required:"true" minItems:"1" title:"Query Embedding" description:"Vector to search by — typically the embedding of the user's question."`
	TopK           int            `json:"topK" minimum:"1" default:"5" title:"Top K" description:"Maximum rows to return."`
	MetadataFilter map[string]any `json:"metadataFilter,omitempty" configurable:"true" title:"Metadata Filter" description:"Optional JSONB containment filter — only rows where metadata @> filter are returned. Empty means no filter."`
}

type Result struct {
	Id       string   `json:"id" title:"Id"`
	Score    float64  `json:"score" title:"Score" description:"Normalised similarity in [0, 1] where 1 = identical. Computed as 1 - distance for cosine/ip; for l2 it's 1 / (1 + distance)."`
	Distance float64  `json:"distance" title:"Distance" description:"Raw pgvector distance — useful for thresholding when you care about the metric directly."`
	Metadata Metadata `json:"metadata,omitempty" title:"Metadata"`
}

type Response struct {
	Context Context  `json:"context,omitempty" configurable:"true" title:"Context"`
	Results []Result `json:"results" title:"Results"`
	Count   int      `json:"count" title:"Count"`
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
			DistanceMetric:  MetricCosine,
		},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Vector Search",
		Info:        "kNN over a pgvector column. Returns the top-K nearest rows by configurable distance metric, with a normalised score in [0,1]. Optional JSONB metadata filter restricts the search. Pair with vector_upsert to build a RAG store — on the zero-config bundle a query before anything is stored returns an empty result (not an error), so the question path answers gracefully on an empty memory.",
		Tags:        []string{"Vectors", "Postgres", "pgvector", "RAG", "DB", "kNN"},
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
	return c.search(ctx, handler, in)
}

func (c *Component) search(ctx context.Context, handler module.Handler, in Request) module.Result {
	idCol := defaultStr(c.settings.IdColumn, "id")
	embCol := defaultStr(c.settings.EmbeddingColumn, "embedding")
	metaCol := c.settings.MetadataColumn
	metric := defaultStr(c.settings.DistanceMetric, MetricCosine)
	topK := in.TopK
	if topK <= 0 {
		topK = defaultTopK
	}

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

	op, err := metricOperator(metric)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	// Empty DSN = zero-config path: the in-cluster pgvector bundle
	// (auto-discovered from env the operator chart injects).
	dsn := in.DSN
	schema := ""
	if dsn == "" {
		var derr error
		if dsn, derr = bundle.PostgresDSN("pgvector"); derr != nil {
			return c.fail(ctx, handler, in.Context, derr)
		}
		// Isolate the shared bundle per project: search only sees rows in
		// this node's identity-derived schema (not settable by the author).
		id := c.Identity()
		schema = pool.TenantSchema(id.Namespace, id.ProjectName)
	}
	p, err := pool.PostgresScoped(ctx, dsn, schema)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	vec := formatVector(in.Embedding)

	var selectExpr, fromClause, whereClause, orderClause string
	selectExpr = fmt.Sprintf("%s, (%s %s $1::vector) AS distance", idCol, embCol, op)
	if metaCol != "" {
		selectExpr += ", " + metaCol
	}
	fromClause = c.settings.Table
	orderClause = fmt.Sprintf("%s %s $1::vector", embCol, op)

	args := []any{vec}
	if len(in.MetadataFilter) > 0 && metaCol != "" {
		filterBytes, err := json.Marshal(in.MetadataFilter)
		if err != nil {
			return c.fail(ctx, handler, in.Context, fmt.Errorf("encode metadata filter: %w", err))
		}
		whereClause = fmt.Sprintf("WHERE %s @> $3::jsonb", metaCol)
		args = append(args, topK, string(filterBytes))
	} else {
		args = append(args, topK)
	}

	sql := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY %s LIMIT $2", selectExpr, fromClause, whereClause, orderClause)

	rows, err := p.Query(ctx, sql, args...)
	if err != nil {
		// Zero-config store queried before anything was written — the table
		// vector_upsert would create doesn't exist yet. That's "no memories",
		// not a failure: return an empty result so the question path answers
		// gracefully instead of erroring the whole flow.
		if in.DSN == "" && isUndefinedTable(err) {
			return handler(ctx, ResponsePort, Response{Context: in.Context, Results: []Result{}, Count: 0})
		}
		return c.fail(ctx, handler, in.Context, c.retryable(err))
	}
	defer rows.Close()

	out := make([]Result, 0, topK)
	for rows.Next() {
		var id string
		var distance float64
		var metaRaw []byte

		dest := []any{&id, &distance}
		if metaCol != "" {
			dest = append(dest, &metaRaw)
		}

		if err := rows.Scan(dest...); err != nil {
			return c.fail(ctx, handler, in.Context, c.retryable(err))
		}

		var meta Metadata
		if len(metaRaw) > 0 {
			if err := json.Unmarshal(metaRaw, &meta); err != nil {
				return c.fail(ctx, handler, in.Context, fmt.Errorf("decode metadata for id %q: %w", id, err))
			}
		}

		out = append(out, Result{
			Id:       id,
			Score:    scoreFromDistance(distance, metric),
			Distance: distance,
			Metadata: meta,
		})
	}
	if err := rows.Err(); err != nil {
		return c.fail(ctx, handler, in.Context, c.retryable(err))
	}

	return handler(ctx, ResponsePort, Response{
		Context: in.Context,
		Results: out,
		Count:   len(out),
	})
}

// retryable marks a failure the server or the network could recover from. kNN
// is a pure read, so re-running the whole handler cannot double-apply anything
// — unlike vector_upsert, which never marks. A SQL error (dimension mismatch,
// unknown column) is left alone; the same query reproduces it. Only the query
// path calls this: the identifier and metric checks above go straight to fail,
// since bad config is not something a backoff clears.
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
	resultSample := Result{Metadata: c.settings.Metadata}
	ports := []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: RequestPort, Label: "Request", Configuration: Request{}, Position: module.Left},
		{
			Name:          ResponsePort,
			Label:         "Response",
			Source:        true,
			Configuration: Response{Results: []Result{resultSample}},
			Position:      module.Right,
		},
	}
	if !c.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name: ErrorPort, Label: "Error", Source: true, Configuration: module.ErrorMessage{}, Position: module.Bottom,
	})
}

func metricOperator(metric string) (string, error) {
	switch metric {
	case MetricCosine, "":
		return "<=>", nil
	case MetricL2:
		return "<->", nil
	case MetricIP:
		return "<#>", nil
	default:
		return "", fmt.Errorf("unknown distance metric %q (supported: cosine, l2, ip)", metric)
	}
}

func scoreFromDistance(d float64, metric string) float64 {
	switch metric {
	case MetricCosine:
		return 1 - d
	case MetricIP:
		return -d
	case MetricL2:
		return 1 / (1 + d)
	default:
		return 1 - d
	}
}

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

// isUndefinedTable reports whether a query error is Postgres "undefined
// table" (SQLSTATE 42P01) — i.e. the store table doesn't exist yet.
func isUndefinedTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "42P01")
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
	_ jsonschema.Preparer    = (*Metadata)(nil)
)

func init() {
	registry.Register(&Component{})
}
