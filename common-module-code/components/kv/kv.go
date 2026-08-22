package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/swaggest/jsonschema-go"
	"github.com/tiny-systems/ajson"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/state"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "kv"

	StorePort       = "store"
	QueryPort       = "query"
	QueryResultPort = "query_result"
	StoreAckPort    = "store_ack"

	OpStore  = "store"
	OpDelete = "delete"

	defaultMaxRecords  = 100
	maxRecordSizeBytes = 32 * 1024 // 32KB per record
)

// Separate context types for schema propagation — platform matches by type name.
// StoreContext flows through store → store_ack, QueryContext through query → query_result.
type StoreContext any
type QueryContext any

// Document is the user-defined value structure
type Document map[string]interface{}

func (d Document) PrepareJSONSchema(schema *jsonschema.Schema) error {
	if len(schema.Properties) == 0 {
		id := jsonschema.Schema{}
		id.AddType(jsonschema.String)
		id.WithTitle("ID")
		schema.WithRequired("id")
		schema.Properties = map[string]jsonschema.SchemaOrBool{
			"id": id.ToSchemaOrBool(),
		}
	}
	return nil
}

// Settings configures the KV store
type Settings struct {
	Document       Document `json:"document,omitempty" type:"object" required:"true" title:"Document" description:"Structure of stored documents. Values are arbitrary. Make sure the document has the primary key defined below." configurable:"true"`
	PrimaryKey     string   `json:"primaryKey" title:"Primary Key" required:"true" default:"id"`
	MaxRecords     int      `json:"maxRecords" title:"Max Records" description:"Maximum number of records (0 = default 100). The real ceiling is the node's ~900KB total state byte budget, not the record count." default:"100"`
	EnableStoreAck bool     `json:"enableStoreAckPort" required:"true" title:"Enable Store Ack Port" description:"Emits acknowledgment after store/delete operations"`
}

// StoreRequest is the input for store/delete operations
type StoreRequest struct {
	Context   StoreContext `json:"context,omitempty" configurable:"true" title:"Context"`
	Operation string       `json:"operation" required:"true" enum:"store,delete" enumTitles:"Store,Delete" default:"store" title:"Operation"`
	Document  Document     `json:"document" required:"true" title:"Document" description:"Document to store or delete"`
}

// StoreAck acknowledges a store/delete operation
type StoreAck struct {
	Context StoreContext `json:"context,omitempty" title:"Context"`
	Request StoreRequest `json:"request" title:"Request"`
}

// QueryRequest is the input for querying records
type QueryRequest struct {
	Context QueryContext `json:"context,omitempty" configurable:"true" title:"Context"`
	Query   string       `json:"query,omitempty" required:"true" title:"Query" description:"JSONPath expression evaluated against each record (e.g. $.status == 'DOWN')"`
}

// QueryResultItem is a single matched record
type QueryResultItem struct {
	Key      string   `json:"key" title:"Key"`
	Document Document `json:"document" title:"Document"`
}

// QueryResult returns all matching records
type QueryResult struct {
	Context QueryContext      `json:"context,omitempty" title:"Context"`
	Results []QueryResultItem `json:"results" title:"Results"`
	Count   int               `json:"count" title:"Count"`
	Query   string            `json:"query" title:"Query"`
}

// Control exposes the reset button on the _control port
type Control struct {
	Records int  `json:"records" title:"Records" readonly:"true"`
	Reset   bool `json:"reset" format:"button" title:"Reset" required:"true"`
}

// Component implements a metadata-backed key-value store on top of
// module.State. State reads through the controller-runtime cache and
// writes via the reconcile-port debouncer, so all replicas of common-module
// see identical kv contents (cache is shared via watch).
//
// No local records map and no storeUsed flag are needed — those existed
// to manage in-memory-vs-metadata divergence which State eliminates.
type Component struct {
	module.Base

	mu       sync.RWMutex
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			PrimaryKey: "id",
			MaxRecords: defaultMaxRecords,
			Document:   Document{"id": "ID"},
		},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Key-Value Store",
		Info:        "Key-value store backed by TinyNode metadata via the SDK State primitive. Stores documents with a configurable schema and primary key. Supports JSONPath queries. Multi-replica safe — all common-module replicas observe the same store via the K8s watch. Document VALUES ARE STRINGS: a count or a flag must be converted before it is stored (an expression can do it, e.g. \"\" + $.count), and comes back as a string to be parsed by whatever reads it.",
		Tags:        []string{"KV", "Storage", "Data"},
	}
}

func (c *Component) recordCount(ctx context.Context) int {
	if c.State() == nil {
		return 0
	}
	keys, err := c.State().List(ctx, "")
	if err != nil {
		return 0
	}
	return len(keys)
}

func (c *Component) getControl(ctx context.Context) Control {
	return Control{Records: c.recordCount(ctx)}
}

// OnSettings receives Settings from the SettingsPort.
func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if len(in.Document) == 0 {
		return fmt.Errorf("document must have at least one field")
	}
	if in.PrimaryKey == "" {
		return fmt.Errorf("primary key cannot be empty")
	}
	if _, ok := in.Document[in.PrimaryKey]; !ok {
		return fmt.Errorf("primary key %q not found in document", in.PrimaryKey)
	}
	if in.MaxRecords <= 0 {
		in.MaxRecords = defaultMaxRecords
	}
	c.mu.Lock()
	c.settings = in
	c.mu.Unlock()
	return nil
}

// OnReconcile is a no-op: State backend handles cache freshness automatically.
func (c *Component) OnReconcile(_ context.Context, _ v1alpha1.TinyNode) error {
	return nil
}

// OnControl handles the Reset button on the dashboard. Deletes every key
// in the State store and pushes an updated Control to the dashboard.
func (c *Component) OnControl(ctx context.Context, msg any) error {
	in, ok := msg.(Control)
	if !ok {
		return fmt.Errorf("invalid control")
	}
	if !in.Reset {
		return nil
	}

	if c.State() != nil {
		keys, err := c.State().List(ctx, "")
		if err != nil {
			return fmt.Errorf("list keys for reset: %v", err)
		}
		for _, k := range keys {
			if err := c.State().Delete(ctx, k); err != nil {
				return fmt.Errorf("delete %q during reset: %v", k, err)
			}
		}
	}

	_ = c.Emit(context.Background(), v1alpha1.ControlPort, c.getControl(ctx))
	return nil
}

// Handle dispatches the business ports (Store and Query). System ports
// are handled by capability methods above.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	switch port {
	case StorePort:
		in, ok := msg.(StoreRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid store request"))
		}
		return c.handleStore(ctx, handler, in)

	case QueryPort:
		in, ok := msg.(QueryRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid query request"))
		}
		return c.handleQuery(ctx, handler, in)
	}
	return module.Fail(fmt.Errorf("unknown port: %s", port))
}

func (c *Component) handleStore(ctx context.Context, handler module.Handler, req StoreRequest) module.Result {
	c.mu.RLock()
	pk := c.settings.PrimaryKey
	maxRecords := c.settings.MaxRecords
	enableAck := c.settings.EnableStoreAck
	c.mu.RUnlock()

	pkVal, ok := req.Document[pk]
	if !ok {
		return module.Fail(fmt.Errorf("primary key %q not found in document", pk))
	}
	pkStr, ok := pkVal.(string)
	if !ok {
		return module.Fail(fmt.Errorf("primary key must be a string, got %T", pkVal))
	}
	if pkStr == "" {
		return module.Fail(fmt.Errorf("primary key cannot be empty"))
	}

	if c.State() == nil {
		return module.Fail(fmt.Errorf("state backend not available"))
	}

	switch req.Operation {
	case OpStore:
		data, err := json.Marshal(req.Document)
		if err != nil {
			return module.Fail(fmt.Errorf("failed to marshal document: %v", err))
		}
		if len(data) > maxRecordSizeBytes {
			return module.Fail(fmt.Errorf("document too large: %d bytes (max %d)", len(data), maxRecordSizeBytes))
		}

		// Capacity check (allow update of existing key)
		if _, exists, _ := c.State().Get(ctx, pkStr); !exists {
			keys, _ := c.State().List(ctx, "")
			if len(keys) >= maxRecords {
				return module.Fail(fmt.Errorf("store full: %d records (max %d)", len(keys), maxRecords))
			}
		}

		if err := c.State().Set(ctx, pkStr, data); err != nil {
			if errors.Is(err, state.ErrStateTooLarge) {
				return module.Fail(fmt.Errorf("state budget exhausted: the node's total state is capped at ~900KB; store smaller records or lower maxRecords: %v", err))
			}
			return module.Fail(fmt.Errorf("state.Set: %v", err))
		}

	case OpDelete:
		if err := c.State().Delete(ctx, pkStr); err != nil {
			return module.Fail(fmt.Errorf("state.Delete: %v", err))
		}

	default:
		return module.Fail(fmt.Errorf("unknown operation: %s", req.Operation))
	}

	c.Emit(context.Background(), v1alpha1.ControlPort, c.getControl(ctx))

	if enableAck {
		return handler(ctx, StoreAckPort, StoreAck{
			Context: req.Context,
			Request: req,
		})
	}
	return module.Result{}
}

func (c *Component) handleQuery(ctx context.Context, handler module.Handler, req QueryRequest) module.Result {
	if c.State() == nil {
		return module.Fail(fmt.Errorf("state backend not available"))
	}

	keys, err := c.State().List(ctx, "")
	if err != nil {
		return module.Fail(fmt.Errorf("list keys: %v", err))
	}

	var results []QueryResultItem

	for _, key := range keys {
		data, found, err := c.State().Get(ctx, key)
		if err != nil || !found {
			continue
		}
		if req.Query == "" {
			doc := Document{}
			if err := json.Unmarshal(data, &doc); err != nil {
				continue
			}
			results = append(results, QueryResultItem{Key: key, Document: doc})
			continue
		}
		// Evaluate JSONPath query against the stored document
		node, err := ajson.Unmarshal(data)
		if err != nil {
			continue
		}
		result, err := ajson.Eval(node, req.Query)
		if err != nil {
			continue
		}
		v, err := result.Unpack()
		if err != nil {
			continue
		}
		if v == true {
			doc := Document{}
			if err = json.Unmarshal(data, &doc); err != nil {
				continue
			}
			results = append(results, QueryResultItem{Key: key, Document: doc})
		}
	}

	if results == nil {
		results = []QueryResultItem{}
	}

	return handler(ctx, QueryResultPort, QueryResult{
		Context: req.Context,
		Results: results,
		Count:   len(results),
		Query:   req.Query,
	})
}

func (c *Component) Ports() []module.Port {
	c.mu.RLock()
	settings := c.settings
	c.mu.RUnlock()

	ports := []module.Port{
		{Name: v1alpha1.ReconcilePort},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: settings,
		},
		{
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Source:        true,
			Configuration: c.getControl(context.Background()),
		},
		{
			Name:  StorePort,
			Label: "Store",
			Configuration: StoreRequest{
				Operation: OpStore,
				Document:  settings.Document,
			},
			Position: module.Left,
		},
		{
			Name:  QueryPort,
			Label: "Query",
			Configuration: QueryRequest{
				Query: "$.status == 'DOWN'",
			},
			Position: module.Left,
		},
		{
			Name:   QueryResultPort,
			Label:  "Query Result",
			Source: true,
			Configuration: QueryResult{
				Results: []QueryResultItem{
					{Document: settings.Document},
				},
			},
			Position: module.Right,
		},
	}

	if settings.EnableStoreAck {
		ports = append(ports, module.Port{
			Name:   StoreAckPort,
			Label:  "Store Ack",
			Source: true,
			Configuration: StoreAck{
				Request: StoreRequest{
					Document: settings.Document,
				},
			},
			Position: module.Right,
		})
	}

	return ports
}

var (
	_ module.Component        = (*Component)(nil)
	_ module.SettingsHandler  = (*Component)(nil)
	_ module.ReconcileHandler = (*Component)(nil)
	_ module.ControlHandler   = (*Component)(nil)
	_ jsonschema.Preparer     = (*Document)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
