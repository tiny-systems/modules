// Package conversation implements conversation — atomic conversation
// memory for llm_chat. It keys message lists by conversationId and makes
// append a single bbolt transaction (load → append → trim → save), so two
// concurrent turns can never lose each other's messages. This replaces
// the fragile document_store wiring (get → append in a mapper → put)
// where the read-modify-write spans three nodes and interleaved turns
// drop messages.
//
// Storage: same backing as document_store — bbolt on the PVC — but a
// separate file (default /data/conversation.db). Sharing document_store's
// file is not an option: bbolt takes an exclusive OS-level flock, and
// document_store's open/close plumbing is private to its component
// instance, so a second opener would block 30s and fail. A separate file
// on the same PVC sidesteps the contention without touching
// document_store at all.
//
// Within this component, however, sharing is mandatory: the scheduler
// creates a fresh Component instance per TinyNode, and every flow's
// nodes run in the same module pod. Two conversation nodes pointing at
// the same path (the default!) would deadlock on the flock. The
// package-level storeRegistry therefore hands out one refcounted
// *bbolt.DB per path — all conversation nodes in the process share the
// handle, and bbolt's internal writer lock serializes their Update
// transactions. That is also what makes the atomicity guarantee hold
// ACROSS nodes: an "append user turn" node and an "append assistant
// turn" node in the same flow funnel through the same single writer.
//
// Windowing: Settings.MaxMessages caps how many messages are kept per
// conversation (default 50, keep-most-recent). 0 disables the cap —
// beware: every append then rewrites an ever-growing document, so
// long-lived conversations inflate both the DB file and each turn's
// payload. Set 0 only when something else bounds conversation length.
//
// Like document_store, this component is single-replica (bbolt locks the
// file); the module ships with replicas: 1 and a PVC.
package conversation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"go.etcd.io/bbolt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "conversation"

	AppendPort       = "append"
	GetPort          = "get"
	ClearPort        = "clear"
	AppendResultPort = "append_ok"
	GetResultPort    = "get_ok"
	ClearResultPort  = "clear_ok"
	ErrorPort        = "error"

	// defaultPath deliberately differs from document_store's
	// /data/store.db — bbolt flocks exclusively, and the two components
	// run in the same pod. Same PVC, separate files.
	defaultPath        = "/data/conversation.db"
	defaultMaxMessages = 50

	// bucketName is the single bbolt bucket holding all conversations,
	// keyed by conversationId. One bucket (vs per-conversation buckets)
	// keeps clear() a plain key delete and get/append O(1) lookups.
	bucketName = "conversations"
)

// boltOpenTimeout mirrors document_store: bounds how long bbolt.Open
// waits for the file lock, long enough for a prior pod's lock to clear
// on restart / PVC reattachment.
const boltOpenTimeout = 30 * time.Second

type Context any

type Settings struct {
	EnableErrorPort bool   `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"Route operational failures (store unavailable, corrupt record, validation errors) to the error port instead of failing the request."`
	MaxMessages     int    `json:"maxMessages" required:"true" minimum:"0" default:"50" title:"Max messages per conversation" description:"Window size: only the most recent N messages are kept per conversation; older ones are trimmed on append. 0 = unlimited — WARNING: state then grows without bound, every append rewrites an ever-larger document. Use 0 only when conversation length is bounded elsewhere."`
	Path            string `json:"path" required:"true" minLength:"1" default:"/data/conversation.db" title:"DB file path" description:"Absolute path to the bbolt file. Must be on the mounted PVC for durability. Keep it distinct from document_store's file (exclusive file lock); all conversation nodes using the same path share one handle."`
}

// --- Port message shapes -----------------------------------------

type AppendRequest struct {
	Context        Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary data passed through unchanged to the result."`
	ConversationID string  `json:"conversationId" required:"true" minLength:"1" title:"Conversation ID" description:"Key of the conversation. Any stable string — a chat/session/user id."`
	Messages       []any   `json:"messages" required:"true" minItems:"1" configurable:"true" title:"Messages" description:"One or more messages to append, in order. Shape-agnostic — pass llm_chat message objects ({role, content}) through unchanged."`
}

type AppendResult struct {
	Context        Context `json:"context"`
	ConversationID string  `json:"conversationId"`
	Messages       []any   `json:"messages" configurable:"true" title:"Messages" description:"The full post-append window — feed this straight into llm_chat's messages."`
}

type GetRequest struct {
	Context        Context `json:"context,omitempty" configurable:"true" title:"Context"`
	ConversationID string  `json:"conversationId" required:"true" minLength:"1" title:"Conversation ID"`
}

type GetResult struct {
	Context        Context `json:"context"`
	ConversationID string  `json:"conversationId"`
	Messages       []any   `json:"messages" configurable:"true" title:"Messages" description:"Current window. Empty array for an unknown conversation — not an error."`
}

type ClearRequest struct {
	Context        Context `json:"context,omitempty" configurable:"true" title:"Context"`
	ConversationID string  `json:"conversationId" required:"true" minLength:"1" title:"Conversation ID"`
}

type ClearResult struct {
	Context        Context `json:"context"`
	ConversationID string  `json:"conversationId"`
	Cleared        bool    `json:"cleared" description:"False when the conversation didn't exist (clear is idempotent)."`
}

// --- Shared bbolt handle registry ---------------------------------
//
// The scheduler builds a fresh Component instance per TinyNode, and all
// flows' nodes share this process. bbolt's flock is exclusive, so if
// each instance opened the file itself, the second conversation node in
// the pod would block on bbolt.Open for 30s and fail. The registry
// refcounts one *bbolt.DB per path; bbolt itself is safe for concurrent
// use within a process (Update transactions serialize on its writer
// lock — the property the atomic-append guarantee rests on).

type storeRegistry struct {
	mu      sync.Mutex
	handles map[string]*storeHandle
}

type storeHandle struct {
	db   *bbolt.DB
	refs int
}

var stores = &storeRegistry{handles: map[string]*storeHandle{}}

func (r *storeRegistry) acquire(path string) (*bbolt.DB, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.handles[path]; ok {
		h.refs++
		return h.db, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("ensure store dir: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: boltOpenTimeout})
	if err != nil {
		return nil, fmt.Errorf("open bbolt at %s: %w "+
			"(another process may hold the file lock — check that a prior pod "+
			"has terminated and the PVC released; if document_store shares this "+
			"pod make sure the two components use different file paths)",
			path, err)
	}
	r.handles[path] = &storeHandle{db: db, refs: 1}
	return db, nil
}

func (r *storeRegistry) release(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.handles[path]
	if !ok {
		return
	}
	h.refs--
	if h.refs > 0 {
		return
	}
	_ = h.db.Close()
	delete(r.handles, path)
}

// --- Component ----------------------------------------------------

type Component struct {
	module.Base

	mu       sync.RWMutex
	settings Settings
	db       *bbolt.DB
	dbPath   string
}

func (c *Component) Instance() module.Component {
	return &Component{settings: Settings{
		Path:        defaultPath,
		MaxMessages: defaultMaxMessages,
	}}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Conversation Memory",
		Info: "Atomic conversation memory for llm_chat — keyed by conversationId, backed by bbolt on the PVC. " +
			"append does load+append+trim+save in ONE transaction, so two concurrent turns can't lose each other's " +
			"messages (unlike a get→append→put chain around document_store). Intended wiring: append the user turn " +
			"→ feed the response's messages into llm_chat → append the assistant turn; or make a single append of " +
			"both turns after llm_chat responds. Every append/get response carries the full current window, ready " +
			"to feed llm_chat directly. get on an unknown conversationId returns an empty messages array — not an " +
			"error — so start a cold conversation by appending the user turn first (that also satisfies llm_chat's " +
			"minItems:1). Message shape is whatever llm_chat uses ({role, content, ...}); objects pass through " +
			"unchanged. Settings.MaxMessages windows each conversation to the most recent N messages (default 50; " +
			"0 = unlimited, unbounded growth). clear deletes a conversation.",
		Tags: []string{"Store", "Memory", "Conversation", "Chat", "LLM", "bbolt"},
	}
}

// OnSettings validates the config and acquires the shared bbolt handle
// for the configured path (releasing the old one if the path changed).
func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if err := validateSettings(in); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.settings = in

	if c.db == nil || c.dbPath != in.Path {
		db, err := stores.acquire(in.Path)
		if err != nil {
			return err
		}
		if c.db != nil {
			stores.release(c.dbPath)
		}
		c.db = db
		c.dbPath = in.Path
	}
	// Ensure the conversations bucket exists. Idempotent.
	return c.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
}

// OnDestroy releases this node's reference on the shared handle. The
// file closes only when the last conversation node using the path is
// destroyed.
func (c *Component) OnDestroy(_ map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db == nil {
		return
	}
	stores.release(c.dbPath)
	c.db = nil
	c.dbPath = ""
}

func validateSettings(s Settings) error {
	if strings.TrimSpace(s.Path) == "" {
		return fmt.Errorf("path required")
	}
	if !filepath.IsAbs(s.Path) {
		return fmt.Errorf("path must be absolute: %q", s.Path)
	}
	if s.MaxMessages < 0 {
		return fmt.Errorf("maxMessages must be >= 0 (0 = unlimited)")
	}
	return nil
}

// Handle dispatches per-port. Each operation runs in a fresh bbolt
// transaction on the shared handle.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	c.mu.RLock()
	db := c.db
	settings := c.settings
	c.mu.RUnlock()

	if db == nil {
		// Transient by nature: settings haven't been delivered yet, or
		// the file lock is still clearing after a pod restart. Marked
		// retryable so an edge retry / retry component clears it —
		// follows document_store's failRetryable classification.
		return c.failRetryable(ctx, handler, contextFromMsg(msg),
			fmt.Errorf("conversation store not initialised — settings not delivered yet (or file lock still clearing); retry shortly"))
	}

	switch port {
	case AppendPort:
		req, ok := msg.(AppendRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid append request"))
		}
		return c.handleAppend(ctx, handler, db, settings, req)
	case GetPort:
		req, ok := msg.(GetRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid get request"))
		}
		return c.handleGet(ctx, handler, db, req)
	case ClearPort:
		req, ok := msg.(ClearRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid clear request"))
		}
		return c.handleClear(ctx, handler, db, req)
	default:
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
}

// handleAppend is THE primitive: load, append, trim to window, save —
// all inside a single bbolt Update transaction. bbolt serializes Update
// transactions on one writer lock, so concurrent appends to the same
// conversation (from any node sharing the path) interleave whole-append
// at a time; no turn can overwrite another's.
func (c *Component) handleAppend(ctx context.Context, handler module.Handler, db *bbolt.DB, settings Settings, req AppendRequest) module.Result {
	if strings.TrimSpace(req.ConversationID) == "" {
		return c.fail(ctx, handler, req.Context, fmt.Errorf("conversationId required"))
	}
	if len(req.Messages) == 0 {
		return c.fail(ctx, handler, req.Context, fmt.Errorf("messages required — append at least one message"))
	}

	var window []any
	err := db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("bucket %q missing — store not initialised", bucketName)
		}
		key := []byte(req.ConversationID)
		msgs, err := decodeMessages(bucket.Get(key), req.ConversationID)
		if err != nil {
			return err
		}
		msgs = append(msgs, req.Messages...)
		msgs = trimWindow(msgs, settings.MaxMessages)
		payload, err := json.Marshal(msgs)
		if err != nil {
			return fmt.Errorf("marshal conversation %q: %w", req.ConversationID, err)
		}
		if err := bucket.Put(key, payload); err != nil {
			return err
		}
		window = msgs
		return nil
	})
	if err != nil {
		return c.fail(ctx, handler, req.Context, err)
	}
	return handler(ctx, AppendResultPort, AppendResult{
		Context:        req.Context,
		ConversationID: req.ConversationID,
		Messages:       window,
	})
}

func (c *Component) handleGet(ctx context.Context, handler module.Handler, db *bbolt.DB, req GetRequest) module.Result {
	if strings.TrimSpace(req.ConversationID) == "" {
		return c.fail(ctx, handler, req.Context, fmt.Errorf("conversationId required"))
	}
	var msgs []any
	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		var raw []byte
		if bucket != nil {
			raw = bucket.Get([]byte(req.ConversationID))
		}
		var err error
		msgs, err = decodeMessages(raw, req.ConversationID)
		return err
	})
	if err != nil {
		return c.fail(ctx, handler, req.Context, err)
	}
	// Unknown conversation → empty array, deliberately NOT an error:
	// a cold conversation starts by appending the user turn, and that
	// append's response is what feeds llm_chat (minItems:1 satisfied).
	return handler(ctx, GetResultPort, GetResult{
		Context:        req.Context,
		ConversationID: req.ConversationID,
		Messages:       msgs,
	})
}

func (c *Component) handleClear(ctx context.Context, handler module.Handler, db *bbolt.DB, req ClearRequest) module.Result {
	if strings.TrimSpace(req.ConversationID) == "" {
		return c.fail(ctx, handler, req.Context, fmt.Errorf("conversationId required"))
	}
	var existed bool
	err := db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return nil
		}
		key := []byte(req.ConversationID)
		existed = bucket.Get(key) != nil
		return bucket.Delete(key)
	})
	if err != nil {
		return c.fail(ctx, handler, req.Context, err)
	}
	return handler(ctx, ClearResultPort, ClearResult{
		Context:        req.Context,
		ConversationID: req.ConversationID,
		Cleared:        existed,
	})
}

// decodeMessages turns a stored record into a message slice. A missing
// record is an empty (non-nil) slice so results serialize as [] rather
// than null; a corrupt record is a hard error — silently resetting a
// conversation would hide data loss.
func decodeMessages(raw []byte, conversationID string) ([]any, error) {
	if raw == nil {
		return []any{}, nil
	}
	var msgs []any
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return nil, fmt.Errorf("conversation %q: corrupt record: %w (clear it to start over)", conversationID, err)
	}
	return msgs, nil
}

// trimWindow keeps the most recent max messages. max <= 0 means
// unlimited.
func trimWindow(msgs []any, max int) []any {
	if max > 0 && len(msgs) > max {
		return msgs[len(msgs)-max:]
	}
	return msgs
}

// fail routes an error to the error port when enabled, else fails the
// request. The canonical module.NewError payload derives its Retryable
// field from the error itself, so retryability marked at the failure
// site survives to the port payload.
func (c *Component) fail(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	c.mu.RLock()
	enabled := c.settings.EnableErrorPort
	c.mu.RUnlock()
	if !enabled {
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, module.NewError(reqCtx, err))
}

// failRetryable is the retryable counterpart to fail, for transient
// storage issues (store still opening after restart). As in
// document_store's fixed helper: mark the ERROR itself via
// module.Retryable — module.ShouldRetry only sees what the error
// carries, and an unmarked bubble silently turns "back off and re-fire"
// into a dead stop when the error port is off.
func (c *Component) failRetryable(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	return c.fail(ctx, handler, reqCtx, module.Retryable(err))
}

// contextFromMsg extracts the Context field from any request shape so
// pre-dispatch error paths can pass it through.
func contextFromMsg(msg any) Context {
	switch req := msg.(type) {
	case AppendRequest:
		return req.Context
	case GetRequest:
		return req.Context
	case ClearRequest:
		return req.Context
	}
	return nil
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: AppendPort, Label: "Append", Configuration: AppendRequest{}, Position: module.Left},
		{Name: GetPort, Label: "Get", Configuration: GetRequest{}, Position: module.Left},
		{Name: ClearPort, Label: "Clear", Configuration: ClearRequest{}, Position: module.Left},
		{Name: AppendResultPort, Label: "Append OK", Source: true, Configuration: AppendResult{}, Position: module.Right},
		{Name: GetResultPort, Label: "Get OK", Source: true, Configuration: GetResult{}, Position: module.Right},
		{Name: ClearResultPort, Label: "Clear OK", Source: true, Configuration: ClearResult{}, Position: module.Right},
	}
	if c.settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name: ErrorPort, Label: "Error", Source: true, Configuration: module.ErrorMessage{}, Position: module.Bottom,
		})
	}
	return ports
}

// Static assertions to surface drift between Component and the SDK
// interfaces at build time.
var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.Destroyer       = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
