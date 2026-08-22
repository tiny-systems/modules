// Package chat is the flow's human touchpoint: one conversation surface that
// replaces the form-era pair of `prompt` (human asks flow) and `ask` (flow
// asks human). The widget renders a message thread, not a form:
//
//   - a human types in the composer → a message bubble, and the flow receives
//     it on the `message` port with a request id
//   - the flow speaks via `say` → a markdown bubble; when its request id
//     matches an outstanding human message, that message stops "working"
//   - the flow asks via `ask` → a question card inline in the thread, with the
//     form's buttons/fields; the human's submission emits on `answer`
//   - a question nobody answers in time expires onto `error` and leaves a
//     note in the thread
//
// This restores the conversation contract that fire-and-forget dropped —
// correlation, working state, completion, expiry — explicitly, on top of the
// async transport. Nothing blocks: continuity lives in this node's persisted
// state, and every hop is its own message.
//
// The thread is a DISPLAY buffer (capped at historyLimit), not conversation
// memory — flows that need memory keep it in a store component.
package chat

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/state"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "chat"

	SayPort     = "say"
	AskPort     = "ask"
	ClearPort   = "clear"
	MessagePort = "message"
	AnswerPort  = "answer"
	ErrorPort   = "error"

	// State keys. The thread's order and the queue's order are themselves
	// state, so each is a slice under one key (the ask/kv pattern).
	threadStateKey = "thread"
	queueStateKey  = "queue"

	// Submission tagging: the widget's control submission says what it is.
	kindField = "_kind"
	qidField  = "_qid"
	textField = "text"

	kindMessage = "message"
	kindAnswer  = "answer"

	defaultHistoryLimit = 50
)

// defaultForm is the question form used when neither the ask request nor the
// settings author one: the overwhelmingly common Approve/Deny.
const defaultForm = `{
  "type": "object",
  "properties": {
    "approve": {"type": "boolean", "title": "Approve", "format": "button", "propertyOrder": 1},
    "deny":    {"type": "boolean", "title": "Deny",    "format": "button", "propertyOrder": 2}
  }
}`

// Context is the passthrough payload, unchanged through the human's decision.
type Context any

// Say is flow → human content.
type Say struct {
	RequestID string  `json:"requestId,omitempty" title:"Request ID" description:"Id from a message emitted on the Message port; carrying it back marks that message answered. Empty or unknown ids are shown as unsolicited messages, not dropped."`
	Role      string  `json:"role,omitempty" title:"Role" enum:"assistant,system,error" description:"How the bubble renders: assistant (default), system note, or error note."`
	Text      string  `json:"text" title:"Text" description:"Markdown is rendered."`
	Context   Context `json:"context,omitempty" configurable:"true" title:"Context"`
}

// AskRequest is flow → human structured question.
type AskRequest struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Payload under review — shown on the question card and passed through to the answer."`
	Form    string  `json:"form,omitempty" title:"Form" format:"code" language:"json" description:"JSON Schema of the question form, overriding the Settings form for this question only. Fields with format:\"button\" are the answers."`
}

// ClearRequest starts the conversation over: the arrival is the whole
// instruction, so the payload carries nothing but passthrough context.
type ClearRequest struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Passthrough payload, carried for correlation only — anything arriving here wipes the displayed conversation and any pending questions. Conversation MEMORY lives in a store component and must be cleared there separately."`
}

// Message is a human's free composer input.
type Message struct {
	RequestID string  `json:"requestId" title:"Request ID" description:"Carry this back on Say so the thread marks the message answered."`
	Text      string  `json:"text" title:"Text"`
	Context   Context `json:"context,omitempty" title:"Context"`
}

// Answer is the human's reply to an ask question.
type Answer struct {
	QuestionID string                 `json:"questionId" title:"Question ID"`
	Values     map[string]interface{} `json:"values" title:"Values" description:"What the human submitted, keyed by the form's field names."`
	Context    Context                `json:"context,omitempty" title:"Context" description:"The ask request payload, unchanged."`
}

// ErrorMessage reports a question nobody answered in time.
type ErrorMessage struct {
	Context Context `json:"context,omitempty" title:"Context" description:"The ask request payload of the expired question, unchanged."`
	Error   string  `json:"error" title:"Error"`
}

type Settings struct {
	Form string `json:"form,omitempty" title:"Question Form" format:"code" language:"json" description:"Default JSON Schema for ask questions (a request's own form overrides it). Leave empty for Approve/Deny."`

	TimeoutSeconds int `json:"timeoutSeconds" title:"Question Timeout Seconds" default:"0" description:"How long a question may stay unanswered before it expires onto the Error port (0 = wait forever). Expiry is passive — checked when messages arrive and on reconcile ticks."`

	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"Emit timed-out questions on the Error port as {context, error}. Without it expired questions are dropped with a log line — enable this when a timeout is set."`

	HistoryLimit int `json:"historyLimit" title:"History Limit" default:"50" description:"Messages kept in the widget thread. Display only — conversation memory belongs in a store component."`

	Placeholder string `json:"placeholder,omitempty" title:"Composer Placeholder" description:"Hint text in the input box, e.g. 'Ask the agent…'."`

	HideComposer bool `json:"hideComposer" title:"Hide Composer" description:"Form mode: the widget shows only question cards and notes — no free-text input. For settings-style surfaces where the flow drives every question (keep the card armed by re-asking after each answer)."`
}

// threadEntry is one bubble of the display buffer.
type threadEntry struct {
	ID      string                 `json:"id"`
	Kind    string                 `json:"kind"` // message | reply | question | answer | note
	Role    string                 `json:"role,omitempty"`
	Text    string                 `json:"text,omitempty"`
	Values  map[string]interface{} `json:"values,omitempty"`
	QID     string                 `json:"qid,omitempty"`
	At      time.Time              `json:"at"`
	Pending bool                   `json:"pending,omitempty"` // human message still awaiting its reply
}

// pendingQuestion is one entry of the durable FIFO. The form is snapshotted at
// ask time so neither a Settings change nor a later request can swap the
// question under the human answering it.
type pendingQuestion struct {
	ID      string          `json:"id"`
	Context Context         `json:"context"`
	Form    json.RawMessage `json:"form"`
	AskedAt time.Time       `json:"askedAt"`
}

// Component is the conversation. Thread and queue live in the node's State
// (TinyNode status metadata), so they survive pod restarts and are visible to
// every replica.
type Component struct {
	module.Base

	mu       sync.Mutex
	settings Settings

	// stateMu serializes read-modify-write of thread+queue within this replica.
	stateMu sync.Mutex

	// rehydrated: republish the widget exactly once after a pod restart; after
	// that only actual changes re-render (no pulling the form out from under a
	// typing human).
	rehydrated bool

	// now is the clock seam for tests.
	now func() time.Time
}

func (c *Component) Instance() module.Component {
	return &Component{settings: Settings{HistoryLimit: defaultHistoryLimit}, now: time.Now}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Chat with a human",
		Info: "The flow's human touchpoint: one conversation widget on the dashboard. " +
			"A human types → Message port (carry requestId back on Say to mark it answered). " +
			"The flow speaks → Say port (markdown bubble; role system/error renders as a note). " +
			"The flow asks → Ask port: a question card with the form's buttons appears in the thread and the submission emits on Answer as {questionId, values, context} — wire Answer to the gated action. " +
			"Questions queue FIFO, persist across restarts, and expire onto Error after timeoutSeconds. " +
			"The flow starts over → Clear port: the thread and any pending questions are wiped (store memory clears separately). " +
			"The thread is display only (historyLimit); keep real conversation memory in a store component. " +
			"Replaces prompt and ask.",
		Tags: []string{"SDK", "dashboard", "Human"},
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if f := in.Form; f != "" && !json.Valid([]byte(f)) {
		return fmt.Errorf("form is not valid JSON")
	}
	if in.TimeoutSeconds < 0 {
		in.TimeoutSeconds = 0
	}
	if in.HistoryLimit <= 0 {
		in.HistoryLimit = defaultHistoryLimit
	}
	c.mu.Lock()
	c.settings = in
	c.mu.Unlock()
	return nil
}

func (c *Component) form() json.RawMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settings.Form != "" {
		return json.RawMessage(c.settings.Form)
	}
	return json.RawMessage(defaultForm)
}

func (c *Component) timeout() (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Duration(c.settings.TimeoutSeconds) * time.Second, c.settings.EnableErrorPort
}

func (c *Component) limit() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.settings.HistoryLimit <= 0 {
		return defaultHistoryLimit
	}
	return c.settings.HistoryLimit
}

func (c *Component) placeholder() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.settings.Placeholder
}

func (c *Component) clock() time.Time {
	if c.now == nil {
		return time.Now()
	}
	return c.now()
}

func newID(prefix string, now time.Time) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%d-%x", prefix, now.UnixNano(), b)
}

// ---- persistence -----------------------------------------------------------

func loadSlice[T any](ctx context.Context, st module.State, key string) []T {
	if st == nil {
		return nil
	}
	raw, found, err := st.Get(ctx, key)
	if err != nil || !found {
		return nil
	}
	var out []T
	if json.Unmarshal(raw, &out) != nil {
		// Unreadable state can never be used; start over rather than wedge.
		return nil
	}
	return out
}

func (c *Component) saveSlice(ctx context.Context, key string, v any, empty bool) error {
	st := c.State()
	if st == nil {
		return fmt.Errorf("state backend not available")
	}
	if empty {
		return st.Delete(ctx, key)
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := st.Set(ctx, key, data); err != nil {
		if errors.Is(err, state.ErrStateTooLarge) {
			return fmt.Errorf("state budget exhausted (~%dKB per node): lower historyLimit or carry smaller context (IDs, not blobs): %v",
				state.MaxStateBytes/1024, err)
		}
		return err
	}
	return nil
}

func (c *Component) loadThread(ctx context.Context) []threadEntry {
	return loadSlice[threadEntry](ctx, c.State(), threadStateKey)
}

func (c *Component) saveThread(ctx context.Context, t []threadEntry) error {
	return c.saveSlice(ctx, threadStateKey, t, len(t) == 0)
}

func (c *Component) loadQueue(ctx context.Context) []pendingQuestion {
	return loadSlice[pendingQuestion](ctx, c.State(), queueStateKey)
}

func (c *Component) saveQueue(ctx context.Context, q []pendingQuestion) error {
	return c.saveSlice(ctx, queueStateKey, q, len(q) == 0)
}

// appendCapped appends and trims the front to the history limit.
func appendCapped(t []threadEntry, limit int, entries ...threadEntry) []threadEntry {
	t = append(t, entries...)
	if len(t) > limit {
		t = t[len(t)-limit:]
	}
	return t
}

// ---- expiry ----------------------------------------------------------------

// sweepExpired splits the queue into kept and expired. Pure; callers persist
// and emit. Zero timeout = wait forever.
func sweepExpired(q []pendingQuestion, now time.Time, timeout time.Duration) (kept []pendingQuestion, expired []ErrorMessage) {
	if timeout <= 0 {
		return q, nil
	}
	for _, p := range q {
		if now.Sub(p.AskedAt) > timeout {
			expired = append(expired, ErrorMessage{
				Context: p.Context,
				Error:   fmt.Sprintf("question expired after %ds", int(timeout/time.Second)),
			})
			continue
		}
		kept = append(kept, p)
	}
	return kept, expired
}

func (c *Component) emitExpired(ctx context.Context, expired []ErrorMessage, errPort bool) {
	for _, e := range expired {
		if errPort {
			c.Emit(ctx, ErrorPort, e)
		} else {
			log.Warn().Str("component", ComponentName).
				Msg(e.Error + " (error port disabled; question dropped)")
		}
	}
}

// expiredNotes turns expiries into thread notes so the human sees the question
// vanish for a reason rather than silently.
func expiredNotes(expired []ErrorMessage, now time.Time) []threadEntry {
	notes := make([]threadEntry, 0, len(expired))
	for _, e := range expired {
		notes = append(notes, threadEntry{
			ID: newID("n", now), Kind: "note", Role: "error",
			Text: e.Error, At: now,
		})
	}
	return notes
}

// ---- flow-facing handlers --------------------------------------------------

// Handle receives Say, Ask and Clear. NOT leader-gated: flow traffic is routed to
// whichever replica the runtime picks, and State converges through the shared
// backend. (Leader-gating an answer path silently dropped every answer once —
// see prompt's history. OnControl and OnReconcile DO gate.)
func (c *Component) Handle(ctx context.Context, _ module.Handler, port string, msg any) module.Result {
	switch port {
	case SayPort:
		in, ok := msg.(Say)
		if !ok {
			return module.Fail(fmt.Errorf("invalid message on Say"))
		}
		return c.handleSay(ctx, in)
	case AskPort:
		in, ok := msg.(AskRequest)
		if !ok {
			return module.Fail(fmt.Errorf("invalid message on Ask"))
		}
		return c.handleAsk(ctx, in)
	case ClearPort:
		if _, ok := msg.(ClearRequest); !ok {
			return module.Fail(fmt.Errorf("invalid message on Clear"))
		}
		return c.handleClear(ctx)
	default:
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
}

func (c *Component) handleSay(ctx context.Context, in Say) module.Result {
	if c.State() == nil {
		return module.Fail(fmt.Errorf("state backend not available"))
	}
	role := in.Role
	if role == "" {
		role = "assistant"
	}
	now := c.clock()

	c.stateMu.Lock()
	t := c.loadThread(ctx)
	// A matching request id closes the "working…" state of the message it
	// answers. Unknown or absent ids still land in the thread: a conversation
	// may receive pushes.
	if in.RequestID != "" {
		for i := range t {
			if t[i].Kind == kindMessage && t[i].ID == in.RequestID {
				t[i].Pending = false
			}
		}
	}
	t = appendCapped(t, c.limit(), threadEntry{
		ID: newID("r", now), Kind: "reply", Role: role, Text: in.Text, At: now,
	})
	if err := c.saveThread(ctx, t); err != nil {
		c.stateMu.Unlock()
		return module.Fail(err)
	}
	c.stateMu.Unlock()

	return c.Emit(ctx, v1alpha1.ControlPort, c.control())
}

func (c *Component) handleAsk(ctx context.Context, in AskRequest) module.Result {
	if c.State() == nil {
		return module.Fail(fmt.Errorf("state backend not available"))
	}
	if in.Form != "" && !json.Valid([]byte(in.Form)) {
		return module.Fail(fmt.Errorf("ask form is not valid JSON"))
	}
	timeout, errPort := c.timeout()
	now := c.clock()

	form := json.RawMessage(in.Form)
	if len(form) == 0 {
		form = c.form()
	}

	c.stateMu.Lock()
	q := c.loadQueue(ctx)
	kept, expired := sweepExpired(q, now, timeout)
	question := pendingQuestion{ID: newID("q", now), Context: in.Context, Form: form, AskedAt: now}
	kept = append(kept, question)
	if err := c.saveQueue(ctx, kept); err != nil {
		c.stateMu.Unlock()
		return module.Fail(err)
	}
	t := c.loadThread(ctx)
	entries := expiredNotes(expired, now)
	entries = append(entries, threadEntry{
		ID: newID("t", now), Kind: "question", QID: question.ID, At: now, Pending: true,
	})
	t = appendCapped(t, c.limit(), entries...)
	if err := c.saveThread(ctx, t); err != nil {
		c.stateMu.Unlock()
		return module.Fail(err)
	}
	c.stateMu.Unlock()

	c.emitExpired(ctx, expired, errPort)
	return c.Emit(ctx, v1alpha1.ControlPort, c.control())
}

// handleClear empties the display buffer and drops every unanswered question.
// Outstanding questions are dropped, not expired: the conversation they belonged
// to no longer exists, so there is nothing for an Error emission to correlate
// with. Deleting both keys is idempotent, so clearing an empty chat is a no-op
// that still republishes — the widget may be showing a stale render.
func (c *Component) handleClear(ctx context.Context) module.Result {
	if c.State() == nil {
		return module.Fail(fmt.Errorf("state backend not available"))
	}

	c.stateMu.Lock()
	if err := c.saveThread(ctx, nil); err != nil {
		c.stateMu.Unlock()
		return module.Fail(err)
	}
	if err := c.saveQueue(ctx, nil); err != nil {
		c.stateMu.Unlock()
		return module.Fail(err)
	}
	c.stateMu.Unlock()

	return c.Emit(ctx, v1alpha1.ControlPort, c.control())
}

// ---- widget-facing handler -------------------------------------------------

// OnControl receives the widget's submission: {_kind: "message", text} from
// the composer, or {_kind: "answer", _qid, ...values} from a question card.
func (c *Component) OnControl(ctx context.Context, msg any) error {
	if !utils.IsLeader(ctx) {
		return nil
	}
	values, ok := msg.(map[string]interface{})
	if !ok {
		return fmt.Errorf("invalid control msg: expected map, got %T", msg)
	}
	kind, _ := values[kindField].(string)
	switch kind {
	case kindMessage:
		return c.controlMessage(ctx, values)
	case kindAnswer:
		return c.controlAnswer(ctx, values)
	default:
		// A re-render or replayed submission, not an action.
		return nil
	}
}

func (c *Component) controlMessage(ctx context.Context, values map[string]interface{}) error {
	text, _ := values[textField].(string)
	if text == "" {
		return nil
	}
	now := c.clock()
	rid := newID("m", now)

	c.stateMu.Lock()
	t := c.loadThread(ctx)
	t = appendCapped(t, c.limit(), threadEntry{
		ID: rid, Kind: kindMessage, Text: text, At: now, Pending: true,
	})
	if err := c.saveThread(ctx, t); err != nil {
		c.stateMu.Unlock()
		return err
	}
	c.stateMu.Unlock()

	// Detached, like signal/ask: the control message must not stay open for
	// the length of the downstream flow.
	go c.Emit(context.Background(), MessagePort, Message{RequestID: rid, Text: text})
	c.Emit(context.Background(), v1alpha1.ControlPort, c.control())
	return nil
}

func (c *Component) controlAnswer(ctx context.Context, values map[string]interface{}) error {
	timeout, errPort := c.timeout()
	now := c.clock()

	c.stateMu.Lock()
	q := c.loadQueue(ctx)
	if len(q) == 0 {
		c.stateMu.Unlock()
		return nil
	}
	// An answer must match the head of the queue: the card carries the head's
	// id, so a replayed card for an answered question cannot consume the NEXT
	// one. Beating the deadline wins over expiry — pop before sweeping.
	var answered *pendingQuestion
	if hasPressedButton(values) && submissionMatches(values, q[0].ID) {
		head := q[0]
		answered = &head
		q = q[1:]
	}
	kept, expired := sweepExpired(q, now, timeout)
	if answered != nil || len(expired) > 0 {
		if err := c.saveQueue(ctx, kept); err != nil {
			c.stateMu.Unlock()
			return err
		}
	}
	var t []threadEntry
	if answered != nil || len(expired) > 0 {
		t = c.loadThread(ctx)
		entries := expiredNotes(expired, now)
		if answered != nil {
			// The question card stops pending; the human's decision joins the
			// thread as its own entry — with write-only fields masked, since
			// the thread persists in state and renders in the widget. The raw
			// values travel only on the Answer port below.
			for i := range t {
				if t[i].Kind == "question" && t[i].QID == answered.ID {
					t[i].Pending = false
				}
			}
			entries = append(entries, threadEntry{
				ID: newID("a", now), Kind: kindAnswer, QID: answered.ID,
				Values: maskSecrets(cleanValues(values), secretFields(answered.Form)), At: now,
			})
		}
		t = appendCapped(t, c.limit(), entries...)
		if err := c.saveThread(ctx, t); err != nil {
			c.stateMu.Unlock()
			return err
		}
	}
	c.stateMu.Unlock()

	c.emitExpired(ctx, expired, errPort)
	if answered == nil && len(expired) == 0 {
		return nil
	}
	if answered != nil {
		go c.Emit(context.Background(), AnswerPort, Answer{
			QuestionID: answered.ID,
			Values:     cleanValues(values),
			Context:    answered.Context,
		})
	}
	c.Emit(context.Background(), v1alpha1.ControlPort, c.control())
	return nil
}

// cleanValues strips the control machinery from a submission, leaving the
// answer fields.
func cleanValues(values map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(values))
	for k, v := range values {
		if k == kindField || k == qidField || k == textField {
			continue
		}
		out[k] = v
	}
	return out
}

// secretMask is what a write-only submission value becomes everywhere the
// conversation is persisted or displayed. The raw value exists only in the
// one Answer emission the flow consumes.
const secretMask = "••••••"

// secretFields reads the question form and returns the names of fields whose
// submitted values must never persist: standard JSON Schema `writeOnly`,
// `format: "password"`, or the editor's legacy `secret` keyword.
func secretFields(form json.RawMessage) map[string]bool {
	var parsed struct {
		Properties map[string]struct {
			WriteOnly bool   `json:"writeOnly"`
			Format    string `json:"format"`
			Secret    bool   `json:"secret"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(form, &parsed); err != nil {
		return nil
	}
	out := map[string]bool{}
	for name, p := range parsed.Properties {
		if p.WriteOnly || p.Secret || p.Format == "password" {
			out[name] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// maskSecrets returns a copy of values with every secret field's non-empty
// value replaced by secretMask. The thread is a display buffer that persists
// in node state and re-renders in the widget — a credential must not survive
// there ([[never in transcript]] is a persistence rule).
func maskSecrets(values map[string]interface{}, secrets map[string]bool) map[string]interface{} {
	if len(secrets) == 0 {
		return values
	}
	out := make(map[string]interface{}, len(values))
	for k, v := range values {
		if secrets[k] && v != nil && v != "" {
			out[k] = secretMask
			continue
		}
		out[k] = v
	}
	return out
}

func hasPressedButton(values map[string]interface{}) bool {
	for _, v := range values {
		if b, ok := v.(bool); ok && b {
			return true
		}
	}
	return false
}

func submissionMatches(values map[string]interface{}, id string) bool {
	v, ok := values[qidField]
	if !ok {
		return true
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return true
	}
	return s == id
}

// ---- reconcile -------------------------------------------------------------

// OnReconcile rehydrates the widget once per pod lifetime and is the idle
// expiry tick. Republish only on actual change — never re-render a thread a
// human is mid-interaction with.
func (c *Component) OnReconcile(ctx context.Context, _ v1alpha1.TinyNode) error {
	if !utils.IsLeader(ctx) {
		return nil
	}
	if c.State() == nil {
		return nil
	}
	timeout, errPort := c.timeout()
	now := c.clock()

	c.stateMu.Lock()
	q := c.loadQueue(ctx)
	kept, expired := sweepExpired(q, now, timeout)
	if len(expired) > 0 {
		if err := c.saveQueue(ctx, kept); err != nil {
			c.stateMu.Unlock()
			return err
		}
		t := c.loadThread(ctx)
		t = appendCapped(t, c.limit(), expiredNotes(expired, now)...)
		if err := c.saveThread(ctx, t); err != nil {
			c.stateMu.Unlock()
			return err
		}
	}
	republish := len(expired) > 0 || !c.rehydrated
	c.rehydrated = true
	c.stateMu.Unlock()

	c.emitExpired(ctx, expired, errPort)
	if republish {
		c.Emit(ctx, v1alpha1.ControlPort, c.control())
	}
	return nil
}

// ---- widget ----------------------------------------------------------------

// control is the widget's data: the whole conversation. The schema half is
// static (format "chat"); everything dynamic — thread, pending question card,
// its form — rides in the data, so the renderer needs no schema round-trips.
func (c *Component) control() map[string]interface{} {
	ctx := context.Background()

	c.stateMu.Lock()
	t := c.loadThread(ctx)
	q := c.loadQueue(ctx)
	c.stateMu.Unlock()

	var pending map[string]interface{}
	if len(q) > 0 {
		var form interface{}
		_ = json.Unmarshal(q[0].Form, &form)
		pending = map[string]interface{}{
			"qid":     q[0].ID,
			"form":    form,
			"context": q[0].Context,
			"waiting": len(q) - 1,
		}
	}
	out := map[string]interface{}{
		"thread":      t,
		"placeholder": c.placeholder(),
	}
	if c.settings.HideComposer {
		out["hideComposer"] = true
	}
	if pending != nil {
		out["pendingQuestion"] = pending
	}
	return out
}

// controlSchema marks the widget for the chat renderer. The editor dispatches
// on the root format; hosts without a chat renderer fall back to a form, so
// the submission fields are declared for them.
func controlSchema() json.RawMessage {
	return json.RawMessage(`{
  "type": "object",
  "format": "chat",
  "properties": {
    "_kind": {"type": "string", "title": "Kind", "readonly": true},
    "text":  {"type": "string", "title": "Message", "format": "textarea"},
    "_qid":  {"type": "string", "title": "Question ID", "readonly": true}
  }
}`)
}

// sampleValues advertises the form's fields on the Answer port so a downstream
// edge reading `$.values.<field>` validates at build time.
func sampleValues(form json.RawMessage) map[string]interface{} {
	out := map[string]interface{}{}
	var parsed struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(form, &parsed); err != nil {
		return out
	}
	for name, prop := range parsed.Properties {
		switch prop.Type {
		case "boolean":
			out[name] = false
		case "string":
			out[name] = ""
		case "number", "integer":
			out[name] = 0
		case "array":
			out[name] = []interface{}{}
		default:
			out[name] = map[string]interface{}{}
		}
	}
	return out
}

func (c *Component) Ports() []module.Port {
	c.mu.Lock()
	settings := c.settings
	c.mu.Unlock()

	ports := []module.Port{
		{Name: v1alpha1.ReconcilePort},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: settings,
		},
		{
			Name:          SayPort,
			Label:         "Say",
			Position:      module.Left,
			Configuration: Say{},
		},
		{
			Name:          AskPort,
			Label:         "Ask",
			Position:      module.Left,
			Configuration: AskRequest{},
		},
		{
			Name:          ClearPort,
			Label:         "Clear",
			Position:      module.Left,
			Configuration: ClearRequest{},
		},
		{
			Name:          MessagePort,
			Label:         "Message",
			Source:        true,
			Position:      module.Right,
			Configuration: Message{},
		},
		{
			Name:          AnswerPort,
			Label:         "Answer",
			Source:        true,
			Position:      module.Right,
			Configuration: Answer{Values: sampleValues(c.form())},
		},
		{
			Name:     v1alpha1.ControlPort,
			Label:    "Control",
			Source:   true,
			Position: module.Top,
			// Data map so an untyped submission has something to decode into;
			// both halves derive from persisted state, so a restarted pod
			// advertises the live conversation with no in-memory handoff.
			Configuration: c.control(),
			Schema:        controlSchema(),
		},
	}

	if settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Position:      module.Bottom,
			Configuration: ErrorMessage{},
		})
	}
	return ports
}

var (
	_ module.Component        = (*Component)(nil)
	_ module.SettingsHandler  = (*Component)(nil)
	_ module.ControlHandler   = (*Component)(nil)
	_ module.ReconcileHandler = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
