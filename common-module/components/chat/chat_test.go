package chat

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/tiny-systems/modules/common-module/internal/testharness"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/pkg/utils"
)

func leaderCtx() context.Context {
	return utils.WithLeader(context.Background(), true)
}

func newChat(t *testing.T, settings *Settings) (*testharness.Harness, *Component) {
	t.Helper()
	comp, ok := (&Component{}).Instance().(*Component)
	if !ok {
		t.Fatal("Instance() did not return *Component")
	}
	h := testharness.New(comp)
	if settings != nil {
		if r := h.Handle(context.Background(), v1alpha1.SettingsPort, *settings); r.Err() != nil {
			t.Fatalf("settings failed: %v", r.Err())
		}
	}
	return h, comp
}

func waitOutputs(t *testing.T, h *testharness.Harness, port string, n int) []any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		outs := h.PortOutputs(port)
		if len(outs) >= n {
			return outs
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d outputs on %q, have %d", n, port, len(outs))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func settleOutputs(h *testharness.Harness, port string) []any {
	time.Sleep(50 * time.Millisecond)
	return h.PortOutputs(port)
}

func thread(c *Component) []threadEntry {
	return c.loadThread(context.Background())
}

// ---- message (human → flow) ------------------------------------------------

func TestComposerMessageEmitsAndPends(t *testing.T) {
	h, c := newChat(t, nil)

	r := h.Handle(leaderCtx(), v1alpha1.ControlPort, map[string]interface{}{
		kindField: kindMessage, textField: "hello agent",
	})
	if r.Err() != nil {
		t.Fatalf("control failed: %v", r.Err())
	}

	outs := waitOutputs(t, h, MessagePort, 1)
	msg, ok := outs[0].(Message)
	if !ok {
		t.Fatalf("expected Message, got %T", outs[0])
	}
	if msg.Text != "hello agent" || msg.RequestID == "" {
		t.Fatalf("bad message: %+v", msg)
	}

	th := thread(c)
	if len(th) != 1 || th[0].Kind != kindMessage || !th[0].Pending || th[0].ID != msg.RequestID {
		t.Fatalf("bad thread: %+v", th)
	}
}

func TestSayAnswersPendingMessage(t *testing.T) {
	h, c := newChat(t, nil)
	h.Handle(leaderCtx(), v1alpha1.ControlPort, map[string]interface{}{
		kindField: kindMessage, textField: "question",
	})
	msg := waitOutputs(t, h, MessagePort, 1)[0].(Message)

	if r := h.Handle(context.Background(), SayPort, Say{RequestID: msg.RequestID, Text: "the answer"}); r.Err() != nil {
		t.Fatalf("say failed: %v", r.Err())
	}

	th := thread(c)
	if len(th) != 2 {
		t.Fatalf("expected 2 entries, got %+v", th)
	}
	if th[0].Pending {
		t.Fatal("message still pending after matching say")
	}
	if th[1].Kind != "reply" || th[1].Role != "assistant" || th[1].Text != "the answer" {
		t.Fatalf("bad reply entry: %+v", th[1])
	}
}

func TestSayUnsolicitedStillLands(t *testing.T) {
	h, c := newChat(t, nil)
	if r := h.Handle(context.Background(), SayPort, Say{RequestID: "nope", Role: "error", Text: "boom"}); r.Err() != nil {
		t.Fatalf("say failed: %v", r.Err())
	}
	th := thread(c)
	if len(th) != 1 || th[0].Role != "error" || th[0].Text != "boom" {
		t.Fatalf("bad thread: %+v", th)
	}
}

func TestEmptyComposerIgnored(t *testing.T) {
	h, c := newChat(t, nil)
	h.Handle(leaderCtx(), v1alpha1.ControlPort, map[string]interface{}{
		kindField: kindMessage, textField: "",
	})
	if outs := settleOutputs(h, MessagePort); len(outs) != 0 {
		t.Fatalf("empty text emitted: %+v", outs)
	}
	if th := thread(c); len(th) != 0 {
		t.Fatalf("empty text landed in thread: %+v", th)
	}
}

// ---- ask (flow → human) ----------------------------------------------------

func askQ(t *testing.T, h *testharness.Harness, ctx any) {
	t.Helper()
	if r := h.Handle(context.Background(), AskPort, AskRequest{Context: ctx}); r.Err() != nil {
		t.Fatalf("ask failed: %v", r.Err())
	}
}

func answerHead(t *testing.T, h *testharness.Harness, c *Component, values map[string]interface{}) {
	t.Helper()
	data := c.control()
	pending, _ := data["pendingQuestion"].(map[string]interface{})
	if pending == nil {
		t.Fatal("no pending question to answer")
	}
	sub := map[string]interface{}{kindField: kindAnswer, qidField: pending["qid"]}
	for k, v := range values {
		sub[k] = v
	}
	if r := h.Handle(leaderCtx(), v1alpha1.ControlPort, sub); r.Err() != nil {
		t.Fatalf("answer failed: %v", r.Err())
	}
}

func TestAskAnswerRoundTrip(t *testing.T) {
	h, c := newChat(t, nil)
	askQ(t, h, map[string]interface{}{"pod": "api-1"})

	data := c.control()
	pending, _ := data["pendingQuestion"].(map[string]interface{})
	if pending == nil || pending["qid"] == "" || pending["form"] == nil {
		t.Fatalf("bad pending question: %+v", data)
	}

	answerHead(t, h, c, map[string]interface{}{"approve": true})

	outs := waitOutputs(t, h, AnswerPort, 1)
	ans := outs[0].(Answer)
	if ans.Values["approve"] != true {
		t.Fatalf("bad values: %+v", ans.Values)
	}
	if ctxMap, _ := ans.Context.(map[string]interface{}); ctxMap["pod"] != "api-1" {
		t.Fatalf("context lost: %+v", ans.Context)
	}
	if _, found := ans.Values[qidField]; found {
		t.Fatal("qid leaked into values")
	}

	if p := c.control()["pendingQuestion"]; p != nil {
		t.Fatalf("queue not drained: %+v", p)
	}
	th := thread(c)
	if len(th) != 2 || th[0].Kind != "question" || th[0].Pending || th[1].Kind != kindAnswer {
		t.Fatalf("bad thread: %+v", th)
	}
}

func TestAskFIFOAndStaleAnswer(t *testing.T) {
	h, c := newChat(t, nil)
	askQ(t, h, "first")
	askQ(t, h, "second")

	head, _ := c.control()["pendingQuestion"].(map[string]interface{})
	firstQID := head["qid"].(string)

	// Stale submission: wrong qid must not consume the head.
	h.Handle(leaderCtx(), v1alpha1.ControlPort, map[string]interface{}{
		kindField: kindAnswer, qidField: "q-stale", "approve": true,
	})
	if outs := settleOutputs(h, AnswerPort); len(outs) != 0 {
		t.Fatalf("stale answer consumed a question: %+v", outs)
	}

	answerHead(t, h, c, map[string]interface{}{"approve": true})
	waitOutputs(t, h, AnswerPort, 1)

	next, _ := c.control()["pendingQuestion"].(map[string]interface{})
	if next == nil || next["qid"] == firstQID {
		t.Fatalf("second question not revealed: %+v", next)
	}
	if ans := waitOutputs(t, h, AnswerPort, 1)[0].(Answer); ans.Context != "first" {
		t.Fatalf("answered wrong question: %+v", ans)
	}
}

func TestAskPerRequestFormOverride(t *testing.T) {
	h, c := newChat(t, nil)
	form := `{"type":"object","properties":{"replicas":{"type":"number","title":"Replicas"},"ok":{"type":"boolean","format":"button"}}}`
	if r := h.Handle(context.Background(), AskPort, AskRequest{Context: "ctx", Form: form}); r.Err() != nil {
		t.Fatalf("ask failed: %v", r.Err())
	}
	pending, _ := c.control()["pendingQuestion"].(map[string]interface{})
	f, _ := pending["form"].(map[string]interface{})
	props, _ := f["properties"].(map[string]interface{})
	if props["replicas"] == nil {
		t.Fatalf("override form not used: %+v", f)
	}
}

func TestQuestionExpiry(t *testing.T) {
	h, c := newChat(t, &Settings{TimeoutSeconds: 10, EnableErrorPort: true})
	base := time.Now()
	c.now = func() time.Time { return base }
	askQ(t, h, "stale-ctx")

	c.now = func() time.Time { return base.Add(11 * time.Second) }
	// New traffic is the expiry heartbeat.
	askQ(t, h, "fresh-ctx")

	outs := waitOutputs(t, h, ErrorPort, 1)
	e := outs[0].(ErrorMessage)
	if e.Context != "stale-ctx" {
		t.Fatalf("wrong expiry: %+v", e)
	}
	pending, _ := c.control()["pendingQuestion"].(map[string]interface{})
	if pending == nil {
		t.Fatal("fresh question missing")
	}
	// Thread carries an expiry note.
	var found bool
	for _, e := range thread(c) {
		if e.Kind == "note" && e.Role == "error" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no expiry note in thread: %+v", thread(c))
	}
}

// ---- clear (flow wipes the surface) ----------------------------------------

func TestClearEmptiesThreadAndRepublishes(t *testing.T) {
	h, c := newChat(t, nil)
	h.Handle(leaderCtx(), v1alpha1.ControlPort, map[string]interface{}{
		kindField: kindMessage, textField: "remember this",
	})
	waitOutputs(t, h, MessagePort, 1)
	if len(thread(c)) != 1 {
		t.Fatalf("seed failed: %+v", thread(c))
	}
	before := len(h.PortOutputs(v1alpha1.ControlPort))

	if r := h.Handle(context.Background(), ClearPort, ClearRequest{Context: "wipe"}); r.Err() != nil {
		t.Fatalf("clear failed: %v", r.Err())
	}

	if th := thread(c); len(th) != 0 {
		t.Fatalf("thread survived clear: %+v", th)
	}
	if outs := waitOutputs(t, h, v1alpha1.ControlPort, before+1); len(outs) <= before {
		t.Fatal("clear did not republish the widget")
	}
}

func TestClearDropsPendingQuestion(t *testing.T) {
	h, c := newChat(t, nil)
	askQ(t, h, "under-review")
	if c.control()["pendingQuestion"] == nil {
		t.Fatal("seed failed: no pending question")
	}

	if r := h.Handle(context.Background(), ClearPort, ClearRequest{}); r.Err() != nil {
		t.Fatalf("clear failed: %v", r.Err())
	}

	if p := c.control()["pendingQuestion"]; p != nil {
		t.Fatalf("queue survived clear: %+v", p)
	}
	if q := c.loadQueue(context.Background()); len(q) != 0 {
		t.Fatalf("queue survived clear: %+v", q)
	}
	if th := thread(c); len(th) != 0 {
		t.Fatalf("thread survived clear: %+v", th)
	}
}

func TestClearOnEmptyChatIsNoOp(t *testing.T) {
	h, c := newChat(t, nil)
	if r := h.Handle(context.Background(), ClearPort, ClearRequest{}); r.Err() != nil {
		t.Fatalf("clear on empty chat failed: %v", r.Err())
	}
	// Idempotent: a second wipe is just as uneventful.
	if r := h.Handle(context.Background(), ClearPort, ClearRequest{}); r.Err() != nil {
		t.Fatalf("second clear failed: %v", r.Err())
	}
	if th := thread(c); len(th) != 0 {
		t.Fatalf("clear invented thread entries: %+v", th)
	}
}

// ---- history cap -----------------------------------------------------------

func TestHistoryLimitCapsThread(t *testing.T) {
	h, c := newChat(t, &Settings{HistoryLimit: 3})
	for i := 0; i < 5; i++ {
		if r := h.Handle(context.Background(), SayPort, Say{Text: "m"}); r.Err() != nil {
			t.Fatalf("say failed: %v", r.Err())
		}
	}
	if th := thread(c); len(th) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(th))
	}
}

// ---- restart ---------------------------------------------------------------

func TestRestartRehydratesConversation(t *testing.T) {
	h, _ := newChat(t, nil)
	h.Handle(leaderCtx(), v1alpha1.ControlPort, map[string]interface{}{
		kindField: kindMessage, textField: "before restart",
	})
	waitOutputs(t, h, MessagePort, 1)
	askQ(t, h, "pending-q")

	// A fresh pod reads the same persisted state: its control port must
	// advertise the full conversation with no in-memory handoff.
	h2 := h.NewPod()
	var control map[string]interface{}
	for _, p := range h2.Ports() {
		if p.Name == v1alpha1.ControlPort {
			control, _ = p.Configuration.(map[string]interface{})
		}
	}
	if control == nil {
		t.Fatal("no control port on restarted pod")
	}
	th, _ := control["thread"].([]threadEntry)
	if len(th) != 2 {
		t.Fatalf("thread lost across restart: %+v", control["thread"])
	}
	if control["pendingQuestion"] == nil {
		t.Fatal("pending question lost across restart")
	}
}

// ---- secrets (writeOnly / password fields) ----------------------------------

func TestSecretAnswerMaskedInThreadRawOnPort(t *testing.T) {
	h, c := newChat(t, nil)

	form := `{
	  "type": "object",
	  "properties": {
	    "token": {"type": "string", "title": "API Key", "writeOnly": true},
	    "note":  {"type": "string", "title": "Note"},
	    "save":  {"type": "boolean", "title": "Save", "format": "button"}
	  }
	}`
	if r := h.Handle(context.Background(), AskPort, AskRequest{Form: form}); r.Err() != nil {
		t.Fatalf("ask failed: %v", r.Err())
	}

	answerHead(t, h, c, map[string]interface{}{
		"token": "sk-live-abc123", "note": "prod key", "save": true,
	})

	// The flow gets the raw value — the validation flow consumes it.
	ans := waitOutputs(t, h, AnswerPort, 1)[0].(Answer)
	if ans.Values["token"] != "sk-live-abc123" {
		t.Fatalf("Answer port must carry the raw secret, got %+v", ans.Values)
	}

	// The persisted thread never sees it.
	var found bool
	for _, e := range thread(c) {
		if e.Kind != kindAnswer {
			continue
		}
		found = true
		if e.Values["token"] != secretMask {
			t.Fatalf("secret leaked into thread: %+v", e.Values)
		}
		if e.Values["note"] != "prod key" || e.Values["save"] != true {
			t.Fatalf("non-secret fields must survive unmasked: %+v", e.Values)
		}
	}
	if !found {
		t.Fatal("no answer entry in thread")
	}

	// And therefore the widget data never sees it either.
	raw, err := json.Marshal(c.control())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "sk-live-abc123") {
		t.Fatal("secret leaked into widget control data")
	}
}

func TestSecretMaskCoversPasswordFormat(t *testing.T) {
	h, c := newChat(t, nil)
	form := `{"type":"object","properties":{
	  "key":  {"type":"string","format":"password"},
	  "save": {"type":"boolean","format":"button"}}}`
	if r := h.Handle(context.Background(), AskPort, AskRequest{Form: form}); r.Err() != nil {
		t.Fatalf("ask failed: %v", r.Err())
	}
	answerHead(t, h, c, map[string]interface{}{"key": "hunter2", "save": true})
	waitOutputs(t, h, AnswerPort, 1)
	for _, e := range thread(c) {
		if e.Kind == kindAnswer && e.Values["key"] != secretMask {
			t.Fatalf("password-format field leaked: %+v", e.Values)
		}
	}
}
