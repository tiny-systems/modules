package conversation

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tiny-systems/module/module"
)

// newTestComponent builds a component instance with settings applied
// against a temp-dir bbolt file. Overrides in `override` are applied on
// top of Instance() defaults.
func newTestComponent(t *testing.T, override func(*Settings)) *Component {
	t.Helper()
	c := (&Component{}).Instance().(*Component)
	settings := c.settings
	settings.Path = filepath.Join(t.TempDir(), "conversation.db")
	if override != nil {
		override(&settings)
	}
	if err := c.OnSettings(context.Background(), settings); err != nil {
		t.Fatalf("OnSettings: %v", err)
	}
	t.Cleanup(func() { c.OnDestroy(nil) })
	return c
}

// capture is a thread-safe recording Handler.
type capture struct {
	mu    sync.Mutex
	ports []string
	data  []any
}

func (r *capture) handler() module.Handler {
	return func(_ context.Context, port string, data any) module.Result {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.ports = append(r.ports, port)
		r.data = append(r.data, data)
		return module.Ok(nil)
	}
}

func (r *capture) last(t *testing.T) (string, any) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ports) == 0 {
		t.Fatal("nothing emitted")
	}
	return r.ports[len(r.ports)-1], r.data[len(r.data)-1]
}

func msg(role, content string) map[string]any {
	return map[string]any{"role": role, "content": content}
}

func appendMsgs(t *testing.T, c *Component, rec *capture, id string, messages ...any) AppendResult {
	t.Helper()
	res := c.Handle(context.Background(), rec.handler(), AppendPort, AppendRequest{
		ConversationID: id,
		Messages:       messages,
	})
	if err := res.Err(); err != nil {
		t.Fatalf("append: %v", err)
	}
	port, data := rec.last(t)
	if port != AppendResultPort {
		t.Fatalf("append emitted on %q, want %q", port, AppendResultPort)
	}
	out, ok := data.(AppendResult)
	if !ok {
		t.Fatalf("append emitted %T, want AppendResult", data)
	}
	return out
}

func getMsgs(t *testing.T, c *Component, rec *capture, id string) GetResult {
	t.Helper()
	res := c.Handle(context.Background(), rec.handler(), GetPort, GetRequest{ConversationID: id})
	if err := res.Err(); err != nil {
		t.Fatalf("get: %v", err)
	}
	port, data := rec.last(t)
	if port != GetResultPort {
		t.Fatalf("get emitted on %q, want %q", port, GetResultPort)
	}
	out, ok := data.(GetResult)
	if !ok {
		t.Fatalf("get emitted %T, want GetResult", data)
	}
	return out
}

func TestAppendGetRoundtrip(t *testing.T) {
	c := newTestComponent(t, nil)
	rec := &capture{}

	out := appendMsgs(t, c, rec, "conv-1",
		msg("user", "hello"),
		msg("assistant", "hi there"),
	)
	if out.ConversationID != "conv-1" {
		t.Errorf("ConversationID = %q, want conv-1", out.ConversationID)
	}
	if len(out.Messages) != 2 {
		t.Fatalf("append window has %d messages, want 2", len(out.Messages))
	}

	got := getMsgs(t, c, rec, "conv-1")
	if len(got.Messages) != 2 {
		t.Fatalf("get returned %d messages, want 2", len(got.Messages))
	}
	first, ok := got.Messages[0].(map[string]any)
	if !ok {
		t.Fatalf("message round-tripped as %T, want map", got.Messages[0])
	}
	if first["role"] != "user" || first["content"] != "hello" {
		t.Errorf("first message = %v, want user/hello", first)
	}
	second, ok := got.Messages[1].(map[string]any)
	if !ok {
		t.Fatalf("message round-tripped as %T, want map", got.Messages[1])
	}
	if second["role"] != "assistant" {
		t.Errorf("second message role = %v, want assistant", second["role"])
	}
}

func TestWindowTrimming(t *testing.T) {
	c := newTestComponent(t, func(s *Settings) { s.MaxMessages = 3 })
	rec := &capture{}

	// 5 messages across two appends; window of 3 keeps the most recent.
	appendMsgs(t, c, rec, "conv-w", msg("user", "m0"), msg("assistant", "m1"))
	out := appendMsgs(t, c, rec, "conv-w", msg("user", "m2"), msg("assistant", "m3"), msg("user", "m4"))
	if len(out.Messages) != 3 {
		t.Fatalf("post-append window has %d messages, want 3", len(out.Messages))
	}
	for i, want := range []string{"m2", "m3", "m4"} {
		m := out.Messages[i].(map[string]any)
		if m["content"] != want {
			t.Errorf("window[%d] = %v, want %s", i, m["content"], want)
		}
	}

	// And the stored record was trimmed too, not just the response.
	got := getMsgs(t, c, rec, "conv-w")
	if len(got.Messages) != 3 {
		t.Fatalf("stored window has %d messages, want 3", len(got.Messages))
	}
	if got.Messages[0].(map[string]any)["content"] != "m2" {
		t.Errorf("stored window starts at %v, want m2", got.Messages[0])
	}
}

func TestWindowUnlimited(t *testing.T) {
	c := newTestComponent(t, func(s *Settings) { s.MaxMessages = 0 })
	rec := &capture{}

	for i := 0; i < 60; i++ {
		appendMsgs(t, c, rec, "conv-u", msg("user", fmt.Sprintf("m%d", i)))
	}
	got := getMsgs(t, c, rec, "conv-u")
	if len(got.Messages) != 60 {
		t.Fatalf("unlimited window has %d messages, want 60", len(got.Messages))
	}
}

// TestConcurrentAppends is the reason this component exists: concurrent
// turns against one conversation must not lose messages. Run with -race.
func TestConcurrentAppends(t *testing.T) {
	c := newTestComponent(t, func(s *Settings) { s.MaxMessages = 0 })
	rec := &capture{}

	const (
		goroutines        = 8
		appendsPerRoutine = 25
	)
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*appendsPerRoutine)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < appendsPerRoutine; i++ {
				res := c.Handle(context.Background(), rec.handler(), AppendPort, AppendRequest{
					ConversationID: "conv-c",
					Messages:       []any{msg("user", fmt.Sprintf("g%d-m%d", g, i))},
				})
				if err := res.Err(); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent append failed: %v", err)
	}

	got := getMsgs(t, c, rec, "conv-c")
	want := goroutines * appendsPerRoutine
	if len(got.Messages) != want {
		t.Fatalf("after %d concurrent appends conversation holds %d messages, want %d — messages were lost",
			want, len(got.Messages), want)
	}
	// Every append must be present exactly once.
	seen := map[string]bool{}
	for _, m := range got.Messages {
		content := m.(map[string]any)["content"].(string)
		if seen[content] {
			t.Fatalf("message %q appears twice", content)
		}
		seen[content] = true
	}
	if len(seen) != want {
		t.Fatalf("distinct messages = %d, want %d", len(seen), want)
	}
}

func TestClear(t *testing.T) {
	c := newTestComponent(t, nil)
	rec := &capture{}

	appendMsgs(t, c, rec, "conv-x", msg("user", "hello"))

	res := c.Handle(context.Background(), rec.handler(), ClearPort, ClearRequest{ConversationID: "conv-x"})
	if err := res.Err(); err != nil {
		t.Fatalf("clear: %v", err)
	}
	port, data := rec.last(t)
	if port != ClearResultPort {
		t.Fatalf("clear emitted on %q, want %q", port, ClearResultPort)
	}
	out := data.(ClearResult)
	if !out.Cleared {
		t.Error("Cleared = false after clearing an existing conversation, want true")
	}

	// Conversation is gone.
	got := getMsgs(t, c, rec, "conv-x")
	if len(got.Messages) != 0 {
		t.Fatalf("get after clear returned %d messages, want 0", len(got.Messages))
	}

	// Clear is idempotent: second clear reports cleared=false, no error.
	res = c.Handle(context.Background(), rec.handler(), ClearPort, ClearRequest{ConversationID: "conv-x"})
	if err := res.Err(); err != nil {
		t.Fatalf("second clear: %v", err)
	}
	_, data = rec.last(t)
	if data.(ClearResult).Cleared {
		t.Error("Cleared = true on already-cleared conversation, want false")
	}
}

// TestColdGetReturnsEmpty: get on an unknown conversation is NOT an
// error — it returns an empty (non-nil) messages array on get_ok.
func TestColdGetReturnsEmpty(t *testing.T) {
	c := newTestComponent(t, nil)
	rec := &capture{}

	got := getMsgs(t, c, rec, "never-seen")
	if got.Messages == nil {
		t.Fatal("cold get returned nil messages, want empty array (serializes as [], not null)")
	}
	if len(got.Messages) != 0 {
		t.Fatalf("cold get returned %d messages, want 0", len(got.Messages))
	}
	if got.ConversationID != "never-seen" {
		t.Errorf("ConversationID = %q, want never-seen", got.ConversationID)
	}
}

// TestSharedPathAcrossInstances verifies the storeRegistry decision:
// the scheduler creates one Component instance per node, and multiple
// nodes with the same path must share one bbolt handle (a second
// exclusive open would hang). Appends from either instance land in the
// same conversation.
func TestSharedPathAcrossInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")

	mk := func() *Component {
		c := (&Component{}).Instance().(*Component)
		settings := c.settings
		settings.Path = path
		if err := c.OnSettings(context.Background(), settings); err != nil {
			t.Fatalf("OnSettings: %v", err)
		}
		t.Cleanup(func() { c.OnDestroy(nil) })
		return c
	}
	c1 := mk()
	c2 := mk() // would block ~30s on the flock without the shared registry

	rec := &capture{}
	appendMsgs(t, c1, rec, "conv-s", msg("user", "from node 1"))
	appendMsgs(t, c2, rec, "conv-s", msg("assistant", "from node 2"))

	got := getMsgs(t, c1, rec, "conv-s")
	if len(got.Messages) != 2 {
		t.Fatalf("conversation holds %d messages across shared instances, want 2", len(got.Messages))
	}

	// First instance destroyed → handle stays open for the second.
	c1.OnDestroy(nil)
	got = getMsgs(t, c2, rec, "conv-s")
	if len(got.Messages) != 2 {
		t.Fatalf("after releasing one instance, get returned %d messages, want 2", len(got.Messages))
	}
}

func TestValidationErrors(t *testing.T) {
	c := newTestComponent(t, nil)
	rec := &capture{}

	res := c.Handle(context.Background(), rec.handler(), AppendPort, AppendRequest{
		ConversationID: "", Messages: []any{msg("user", "x")},
	})
	if res.Err() == nil {
		t.Error("append with empty conversationId succeeded, want error")
	}
	if module.ShouldRetry(res.Err()) {
		t.Error("validation error marked retryable, want permanent")
	}

	res = c.Handle(context.Background(), rec.handler(), AppendPort, AppendRequest{
		ConversationID: "conv-v", Messages: nil,
	})
	if res.Err() == nil {
		t.Error("append with no messages succeeded, want error")
	}
}

// TestUninitialisedStoreIsRetryable: before settings arrive the store is
// closed; the failure must carry module.Retryable so edge auto-retry /
// the retry component can clear it (document_store's failRetryable
// classification).
func TestUninitialisedStoreIsRetryable(t *testing.T) {
	c := (&Component{}).Instance().(*Component)
	rec := &capture{}

	res := c.Handle(context.Background(), rec.handler(), GetPort, GetRequest{ConversationID: "conv-r"})
	err := res.Err()
	if err == nil {
		t.Fatal("get on uninitialised store succeeded, want error")
	}
	if !module.ShouldRetry(err) {
		t.Errorf("uninitialised-store error not retryable: %v", err)
	}
}

// TestErrorPortCarriesRetryable: with the error port enabled the
// canonical ErrorMessage payload must reflect the error's retryability.
func TestErrorPortCarriesRetryable(t *testing.T) {
	c := (&Component{}).Instance().(*Component)
	c.settings.EnableErrorPort = true
	rec := &capture{}

	res := c.Handle(context.Background(), rec.handler(), GetPort, GetRequest{ConversationID: "conv-e"})
	if err := res.Err(); err != nil {
		t.Fatalf("expected error routed to error port, got failure: %v", err)
	}
	port, data := rec.last(t)
	if port != ErrorPort {
		t.Fatalf("emitted on %q, want %q", port, ErrorPort)
	}
	em, ok := data.(module.ErrorMessage)
	if !ok {
		t.Fatalf("error port payload is %T, want module.ErrorMessage", data)
	}
	if !em.Retryable {
		t.Error("ErrorMessage.Retryable = false for uninitialised store, want true")
	}
}
