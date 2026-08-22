package textchunk

import (
	"context"
	"strings"
	"testing"

	"github.com/tiny-systems/module/module"
)

func run(t *testing.T, in Request, settings Settings) (string, interface{}, error) {
	t.Helper()
	c, ok := (&Component{}).Instance().(*Component)
	if !ok {
		t.Fatal("Instance() did not return *Component")
	}
	if err := c.OnSettings(context.Background(), settings); err != nil {
		t.Fatalf("settings: %v", err)
	}

	var gotPort string
	var gotMsg interface{}
	res := c.Handle(context.Background(), func(_ context.Context, port string, msg interface{}) module.Result {
		gotPort, gotMsg = port, msg
		return module.Result{}
	}, RequestPort, in)
	return gotPort, gotMsg, res.Err()
}

func chunk(t *testing.T, in Request, settings Settings) Response {
	t.Helper()
	port, msg, err := run(t, in, settings)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if port != ResponsePort {
		t.Fatalf("emitted on %q, want %q", port, ResponsePort)
	}
	out, ok := msg.(Response)
	if !ok {
		t.Fatalf("emitted %T", msg)
	}
	return out
}

func paragraphs(n, size int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = strings.Repeat("x", size)
	}
	return strings.Join(parts, "\n\n")
}

// The whole point: pieces small enough to embed, from a document that is not.
func TestSplitsALongDocument(t *testing.T) {
	out := chunk(t, Request{Text: paragraphs(10, 300)}, Settings{ChunkSize: 500, Overlap: 50})
	if out.Count < 2 {
		t.Fatalf("count = %d — a 3000-character document did not get split at 500", out.Count)
	}
	if out.Count != len(out.Chunks) {
		t.Errorf("count = %d but %d chunks", out.Count, len(out.Chunks))
	}
	if out.Truncated {
		t.Error("truncated on a document well under maxChunks")
	}
}

// A document shorter than one chunk is one chunk, not zero and not a copy per
// paragraph.
func TestShortDocumentIsOneChunk(t *testing.T) {
	out := chunk(t, Request{Text: "one short paragraph."}, Settings{ChunkSize: 1000})
	if out.Count != 1 {
		t.Fatalf("count = %d, want 1", out.Count)
	}
	if out.Chunks[0].Text != "one short paragraph." {
		t.Errorf("text = %q, want the document unchanged", out.Chunks[0].Text)
	}
}

func TestEmptyTextProducesNoChunks(t *testing.T) {
	out := chunk(t, Request{Text: ""}, Settings{})
	if out.Count != 0 {
		t.Fatalf("count = %d, want 0", out.Count)
	}
	if out.Chunks == nil {
		t.Error("chunks should be an empty array, not null — a downstream array_split over null is a different failure")
	}
}

// Offsets exist so a retrieved chunk can be pointed at in the document it came
// from. If they don't line up, a citation is a lie.
func TestOffsetsLocateTheChunkInTheOriginal(t *testing.T) {
	text := paragraphs(8, 200)
	out := chunk(t, Request{Text: text}, Settings{ChunkSize: 400, Overlap: 40})

	for _, ch := range out.Chunks {
		if ch.Start < 0 || ch.End > len(text) || ch.Start >= ch.End {
			t.Fatalf("chunk %d has offsets [%d,%d) against a %d-character document", ch.Index, ch.Start, ch.End, len(text))
		}
		if !strings.Contains(text[ch.Start:ch.End], ch.Text) {
			t.Errorf("chunk %d text is not found at its own offsets", ch.Index)
		}
	}
}

// Indexes are what array_split hands downstream; a gap or a repeat puts two
// chunks in the same slot in whatever store receives them.
func TestIndexesAreSequential(t *testing.T) {
	out := chunk(t, Request{Text: paragraphs(12, 250)}, Settings{ChunkSize: 500, Overlap: 50})
	for i, ch := range out.Chunks {
		if ch.Index != i {
			t.Fatalf("chunk at position %d carries index %d", i, ch.Index)
		}
	}
}

// Overlap is the reason a sentence spanning a boundary stays retrievable.
// Without it the two halves live in different chunks and neither matches.
func TestOverlapRepeatsTheEndOfThePreviousChunk(t *testing.T) {
	out := chunk(t, Request{Text: strings.Repeat("abcdefghij", 100)}, Settings{
		ChunkSize: 200, Overlap: 50, SplitOn: SplitNone,
	})
	if out.Count < 2 {
		t.Fatalf("count = %d, want several", out.Count)
	}
	first, second := out.Chunks[0], out.Chunks[1]
	if second.Start != first.End-50 {
		t.Fatalf("second chunk starts at %d, want %d (previous end %d minus the 50-character overlap)", second.Start, first.End-50, first.End)
	}
}

// Prose reads badly when a chunk begins mid-sentence, so a break lands on a
// paragraph where one is available.
func TestBreaksOnParagraphBoundary(t *testing.T) {
	text := strings.Repeat("x", 180) + "\n\n" + strings.Repeat("y", 180)
	out := chunk(t, Request{Text: text}, Settings{ChunkSize: 200, Overlap: 0, SplitOn: SplitParagraph})
	if out.Count != 2 {
		t.Fatalf("count = %d, want 2", out.Count)
	}
	if strings.Contains(out.Chunks[0].Text, "y") {
		t.Error("first chunk ran past the paragraph break")
	}
	if strings.Contains(out.Chunks[1].Text, "x") {
		t.Error("second chunk carried part of the first paragraph")
	}
}

func TestBreaksOnLineBoundary(t *testing.T) {
	text := strings.Repeat("x", 180) + "\n" + strings.Repeat("y", 180)
	out := chunk(t, Request{Text: text}, Settings{ChunkSize: 200, Overlap: 0, SplitOn: SplitLine})
	if out.Count != 2 || strings.Contains(out.Chunks[0].Text, "y") {
		t.Fatalf("count = %d, first chunk = %.20q… — a line break was available at 180", out.Count, out.Chunks[0].Text)
	}
}

func TestBreaksOnSentenceBoundary(t *testing.T) {
	text := strings.Repeat("x", 178) + ". " + strings.Repeat("y", 180)
	out := chunk(t, Request{Text: text}, Settings{ChunkSize: 200, Overlap: 0, SplitOn: SplitSentence})
	if out.Count != 2 || strings.Contains(out.Chunks[0].Text, "y") {
		t.Fatalf("count = %d — a sentence end was available at 178", out.Count)
	}
}

// An unbreakable run must still terminate. Falling back to a hard cut produces
// a slightly wrong chunk; refusing to cut produces a hang.
func TestUnbreakableRunStillTerminates(t *testing.T) {
	out := chunk(t, Request{Text: strings.Repeat("x", 5000)}, Settings{ChunkSize: 500, Overlap: 100, SplitOn: SplitParagraph})
	if out.Count < 2 {
		t.Fatalf("count = %d — 5000 characters with no boundary came back as one chunk", out.Count)
	}
	for _, ch := range out.Chunks {
		if len(ch.Text) > 500 {
			t.Fatalf("chunk %d is %d characters, past the 500 target with a hard cut available", ch.Index, len(ch.Text))
		}
	}
}

// A document of many short paragraphs must not degenerate into one chunk per
// paragraph — that is the failure mode of always honouring the first boundary.
func TestManyShortParagraphsPackIntoChunks(t *testing.T) {
	out := chunk(t, Request{Text: paragraphs(40, 20)}, Settings{ChunkSize: 500, Overlap: 0})
	if out.Count > 10 {
		t.Fatalf("count = %d for ~880 characters at chunkSize 500 — paragraphs are not being packed", out.Count)
	}
}

// A file larger than expected must not flood the embedder, and must say so.
func TestMaxChunksTruncatesAndReportsIt(t *testing.T) {
	out := chunk(t, Request{Text: strings.Repeat("x", 10000)}, Settings{
		ChunkSize: 100, Overlap: 0, SplitOn: SplitNone, MaxChunks: 5,
	})
	if out.Count != 5 {
		t.Fatalf("count = %d, want the 5-chunk ceiling", out.Count)
	}
	if !out.Truncated {
		t.Fatal("truncated is false after hitting maxChunks — a half-indexed document answers questions wrongly rather than not at all")
	}
}

// An overlap at least as large as a chunk never advances through the document.
// Failing is the only honest answer.
func TestOverlapAtLeastChunkSizeIsRefused(t *testing.T) {
	if _, _, err := run(t, Request{Text: "anything"}, Settings{ChunkSize: 100, Overlap: 100}); err == nil {
		t.Fatal("an overlap equal to the chunk size was accepted")
	}
}

// Zero means "not configured", not "chunk every character".
func TestDefaultsApplyWhenUnset(t *testing.T) {
	out := chunk(t, Request{Text: paragraphs(6, 400)}, Settings{})
	if out.Count == 0 {
		t.Fatal("no chunks with settings left at their zero values")
	}
	for _, ch := range out.Chunks {
		if len(ch.Text) < 100 {
			t.Fatalf("chunk %d is %d characters — the default chunk size did not apply", ch.Index, len(ch.Text))
		}
	}
}

// The passthrough is how a document id reaches vector_upsert; losing it stores
// chunks nothing can attribute.
func TestContextIsCarried(t *testing.T) {
	out := chunk(t, Request{
		Context: map[string]any{"docId": "doc-1"},
		Text:    "some text",
	}, Settings{})
	carried, _ := out.Context.(map[string]any)
	if carried["docId"] != "doc-1" {
		t.Fatalf("context = %v, want it carried", out.Context)
	}
}

func TestErrorPortRoutesInsteadOfFailing(t *testing.T) {
	port, msg, err := run(t, Request{Text: "anything"}, Settings{
		ChunkSize: 100, Overlap: 200, EnableErrorPort: true,
	})
	if err != nil {
		t.Fatalf("with the error port on, the run must not fail: %v", err)
	}
	if port != ErrorPort {
		t.Fatalf("emitted on %q, want %q", port, ErrorPort)
	}
	if msg.(Error).Error == "" {
		t.Error("the error port carried no message")
	}
}

func TestUnknownPortIsRefused(t *testing.T) {
	c := &Component{}
	if res := c.Handle(context.Background(), nil, "nope", Request{}); res.Err() == nil {
		t.Fatal("an unknown port was accepted")
	}
}
