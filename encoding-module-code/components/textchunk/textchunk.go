// Package textchunk splits a document into pieces small enough to embed.
//
// Retrieval was well covered — embed_text, vector_upsert, vector_search — and
// the step before it was not: nothing stood between "I have a document" and "I
// have embeddable pieces". Every RAG build wrote the same twenty lines of
// JavaScript, and each one re-decided what to do about word boundaries.
package textchunk

import (
	"context"
	"fmt"
	"strings"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "text_chunk"

	RequestPort  = "request"
	ResponsePort = "response"
	ErrorPort    = "error"

	SplitParagraph = "paragraph"
	SplitLine      = "line"
	SplitSentence  = "sentence"
	SplitNone      = "none"

	defaultChunkSize = 1000
	defaultOverlap   = 200
	defaultMaxChunks = 500
)

type Context any

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Passthrough — carry the document's id or source here so each chunk can be traced back to it."`
	Text    string  `json:"text" required:"true" format:"textarea" title:"Text" description:"The document to split."`
}

// Chunk is one piece, with the offsets it came from so a citation can point
// back into the original rather than at a copy of it.
type Chunk struct {
	Index int    `json:"index" title:"Index"`
	Text  string `json:"text" title:"Text"`
	Start int    `json:"start" title:"Start" description:"Byte offset in the original text."`
	End   int    `json:"end" title:"End"`
}

type Response struct {
	Context   Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Chunks    []Chunk `json:"chunks" title:"Chunks" description:"Wire through array_split so each chunk reaches embed_text on its own."`
	Count     int     `json:"count" title:"Count"`
	Truncated bool    `json:"truncated" title:"Truncated" description:"True when the document produced more pieces than maxChunks and the rest were dropped. Check it — a silently half-indexed document answers questions wrongly rather than not at all."`
}

type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

type Settings struct {
	ChunkSize int    `json:"chunkSize" required:"true" default:"1000" title:"Chunk Size" description:"Target size in characters. A chunk may come in under it when a boundary falls early, and may exceed it only when a single unbreakable run is longer."`
	Overlap   int    `json:"overlap" default:"200" title:"Overlap" description:"Characters repeated from the end of one chunk at the start of the next, so a sentence spanning a boundary is still retrievable. Must be smaller than the chunk size."`
	SplitOn   string `json:"splitOn" default:"paragraph" enum:"paragraph,line,sentence,none" enumTitles:"Paragraph,Line,Sentence,Anywhere" title:"Split On" description:"Where a break is allowed. Prose splits best on paragraph; a log or CSV-ish file on line; 'none' cuts at exactly the chunk size and will cut mid-word."`
	MaxChunks int    `json:"maxChunks" default:"500" title:"Max Chunks" description:"Ceiling on pieces from one document, so an unexpectedly large file cannot flood the embedder. Reaching it sets truncated."`

	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to the error port instead of failing the run."`
}

type Component struct {
	module.Base
	settings Settings
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Text Chunk",
		Info: "Splits a document into overlapping pieces for embedding — the step between having a document and " +
			"having something vector_upsert can store. Breaks on a paragraph, line or sentence boundary where it can " +
			"so a piece is a whole thought, and carries each piece's offsets so a citation points back into the " +
			"original. " +
			"Wire array_split after it to send one chunk at a time to embed_text, then vector_upsert. " +
			"Overlap exists so a sentence that straddles a boundary is still findable; without it, retrieval misses " +
			"exactly the passages that span two chunks. " +
			"Check truncated on the way out: a document that produced more pieces than maxChunks is indexed in part, " +
			"and a half-indexed document answers questions wrongly rather than not at all.",
		Tags: []string{"text", "rag", "embedding"},
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

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}

	size, overlap, maxChunks := c.limits()
	if overlap >= size {
		return c.handleError(ctx, handler, in.Context,
			fmt.Errorf("overlap (%d) must be smaller than chunkSize (%d) — an overlap at least as large as a chunk never advances through the document", overlap, size))
	}

	chunks, truncated := split(in.Text, size, overlap, c.settings.SplitOn, maxChunks)
	return handler(ctx, ResponsePort, Response{
		Context:   in.Context,
		Chunks:    chunks,
		Count:     len(chunks),
		Truncated: truncated,
	})
}

// limits applies the defaults. A zero here means "not configured" rather than
// "zero", since a chunk size of nothing would produce a piece per character.
func (c *Component) limits() (size, overlap, maxChunks int) {
	size, overlap, maxChunks = c.settings.ChunkSize, c.settings.Overlap, c.settings.MaxChunks
	if size <= 0 {
		size = defaultChunkSize
	}
	if overlap < 0 {
		overlap = defaultOverlap
	}
	if maxChunks <= 0 {
		maxChunks = defaultMaxChunks
	}
	return size, overlap, maxChunks
}

// split walks the text, ending each chunk at the last allowed boundary at or
// before the target size, and stepping back by the overlap before the next.
//
// Offsets are byte offsets into the original, so a chunk can be located in the
// document it came from rather than only compared against it.
func split(text string, size, overlap int, splitOn string, maxChunks int) ([]Chunk, bool) {
	if text == "" {
		return []Chunk{}, false
	}

	chunks := make([]Chunk, 0, 8)
	start := 0
	for start < len(text) {
		if len(chunks) >= maxChunks {
			return chunks, true
		}

		end := start + size
		if end >= len(text) {
			end = len(text)
		} else {
			end = boundaryBefore(text, start, end, splitOn)
		}

		piece := strings.TrimSpace(text[start:end])
		if piece != "" {
			chunks = append(chunks, Chunk{Index: len(chunks), Text: piece, Start: start, End: end})
		}
		if end >= len(text) {
			break
		}

		// Step back by the overlap, but never behind where this chunk began:
		// a boundary that landed early could otherwise put the next start
		// before the last one and loop forever.
		next := end - overlap
		if next <= start {
			next = end
		}
		start = next
	}
	return chunks, false
}

// boundaryBefore finds the latest place a break is allowed at or before limit,
// falling back to the limit itself when the run is unbreakable — a chunk that
// is slightly wrong is better than one that never ends.
func boundaryBefore(text string, start, limit int, splitOn string) int {
	if splitOn == SplitNone {
		return limit
	}

	window := text[start:limit]
	var seps []string
	switch splitOn {
	case SplitLine:
		seps = []string{"\n"}
	case SplitSentence:
		seps = []string{". ", "! ", "? ", ".\n", "!\n", "?\n"}
	default: // paragraph
		seps = []string{"\n\n", "\r\n\r\n"}
	}

	best := -1
	for _, sep := range seps {
		if idx := strings.LastIndex(window, sep); idx > best {
			best = idx + len(sep)
		}
	}
	// Refuse a boundary in the first fifth: honouring it would produce a
	// sliver and push nearly everything into the next chunk, which for a
	// document of many short paragraphs degenerates into one chunk per
	// paragraph however large chunkSize is.
	if best <= (limit-start)/5 {
		return limit
	}
	return start + best
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	if !c.settings.EnableErrorPort {
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, Error{Context: reqCtx, Error: err.Error()})
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          RequestPort,
			Label:         "Request",
			Configuration: Request{},
			Position:      module.Left,
		},
		{
			Name:          ResponsePort,
			Label:         "Response",
			Source:        true,
			Configuration: Response{},
			Position:      module.Right,
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: c.settings,
		},
	}
	if c.settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Configuration: Error{},
			Position:      module.Bottom,
		})
	}
	return ports
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
