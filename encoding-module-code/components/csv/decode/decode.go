// Package decode turns a CSV string into rows the rest of the flow can read.
//
// CSV is what an export button produces — a finance report, a Grafana download,
// a spreadsheet mailed once a week. Until now a flow that fetched one had to
// hand it to js_eval and split it by hand, and the hand-written version always
// got quoting wrong: a field containing a comma, or a newline inside quotes,
// silently shifted every column after it.
package decode

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "csv_decode"

	RequestPort  = "request"
	ResponsePort = "response"
	ErrorPort    = "error"

	DelimiterComma     = "comma"
	DelimiterSemicolon = "semicolon"
	DelimiterTab       = "tab"
	DelimiterPipe      = "pipe"

	defaultMaxRows = 10000
)

type Context any

// Rows is the decoded table. Like json_decode's decoded, it carries no shape of
// its own, so the setting below is where the author says what the columns are.
type Rows any

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Passthrough — carry the file's name or source here so a row can be traced back to it."`
	Encoded string  `json:"encoded" required:"true" format:"textarea" title:"CSV" description:"The CSV text to decode."`
}

type Response struct {
	Context   Context  `json:"context,omitempty" configurable:"true" title:"Context"`
	Rows      Rows     `json:"rows" configurable:"true" title:"Rows" description:"One object per record. Wire through array_split so each row arrives as its own message."`
	Headers   []string `json:"headers" title:"Headers" description:"The column names used as keys. Check these when a mapping comes back empty — an export that renamed a column breaks quietly otherwise."`
	Count     int      `json:"count" title:"Count"`
	Truncated bool     `json:"truncated" title:"Truncated" description:"True when the file had more records than maxRows and the rest were dropped."`
}

type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

type Settings struct {
	Delimiter string `json:"delimiter" required:"true" default:"comma" enum:"comma,semicolon,tab,pipe" enumTitles:"Comma ,|Semicolon ;|Tab|Pipe |" title:"Delimiter" description:"The field separator. European exports and anything produced from a locale that uses a comma for decimals are usually semicolon-separated."`
	// Phrased as the negative on purpose. A bool cannot tell "unset" from
	// "false", so whichever case the zero value lands on is the one a node
	// created without touching this setting will get — and a header eaten as
	// data is the expensive mistake, not a headerless file with named columns.
	NoHeader   bool `json:"noHeader" title:"First Row Is Data, Not A Header" description:"Off (default): the first record names the columns. On: there is no header, and columns are named col1, col2 … so the output shape stays an object either way."`
	TrimSpace  bool `json:"trimSpace" title:"Trim Leading Space" description:"Ignore space that follows a delimiter. Turn this on for a file written with ', ' between fields."`
	LazyQuotes bool `json:"lazyQuotes" title:"Tolerate Bad Quoting" description:"Accept a bare quote inside an unquoted field instead of failing. Real exports contain them; the cost is that a genuinely malformed file decodes to something wrong rather than reporting itself."`

	// A ragged file is the normal shape of a hand-edited export, and refusing
	// it outright means the flow cannot run at all. Padding is recoverable and
	// visible in the row; a hard failure is neither.
	AllowRagged bool `json:"allowRagged" title:"Allow Rows With Missing Or Extra Fields" description:"On: a short row has its missing columns filled with empty strings and extra fields land in col<N>. Off (default): a row whose field count differs from the header fails, so a shifted file is reported rather than silently mis-keyed."`

	// Off by default because inference destroys data that only looks numeric:
	// a zip code 01234, an order id 007, a phone number. A number arriving as a
	// string is a visible failure; a corrupted identifier is not.
	InferTypes bool `json:"inferTypes" title:"Infer Numbers And Booleans" description:"On: a field that parses as a number or as true/false becomes one, so a downstream $.restarts > 3 works. Off (default): every field stays a string. Leave it off when a column holds an identifier — 007 and 01234 do not survive inference."`

	MaxRows int  `json:"maxRows" default:"10000" title:"Max Rows" description:"Ceiling on records from one file. Reaching it sets truncated."`
	Rows    Rows `json:"rows" configurable:"true" title:"Rows shape" description:"An example of the decoded rows — one representative object is enough. A CSV string has no shape, so without this every downstream edge is unverifiable: {{$.rows[0].amount}} is accepted when the flow is built and resolves to null at runtime."`

	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to the error port instead of failing the run."`
}

type Component struct {
	module.Base
	settings Settings
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "CSV Decoder",
		Info: "Parses CSV into one object per row, with quoting, embedded newlines and escaped delimiters handled — the " +
			"things a hand-written split gets wrong in a way that shifts every column after the offending field. " +
			"SET THE `rows` SETTING to an example row, for the same reason json_decode needs one: a string has no shape, " +
			"so an expression over a column nobody declared resolves to null at runtime instead of failing when the flow " +
			"is built. " +
			"Wire array_split after it so each row arrives as its own message. " +
			"Every field is a string unless inferTypes is on — turn it on when a column is compared or summed, and leave " +
			"it off when a column holds an identifier, because 007 and 01234 do not survive inference. " +
			"Check truncated: a file larger than maxRows is decoded in part.",
		Tags: []string{"csv", "agent_tool"},
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

	rows, headers, truncated, err := c.decode(in.Encoded)
	if err != nil {
		return c.handleError(ctx, handler, in.Context, err)
	}

	return handler(ctx, ResponsePort, Response{
		Context:   in.Context,
		Rows:      rows,
		Headers:   headers,
		Count:     len(rows),
		Truncated: truncated,
	})
}

func (c *Component) decode(encoded string) ([]any, []string, bool, error) {
	reader := csv.NewReader(strings.NewReader(encoded))
	reader.Comma = delimiter(c.settings.Delimiter)
	reader.TrimLeadingSpace = c.settings.TrimSpace
	reader.LazyQuotes = c.settings.LazyQuotes
	reader.ReuseRecord = true
	if c.settings.AllowRagged {
		reader.FieldsPerRecord = -1
	}

	maxRows := c.settings.MaxRows
	if maxRows <= 0 {
		maxRows = defaultMaxRows
	}

	rows := make([]any, 0, 16)
	var headers []string

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, false, fmt.Errorf("csv: %w", err)
		}

		if headers == nil {
			headers = headersFrom(record, !c.settings.NoHeader)
			if !c.settings.NoHeader {
				continue
			}
		}
		if len(rows) >= maxRows {
			return rows, headers, true, nil
		}
		rows = append(rows, c.row(record, headers))
	}

	if headers == nil {
		headers = []string{}
	}
	return rows, headers, false, nil
}

// headersFrom names the columns. A headerless file still produces objects — one
// output shape is worth more than a second, array-shaped one that every
// downstream expression would have to be written twice for.
func headersFrom(record []string, hasHeader bool) []string {
	names := make([]string, len(record))
	for i, field := range record {
		if hasHeader && strings.TrimSpace(field) != "" {
			names[i] = strings.TrimSpace(field)
			continue
		}
		names[i] = columnName(i)
	}
	return dedupe(names)
}

// dedupe keeps a repeated header from silently overwriting its twin — a file
// with two columns called "date" would otherwise lose one of them.
func dedupe(names []string) []string {
	seen := make(map[string]int, len(names))
	for i, name := range names {
		if n, ok := seen[name]; ok {
			seen[name] = n + 1
			names[i] = fmt.Sprintf("%s_%d", name, n+1)
			continue
		}
		seen[name] = 0
	}
	return names
}

func columnName(i int) string {
	return "col" + strconv.Itoa(i+1)
}

func (c *Component) row(record []string, headers []string) map[string]any {
	row := make(map[string]any, len(headers))
	for i, name := range headers {
		if i < len(record) {
			row[name] = c.value(record[i])
			continue
		}
		// A short row under allowRagged: the column exists in the table, so it
		// exists in the object, empty rather than absent. A missing key and an
		// empty one read differently downstream.
		row[name] = ""
	}
	for i := len(headers); i < len(record); i++ {
		row[columnName(i)] = c.value(record[i])
	}
	return row
}

func (c *Component) value(field string) any {
	if !c.settings.InferTypes {
		return field
	}
	return infer(field)
}

// infer converts only what is unambiguous. A leading zero means the field is an
// identifier that happens to be digits, and turning 007 into 7 loses the thing
// the column was for.
func infer(field string) any {
	trimmed := strings.TrimSpace(field)
	if trimmed == "" {
		return field
	}
	switch strings.ToLower(trimmed) {
	case "true":
		return true
	case "false":
		return false
	}
	if leadingZero(trimmed) {
		return field
	}
	if n, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return n
	}
	return field
}

func leadingZero(s string) bool {
	s = strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	return len(s) > 1 && s[0] == '0' && s[1] != '.'
}

func delimiter(name string) rune {
	switch name {
	case DelimiterSemicolon:
		return ';'
	case DelimiterTab:
		return '\t'
	case DelimiterPipe:
		return '|'
	default:
		return ','
	}
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
			Configuration: Response{Rows: c.settings.Rows},
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
