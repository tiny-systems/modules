// Package encode turns rows back into CSV.
//
// The counterpart to csv_decode, and the shape almost every report wants to
// leave in: a spreadsheet somebody opens, an attachment on a weekly email, an
// object written to a bucket. Building it with a template puts the quoting
// burden back on the author, and a value containing a comma quietly corrupts
// every column after it.
package encode

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "csv_encode"

	RequestPort  = "request"
	ResponsePort = "response"
	ErrorPort    = "error"

	DelimiterComma     = "comma"
	DelimiterSemicolon = "semicolon"
	DelimiterTab       = "tab"
	DelimiterPipe      = "pipe"
)

type Context any

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Passthrough, emitted with the encoded file."`
	Rows    []any   `json:"rows" required:"true" title:"Rows" description:"The objects to write, one row each. Map an upstream array straight in: {{$.rows}}. Wire collect before this when the rows arrive one at a time."`
}

type Response struct {
	Context Context  `json:"context,omitempty" configurable:"true" title:"Context"`
	Encoded string   `json:"encoded" title:"CSV" description:"The encoded file, ready for an attachment, an object write or an HTTP body."`
	Columns []string `json:"columns" title:"Columns" description:"The columns written, in order."`
	Count   int      `json:"count" title:"Count" description:"Rows written, not counting the header."`
}

type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

type Settings struct {
	Delimiter string `json:"delimiter" required:"true" default:"comma" enum:"comma,semicolon,tab,pipe" enumTitles:"Comma ,|Semicolon ;|Tab|Pipe |" title:"Delimiter"`

	// Without this the column order comes from sorting the keys, because a Go
	// map has no order and an unstable column order makes a diff between two
	// reports unreadable. Sorted is at least the same every time; declaring the
	// columns is how the author gets the order they meant.
	Columns []string `json:"columns" title:"Columns" description:"The columns to write, in order. Leave empty to use every key found across the rows, sorted — deterministic, but rarely the order a reader wants. Naming them also drops the fields a report should not carry."`

	// Same reasoning as csv_decode's noHeader: a bool cannot tell "unset" from
	// "false", so the zero value has to land on the case a node created without
	// touching this setting should get.
	OmitHeader bool `json:"omitHeader" title:"Omit The Header Row" description:"Off (default): the first line names the columns. On: rows only, for appending to a file that already has a header."`

	// A row missing a column is normal when the rows came from different
	// branches. An empty cell is a readable answer; refusing to produce the
	// file at all is not.
	FailOnMissing bool `json:"failOnMissing" title:"Fail When A Row Is Missing A Column" description:"Off (default): a row without one of the columns gets an empty cell. On: it fails, for a report where a blank would be read as a zero."`

	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to the error port instead of failing the run."`
}

type Component struct {
	module.Base
	settings Settings
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "CSV Encoder",
		Info: "Writes objects out as CSV, quoting whatever needs it — a value containing the delimiter, a quote or a " +
			"newline survives instead of shifting every column after it. " +
			"Set columns to fix the order and to leave out fields a report should not carry; without it the columns are " +
			"every key found, sorted, which is stable but rarely the order a reader wants. " +
			"Wire collect before this when the rows arrive one at a time.",
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

	rows, err := objects(in.Rows)
	if err != nil {
		return c.handleError(ctx, handler, in.Context, err)
	}

	columns := c.settings.Columns
	if len(columns) == 0 {
		columns = keysOf(rows)
	}

	encoded, err := c.write(rows, columns)
	if err != nil {
		return c.handleError(ctx, handler, in.Context, err)
	}

	return handler(ctx, ResponsePort, Response{
		Context: in.Context,
		Encoded: encoded,
		Columns: columns,
		Count:   len(rows),
	})
}

// objects refuses anything that is not a row. A string or a number in the array
// has no columns, and writing it as a one-cell line produces a file that opens
// but means nothing.
func objects(rows []any) ([]map[string]any, error) {
	converted := make([]map[string]any, 0, len(rows))
	for i, row := range rows {
		object, ok := row.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("row %d is %T, not an object — csv_encode writes objects as rows, one key per column", i, row)
		}
		converted = append(converted, object)
	}
	return converted, nil
}

// keysOf collects every key across all rows, not only the first: rows built by
// different branches of a flow can carry different fields, and a column that
// exists in row 40 but not row 1 would otherwise be dropped silently.
func keysOf(rows []map[string]any) []string {
	seen := make(map[string]struct{})
	for _, row := range rows {
		for key := range row {
			seen[key] = struct{}{}
		}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (c *Component) write(rows []map[string]any, columns []string) (string, error) {
	var out bytes.Buffer
	writer := csv.NewWriter(&out)
	writer.Comma = delimiter(c.settings.Delimiter)

	if !c.settings.OmitHeader {
		if err := writer.Write(columns); err != nil {
			return "", err
		}
	}

	record := make([]string, len(columns))
	for i, row := range rows {
		for j, column := range columns {
			value, ok := row[column]
			if !ok && c.settings.FailOnMissing {
				return "", fmt.Errorf("row %d has no %q — an empty cell would read as a value it does not have", i, column)
			}
			record[j] = cell(value)
		}
		if err := writer.Write(record); err != nil {
			return "", err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	return out.String(), nil
}

// cell renders one value. A float that is a whole number is written without the
// decimal point it never had — JSON has one number type, and a count arriving
// as 7 must not leave as 7.000000.
func cell(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case bool:
		return strconv.FormatBool(v)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return fmt.Sprint(v)
	}
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
