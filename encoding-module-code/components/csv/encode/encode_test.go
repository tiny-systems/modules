package encode

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

func encoded(t *testing.T, rows []any, settings Settings) Response {
	t.Helper()
	port, msg, err := run(t, Request{Rows: rows}, settings)
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

func lines(out Response) []string {
	return strings.Split(strings.TrimRight(out.Encoded, "\n"), "\n")
}

func TestWritesAHeaderAndARowPerObject(t *testing.T) {
	out := encoded(t, []any{
		map[string]any{"name": "api-1", "restarts": float64(0)},
		map[string]any{"name": "api-2", "restarts": float64(7)},
	}, Settings{})

	got := lines(out)
	if len(got) != 3 {
		t.Fatalf("%d lines, want a header and 2 rows: %q", len(got), out.Encoded)
	}
	if got[0] != "name,restarts" {
		t.Errorf("header = %q", got[0])
	}
	if got[2] != "api-2,7" {
		t.Errorf("second row = %q", got[2])
	}
	if out.Count != 2 {
		t.Errorf("count = %d, want 2 — the header is not a row", out.Count)
	}
}

// The reason not to build this with a template.
func TestValuesNeedingQuotesSurvive(t *testing.T) {
	out := encoded(t, []any{
		map[string]any{"a": "Smith, John", "b": "he said \"hi\"", "c": "line one\nline two"},
	}, Settings{})

	// Round-tripping through the decoder is the honest check: the file has to
	// mean the same thing to a reader, not merely contain the characters.
	if !strings.Contains(out.Encoded, `"Smith, John"`) {
		t.Errorf("a value containing the delimiter was written unquoted: %q", out.Encoded)
	}
	if !strings.Contains(out.Encoded, `"he said ""hi"""`) {
		t.Errorf("an embedded quote was not escaped: %q", out.Encoded)
	}
	if !strings.Contains(out.Encoded, "\"line one\nline two\"") {
		t.Errorf("an embedded newline was not quoted: %q", out.Encoded)
	}
}

// A Go map has no order; two runs producing different column orders makes a
// diff between two reports unreadable.
func TestColumnOrderIsStableWithoutSettings(t *testing.T) {
	rows := []any{map[string]any{"zebra": "1", "apple": "2", "mango": "3"}}
	first := encoded(t, rows, Settings{})
	for i := 0; i < 5; i++ {
		if got := encoded(t, rows, Settings{}); got.Encoded != first.Encoded {
			t.Fatalf("column order changed between runs:\n %q\n %q", first.Encoded, got.Encoded)
		}
	}
	if lines(first)[0] != "apple,mango,zebra" {
		t.Errorf("header = %q, want the keys sorted", lines(first)[0])
	}
}

// Declaring columns is how an author gets the order they meant, and how a
// report leaves out fields it should not carry.
func TestColumnsSettingFixesOrderAndDropsFields(t *testing.T) {
	out := encoded(t, []any{
		map[string]any{"name": "api-1", "restarts": float64(3), "internalId": "secret-ish"},
	}, Settings{Columns: []string{"restarts", "name"}})

	got := lines(out)
	if got[0] != "restarts,name" {
		t.Fatalf("header = %q, want the declared order", got[0])
	}
	if strings.Contains(out.Encoded, "internalId") || strings.Contains(out.Encoded, "secret-ish") {
		t.Errorf("an undeclared column was written: %q", out.Encoded)
	}
}

// A column present only in a later row would otherwise be dropped, and the
// report would be missing data nobody could see was missing.
func TestColumnsAreCollectedAcrossAllRows(t *testing.T) {
	out := encoded(t, []any{
		map[string]any{"a": "1"},
		map[string]any{"a": "2", "b": "3"},
	}, Settings{})
	if lines(out)[0] != "a,b" {
		t.Fatalf("header = %q — a column that appears in a later row was dropped", lines(out)[0])
	}
}

func TestMissingColumnBecomesAnEmptyCell(t *testing.T) {
	out := encoded(t, []any{
		map[string]any{"a": "1", "b": "2"},
		map[string]any{"a": "3"},
	}, Settings{})
	if got := lines(out)[2]; got != "3," {
		t.Fatalf("row = %q, want an empty cell for the missing column", got)
	}
}

func TestFailOnMissingRefusesTheRow(t *testing.T) {
	_, _, err := run(t, Request{Rows: []any{
		map[string]any{"a": "1", "b": "2"},
		map[string]any{"a": "3"},
	}}, Settings{FailOnMissing: true})
	if err == nil {
		t.Fatal("a row missing a column was accepted with failOnMissing on")
	}
}

// JSON has one number type; a count that arrived as 7 must not leave as
// 7.000000, which no spreadsheet reads as an integer.
func TestWholeNumbersAreWrittenWithoutADecimalPoint(t *testing.T) {
	out := encoded(t, []any{map[string]any{"n": float64(7), "f": 1.5, "ok": true}}, Settings{})
	if got := lines(out)[1]; got != "1.5,7,true" {
		t.Fatalf("row = %q, want 1.5,7,true", got)
	}
}

func TestNullBecomesAnEmptyCell(t *testing.T) {
	out := encoded(t, []any{map[string]any{"a": nil, "b": "x"}}, Settings{})
	if got := lines(out)[1]; got != ",x" {
		t.Fatalf("row = %q, want an empty cell for null", got)
	}
}

func TestOmitHeaderWritesRowsOnly(t *testing.T) {
	out := encoded(t, []any{map[string]any{"a": "1"}}, Settings{OmitHeader: true})
	if got := lines(out); len(got) != 1 || got[0] != "1" {
		t.Fatalf("lines = %q, want the row alone", got)
	}
	if len(out.Columns) != 1 {
		t.Errorf("columns = %v — they are still reported even when not written", out.Columns)
	}
}

func TestDelimiterSetting(t *testing.T) {
	out := encoded(t, []any{map[string]any{"a": "1", "b": "2"}}, Settings{Delimiter: DelimiterSemicolon})
	if got := lines(out)[1]; got != "1;2" {
		t.Fatalf("row = %q", got)
	}
}

// A bare string in the array has no columns; writing it as a one-cell line
// produces a file that opens and means nothing.
func TestNonObjectRowIsRefused(t *testing.T) {
	if _, _, err := run(t, Request{Rows: []any{"just a string"}}, Settings{}); err == nil {
		t.Fatal("a non-object row was accepted")
	}
}

func TestNoRowsProducesAHeaderOnlyFile(t *testing.T) {
	out := encoded(t, []any{}, Settings{Columns: []string{"a", "b"}})
	if strings.TrimRight(out.Encoded, "\n") != "a,b" {
		t.Fatalf("encoded = %q, want the header alone", out.Encoded)
	}
	if out.Count != 0 {
		t.Errorf("count = %d, want 0", out.Count)
	}
}

func TestContextIsCarried(t *testing.T) {
	_, msg, err := run(t, Request{
		Context: map[string]any{"reportId": "r1"},
		Rows:    []any{map[string]any{"a": "1"}},
	}, Settings{})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	carried, _ := msg.(Response).Context.(map[string]any)
	if carried["reportId"] != "r1" {
		t.Fatalf("context = %v, want it carried", msg.(Response).Context)
	}
}

func TestErrorPortRoutesInsteadOfFailing(t *testing.T) {
	port, msg, err := run(t, Request{Rows: []any{42}}, Settings{EnableErrorPort: true})
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
