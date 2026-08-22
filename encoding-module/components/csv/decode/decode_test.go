package decode

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

func decoded(t *testing.T, csv string, settings Settings) Response {
	t.Helper()
	port, msg, err := run(t, Request{Encoded: csv}, settings)
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

func rows(t *testing.T, out Response) []map[string]any {
	t.Helper()
	list, ok := out.Rows.([]any)
	if !ok {
		t.Fatalf("rows is %T, want a list", out.Rows)
	}
	converted := make([]map[string]any, len(list))
	for i, row := range list {
		m, ok := row.(map[string]any)
		if !ok {
			t.Fatalf("row %d is %T, want an object", i, row)
		}
		converted[i] = m
	}
	return converted
}

func TestHeaderNamesTheColumns(t *testing.T) {
	out := decoded(t, "name,restarts\napi-1,0\napi-2,7\n", Settings{})
	if out.Count != 2 {
		t.Fatalf("count = %d, want 2 — the header row must not be a record", out.Count)
	}
	got := rows(t, out)
	if got[0]["name"] != "api-1" || got[1]["restarts"] != "7" {
		t.Fatalf("rows = %v", got)
	}
	if strings.Join(out.Headers, ",") != "name,restarts" {
		t.Errorf("headers = %v", out.Headers)
	}
}

// The reason this component exists: a hand-written split shifts every column
// after a quoted comma.
func TestQuotedFieldsAndEmbeddedNewlines(t *testing.T) {
	got := rows(t, decoded(t, "name,note\n\"Smith, John\",\"line one\nline two\"\n", Settings{}))
	if got[0]["name"] != "Smith, John" {
		t.Errorf("name = %q — the quoted comma split the row", got[0]["name"])
	}
	if got[0]["note"] != "line one\nline two" {
		t.Errorf("note = %q — the embedded newline ended the record", got[0]["note"])
	}
}

func TestSemicolonAndTabDelimiters(t *testing.T) {
	for name, tc := range map[string]struct{ csv, delim string }{
		"semicolon": {"a;b\n1;2\n", DelimiterSemicolon},
		"tab":       {"a\tb\n1\t2\n", DelimiterTab},
		"pipe":      {"a|b\n1|2\n", DelimiterPipe},
	} {
		got := rows(t, decoded(t, tc.csv, Settings{Delimiter: tc.delim}))
		if len(got) != 1 || got[0]["b"] != "2" {
			t.Errorf("%s: rows = %v", name, got)
		}
	}
}

// A headerless file still produces objects, so a downstream expression is
// written once rather than once per shape.
func TestHeaderlessFileGetsGeneratedColumnNames(t *testing.T) {
	out := decoded(t, "api-1,0\napi-2,7\n", Settings{NoHeader: true})
	if out.Count != 2 {
		t.Fatalf("count = %d, want 2 — the first row is data when there is no header", out.Count)
	}
	got := rows(t, out)
	if got[0]["col1"] != "api-1" || got[0]["col2"] != "0" {
		t.Fatalf("rows = %v, want col1/col2 keys", got)
	}
}

// Two columns with the same name would otherwise overwrite each other and the
// file would decode to fewer fields than it has.
func TestRepeatedHeaderIsKept(t *testing.T) {
	out := decoded(t, "date,value,date\n2026-01-01,3,2026-02-01\n", Settings{})
	got := rows(t, out)
	if got[0]["date"] != "2026-01-01" || got[0]["date_1"] != "2026-02-01" {
		t.Fatalf("rows = %v, want both date columns present", got)
	}
}

// Off by default: a shifted file must report itself rather than decode into
// plausible nonsense.
func TestRaggedRowFailsByDefault(t *testing.T) {
	if _, _, err := run(t, Request{Encoded: "a,b,c\n1,2\n"}, Settings{}); err == nil {
		t.Fatal("a row with fewer fields than the header was accepted")
	}
}

func TestRaggedRowIsPaddedWhenAllowed(t *testing.T) {
	got := rows(t, decoded(t, "a,b,c\n1,2\n", Settings{AllowRagged: true}))
	if got[0]["c"] != "" {
		t.Fatalf("c = %v, want an empty string — a missing key and an empty one read differently downstream", got[0]["c"])
	}
}

func TestExtraFieldsLandInGeneratedColumns(t *testing.T) {
	got := rows(t, decoded(t, "a,b\n1,2,3\n", Settings{AllowRagged: true}))
	if got[0]["col3"] != "3" {
		t.Fatalf("rows = %v, want the extra field kept as col3", got)
	}
}

// Everything is a string until asked otherwise, so $.restarts > 3 needs the
// setting — and the setting is what makes it work.
func TestInferTypesProducesNumbersAndBooleans(t *testing.T) {
	got := rows(t, decoded(t, "restarts,ready\n7,true\n", Settings{InferTypes: true}))
	if n, ok := got[0]["restarts"].(float64); !ok || n != 7 {
		t.Errorf("restarts = %#v, want the number 7", got[0]["restarts"])
	}
	if b, ok := got[0]["ready"].(bool); !ok || !b {
		t.Errorf("ready = %#v, want true", got[0]["ready"])
	}
}

// The reason inference is off by default. A zip code is not a number, and
// turning 01234 into 1234 destroys the thing the column was for.
func TestInferTypesLeavesLeadingZeroIdentifiersAlone(t *testing.T) {
	got := rows(t, decoded(t, "zip,order\n01234,007\n", Settings{InferTypes: true}))
	if got[0]["zip"] != "01234" || got[0]["order"] != "007" {
		t.Fatalf("rows = %v — a leading-zero identifier was converted to a number", got)
	}
}

func TestEverythingIsAStringByDefault(t *testing.T) {
	got := rows(t, decoded(t, "n\n7\n", Settings{}))
	if _, ok := got[0]["n"].(string); !ok {
		t.Fatalf("n = %#v, want a string when inferTypes is off", got[0]["n"])
	}
}

func TestMaxRowsTruncatesAndReportsIt(t *testing.T) {
	out := decoded(t, "n\n1\n2\n3\n4\n", Settings{MaxRows: 2})
	if out.Count != 2 {
		t.Fatalf("count = %d, want the 2-row ceiling", out.Count)
	}
	if !out.Truncated {
		t.Fatal("truncated is false after hitting maxRows")
	}
}

func TestEmptyInputIsNotAnError(t *testing.T) {
	out := decoded(t, "", Settings{})
	if out.Count != 0 {
		t.Fatalf("count = %d, want 0", out.Count)
	}
	if out.Rows == nil {
		t.Error("rows should be an empty array, not null — a downstream array_split over null is a different failure")
	}
}

// A file with only a header decodes to no rows, which is an answer, not a
// failure — an export with nothing to report looks exactly like this.
func TestHeaderOnlyFileIsZeroRows(t *testing.T) {
	out := decoded(t, "name,restarts\n", Settings{})
	if out.Count != 0 {
		t.Fatalf("count = %d, want 0", out.Count)
	}
	if len(out.Headers) != 2 {
		t.Errorf("headers = %v, want the columns even with no records", out.Headers)
	}
}

func TestTrimSpaceHandlesSpacedDelimiters(t *testing.T) {
	got := rows(t, decoded(t, "a, b\n1, 2\n", Settings{TrimSpace: true}))
	if got[0]["b"] != "2" {
		t.Fatalf("rows = %v, want the space after the delimiter ignored", got)
	}
}

func TestMalformedQuotingFailsUnlessTolerated(t *testing.T) {
	bad := "a,b\n1,he said \"hi\" loudly\n"
	if _, _, err := run(t, Request{Encoded: bad}, Settings{}); err == nil {
		t.Fatal("a bare quote inside an unquoted field was accepted with lazyQuotes off")
	}
	if _, _, err := run(t, Request{Encoded: bad}, Settings{LazyQuotes: true}); err != nil {
		t.Fatalf("with lazyQuotes on it must decode: %v", err)
	}
}

func TestContextIsCarried(t *testing.T) {
	_, msg, err := run(t, Request{
		Context: map[string]any{"file": "report.csv"},
		Encoded: "a\n1\n",
	}, Settings{})
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	carried, _ := msg.(Response).Context.(map[string]any)
	if carried["file"] != "report.csv" {
		t.Fatalf("context = %v, want it carried", msg.(Response).Context)
	}
}

func TestErrorPortRoutesInsteadOfFailing(t *testing.T) {
	port, msg, err := run(t, Request{Encoded: "a,b,c\n1,2\n"}, Settings{EnableErrorPort: true})
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
