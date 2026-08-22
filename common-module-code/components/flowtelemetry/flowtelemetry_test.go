package flowtelemetry

import "testing"

// The bounds must leave room to page: a scan cap below one page of results
// would stop before the second page and report every window as truncated.
func TestScanBoundsAllowPaging(t *testing.T) {
	if maxScan <= maxTraces {
		t.Errorf("maxScan (%d) must exceed maxTraces (%d), or errorsOnly can never read past its own results", maxScan, maxTraces)
	}
	if maxPages < 2 {
		t.Errorf("maxPages (%d) must allow more than one page", maxPages)
	}
}

// Truncated is the honesty flag: an empty errorsOnly result means "nothing
// failed" only when the whole window was read. These are the cases that
// decide it.
func TestTruncationMeaning(t *testing.T) {
	cases := []struct {
		name            string
		total, offset   int64
		scanned         int
		gotEnoughTraces bool
		want            bool
	}{
		{"whole window read", 40, 40, 40, false, false},
		{"stopped with results left", 500, 200, 200, true, true},
		{"filled the reply exactly at the end", 200, 200, 200, true, false},
		{"scan cap hit", 100000, 5000, maxScan, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var truncated bool
			if c.gotEnoughTraces {
				truncated = c.total > 0 && c.offset < c.total
			} else if c.scanned >= maxScan {
				truncated = true
			}
			if truncated != c.want {
				t.Errorf("truncated = %v, want %v", truncated, c.want)
			}
		})
	}
}
