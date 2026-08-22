package budgetguard

import "testing"

func TestExceeded(t *testing.T) {
	set := Settings{MaxIterations: 3, MaxTokens: 1000, MaxCostUSD: 0.50}
	cases := []struct {
		name        string
		iteration   int
		spentTokens int
		spentUSD    float64
		wantStop    bool
	}{
		{"first pass", 1, 10, 0.01, false},
		{"at the iteration limit still runs", 3, 10, 0.01, false},
		{"one past the iteration limit stops", 4, 10, 0.01, true},
		{"under the token budget", 2, 999, 0.01, false},
		{"over the token budget", 2, 1001, 0.01, true},
		{"over the cost budget", 2, 10, 0.51, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := exceeded(set, c.iteration, c.spentTokens, c.spentUSD) != ""
			if got != c.wantStop {
				t.Errorf("exceeded = %v, want %v", got, c.wantStop)
			}
		})
	}
}

// Optional ceilings must not fire when unset, or a loop that never reports
// tokens would stop on its first pass.
func TestZeroCeilingsAreIgnored(t *testing.T) {
	set := Settings{MaxIterations: 5}
	if r := exceeded(set, 1, 999999, 999.0); r != "" {
		t.Errorf("unset token/cost ceilings should not stop the loop, got %q", r)
	}
	if r := exceeded(set, 6, 0, 0); r == "" {
		t.Error("iteration ceiling must always apply")
	}
}

// The reason has to name the ceiling: "the agent stopped" is not actionable.
func TestReasonNamesTheCeiling(t *testing.T) {
	set := Settings{MaxIterations: 2, MaxTokens: 100, MaxCostUSD: 1}
	if r := exceeded(set, 3, 0, 0); r == "" || !contains(r, "iteration") {
		t.Errorf("reason = %q, want it to mention iterations", r)
	}
	if r := exceeded(set, 1, 101, 0); r == "" || !contains(r, "token") {
		t.Errorf("reason = %q, want it to mention tokens", r)
	}
	if r := exceeded(set, 1, 0, 1.5); r == "" || !contains(r, "cost") {
		t.Errorf("reason = %q, want it to mention cost", r)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// A ceiling of zero would let everything through, defeating the purpose.
func TestSettingsRejectNoCeiling(t *testing.T) {
	c := &Component{}
	if err := c.OnSettings(nil, Settings{MaxIterations: 0}); err == nil {
		t.Error("maxIterations 0 must be rejected")
	}
	if err := c.OnSettings(nil, Settings{MaxIterations: 5}); err != nil {
		t.Errorf("valid settings rejected: %v", err)
	}
}
