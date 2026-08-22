package server

import (
	"strings"
	"testing"
)

// A router branching on requestURI matched "/webhook" until somebody appended
// ?retry=1, and then silently stopped matching — the flow still ran, down the
// wrong branch, with nothing reporting a problem. Path is the field to branch
// on, and it must never carry the query string.
func TestPathSegments_DropsEmptyParts(t *testing.T) {
	cases := map[string][]string{
		"/hooks/github":   {"hooks", "github"},
		"/hooks/github/":  {"hooks", "github"},
		"//hooks//github": {"hooks", "github"},
		"/":               {},
		"":                {},
		"/single":         {"single"},
	}
	for path, want := range cases {
		got := pathSegments(path)
		if len(got) != len(want) {
			t.Errorf("%q gave %v, want %v", path, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%q gave %v, want %v", path, got, want)
				break
			}
		}
	}
}

// A trailing slash is the same route to anyone writing it.
func TestPathSegments_TrailingSlashIsTheSameRoute(t *testing.T) {
	with := strings.Join(pathSegments("/api/v1/orders/"), "/")
	without := strings.Join(pathSegments("/api/v1/orders"), "/")
	if with != without {
		t.Fatalf("%q and %q segment differently", with, without)
	}
}

// Segments exist because the expression engine can split a string but cannot
// index into the result, so a router needing one part of a path had nowhere
// to get it.
func TestPathSegments_AreIndexable(t *testing.T) {
	got := pathSegments("/hooks/github/push")
	if len(got) < 2 || got[1] != "github" {
		t.Fatalf("got %v, want the second segment to be addressable", got)
	}
}
