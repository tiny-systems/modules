package eval

import (
	"context"
	"strings"
	"testing"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
)

func reshapeDefinition() Definition {
	return Definition{
		Name:         "summarise_pods",
		Description:  "Pod list to a summary line",
		Script:       `export default function(i){ return {count: i.pods.length, first: i.pods[0]} }`,
		InputSchema:  []byte(`{"type":"object","properties":{"pods":{"type":"array","items":{"type":"string"}}}}`),
		OutputSchema: []byte(`{"type":"object","properties":{"count":{"type":"integer"},"first":{"type":"string"}}}`),
	}
}

// The point of the whole exercise: a component that exists because someone
// wrote a resource, not because someone shipped a binary.
func TestADefinedComponentRunsItsScript(t *testing.T) {
	c, err := NewDefined(reshapeDefinition())
	if err != nil {
		t.Fatalf("NewDefined: %v", err)
	}
	if got := c.GetInfo().Name; got != "summarise_pods" {
		t.Errorf("name = %q", got)
	}

	var got any
	res := c.Handle(context.Background(),
		func(_ context.Context, port string, msg any) module.Result {
			if port == ResponsePort {
				got = msg.(Response).OutputData
			}
			return module.Result{}
		},
		RequestPort,
		Request{InputData: map[string]any{"pods": []any{"web-1", "web-2"}}},
	)
	if err := res.Err(); err != nil {
		t.Fatalf("handle: %v", err)
	}
	out, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("output = %#v", got)
	}
	if out["first"] != "web-1" {
		t.Errorf("first = %v, want web-1", out["first"])
	}
}

// A script that will never parse must be rejected when the resource is applied,
// while somebody is looking at it — not on the first message, hours later, in
// the middle of real traffic.
func TestABrokenScriptFailsAtDefinitionTime(t *testing.T) {
	d := reshapeDefinition()
	d.Script = `this is not javascript {{{`
	if _, err := NewDefined(d); err == nil {
		t.Fatal("a script that cannot compile was accepted")
	}
}

// Both schemas are required: an edge cannot be validated against a shape nobody
// declared, in either direction.
func TestSchemasAreRequired(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*Definition)
	}{
		{"no input", func(d *Definition) { d.InputSchema = nil }},
		{"no output", func(d *Definition) { d.OutputSchema = nil }},
		{"no script", func(d *Definition) { d.Script = "" }},
		{"no name", func(d *Definition) { d.Name = "" }},
	} {
		d := reshapeDefinition()
		tc.mut(&d)
		if _, err := NewDefined(d); err == nil {
			t.Errorf("%s was accepted", tc.name)
		}
	}
}

// The declared schemas travel on the ports, so edges validate against what the
// author wrote rather than a shape guessed from example data. And no _settings
// port: the resource is the settings.
func TestPortsCarryTheDeclaredSchemasAndNoSettings(t *testing.T) {
	c, err := NewDefined(reshapeDefinition())
	if err != nil {
		t.Fatal(err)
	}
	var sawRequest, sawResponse bool
	for _, p := range c.Ports() {
		switch p.Name {
		case RequestPort:
			sawRequest = true
			if !strings.Contains(string(p.Schema), `"pods"`) {
				t.Errorf("request port lost the declared schema: %s", p.Schema)
			}
		case ResponsePort:
			sawResponse = true
			if !strings.Contains(string(p.Schema), `"count"`) {
				t.Errorf("response port lost the declared schema: %s", p.Schema)
			}
		case v1alpha1.SettingsPort:
			t.Error("a defined component exposed a settings port; the resource is the settings")
		}
	}
	if !sawRequest || !sawResponse {
		t.Error("request/response ports missing")
	}
}

// Each node needs its own sobek runtime — they are single-threaded, and two
// nodes handling messages through one would corrupt evaluation state.
func TestInstanceIsAFreshRuntime(t *testing.T) {
	c, err := NewDefined(reshapeDefinition())
	if err != nil {
		t.Fatal(err)
	}
	other, ok := c.Instance().(*Defined)
	if !ok {
		t.Fatal("Instance did not return a Defined")
	}
	if other == c {
		t.Error("Instance returned the same component")
	}
	if other.Component.runtime == c.Component.runtime {
		t.Error("two instances share one sobek runtime")
	}
	if other.GetInfo().Name != c.GetInfo().Name {
		t.Error("the instance lost its identity")
	}
}

// Per-node settings are refused: two nodes naming one component must not be
// able to behave differently.
func TestPerNodeSettingsAreRefused(t *testing.T) {
	c, _ := NewDefined(reshapeDefinition())
	if err := c.OnSettings(context.Background(), Settings{}); err == nil {
		t.Error("per-node settings were accepted")
	}
	res := c.Handle(context.Background(),
		func(context.Context, string, any) module.Result { return module.Result{} },
		v1alpha1.SettingsPort, Settings{})
	if res.Err() == nil {
		t.Error("a message to the settings port was accepted")
	}
}
