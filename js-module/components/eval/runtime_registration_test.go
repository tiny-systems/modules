package eval

import (
	"testing"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

// The registration that makes a definition runnable: without it the controller
// finds no factory for "js" and every definition reports uninstalled.
func TestJSRuntimeIsRegistered(t *testing.T) {
	f, ok := registry.GetRuntime("js")
	if !ok {
		t.Fatal("js runtime is not registered")
	}
	c, err := f(module.ComponentDefinition{
		Name:         "via_factory",
		Script:       `export default function(i){ return {n: i.n * 2} }`,
		InputSchema:  []byte(`{"type":"object","properties":{"n":{"type":"integer"}}}`),
		OutputSchema: []byte(`{"type":"object","properties":{"n":{"type":"integer"}}}`),
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if c.GetInfo().Name != "via_factory" {
		t.Errorf("name = %q", c.GetInfo().Name)
	}
	// The tag is what lets the controller tell its own installs from a compiled
	// component, so a definition can be edited without being refused as a shadow.
	var tagged bool
	for _, tg := range c.GetInfo().Tags {
		if tg == "Defined" {
			tagged = true
		}
	}
	if !tagged {
		t.Errorf("component is not tagged Defined: %v", c.GetInfo().Tags)
	}
}
