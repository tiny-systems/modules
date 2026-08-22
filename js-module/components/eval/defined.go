package eval

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
)

// A component defined by a TinyComponent resource rather than by Go source.
//
// It is js_eval with the settings fixed and a name of its own. Everything that
// makes js_eval work — the sobek runtime, the watchdog that interrupts a
// runaway loop, the JSON round-trip that lets a script push onto its input —
// is reused unchanged, because the difference is not how a script runs. The
// difference is that this one can be referred to.
//
// An inline js_eval script cannot be named, so the same reshape gets rewritten
// node after node and a fix reaches only the copy in front of you. A node using
// this names it instead, so editing the definition changes every instance.
//
// What it deliberately does NOT expose is a _settings port. The definition IS
// the settings; a per-node override would mean two nodes claiming the same
// component name while behaving differently, which is exactly the confusion
// this exists to remove.
type Defined struct {
	*Component

	name        string
	description string
	info        string

	// inSchema / outSchema are the declared port shapes, carried verbatim from
	// the resource so edges validate against what the author wrote rather than
	// against a value inferred from an example.
	inSchema  []byte
	outSchema []byte
}

// Definition is what a TinyComponent carries, reduced to what this needs.
// Taking a struct rather than the CRD type keeps this package free of a
// dependency on the API group.
type Definition struct {
	Name            string
	Description     string
	Info            string
	Script          string
	InputSchema     []byte
	OutputSchema    []byte
	EnableErrorPort bool
	TimeoutSeconds  int
}

// NewDefined compiles a definition into a runnable component.
//
// Compilation happens here, not on first message: a script that will never
// parse should be reported when the resource is applied, while somebody is
// looking at it, rather than failing the first time real traffic arrives.
func NewDefined(def Definition) (*Defined, error) {
	if def.Name == "" {
		return nil, fmt.Errorf("component name is required")
	}
	if def.Script == "" {
		return nil, fmt.Errorf("script is required")
	}
	// Both schemas are required. An edge cannot be checked against a shape
	// nobody declared, and js_eval's own settings say as much about its output:
	// leaving it empty makes every edge out of the node unverifiable.
	if len(def.InputSchema) == 0 {
		return nil, fmt.Errorf("input schema is required")
	}
	if len(def.OutputSchema) == 0 {
		return nil, fmt.Errorf("output schema is required")
	}

	in, err := decodeShape(def.InputSchema)
	if err != nil {
		return nil, fmt.Errorf("input schema: %w", err)
	}
	out, err := decodeShape(def.OutputSchema)
	if err != nil {
		return nil, fmt.Errorf("output schema: %w", err)
	}

	c := &Component{
		settings: Settings{
			EnableErrorPort: def.EnableErrorPort,
			TimeoutSeconds:  def.TimeoutSeconds,
			InputData:       in,
			OutputData:      out,
			Script:          Script{Name: mainModule, Content: def.Script},
		},
	}
	if err := c.init(c.settings); err != nil {
		return nil, fmt.Errorf("script: %w", err)
	}

	return &Defined{
		Component:   c,
		name:        def.Name,
		description: def.Description,
		info:        def.Info,
		inSchema:    def.InputSchema,
		outSchema:   def.OutputSchema,
	}, nil
}

// decodeShape turns a declared JSON Schema into the example-shaped value the
// port configuration carries. A schema with no example decodes to nil, which is
// still a valid configuration — the schema itself travels on the port.
func decodeShape(schema []byte) (any, error) {
	var doc map[string]any
	if err := json.Unmarshal(schema, &doc); err != nil {
		return nil, err
	}
	// An `example` on the root is the closest thing to the value js_eval's own
	// settings hold, so prefer it when the author supplied one.
	if ex, ok := doc["example"]; ok {
		return ex, nil
	}
	return nil, nil
}

func (d *Defined) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        d.name,
		Description: d.description,
		Info:        d.info,
		Tags:        []string{"Defined"},
	}
}

// Ports carries the DECLARED schemas rather than shapes inferred from example
// data, and omits _settings: the resource is the settings.
func (d *Defined) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          RequestPort,
			Label:         "Request",
			Position:      module.Left,
			Configuration: Request{InputData: d.Component.settings.InputData},
			Schema:        d.inSchema,
		},
		{
			Name:          ResponsePort,
			Label:         "Response",
			Position:      module.Right,
			Source:        true,
			Configuration: Response{OutputData: d.Component.settings.OutputData},
			Schema:        d.outSchema,
		},
	}
	if !d.Component.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name:          ErrorPort,
		Label:         "Error",
		Position:      module.Bottom,
		Source:        true,
		Configuration: Error{},
	})
}

// Instance returns a fresh runner sharing this definition.
//
// The scheduler builds one per node, and each needs its own sobek runtime —
// they are single-threaded, and two nodes handling messages concurrently
// through one runtime corrupts evaluation state. Recompiling is what gives each
// its own; a compile failure here cannot be reported, so it falls back to the
// already-compiled component rather than returning something that will panic.
func (d *Defined) Instance() module.Component {
	fresh, err := NewDefined(Definition{
		Name:            d.name,
		Description:     d.description,
		Info:            d.info,
		Script:          d.Component.settings.Script.Content,
		InputSchema:     d.inSchema,
		OutputSchema:    d.outSchema,
		EnableErrorPort: d.Component.settings.EnableErrorPort,
		TimeoutSeconds:  d.Component.settings.TimeoutSeconds,
	})
	if err != nil {
		return d
	}
	return fresh
}

// OnSettings refuses per-node configuration.
//
// Silently accepting it would let two nodes name one component and behave
// differently, which is the confusion a named definition exists to remove. The
// settings port is not exposed, so this is only reachable if something sends to
// it anyway.
func (d *Defined) OnSettings(_ context.Context, _ any) error {
	return fmt.Errorf("%s is defined by a TinyComponent resource; edit the resource rather than the node", d.name)
}

// Handle delegates to js_eval unchanged — running the script is the part that
// was already right.
func (d *Defined) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port == v1alpha1.SettingsPort {
		return module.Fail(fmt.Errorf("%s takes no settings", d.name))
	}
	return d.Component.Handle(ctx, handler, port, msg)
}

var (
	_ module.Component       = (*Defined)(nil)
	_ module.SettingsHandler = (*Defined)(nil)
)
