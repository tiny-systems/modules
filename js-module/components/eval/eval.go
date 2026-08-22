package eval

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/grafana/sobek"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/modules/js-module/modules"
	"sync"
	"testing/fstest"
	"time"
)

const (
	ComponentName = "js_eval"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

const (
	mainModule    = "main.js"
	defaultExport = "default"
)

type Context any
type InputData any
type OutputData any

type Script struct {
	Name    string `json:"name" required:"true" title:"File name" Description:"e.g. utils.js"`
	Content string `json:"content" required:"true" language:"javascript" title:"Javascript code" format:"code"`
}

// ScriptItem to avoid confusion of Script definition generated from Scripts array and from Script property
type ScriptItem Script

type Settings struct {
	EnableErrorPort bool         `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"If error happen, error port will emit an error message" tab:"Settings"`
	InputData       InputData    `json:"inputData" configurable:"true" title:"Input object" description:"Schema and example data of the script's input. Downstream edges feeding this node must produce this shape." tab:"Settings"`
	OutputData      OutputData   `json:"outputData" required:"true" configurable:"true" title:"Output object" description:"Schema and example data of the script's output. REQUIRED: downstream edges from this node are validated against this shape, so leaving it empty makes every edge out of this node unverifiable. Set it to an example of exactly what your script returns." tab:"Settings"`
	Script          Script       `json:"script" required:"true" title:"Script" description:"Full ECMAScript 5.1 support. Experimental ESM support. Please CDN only ESM modules" tab:"Main script"`
	Modules         []ScriptItem `json:"modules" required:"true" title:"Modules" description:"Full ECMAScript 5.1 support. Experimental ESM support. Please CDN only ESM modules." uniqueItems:"true" tab:"Includes"`
	TimeoutSeconds  int          `json:"timeoutSeconds" title:"Timeout (seconds)" description:"Wall-clock cap for one invocation; a script that exceeds it (e.g. an accidental infinite loop) is interrupted and fails the hop. 0 = default 30." tab:"Settings"`
}

type Error struct {
	Context Context `json:"context"`
	Error   string  `json:"error"`
}

type Request struct {
	Context   Context   `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary message to be send alongside with rendered content"`
	InputData InputData `json:"inputData,omitempty" configurable:"true" title:"Input data" description:"Input data" prompt:"generate JSON schema"`
}

type Response struct {
	Context    Context    `json:"context"`
	OutputData OutputData `json:"outputData"`
}

type Component struct {
	settings Settings
	handler  sobek.Callable
	runtime  *sobek.Runtime
	// mu serializes every use of the runtime. Sobek runtimes are
	// single-threaded, but the scheduler dispatches each message in its
	// own goroutine and OnSettings swaps the runtime under live traffic —
	// unlocked concurrent entry corrupts evaluation state.
	mu sync.Mutex
}

var defaultEngineSettings = Settings{
	Script: Script{
		Name: mainModule,
		Content: `import {lodash} from "https://cdn.jsdelivr.net/npm/@esm-bundle/lodash@4.17.21/+esm";
import {typeOf} from "utils.js";
export default function(inp) {
  return lodash.isFunction(typeOf) + typeOf(inp)
}`,
	},
	Modules: []ScriptItem{
		{
			Name:    "utils.js",
			Content: `export function typeOf(input) {return typeof input}`,
		},
	},
}

func (h *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "JS Eval",
		Info:        "Escape hatch: run arbitrary logic inline when no typed component does what you need — reshape data, branch, compute, glue a flow together. No compile step, runs instantly (prefer this over wasm_eval for interactive work). Each invocation is capped by timeoutSeconds (default 30s) — runaway loops are interrupted, not wedged. JavaScript evaluation (ECMAScript 5.1 + ESM imports). Script must export a default function: export default function(inputData) { return { result: inputData.value * 2 }; }. The function receives inputData as its only argument; the return value becomes outputData on the response port. Context is NOT available inside the script — it passes through automatically from request to response. Define settings.inputData (example + schema of the script's argument) and settings.outputData (example + schema of the script's return) so the validator can check incoming and outgoing edges without running the flow.",
		Tags:        []string{"js", "javascript", "engine"},
	}
}

// OnSettings stores the component settings.
func (h *Component) OnSettings(_ context.Context, msg any) error {

	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.settings = in
	return h.init(in)
}

// Handle dispatches the RequestPort. System ports go through capabilities.
func (h *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}

	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid input"))
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.handler == nil {
		return module.Fail(fmt.Errorf("handler is not initialised"))
	}

	// Watchdog: interrupt the VM when the wall-clock cap passes or the hop's
	// context is cancelled — otherwise an accidental while(true) wedges this
	// goroutine (and, with the lock, the whole node) forever. The interrupt
	// surfaces as an error from the call; ClearInterrupt re-arms the runtime
	// for the next message.
	timeout := time.Duration(h.settings.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go func(rt *sobek.Runtime) {
		select {
		case <-watchdogDone:
		case <-ctx.Done():
			rt.Interrupt(fmt.Sprintf("script interrupted: %v", ctx.Err()))
		case <-time.After(timeout):
			rt.Interrupt(fmt.Sprintf("script timeout after %s (timeoutSeconds setting)", timeout))
		}
	}(h.runtime)
	defer h.runtime.ClearInterrupt()

	// Round-trip inputData through JSON.parse so the script receives a native,
	// mutable JS value. ToValue wraps Go slices/maps as proxies whose in-place
	// mutations (Array.push, etc.) are lost when the return value is Export()ed
	// — the classic broken ReAct-fold symptom (had to build a fresh array
	// instead of pushing onto the input). With a native value, push works.
	arg, err := h.jsValue(in.InputData)
	if err != nil {
		if !h.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return handler(ctx, ErrorPort, Error{Context: in.Context, Error: err.Error()})
	}

	res, err := h.handler(sobek.Undefined(), arg)
	if err != nil {
		if !h.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return handler(ctx, ErrorPort, Error{
			Context: in.Context,
			Error:   err.Error(),
		})
	}

	result := res.Export()

	if pr, ok := result.(*sobek.Promise); ok {
		if pr.State() != sobek.PromiseStateFulfilled {
			return module.Fail(fmt.Errorf("%s", pr.Result().Export()))
		}
		result = pr.Result().Export()
	}

	return handler(ctx, ResponsePort, Response{
		Context:    in.Context,
		OutputData: result,
	})
}

// jsValue converts a Go value into a native (JS-owned) sobek value by round-
// tripping through JSON.parse, so a script can mutate it (Array.push, object
// assignment, ...) and have those changes survive Export(). Returns Undefined
// for nil input.
func (h *Component) jsValue(v any) (sobek.Value, error) {
	if v == nil {
		return sobek.Undefined(), nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}
	jsonObj := h.runtime.Get("JSON").ToObject(h.runtime)
	parse, ok := sobek.AssertFunction(jsonObj.Get("parse"))
	if !ok {
		return nil, fmt.Errorf("JSON.parse unavailable in runtime")
	}
	return parse(sobek.Undefined(), h.runtime.ToValue(string(data)))
}

func (h *Component) init(s Settings) error {
	if s.Script.Content == "" {
		return fmt.Errorf("empty script")
	}

	mapFS := make(fstest.MapFS)
	for _, script := range s.Modules {
		mapFS[script.Name] = &fstest.MapFile{
			Data: []byte(script.Content),
		}
	}
	mapFS[mainModule] = &fstest.MapFile{
		Data: []byte(s.Script.Content),
	}

	vm := sobek.New()
	r := modules.NewResolver(mapFS)

	m, err := r.Resolve(nil, mainModule)
	if err != nil {
		return err
	}

	p := m.(*sobek.SourceTextModuleRecord)
	if err = p.Link(); err != nil {
		return fmt.Errorf("failed to link source text: %w", err)
	}

	promise := vm.CyclicModuleRecordEvaluate(p, r.Resolve)
	if promise.State() != sobek.PromiseStateFulfilled {
		err = promise.Result().Export().(error)
		return fmt.Errorf("failed to evaluate promise: %w", err)
	}

	val := vm.GetModuleInstance(m).GetBindingValue(defaultExport)
	fn, ok := sobek.AssertFunction(val)
	if !ok {
		return fmt.Errorf("failed to assert default export function")
	}

	h.handler = fn
	h.runtime = vm
	return nil
}

func (h *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:     RequestPort,
			Label:    "Request",
			Position: module.Left,
			Configuration: Request{
				InputData: h.settings.InputData,
			},
		},
		{
			Name:     ResponsePort,
			Position: module.Right,
			Label:    "Response",
			Source:   true,
			Configuration: Response{
				OutputData: h.settings.OutputData,
			},
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: h.settings,
		},
	}
	if !h.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Position:      module.Bottom,
		Name:          ErrorPort,
		Label:         "Error",
		Source:        true,
		Configuration: Error{},
	})
}

func (h *Component) Instance() module.Component {
	return &Component{
		settings: defaultEngineSettings,
	}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})

	// This module can execute a TinyComponent whose runtime is "js". The SDK
	// owns the resource and its controller; all a runtime contributes is the
	// ability to turn a definition into something runnable.
	registry.RegisterRuntime("js", func(d module.ComponentDefinition) (module.Component, error) {
		return NewDefined(Definition{
			Name:            d.Name,
			Description:     d.Description,
			Info:            d.Info,
			Script:          d.Script,
			InputSchema:     d.InputSchema,
			OutputSchema:    d.OutputSchema,
			EnableErrorPort: d.EnableErrorPort,
			TimeoutSeconds:  d.TimeoutSeconds,
		})
	})
}
