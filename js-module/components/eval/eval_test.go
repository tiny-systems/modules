package eval

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"sync"
	"testing"
	"time"
)

func TestComponent_Handle(t *testing.T) {
	type args struct {
		handler module.Handler
		port    string
		msg     interface{}
		wantErr bool
	}
	tests := []struct {
		name string
		args []args
	}{
		{
			name: "wrong settings, syntax error main script",

			args: []args{
				{
					port:    v1alpha1.SettingsPort,
					wantErr: true,
					msg: Settings{
						Script: Script{
							Content: `export default fction () {}`,
						},
					},
				},
			},
		},
		{
			name: "wrong settings, no default export function",

			args: []args{
				{
					port:    v1alpha1.SettingsPort,
					wantErr: true,
					msg: Settings{
						Script: Script{
							Content: `export named function () {}`,
						},
					},
				},
			},
		},
		{
			name: "success match result string",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default function () { return "result";}`,
						},
					},
				},
				{
					port: RequestPort,
					msg:  Request{},
					handler: func(_ context.Context, port string, data any) module.Result {
						if port != ResponsePort {
							return module.Fail(fmt.Errorf("response sent to the wrong port: %s", port))
						}
						resp, ok := data.(Response)
						if !ok {
							return module.Fail(fmt.Errorf("response type is invalid"))
						}
						if fmt.Sprint(resp.OutputData) != "result" {
							return module.Fail(fmt.Errorf("response sent the wrong result: %v", resp.OutputData))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "success match result check type int",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default function () { return 34;}`,
						},
					},
				},
				{
					port: RequestPort,
					msg:  Request{},
					handler: func(_ context.Context, port string, data any) module.Result {
						if port != ResponsePort {
							return module.Fail(fmt.Errorf("response sent to the wrong port: %s", port))
						}
						resp, ok := data.(Response)
						if !ok {
							return module.Fail(fmt.Errorf("response type is invalid"))
						}

						res, _ := resp.OutputData.(int64)
						if res != 34 {
							return module.Fail(fmt.Errorf("response sent the wrong result: %v", res))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "success match response use request",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default function (i) { return i + " world";}`,
						},
					},
				},
				{
					port: RequestPort,
					msg: Request{
						InputData: "hello",
					},
					handler: func(_ context.Context, port string, data any) module.Result {
						if port != ResponsePort {
							return module.Fail(fmt.Errorf("response sent to the wrong port: %s", port))
						}
						resp, ok := data.(Response)
						if !ok {
							return module.Fail(fmt.Errorf("response type is invalid"))
						}
						if fmt.Sprint(resp.OutputData) != "hello world" {
							return module.Fail(fmt.Errorf("response sent the wrong result: %v", resp.OutputData))
						}
						return module.Result{}
					},
				},
			},
		},

		{
			name: "success use promises",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default async function (i) { return await new Promise((resolve, reject) => {resolve("done")})}`,
						},
					},
				},
				{
					port: RequestPort,
					msg: Request{
						InputData: "hello",
					},
					handler: func(_ context.Context, port string, data any) module.Result {
						if port != ResponsePort {
							return module.Fail(fmt.Errorf("response sent to the wrong port: %s", port))
						}
						resp, ok := data.(Response)
						if !ok {
							return module.Fail(fmt.Errorf("response type is invalid"))
						}
						if fmt.Sprint(resp.OutputData) != "done" {
							return module.Fail(fmt.Errorf("response sent the wrong result: %v", resp.OutputData))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "reject should trigger error",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default async function (i) { return await new Promise((resolve, reject) => {reject("error")})}`,
						},
					},
				},
				{
					port: RequestPort,
					msg:  Request{},
					handler: func(_ context.Context, port string, data any) module.Result {
						return module.Result{}
					},
					wantErr: true,
				},
			},
		},
		{
			name: "success promise of promise",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default async function (i) { return await new Promise((resolve, reject) => {resolve(new Promise((resolve, reject) => {resolve("done")}))})}`,
						},
					},
				},
				{
					port: RequestPort,
					msg: Request{
						InputData: "hello",
					},
					handler: func(_ context.Context, port string, data any) module.Result {
						if port != ResponsePort {
							return module.Fail(fmt.Errorf("response sent to the wrong port: %s", port))
						}
						resp, ok := data.(Response)
						if !ok {
							return module.Fail(fmt.Errorf("response type is invalid"))
						}
						if fmt.Sprint(resp.OutputData) != "done" {
							return module.Fail(fmt.Errorf("response sent the wrong result: %v", resp.OutputData))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "error port enabled, js throw routes to error port",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						EnableErrorPort: true,
						Script: Script{
							Content: `export default function () { throw new Error("something broke"); }`,
						},
					},
				},
				{
					port: RequestPort,
					msg: Request{
						Context: "trace-123",
					},
					handler: func(_ context.Context, port string, data any) module.Result {
						if port != ErrorPort {
							return module.Fail(fmt.Errorf("expected error port, got: %s", port))
						}
						e, ok := data.(Error)
						if !ok {
							return module.Fail(fmt.Errorf("expected Error type"))
						}
						if e.Context != "trace-123" {
							return module.Fail(fmt.Errorf("context not passed through: %v", e.Context))
						}
						if e.Error == "" {
							return module.Fail(fmt.Errorf("error message is empty"))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "error port disabled, js throw returns error",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						EnableErrorPort: false,
						Script: Script{
							Content: `export default function () { throw new Error("fail"); }`,
						},
					},
				},
				{
					port:    RequestPort,
					msg:     Request{},
					wantErr: true,
				},
			},
		},
		{
			name: "context passthrough to response",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default function (i) { return i; }`,
						},
					},
				},
				{
					port: RequestPort,
					msg: Request{
						Context:   map[string]any{"traceId": "abc", "step": 42},
						InputData: "data",
					},
					handler: func(_ context.Context, port string, data any) module.Result {
						resp := data.(Response)
						ctx, ok := resp.Context.(map[string]any)
						if !ok {
							return module.Fail(fmt.Errorf("context type lost: %T", resp.Context))
						}
						if ctx["traceId"] != "abc" {
							return module.Fail(fmt.Errorf("context traceId mismatch: %v", ctx["traceId"]))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "context passthrough to error port",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						EnableErrorPort: true,
						Script: Script{
							Content: `export default function () { throw new Error("fail"); }`,
						},
					},
				},
				{
					port: RequestPort,
					msg: Request{
						Context: "my-ctx",
					},
					handler: func(_ context.Context, port string, data any) module.Result {
						e := data.(Error)
						if e.Context != "my-ctx" {
							return module.Fail(fmt.Errorf("context not passed to error: %v", e.Context))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "local module import from includes",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `import {add} from "math.js"; export default function(i) { return add(i, 10); }`,
						},
						Modules: []ScriptItem{
							{
								Name:    "math.js",
								Content: `export function add(a, b) { return a + b; }`,
							},
						},
					},
				},
				{
					port: RequestPort,
					msg: Request{
						InputData: 5,
					},
					handler: func(_ context.Context, port string, data any) module.Result {
						resp := data.(Response)
						res, _ := resp.OutputData.(int64)
						if res != 15 {
							return module.Fail(fmt.Errorf("expected 15, got: %v", resp.OutputData))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "object input and output",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default function(inp) { return {name: inp.name, upper: inp.name.toUpperCase()}; }`,
						},
					},
				},
				{
					port: RequestPort,
					msg: Request{
						InputData: map[string]any{"name": "alice"},
					},
					handler: func(_ context.Context, port string, data any) module.Result {
						resp := data.(Response)
						out, ok := resp.OutputData.(map[string]any)
						if !ok {
							return module.Fail(fmt.Errorf("expected map output, got: %T", resp.OutputData))
						}
						if out["name"] != "alice" || out["upper"] != "ALICE" {
							return module.Fail(fmt.Errorf("unexpected output: %v", out))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "return undefined",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default function() {}`,
						},
					},
				},
				{
					port: RequestPort,
					msg:  Request{},
					handler: func(_ context.Context, port string, data any) module.Result {
						resp := data.(Response)
						if resp.OutputData != nil {
							return module.Fail(fmt.Errorf("expected nil for undefined, got: %v", resp.OutputData))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "return null",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `export default function() { return null; }`,
						},
					},
				},
				{
					port: RequestPort,
					msg:  Request{},
					handler: func(_ context.Context, port string, data any) module.Result {
						resp := data.(Response)
						if resp.OutputData != nil {
							return module.Fail(fmt.Errorf("expected nil for null, got: %v", resp.OutputData))
						}
						return module.Result{}
					},
				},
			},
		},
		{
			name: "empty script content",
			args: []args{
				{
					port:    v1alpha1.SettingsPort,
					wantErr: true,
					msg: Settings{
						Script: Script{
							Content: "",
						},
					},
				},
			},
		},
		{
			name: "request before settings returns error",
			args: []args{
				{
					port:    RequestPort,
					msg:     Request{},
					wantErr: true,
				},
			},
		},
		{
			name: "multiple requests reuse compiled script",
			args: []args{
				{
					port: v1alpha1.SettingsPort,
					msg: Settings{
						Script: Script{
							Content: `let counter = 0; export default function() { counter++; return counter; }`,
						},
					},
				},
				{
					port: RequestPort,
					msg:  Request{},
					handler: func(_ context.Context, port string, data any) module.Result {
						resp := data.(Response)
						res, _ := resp.OutputData.(int64)
						if res != 1 {
							return module.Fail(fmt.Errorf("first call expected 1, got: %v", resp.OutputData))
						}
						return module.Result{}
					},
				},
				{
					port: RequestPort,
					msg:  Request{},
					handler: func(_ context.Context, port string, data any) module.Result {
						resp := data.(Response)
						res, _ := resp.OutputData.(int64)
						if res != 2 {
							return module.Fail(fmt.Errorf("second call expected 2, got: %v", resp.OutputData))
						}
						return module.Result{}
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := (&Component{}).Instance()
			for _, a := range tt.args {
				if err := dispatch(h, a.handler, a.port, a.msg); (err != nil) != a.wantErr {
					t.Errorf("dispatch() error = %v, wantErr %v", err, a.wantErr)
				}
			}
		})
	}
}

// dispatch mirrors what the runner does: route system ports through the
// matching capability interface, fall back to Handle for business ports.
// Lets the table-driven tests stay shaped around (port, msg) tuples.
func dispatch(c module.Component, handler module.Handler, port string, msg any) error {
	switch port {
	case v1alpha1.SettingsPort:
		if h, ok := c.(module.SettingsHandler); ok {
			return h.OnSettings(context.Background(), msg)
		}
	case v1alpha1.ControlPort:
		if h, ok := c.(module.ControlHandler); ok {
			return h.OnControl(context.Background(), msg)
		}
	}
	return c.Handle(context.Background(), handler, port, msg).Err()
}

// TestComponent_ConcurrentHandle hammers one component from many goroutines —
// the scheduler dispatches every message in its own goroutine, and an
// unserialized sobek runtime corrupts state or panics under this load.
// Run with -race to make the old defect visible.
func TestComponent_ConcurrentHandle(t *testing.T) {
	c := (&Component{}).Instance().(*Component)
	err := c.OnSettings(context.Background(), Settings{
		Script: Script{Name: "index.js", Content: `export default function(inp) { return {doubled: inp.n * 2} }`},
	})
	if err != nil {
		t.Fatalf("OnSettings: %v", err)
	}
	handler := func(ctx context.Context, port string, msg any) module.Result { return module.Ok(nil) }

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			res := c.Handle(context.Background(), handler, RequestPort, Request{InputData: map[string]any{"n": n}})
			if res.Err() != nil {
				t.Errorf("Handle(%d): %v", n, res.Err())
			}
		}(i)
	}
	wg.Wait()
}

// TestComponent_TimeoutInterruptsInfiniteLoop proves an accidental
// while(true) fails the hop instead of wedging the goroutine forever,
// and that the runtime stays usable for the next message.
func TestComponent_TimeoutInterruptsInfiniteLoop(t *testing.T) {
	c := (&Component{}).Instance().(*Component)
	err := c.OnSettings(context.Background(), Settings{
		Script:         Script{Name: "index.js", Content: `export default function(inp) { if (inp.spin) { while(true){} } return {ok: true} }`},
		TimeoutSeconds: 1,
	})
	if err != nil {
		t.Fatalf("OnSettings: %v", err)
	}
	handler := func(ctx context.Context, port string, msg any) module.Result { return module.Ok(nil) }

	start := time.Now()
	res := c.Handle(context.Background(), handler, RequestPort, Request{InputData: map[string]any{"spin": true}})
	if res.Err() == nil {
		t.Fatal("expected timeout error, got success")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("interrupt took too long: %s", elapsed)
	}

	// The runtime must recover for the next message.
	res = c.Handle(context.Background(), handler, RequestPort, Request{InputData: map[string]any{"spin": false}})
	if res.Err() != nil {
		t.Fatalf("runtime unusable after interrupt: %v", res.Err())
	}
}
