package async

import (
	"context"
	"fmt"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"go.opentelemetry.io/otel/trace"
)

const (
	ComponentName        = "async"
	InPort        string = "in"
	OutPort       string = "out"

	// defaultMaxConcurrency limits concurrent goroutines to prevent memory exhaustion
	defaultMaxConcurrency = 100
)

type Context any

type InMessage struct {
	Context Context `json:"context" configurable:"true" required:"true" title:"Context" description:"Arbitrary message to be modified"`
}

type Settings struct {
	MaxConcurrency int `json:"maxConcurrency" title:"Max Concurrency" description:"Maximum number of concurrent async operations. 0 means use default (100)."`
}

// OutMessage keeps the passthrough Context under a `context` key so a downstream
// edge reads $.context.<field> — the mid-chain convention (same as the router,
// pod_logs_get, llm_tools). Was emitting the raw Context value at root.
type OutMessage struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Passthrough — the message, unchanged"`
}

type Component struct {
	settings  Settings
	semaphore chan struct{}
}

func (t *Component) Instance() module.Component {
	return &Component{
		settings:  Settings{MaxConcurrency: defaultMaxConcurrency},
		semaphore: make(chan struct{}, defaultMaxConcurrency),
	}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Async",
		Info:        "Non-blocking pass-through. Returns immediately (unblocks sender), then emits context on Out in a goroutine. Limits concurrent goroutines via maxConcurrency setting to prevent memory issues.",
		Tags:        []string{"SDK"},
	}
}

// OnSettings receives Settings from the SettingsPort and resizes the
// concurrency semaphore.
func (t *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	t.settings = in
	maxConc := in.MaxConcurrency
	if maxConc <= 0 {
		maxConc = defaultMaxConcurrency
	}
	t.semaphore = make(chan struct{}, maxConc)
	return nil
}

// Handle dispatches the InPort. System ports go through the capability
// interfaces above.
func (t *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {
	if port != InPort {
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
	in, ok := msg.(InMessage)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}

	// Try to acquire semaphore slot (non-blocking for async behavior)
	select {
	case t.semaphore <- struct{}{}:
		// Got a slot, spawn goroutine. The handler return is intentionally
		// discarded — async by definition has no upstream waiting on the
		// downstream response, so this is the one legitimate fire-and-forget
		// drop point in this component.
		go func() {
			defer func() { <-t.semaphore }() // Release slot when done
			_ = handler(trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)), OutPort, OutMessage{Context: in.Context})
		}()
	default:
		// Semaphore full - run synchronously to apply backpressure
		return handler(trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)), OutPort, OutMessage{Context: in.Context})
	}
	return module.Result{}
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
		{
			Name:          InPort,
			Label:         "In",
			Configuration: InMessage{},
			Position:      module.Left,
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Configuration: new(OutMessage),
			Position:      module.Right,
		},
	}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
