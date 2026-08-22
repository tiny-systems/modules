// Package retry implements the retry component — the explicit
// supervisor primitive that pairs with single-shot edge delivery in
// the SDK (since v0.10.16). Components no longer retry transient
// failures implicitly; flow authors wire retry into their error
// paths to get backoff + bounded attempts.
//
// Topology:
//
//	llm_complete.request ──→ llm_complete
//	     ▲                       │
//	     │                       ├─ response → … rest of flow
//	     │                       │
//	     │                       └─ error ───→ retry.request
//	     │                                        │
//	     │                                        ├─ retry  ─┐
//	     │                                        │          │ sleeps NextDelayMs
//	     └────────────────────────────────────────┘          │ then loops back
//	                                              │
//	                                              └─ failed → dead-letter / alert
//
// The component is stateless. Attempt state rides in the payload as
// `attempt` and is incremented on every loop. Flow authors compose
// the loop explicitly; the framework no longer hides retry decisions.
package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "retry"
	RequestPort   = "request"
	RetryPort     = "retry"
	FailedPort    = "failed"

	BackoffExponential = "exponential"
	BackoffLinear      = "linear"
	BackoffConstant    = "constant"

	ReasonMaxAttempts  = "max_attempts"
	ReasonNotRetryable = "not_retryable"

	defaultMaxAttempts = 5
	defaultInitialMs   = 500
	defaultMaxMs       = 30000
)

type Context any

type Settings struct {
	MaxAttempts    int    `json:"maxAttempts" required:"true" minimum:"1" default:"5" title:"Max Attempts" description:"Upper bound on retry loops. Counts the first attempt; e.g. 5 = original + 4 retries."`
	InitialDelayMs int    `json:"initialDelayMs" required:"true" minimum:"0" default:"500" title:"Initial Delay (ms)" description:"Wait before the first retry."`
	MaxDelayMs     int    `json:"maxDelayMs" required:"true" minimum:"0" default:"30000" title:"Max Delay (ms)" description:"Upper bound on a single wait. Caps exponential growth."`
	Backoff        string `json:"backoff" required:"true" enum:"exponential,linear,constant" default:"exponential" title:"Backoff" description:"exponential: initial * 2^n. linear: initial * (n+1). constant: always initial."`
	Jitter         bool   `json:"jitter" title:"Jitter" description:"Randomise each delay by ±25% to avoid thundering herd when many flows retry the same upstream."`
}

// Request is the input shape. Wire an error port that emits
// {context, error, retryable} into this. That is the canonical
// module.ErrorMessage shape (SDK), so any component built with
// module.NewError conforms with no transform.
type Request struct {
	Context   Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Original request payload — pass through unchanged so the loop can re-invoke the upstream component."`
	Attempt   int     `json:"attempt" title:"Attempt" description:"Zero or absent on first arrival. Component increments it on every retry."`
	Retryable bool    `json:"retryable" title:"Retryable" description:"From the error port. False = straight to failed without sleeping."`
	Error     string  `json:"error,omitempty" title:"Last Error" description:"Last error message, forwarded to failed for the dead-letter path."`
}

// Retry fires after the backoff sleep. Wire this back into the
// original component's request port to re-invoke.
type Retry struct {
	Context     Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Attempt     int     `json:"attempt" title:"Attempt" description:"Incremented before emit. Next call lands here with this value."`
	NextDelayMs int     `json:"nextDelayMs" title:"Slept (ms)" description:"How long the component just slept. Informational — useful for traces."`
}

// Failed fires when the loop terminates. Two reasons: attempt limit
// hit, or the error was marked non-retryable.
type Failed struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Attempt int     `json:"attempt" title:"Attempt" description:"Final attempt count when the loop gave up."`
	Error   string  `json:"error,omitempty" title:"Last Error"`
	Reason  string  `json:"reason" title:"Reason" description:"max_attempts | not_retryable"`
}

type Component struct {
	settings Settings
	rnd      *rand.Rand
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			MaxAttempts:    defaultMaxAttempts,
			InitialDelayMs: defaultInitialMs,
			MaxDelayMs:     defaultMaxMs,
			Backoff:        BackoffExponential,
			Jitter:         true,
		},
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Retry",
		Info:        "Explicit retry supervisor with bounded attempts and configurable backoff. Pairs with the error port of any component that emits the canonical module.ErrorMessage shape {context, error, retryable} (build it with module.NewError; mark transient failures with module.Retryable). Any conforming component works — llm_*, http_request, and any third-party module that uses the SDK error contract. Stateless: attempt count rides in the payload, so the same component can supervise many concurrent flows without cross-talk.",
		Tags:        []string{"SDK", "Resilience", "Backoff"},
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settings = in
	return nil
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	return c.dispatch(ctx, handler, in)
}

func (c *Component) dispatch(ctx context.Context, handler module.Handler, in Request) module.Result {
	if !in.Retryable {
		return handler(ctx, FailedPort, Failed{
			Context: in.Context,
			Attempt: in.Attempt,
			Error:   in.Error,
			Reason:  ReasonNotRetryable,
		})
	}

	maxAttempts := c.settings.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}

	nextAttempt := in.Attempt + 1
	if nextAttempt >= maxAttempts {
		return handler(ctx, FailedPort, Failed{
			Context: in.Context,
			Attempt: nextAttempt,
			Error:   in.Error,
			Reason:  ReasonMaxAttempts,
		})
	}

	delayMs := c.computeDelay(in.Attempt)
	select {
	case <-time.After(time.Duration(delayMs) * time.Millisecond):
	case <-ctx.Done():
		return module.Fail(ctx.Err())
	}

	return handler(ctx, RetryPort, Retry{
		Context:     in.Context,
		Attempt:     nextAttempt,
		NextDelayMs: delayMs,
	})
}

// computeDelay returns the wait, in ms, before the (in.Attempt+1)th
// attempt. Capped at MaxDelayMs and never below 0.
func (c *Component) computeDelay(prevAttempt int) int {
	initial := c.settings.InitialDelayMs
	if initial < 0 {
		initial = defaultInitialMs
	}
	maxMs := c.settings.MaxDelayMs
	if maxMs <= 0 {
		maxMs = defaultMaxMs
	}

	var raw float64
	switch c.settings.Backoff {
	case BackoffLinear:
		raw = float64(initial) * float64(prevAttempt+1)
	case BackoffConstant:
		raw = float64(initial)
	case BackoffExponential, "":
		raw = float64(initial) * math.Pow(2, float64(prevAttempt))
	default:
		raw = float64(initial) * math.Pow(2, float64(prevAttempt))
	}
	if raw > float64(maxMs) {
		raw = float64(maxMs)
	}

	if c.settings.Jitter && c.rnd != nil {
		// ±25% jitter around the computed delay.
		factor := 0.75 + c.rnd.Float64()*0.5
		raw *= factor
		if raw > float64(maxMs) {
			raw = float64(maxMs)
		}
	}

	if raw < 0 {
		raw = 0
	}
	return int(raw)
}

func (c *Component) Ports() []module.Port {
	return []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: RequestPort, Label: "Request", Configuration: Request{}, Position: module.Left},
		{Name: RetryPort, Label: "Retry", Source: true, Configuration: Retry{}, Position: module.Right},
		{Name: FailedPort, Label: "Failed", Source: true, Configuration: Failed{}, Position: module.Bottom},
	}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
