// Package budgetguard stops an agent loop before it runs away.
//
// A ReAct loop is closed by the graph: a tool result folds back into the model,
// which may call another tool, forever. Nothing in that circuit counts, so a
// model that will not converge — or a tool that keeps returning something it
// finds interesting — bills every iteration until someone notices. This
// project has already paid for that once, which is why implicit retries were
// removed from the scheduler in May 2026.
//
// Put this in the loop and it becomes the one place with a ceiling.
package budgetguard

import (
	"context"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "budget_guard"
	RequestPort   = "request"
	ProceedPort   = "proceed"
	ExceededPort  = "exceeded"
)

type Context any

type Settings struct {
	// MaxIterations is the ceiling that always applies. Tokens are optional
	// because not every loop calls a model, but a loop with no iteration limit
	// is a loop with no limit at all.
	MaxIterations int `json:"maxIterations" required:"true" title:"Max Iterations" description:"How many times the loop may go round before it is cut off."`
	MaxTokens     int `json:"maxTokens,omitempty" title:"Max Tokens" description:"Total input+output tokens the loop may spend. 0 means no token ceiling — iterations still apply."`
	// MaxCostUSD is priced by the caller rather than here: rates change per
	// model and per provider, and a table baked into a component would be
	// wrong within a month.
	MaxCostUSD float64 `json:"maxCostUSD,omitempty" title:"Max Cost (USD)" description:"Total spend the loop may reach, using the cost you supply per iteration. 0 means no cost ceiling."`
}

// Request carries the running totals. They live in the payload, not in the
// component, so one guard supervises many concurrent loops without their
// counts bleeding into each other — the same reason retry keeps its attempt
// count on the message.
type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Loop payload — passed through unchanged so the next iteration can carry on."`
	// Iteration is absent on the first pass and echoed back from proceed
	// afterwards. A caller that forgets to thread it through gets a loop that
	// never counts, so proceed emits it ready to map straight back in.
	Iteration    int     `json:"iteration,omitempty" title:"Iteration" description:"Zero or absent on first arrival. Map the value from proceed back here on each pass."`
	InputTokens  int     `json:"inputTokens,omitempty" title:"Input Tokens" description:"Tokens used by THIS iteration — map from the model's usage. Accumulated by the guard."`
	OutputTokens int     `json:"outputTokens,omitempty" title:"Output Tokens" description:"Tokens produced by THIS iteration — map from the model's usage."`
	CostUSD      float64 `json:"costUSD,omitempty" title:"Cost (USD)" description:"Cost of THIS iteration, if you price it. Accumulated by the guard."`
	// Spent totals arrive back from proceed; the guard adds this iteration's
	// usage to them.
	SpentTokens int     `json:"spentTokens,omitempty" title:"Spent Tokens" description:"Running total so far. Map from proceed."`
	SpentUSD    float64 `json:"spentUSD,omitempty" title:"Spent (USD)" description:"Running total so far. Map from proceed."`
}

// Proceed means the loop is still within budget. Its fields are the next
// iteration's counters, ready to be mapped back into the guard.
type Proceed struct {
	Context     Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Iteration   int     `json:"iteration" title:"Iteration" description:"Incremented. Map back into the guard on the next pass."`
	SpentTokens int     `json:"spentTokens" title:"Spent Tokens" description:"Running total including this iteration."`
	SpentUSD    float64 `json:"spentUSD" title:"Spent (USD)" description:"Running total including this iteration."`
	Remaining   int     `json:"remainingIterations" title:"Remaining Iterations"`
}

// Exceeded is the stop. It says which ceiling was hit, because "the agent
// stopped" is not something anyone can act on.
type Exceeded struct {
	Context     Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Loop payload at the moment it was cut off — enough to report or resume deliberately."`
	Reason      string  `json:"reason" title:"Reason" description:"Which ceiling was reached: iterations, tokens, or cost."`
	Iteration   int     `json:"iteration" title:"Iteration"`
	SpentTokens int     `json:"spentTokens" title:"Spent Tokens"`
	SpentUSD    float64 `json:"spentUSD" title:"Spent (USD)"`
}

type Component struct {
	settings     Settings
	settingsLock sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{settings: Settings{MaxIterations: 10}}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Budget Guard",
		Info: "Bounds an agent loop. Wire it into the cycle — typically between a tool result and the model it feeds — and it emits on Proceed while the loop is within budget, or on Exceeded once it is not. " +
			"Ceilings: iterations always, plus optional total tokens and cost. Map the model's usage into inputTokens/outputTokens, and map iteration/spentTokens/spentUSD from Proceed back into the guard on each pass; the counters travel in the payload so one guard can supervise many loops at once. " +
			"Without something like this a ReAct loop has no limit at all: the graph closes the circuit and every iteration bills. Wire Exceeded to a report, or to ask so a human decides whether to continue.",
		Tags: []string{"SDK", "Agent", "Safety"},
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	// A zero or negative ceiling would let every loop through, which is the
	// exact failure this exists to prevent — refuse it at configuration time
	// rather than discovering it on a bill.
	if in.MaxIterations <= 0 {
		return fmt.Errorf("maxIterations must be at least 1")
	}
	if in.MaxTokens < 0 {
		return fmt.Errorf("maxTokens cannot be negative")
	}
	if in.MaxCostUSD < 0 {
		return fmt.Errorf("maxCostUSD cannot be negative")
	}
	c.settingsLock.Lock()
	c.settings = in
	c.settingsLock.Unlock()
	return nil
}

func (c *Component) getSettings() Settings {
	c.settingsLock.RLock()
	defer c.settingsLock.RUnlock()
	return c.settings
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}

	set := c.getSettings()

	// Count this pass and fold its usage into the totals before testing, so a
	// single iteration that blows the whole budget is caught on the pass that
	// spent it rather than the one after.
	iteration := in.Iteration + 1
	spentTokens := in.SpentTokens + in.InputTokens + in.OutputTokens
	spentUSD := in.SpentUSD + in.CostUSD

	if reason := exceeded(set, iteration, spentTokens, spentUSD); reason != "" {
		return handler(ctx, ExceededPort, Exceeded{
			Context:     in.Context,
			Reason:      reason,
			Iteration:   iteration,
			SpentTokens: spentTokens,
			SpentUSD:    spentUSD,
		})
	}

	return handler(ctx, ProceedPort, Proceed{
		Context:     in.Context,
		Iteration:   iteration,
		SpentTokens: spentTokens,
		SpentUSD:    spentUSD,
		Remaining:   set.MaxIterations - iteration,
	})
}

// exceeded names the ceiling that was reached, or "" while the loop may
// continue. Iterations are checked first because that limit always exists.
func exceeded(set Settings, iteration, spentTokens int, spentUSD float64) string {
	if iteration > set.MaxIterations {
		return fmt.Sprintf("iteration limit reached (%d)", set.MaxIterations)
	}
	if set.MaxTokens > 0 && spentTokens > set.MaxTokens {
		return fmt.Sprintf("token budget reached (%d of %d)", spentTokens, set.MaxTokens)
	}
	if set.MaxCostUSD > 0 && spentUSD > set.MaxCostUSD {
		return fmt.Sprintf("cost budget reached (%.4f of %.4f USD)", spentUSD, set.MaxCostUSD)
	}
	return ""
}

func (c *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: c.getSettings(),
		},
		{
			Name:          RequestPort,
			Label:         "Request",
			Position:      module.Left,
			Configuration: Request{},
		},
		{
			Name:     ProceedPort,
			Label:    "Proceed",
			Source:   true,
			Position: module.Right,
			// Concrete values so an edge mapping the counters back in is
			// checkable when the flow is built rather than resolving to null.
			Configuration: Proceed{Iteration: 1, SpentTokens: 0, SpentUSD: 0, Remaining: 9},
		},
		{
			Name:          ExceededPort,
			Label:         "Exceeded",
			Source:        true,
			Position:      module.Bottom,
			Configuration: Exceeded{Reason: "iteration limit reached (10)", Iteration: 11},
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
