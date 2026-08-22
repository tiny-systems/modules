// Package arrayfilter keeps the elements of an array that match a predicate.
//
// Among a hundred-odd components there was no filter, no map and no count.
// array_get takes one element by index, array_split iterates, group_by buckets
// and collect fans in — every one of them sidesteps the question "which of
// these do I care about". So a flow that lists pods and wants the unhealthy
// ones writes JavaScript, and pod_list has exactly one setting, so every
// consumer writes it again.
//
// The predicate is the same JSONPath dialect kv uses for its query port. One
// dialect across the palette is worth more than a cleverer second one.
package arrayfilter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tiny-systems/ajson"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "array_filter"

	RequestPort = "request"
	ResultPort  = "result"
	ErrorPort   = "error"
)

type Context any

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Passthrough, emitted with the result."`
	Array   []any   `json:"array" required:"true" title:"Array" description:"The items to filter. Map an upstream array straight in: {{$.pods}}."`
	Query   string  `json:"query" required:"true" title:"Query" description:"JSONPath predicate evaluated against EACH item, the same dialect kv uses — e.g. $.hasProblem, $.phase == 'Running', $.restarts > 3."`
}

type Result struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Items   []any   `json:"items" title:"Items" description:"The items the predicate kept, in their original order."`
	Count   int     `json:"count" title:"Count" description:"How many matched — wire this to a router to branch on 'any' versus 'none'."`
	Total   int     `json:"total" title:"Total" description:"How many were examined. count == total means the predicate kept everything, which usually means it is wrong."`
}

type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

type Settings struct {
	// A predicate that cannot be evaluated against an item is ambiguous: it
	// may be a broken expression, or an item that simply lacks the field. The
	// two need different answers and only the flow's author knows which.
	SkipUnevaluable bool `json:"skipUnevaluable" title:"Skip Items The Query Cannot Evaluate" description:"On: an item the predicate cannot be evaluated against is treated as not matching. Off (default): it fails, so a mistyped field name is reported rather than silently filtering everything away."`
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to the error port instead of failing the run."`
}

type Component struct {
	module.Base
	settings Settings
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Array Filter",
		Info: "Keeps the array elements matching a JSONPath predicate, and counts them. " +
			"The query runs against each item on its own, so $.hasProblem, $.phase == 'Running' and $.restarts > 3 " +
			"all work; it is the same dialect kv's query port uses. Returns items, count and total — wire count into " +
			"a router to branch on whether anything matched at all. " +
			"By default an item the predicate cannot be evaluated against fails the run, so a mistyped field is " +
			"reported instead of silently filtering everything away; turn on skipUnevaluable when a ragged array is " +
			"expected.",
		Tags: []string{"SDK", "ARRAY", "agent_tool"},
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

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}
	if in.Query == "" {
		return c.handleError(ctx, handler, in.Context, fmt.Errorf("query is required — a filter with no predicate is the array it was given"))
	}

	items := make([]any, 0, len(in.Array))
	for i, item := range in.Array {
		matched, err := matches(item, in.Query)
		if err != nil {
			if c.settings.SkipUnevaluable {
				continue
			}
			return c.handleError(ctx, handler, in.Context,
				fmt.Errorf("query %q could not be evaluated against item %d: %w — check the field name, or turn on skipUnevaluable if the array is ragged", in.Query, i, err))
		}
		if matched {
			items = append(items, item)
		}
	}

	return handler(ctx, ResultPort, Result{
		Context: in.Context,
		Items:   items,
		Count:   len(items),
		Total:   len(in.Array),
	})
}

// matches evaluates the predicate against one item.
//
// Anything other than a true result is a non-match, including a number or a
// string: a predicate is a question with a yes or no answer, and treating a
// non-boolean as truthy would quietly keep items for reasons nobody wrote.
func matches(item any, query string) (bool, error) {
	encoded, err := json.Marshal(item)
	if err != nil {
		return false, err
	}
	node, err := ajson.Unmarshal(encoded)
	if err != nil {
		return false, err
	}
	result, err := ajson.Eval(node, query)
	if err != nil {
		return false, err
	}
	value, err := result.Unpack()
	if err != nil {
		return false, err
	}
	kept, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("predicate answered %v (%T), not true or false", value, value)
	}
	return kept, nil
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	if !c.settings.EnableErrorPort {
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, Error{Context: reqCtx, Error: err.Error()})
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          RequestPort,
			Label:         "Request",
			Configuration: Request{},
			Position:      module.Left,
		},
		{
			Name:          ResultPort,
			Label:         "Result",
			Source:        true,
			Configuration: Result{},
			Position:      module.Right,
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: c.settings,
		},
	}
	if c.settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Configuration: Error{},
			Position:      module.Bottom,
		})
	}
	return ports
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
