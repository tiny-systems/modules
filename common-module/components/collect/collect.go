package collect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/state"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "collect"

	ItemPort     = "item"
	ResponsePort = "response"
	ErrorPort    = "error"

	defaultTimeoutSeconds = 300
	defaultMaxGroups      = 100
)

// Context is the passthrough payload. The group's Response carries the
// context of the LAST item to arrive for that group.
type Context any

// Settings configures the collector.
type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"Emit timed-out groups on the Error port. Without it expired groups are dropped silently (a warning is logged) — enable this in production."`
	TimeoutSeconds  int  `json:"timeoutSeconds" title:"Timeout Seconds" default:"300" description:"How long to wait for a group's stragglers after its first item arrives (0 = default 300). Expiry is checked when messages arrive, so an idle node holds stragglers until the next message."`
	MaxGroups       int  `json:"maxGroups" title:"Max Groups" default:"100" description:"Maximum number of in-flight groups (0 = default 100). Guards the node's ~900KB state budget."`
}

// InMessage is one fanned-out item to buffer.
type InMessage struct {
	Context  Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Passthrough payload; the group's Response carries the context of its LAST arriving item."`
	GroupKey string  `json:"groupKey" required:"true" title:"Group Key" description:"Identity of the fan-out this item belongs to — map something unique per source message (e.g. $.context.runId). Every item of one array must share it; different runs must differ."`
	Index    int     `json:"index" title:"Index" description:"0-based position of this item in the reassembled array — map array_split's index. A duplicate index overwrites; it does not double-count."`
	Total    int     `json:"total" required:"true" title:"Total" description:"Number of items the group expects — map array_split's total. Must be >= 1 and identical for every item of the group."`
	Item     any     `json:"item" title:"Item" description:"The per-item payload to reassemble."`
}

// OutMessage is the reassembled group.
type OutMessage struct {
	Context Context `json:"context,omitempty" title:"Context" description:"Context of the group's last arriving item."`
	Items   []any   `json:"items" title:"Items" description:"All items of the group, ordered by index."`
}

// ErrorMessage reports a group that timed out before completing.
type ErrorMessage struct {
	Context  Context `json:"context,omitempty" title:"Context" description:"Context of the group's last arriving item."`
	GroupKey string  `json:"groupKey" title:"Group Key"`
	Received int     `json:"received" title:"Received"`
	Total    int     `json:"total" title:"Total"`
	Error    string  `json:"error" title:"Error"`
}

// groupState is the persisted per-group buffer. One State key per group,
// keyed by the group key itself.
type groupState struct {
	FirstSeen time.Time   `json:"firstSeen"`
	Total     int         `json:"total"`
	Context   Context     `json:"context"`
	Items     map[int]any `json:"items"`
}

// Component buffers fanned-out items per group key in the node's State
// (survives restarts; multi-replica safe — same backing as kv) and emits the
// reassembled array once all items of a group have arrived.
type Component struct {
	module.Base

	mu       sync.Mutex
	settings Settings

	// stateMu serializes the read-modify-write of group buffers within this
	// replica so concurrent items of one group cannot lose updates.
	stateMu sync.Mutex

	// now is the clock; a seam so tests can drive timeout expiry without
	// sleeping. Nil-safe via clock().
	now func() time.Time
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			TimeoutSeconds: defaultTimeoutSeconds,
			MaxGroups:      defaultMaxGroups,
		},
		now: time.Now,
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Collect",
		Info:        "Fan-in for map-reduce — the pair of array_split. array_split emits {context, item, index, total} per element; route each item through the per-item work, then map into Item here: groupKey MUST be unique per fan-out (e.g. $.context.runId — every item of one array shares it, different runs differ), and index/total come straight from array_split. Items are buffered per groupKey in the node's State, so assembly survives restarts and is multi-replica safe. Once a group holds all `total` items, Response emits {context, items}: items ordered by index regardless of arrival order (a duplicate index overwrites, never double-counts), context taken from the group's LAST arriving item. Groups older than timeoutSeconds fail onto the Error port as {context, groupKey, received, total, error}. Expiry is passive — checked when messages arrive — so an idle node holds stragglers until the next message on any group. State is capped at ~900KB per node: keep buffered items small (carry IDs, not blobs) or lower maxGroups.",
		Tags:        []string{"SDK", "ARRAY"},
	}
}

// OnSettings receives Settings from the SettingsPort.
func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if in.TimeoutSeconds <= 0 {
		in.TimeoutSeconds = defaultTimeoutSeconds
	}
	if in.MaxGroups <= 0 {
		in.MaxGroups = defaultMaxGroups
	}
	c.mu.Lock()
	c.settings = in
	c.mu.Unlock()
	return nil
}

// OnReconcile is a no-op: the State backend handles cache freshness.
func (c *Component) OnReconcile(_ context.Context, _ v1alpha1.TinyNode) error {
	return nil
}

func (c *Component) clock() time.Time {
	if c.now == nil {
		return time.Now()
	}
	return c.now()
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != ItemPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(InMessage)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}
	return c.handleItem(ctx, handler, in)
}

// outcome is what process decides under the state lock; emissions happen
// outside the lock so a downstream hop cannot deadlock back into this node.
type outcome struct {
	expired  []ErrorMessage // other groups that passed the deadline
	response *OutMessage    // this item completed its group
	ownError *ErrorMessage  // this item's own group is past the deadline and incomplete
	fail     error
}

func (c *Component) handleItem(ctx context.Context, handler module.Handler, in InMessage) module.Result {
	if in.GroupKey == "" {
		return module.Fail(fmt.Errorf("groupKey is required: map something unique per fan-out (e.g. $.context.runId)"))
	}
	if in.Total < 1 {
		return module.Fail(fmt.Errorf("total must be >= 1, got %d: map array_split's total", in.Total))
	}
	if in.Index < 0 || in.Index >= in.Total {
		return module.Fail(fmt.Errorf("index %d out of range [0, %d): map array_split's index", in.Index, in.Total))
	}
	if c.State() == nil {
		return module.Fail(fmt.Errorf("state backend not available"))
	}

	c.mu.Lock()
	timeout := time.Duration(c.settings.TimeoutSeconds) * time.Second
	maxGroups := c.settings.MaxGroups
	errPort := c.settings.EnableErrorPort
	c.mu.Unlock()

	o := c.process(ctx, in, timeout, maxGroups)

	// Timed-out groups belong to older messages; emit them detached from this
	// item's trace via the long-lived emitter.
	for _, e := range o.expired {
		if errPort {
			c.Emit(ctx, ErrorPort, e)
		} else {
			log.Warn().Str("component", ComponentName).Str("groupKey", e.GroupKey).
				Msg(e.Error + " (error port disabled; group dropped)")
		}
	}

	if o.fail != nil {
		return module.Fail(o.fail)
	}
	if o.response != nil {
		return handler(ctx, ResponsePort, *o.response)
	}
	if o.ownError != nil {
		if errPort {
			return handler(ctx, ErrorPort, *o.ownError)
		}
		return module.Fail(errors.New(o.ownError.Error))
	}
	return module.Result{}
}

// process performs the state read-modify-write for one arriving item and the
// passive expiry sweep, all under stateMu. It only decides; it never emits.
func (c *Component) process(ctx context.Context, in InMessage, timeout time.Duration, maxGroups int) outcome {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()

	st := c.State()
	now := c.clock()

	// Sweep other groups past the deadline first so they free MaxGroups
	// slots. The arriving item's own group is excluded: a straggler that
	// completes its group is a success, not a timeout.
	var o outcome
	o.expired = c.sweepExpired(ctx, now, timeout, in.GroupKey)

	var g groupState
	raw, found, err := st.Get(ctx, in.GroupKey)
	if err != nil {
		o.fail = fmt.Errorf("state.Get: %v", err)
		return o
	}
	if found {
		if err := json.Unmarshal(raw, &g); err != nil {
			// An unreadable buffer can never complete; start the group over.
			found = false
		}
	}
	if !found {
		keys, err := st.List(ctx, "")
		if err != nil {
			o.fail = fmt.Errorf("state.List: %v", err)
			return o
		}
		if len(keys) >= maxGroups {
			o.fail = fmt.Errorf("too many in-flight groups: %d (max %d); raise maxGroups or check that groups complete or time out", len(keys), maxGroups)
			return o
		}
		g = groupState{FirstSeen: now, Total: in.Total, Items: map[int]any{}}
	}
	if g.Total != in.Total {
		o.fail = fmt.Errorf("total mismatch for group %q: got %d, group started with %d — every item of a group must carry the same total", in.GroupKey, in.Total, g.Total)
		return o
	}

	g.Items[in.Index] = in.Item
	g.Context = in.Context

	// Complete — emit ordered by index and drop the buffer. Also covers a
	// straggler finishing an already-expired group: late success beats failure.
	if len(g.Items) >= g.Total {
		if err := st.Delete(ctx, in.GroupKey); err != nil {
			o.fail = fmt.Errorf("state.Delete: %v", err)
			return o
		}
		items := make([]any, g.Total)
		for i := range items {
			items[i] = g.Items[i]
		}
		o.response = &OutMessage{Context: g.Context, Items: items}
		return o
	}

	// Incomplete and past the deadline: this straggler cannot save the group,
	// so fail it now (counting the straggler) instead of waiting for more traffic.
	if now.Sub(g.FirstSeen) > timeout {
		if err := st.Delete(ctx, in.GroupKey); err != nil {
			o.fail = fmt.Errorf("state.Delete: %v", err)
			return o
		}
		o.ownError = timeoutError(in.GroupKey, g)
		return o
	}

	data, err := json.Marshal(g)
	if err != nil {
		o.fail = fmt.Errorf("failed to marshal group state: %v", err)
		return o
	}
	if err := st.Set(ctx, in.GroupKey, data); err != nil {
		if errors.Is(err, state.ErrStateTooLarge) {
			o.fail = fmt.Errorf("state budget exhausted: the node's total state is capped at ~%dKB and group %q no longer fits; collect smaller items (carry IDs, not payloads) or lower maxGroups: %v",
				state.MaxStateBytes/1024, in.GroupKey, err)
			return o
		}
		o.fail = fmt.Errorf("state.Set: %v", err)
		return o
	}
	return o
}

// sweepExpired deletes and reports every group (except exclude) whose first
// item is older than timeout. Passive expiry: this only runs when a message
// arrives, so an idle node holds stragglers until the next message.
func (c *Component) sweepExpired(ctx context.Context, now time.Time, timeout time.Duration, exclude string) []ErrorMessage {
	st := c.State()
	keys, err := st.List(ctx, "")
	if err != nil {
		return nil
	}
	var out []ErrorMessage
	for _, k := range keys {
		if k == exclude {
			continue
		}
		raw, found, err := st.Get(ctx, k)
		if err != nil || !found {
			continue
		}
		var g groupState
		if err := json.Unmarshal(raw, &g); err != nil {
			// Unreadable buffers can never complete; drop them.
			_ = st.Delete(ctx, k)
			continue
		}
		if now.Sub(g.FirstSeen) <= timeout {
			continue
		}
		if err := st.Delete(ctx, k); err != nil {
			continue
		}
		out = append(out, *timeoutError(k, g))
	}
	return out
}

func timeoutError(groupKey string, g groupState) *ErrorMessage {
	return &ErrorMessage{
		Context:  g.Context,
		GroupKey: groupKey,
		Received: len(g.Items),
		Total:    g.Total,
		Error:    fmt.Sprintf("collect timeout: got %d of %d", len(g.Items), g.Total),
	}
}

func (c *Component) Ports() []module.Port {
	c.mu.Lock()
	settings := c.settings
	c.mu.Unlock()

	ports := []module.Port{
		{Name: v1alpha1.ReconcilePort},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: settings,
		},
		{
			Name:          ItemPort,
			Label:         "Item",
			Configuration: InMessage{Total: 1},
			Position:      module.Left,
		},
		{
			Name:          ResponsePort,
			Label:         "Response",
			Source:        true,
			Configuration: OutMessage{Items: []any{}},
			Position:      module.Right,
		},
	}

	if settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Configuration: ErrorMessage{},
			Position:      module.Bottom,
		})
	}

	return ports
}

var (
	_ module.Component        = (*Component)(nil)
	_ module.SettingsHandler  = (*Component)(nil)
	_ module.ReconcileHandler = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
