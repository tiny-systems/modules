package groupby

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "group_by"
	InPort        = "in"
	OutPort       = "out"
)

// Context type alias for schema generation
type Context any

// Item type alias for schema traceability between input and output
type Item any

// InMessage is the input
type InMessage struct {
	Context     Context `json:"context" configurable:"true" title:"Context" description:"Arbitrary data passed through to output"`
	Items       []Item  `json:"items" required:"true" configurable:"true" title:"Items" description:"Array of items to group"`
	GroupByPath string  `json:"groupByPath" required:"true" configurable:"true" title:"Group By Path" description:"JSON path to group by (e.g., 'labels.app', 'namespace', 'kind')"`
}

// Group represents a single group
type Group struct {
	Key   string `json:"key" title:"Key" description:"The group key value"`
	Items []Item `json:"items" title:"Items" description:"Items in this group"`
	Count int    `json:"count" title:"Count" description:"Number of items in group"`
}

// OutMessage is the output
type OutMessage struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Groups  []Group `json:"groups" title:"Groups" description:"Grouped items"`
	Total   int     `json:"total" title:"Total" description:"Total number of items"`
}

// Component implements the group_by logic
type Component struct{}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Group By",
		Info:        "Groups an array of items by a specified field path. Input: items array + groupByPath (e.g., 'labels.app'). Output: array of groups sorted by key, each with key, items, and count.",
		Tags:        []string{"SDK", "Array", "Aggregate"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != InPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}

	in, ok := msg.(InMessage)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}

	if in.GroupByPath == "" {
		return module.Fail(fmt.Errorf("groupByPath is required"))
	}

	// Group items by the path
	groups := make(map[string][]Item)
	pathParts := strings.Split(in.GroupByPath, ".")

	for _, item := range in.Items {
		key := extractPath(item, pathParts)
		groups[key] = append(groups[key], item)
	}

	// Get sorted keys for deterministic output
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Convert to output format with sorted keys
	result := make([]Group, 0, len(groups))
	for _, key := range keys {
		items := groups[key]
		result = append(result, Group{
			Key:   key,
			Items: items,
			Count: len(items),
		})
	}

	return handler(ctx, OutPort, OutMessage{
		Context: in.Context,
		Groups:  result,
		Total:   len(in.Items),
	})
}

// extractPath extracts a value from a nested structure by path
func extractPath(item any, pathParts []string) string {
	current := item

	for _, part := range pathParts {
		if current == nil {
			return ""
		}

		switch v := current.(type) {
		case map[string]any:
			current = v[part]
		case map[string]string:
			if val, ok := v[part]; ok {
				return val
			}
			return ""
		default:
			// Try reflection for struct fields
			rv := reflect.ValueOf(current)
			if rv.Kind() == reflect.Ptr {
				rv = rv.Elem()
			}
			if rv.Kind() == reflect.Struct {
				field := rv.FieldByNameFunc(func(name string) bool {
					return strings.EqualFold(name, part)
				})
				if field.IsValid() {
					current = field.Interface()
					continue
				}
			}
			return ""
		}
	}

	// Convert final value to string
	if current == nil {
		return ""
	}
	return fmt.Sprintf("%v", current)
}

func (c *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          InPort,
			Label:         "In",
			Configuration: InMessage{},
			Position:      module.Left,
		},
		{
			Name:   OutPort,
			Label:  "Out",
			Source: true,
			Configuration: OutMessage{
				Groups: []Group{
					{Key: "group-a", Count: 2, Items: []Item{}},
					{Key: "group-b", Count: 1, Items: []Item{}},
				},
				Total: 3,
			},
			Position: module.Right,
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
