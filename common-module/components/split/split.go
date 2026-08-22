package split

import (
	"context"
	"fmt"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName        = "array_split"
	OutPort       string = "out"
	InPort        string = "in"
)

type Context any

type ItemContext any

type InMessage struct {
	Context Context       `json:"context" title:"Context" configurable:"true" description:"Message to be send further with each item"`
	Array   []ItemContext `json:"array" title:"Array" default:"null" description:"Array of items to be split" required:"true" shared:"true"`
}

type OutMessage struct {
	Context Context     `json:"context"`
	Item    ItemContext `json:"item" shared:"true"`
	Index   int         `json:"index" title:"Index" description:"0-based position of this item in the source array"`
	Total   int         `json:"total" title:"Total" description:"Number of items in the source array"`
}

type Component struct {
}

func (t *Component) Instance() module.Component {
	return &Component{}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Split Array",
		Info:        "Array iterator. Input: context + array. Emits one message per array element on Out, each containing {context, item, index, total} — index is the element's 0-based position, total the array length, context the incoming message's context passed through unchanged per item. Elements are processed sequentially - next item sent after previous Out completes. Use to process lists item by item. For map-reduce, pair with `collect`: route each item through the per-item work, map index/total (and a group key unique per source message) into collect, and it reassembles the array once all items arrive.",
		Tags:        []string{"SDK", "ARRAY"},
	}
}

func (t *Component) Handle(ctx context.Context, handler module.Handler, _ string, msg interface{}) module.Result {
	if in, ok := msg.(InMessage); ok {
		total := len(in.Array)
		for idx, item := range in.Array {
			r := handler(ctx, OutPort, OutMessage{
				Context: in.Context,
				Item:    item,
				Index:   idx,
				Total:   total,
			})
			if r.IsErr() {
				return r
			}
		}
		return module.Result{}
	}
	return module.Fail(fmt.Errorf("invalid message"))
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
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
			Configuration: OutMessage{},
			Position:      module.Right,
		},
	}
}

func init() {
	registry.Register((&Component{}).Instance())
}
