// Package bloblist lists objects in an S3-compatible store bucket by prefix.
// It answers "what is in there?" — feed the keys to blob_get, blob_presign,
// or a loop over per-object processing.
package bloblist

import (
	"context"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/storage-module/internal/s3conn"
)

const (
	ComponentName = "blob_list"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"

	DefaultMax = 100
	MaxMax     = 1000
)

type Context any

type Settings struct {
	s3conn.Conn

	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Emit failures on the error port as {context, error, retryable} instead of failing the flow — route them to a fallback or the retry component."`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context passed through to the response unchanged."`

	Bucket string `json:"bucket,omitempty" title:"Bucket" description:"Bucket to list. Empty falls back to the component's settings.bucket default."`

	Prefix string `json:"prefix,omitempty" title:"Prefix" description:"Only keys starting with this prefix are returned, e.g. reports/2026-08/. Empty lists the whole bucket (recursively)."`

	Max int `json:"max" default:"100" minimum:"1" maximum:"1000" title:"Max" description:"Maximum items to return. Default 100, hard cap 1000 (larger values are clamped). When more objects match, truncated=true — narrow the prefix to see the rest."`
}

type Item struct {
	Key          string `json:"key" title:"Key"`
	Size         int64  `json:"size" title:"Size" description:"Object size in bytes."`
	LastModified string `json:"lastModified" title:"Last Modified" description:"RFC 3339 timestamp."`
	ETag         string `json:"etag" title:"ETag"`
}

type Response struct {
	Context   Context `json:"context,omitempty" title:"Context"`
	Bucket    string  `json:"bucket" title:"Bucket" description:"Bucket that was listed (after settings fallback)."`
	Items     []Item  `json:"items" title:"Items" description:"Matching objects, at most max of them. Empty array when nothing matches."`
	Truncated bool    `json:"truncated" title:"Truncated" description:"True when more objects matched than max allowed — narrow the prefix or raise max (cap 1000)."`
}

type Component struct {
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			Conn: s3conn.Conn{UseSSL: true},
		},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Blob List",
		Info: "Lists objects in an S3-compatible store (AWS S3, MinIO, Cloudflare R2, GCS interop) by prefix and emits {items: [{key, size, lastModified, etag}], truncated}. Use it to discover what to blob_get or blob_presign — e.g. all of reports/2026-08/. " +
			"Bucket precedence: request.bucket wins; empty falls back to settings.bucket. " +
			"Listing is recursive under the prefix. At most max items are returned (default 100, hard cap 1000); truncated=true means more matched — narrow the prefix rather than paging, there is no cursor. items is an empty array, not an error, when nothing matches.",
		Tags: []string{"Storage", "S3", "MinIO", "Blob", "List"},
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
	return c.list(ctx, handler, in)
}

// capMax applies the default and the hard cap: <=0 becomes DefaultMax,
// anything above MaxMax is clamped to MaxMax.
func capMax(max int) int {
	if max <= 0 {
		return DefaultMax
	}
	if max > MaxMax {
		return MaxMax
	}
	return max
}

func (c *Component) list(ctx context.Context, handler module.Handler, in Request) module.Result {
	bucket, err := c.settings.ResolveBucket(in.Bucket)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	client, err := s3conn.Client(c.settings.Conn)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	max := capMax(in.Max)

	// Cancel the listing as soon as we have seen one object past max — that
	// one extra object is the truncation signal, everything beyond it is
	// wasted transfer.
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	items := make([]Item, 0, max)
	truncated := false
	for obj := range client.ListObjects(listCtx, bucket, minio.ListObjectsOptions{
		Prefix:    in.Prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return c.fail(ctx, handler, in.Context, s3conn.Classify(obj.Err))
		}
		if len(items) == max {
			truncated = true
			break
		}
		items = append(items, Item{
			Key:          obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified.UTC().Format(time.RFC3339),
			ETag:         obj.ETag,
		})
	}

	return handler(ctx, ResponsePort, Response{
		Context:   in.Context,
		Bucket:    bucket,
		Items:     items,
		Truncated: truncated,
	})
}

func (c *Component) fail(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	if !c.settings.EnableErrorPort {
		// Bubble unchanged so retryability marked at the call site survives.
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, module.NewError(reqCtx, err))
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{
			Name:     RequestPort,
			Label:    "Request",
			Position: module.Left,
			Configuration: Request{
				Prefix: "reports/2026-08/",
				Max:    DefaultMax,
			},
		},
		{
			Name:     ResponsePort,
			Label:    "Response",
			Source:   true,
			Position: module.Right,
			// Concrete sample so edges reading $.items[0].key are checkable
			// at build time instead of resolving to null at runtime.
			Configuration: Response{
				Bucket: "reports",
				Items: []Item{
					{
						Key:          "reports/2026-08/summary.csv",
						Size:         27,
						LastModified: "2026-08-01T10:00:00Z",
						ETag:         "9b2cf535f27731c974343645a3985328",
					},
				},
				Truncated: false,
			},
		},
	}
	if !c.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name: ErrorPort, Label: "Error", Source: true, Configuration: module.ErrorMessage{}, Position: module.Bottom,
	})
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
