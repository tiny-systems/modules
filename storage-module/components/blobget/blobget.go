// Package blobget downloads an object from an S3-compatible store (AWS S3,
// MinIO, Cloudflare R2, GCS interop) into the flow as a string — text as-is,
// binary safely via base64. A size cap refuses surprise-huge objects before
// any bytes are pulled into memory.
package blobget

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/modules/storage-module/internal/s3conn"
)

const (
	ComponentName = "blob_get"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"

	// DefaultMaxBytes caps how much of an object blob_get pulls into the flow:
	// 10 MiB. Blobs ride the flow as message payloads, so the cap protects the
	// runtime, not the store.
	DefaultMaxBytes = 10 * 1024 * 1024
)

type Context any

type Settings struct {
	s3conn.Conn

	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Emit failures on the error port as {context, error, retryable} instead of failing the flow — route them to a fallback or the retry component."`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context passed through to the response unchanged."`

	Bucket string `json:"bucket,omitempty" title:"Bucket" description:"Source bucket. Empty falls back to the component's settings.bucket default."`

	Key string `json:"key" required:"true" minLength:"1" title:"Key" description:"Object key to fetch, e.g. reports/2026-08/summary.csv."`

	MaxBytes int `json:"maxBytes" default:"10485760" minimum:"1" title:"Max Bytes" description:"Refuse objects larger than this instead of pulling them into the flow. Default 10485760 (10 MiB). The refusal is a permanent error naming the object's actual size — raise maxBytes deliberately if you really want the object in-flow."`

	AsBase64 bool `json:"asBase64" title:"As Base64" description:"Return data as standard base64 of the object bytes. REQUIRED for binary objects (images, PDFs, archives): a raw binary string does not survive JSON transport intact. Leave false for text."`
}

type Response struct {
	Context     Context `json:"context,omitempty" title:"Context"`
	Bucket      string  `json:"bucket" title:"Bucket" description:"Bucket the object was read from (after settings fallback)."`
	Key         string  `json:"key" title:"Key"`
	Data        string  `json:"data" title:"Data" description:"Object content — raw text, or standard base64 when asBase64 was set."`
	ContentType string  `json:"contentType" title:"Content Type" description:"Content-Type stored with the object."`
	Size        int64   `json:"size" title:"Size" description:"Object size in bytes (the original bytes, not the base64 length)."`
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
		Description: "Blob Get",
		Info: "Downloads an object from an S3-compatible store (AWS S3, MinIO, Cloudflare R2, GCS interop) and emits {data, contentType, size}. Use it to read back what blob_put stored, or to pull an externally produced file — a data dump, an uploaded CSV — into the flow. " +
			"Bucket precedence: request.bucket wins; empty falls back to settings.bucket. " +
			"Objects larger than maxBytes (default 10485760 = 10 MiB) are refused with a permanent error naming the actual size — blobs travel as in-flow message payloads, so raise maxBytes deliberately, not reflexively. " +
			"For binary objects set asBase64=true: data is then standard base64 of the object bytes (size still counts the original bytes). Text objects can be read as-is with asBase64=false.",
		Tags: []string{"Storage", "S3", "MinIO", "Blob", "Download"},
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
	return c.get(ctx, handler, in)
}

func (c *Component) get(ctx context.Context, handler module.Handler, in Request) module.Result {
	if in.Key == "" {
		return c.fail(ctx, handler, in.Context, module.Permanent(errors.New("key is required")))
	}

	bucket, err := c.settings.ResolveBucket(in.Bucket)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	maxBytes := int64(in.MaxBytes)
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}

	client, err := s3conn.Client(c.settings.Conn)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	// Stat first: the size cap must refuse an oversized object BEFORE any of
	// its bytes are pulled into memory, and stat also yields the content type.
	stat, err := client.StatObject(ctx, bucket, in.Key, minio.StatObjectOptions{})
	if err != nil {
		return c.fail(ctx, handler, in.Context, s3conn.Classify(err))
	}
	if stat.Size > maxBytes {
		return c.fail(ctx, handler, in.Context, module.Permanent(fmt.Errorf(
			"object %s/%s is %d bytes, larger than maxBytes=%d: raise request.maxBytes if you really want it in-flow", bucket, in.Key, stat.Size, maxBytes)))
	}

	obj, err := client.GetObject(ctx, bucket, in.Key, minio.GetObjectOptions{})
	if err != nil {
		return c.fail(ctx, handler, in.Context, s3conn.Classify(err))
	}
	defer obj.Close()

	// Belt and braces: the object may have been replaced with a bigger one
	// between stat and get, so the read itself is capped too.
	raw, err := io.ReadAll(io.LimitReader(obj, maxBytes+1))
	if err != nil {
		return c.fail(ctx, handler, in.Context, s3conn.Classify(err))
	}
	if int64(len(raw)) > maxBytes {
		return c.fail(ctx, handler, in.Context, module.Permanent(fmt.Errorf(
			"object %s/%s exceeds maxBytes=%d: raise request.maxBytes if you really want it in-flow", bucket, in.Key, maxBytes)))
	}

	data := string(raw)
	if in.AsBase64 {
		data = base64.StdEncoding.EncodeToString(raw)
	}
	return handler(ctx, ResponsePort, Response{
		Context:     in.Context,
		Bucket:      bucket,
		Key:         in.Key,
		Data:        data,
		ContentType: stat.ContentType,
		Size:        int64(len(raw)),
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
				Key:      "reports/2026-08/summary.csv",
				MaxBytes: DefaultMaxBytes,
			},
		},
		{
			Name:     ResponsePort,
			Label:    "Response",
			Source:   true,
			Position: module.Right,
			// Concrete sample so edges reading $.data / $.contentType are
			// checkable at build time instead of resolving to null at runtime.
			Configuration: Response{
				Bucket:      "reports",
				Key:         "reports/2026-08/summary.csv",
				Data:        "date,visits\n2026-08-01,42\n",
				ContentType: "text/csv",
				Size:        27,
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
