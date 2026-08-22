// Package blobput uploads an object to an S3-compatible store (AWS S3, MinIO,
// Cloudflare R2, GCS interop). It is the flow's way to STORE a blob — a
// fetched report, a generated CSV, a decoded attachment — under a bucket/key
// so blob_get, blob_presign, or any external consumer can pick it up.
package blobput

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/storage-module/internal/s3conn"
)

const (
	ComponentName = "blob_put"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"

	DefaultContentType = "application/octet-stream"
)

type Context any

type Settings struct {
	s3conn.Conn

	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Emit failures on the error port as {context, error, retryable} instead of failing the flow — route them to a fallback or the retry component."`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context passed through to the response unchanged."`

	Bucket string `json:"bucket,omitempty" title:"Bucket" description:"Target bucket. Empty falls back to the component's settings.bucket default."`

	Key string `json:"key" required:"true" minLength:"1" title:"Key" description:"Object key (path) inside the bucket, e.g. reports/2026-08/summary.csv."`

	Data string `json:"data" title:"Data" format:"textarea" description:"Object content. Stored byte-for-byte as given. For binary content set dataBase64=true and pass standard base64 here. Empty data writes a zero-byte object."`

	DataBase64 bool `json:"dataBase64" title:"Data Is Base64" description:"When true, data is standard base64 of the binary object content and is decoded before upload — the store receives the original bytes."`

	ContentType string `json:"contentType,omitempty" title:"Content Type" description:"Content-Type stored with the object; defaults to application/octet-stream. Set text/csv, application/pdf, image/png, ... so downloads and presigned links serve correctly."`
}

type Response struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Bucket  string  `json:"bucket" title:"Bucket" description:"Bucket the object was written to (after settings fallback)."`
	Key     string  `json:"key" title:"Key"`
	ETag    string  `json:"etag" title:"ETag" description:"Entity tag the store assigned to the written object."`
	Size    int64   `json:"size" title:"Size" description:"Stored size in bytes (after base64 decoding, when used)."`
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
		Description: "Blob Put",
		Info: "Uploads an object to an S3-compatible store (AWS S3, MinIO, Cloudflare R2, GCS interop) and emits {bucket, key, etag, size}. Use it to persist anything a flow produced or fetched — a report, a CSV, an attachment — so blob_get can read it back or blob_presign can hand out a download link. " +
			"Bucket precedence: request.bucket wins; empty falls back to settings.bucket; neither set is an error. " +
			"Text content goes in data as-is; for binary content set dataBase64=true and pass standard base64 in data — the store receives the decoded bytes. Set contentType (default application/octet-stream) so downloads serve with the right type. " +
			"Network failures during upload are marked retryable: S3 PutObject is a full-object replace of the same key with the same bytes, so re-running a possibly-landed upload converges on the same stored object.",
		Tags: []string{"Storage", "S3", "MinIO", "Blob", "Upload"},
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
	return c.put(ctx, handler, in)
}

func (c *Component) put(ctx context.Context, handler module.Handler, in Request) module.Result {
	if in.Key == "" {
		return c.fail(ctx, handler, in.Context, module.Permanent(errors.New("key is required")))
	}

	bucket, err := c.settings.ResolveBucket(in.Bucket)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	payload := []byte(in.Data)
	if in.DataBase64 {
		payload, err = base64.StdEncoding.DecodeString(in.Data)
		if err != nil {
			return c.fail(ctx, handler, in.Context, module.Permanent(fmt.Errorf("data is not valid standard base64 (dataBase64=true): %w", err)))
		}
	}

	contentType := in.ContentType
	if contentType == "" {
		contentType = DefaultContentType
	}

	client, err := s3conn.Client(c.settings.Conn)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	info, err := client.PutObject(ctx, bucket, in.Key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		// A write, but a safely retryable one: S3 PutObject is idempotent
		// (same key + same bytes = same stored object), so Classify may mark
		// transient failures retryable without risking a duplicate.
		return c.fail(ctx, handler, in.Context, s3conn.Classify(err))
	}

	size := info.Size
	if size == 0 {
		size = int64(len(payload))
	}
	return handler(ctx, ResponsePort, Response{
		Context: in.Context,
		Bucket:  bucket,
		Key:     in.Key,
		ETag:    info.ETag,
		Size:    size,
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
				Key:         "reports/2026-08/summary.csv",
				Data:        "date,visits\n2026-08-01,42\n",
				ContentType: "text/csv",
			},
		},
		{
			Name:     ResponsePort,
			Label:    "Response",
			Source:   true,
			Position: module.Right,
			// Concrete sample so edges reading $.etag / $.size are checkable
			// at build time instead of resolving to null at runtime.
			Configuration: Response{
				Bucket: "reports",
				Key:    "reports/2026-08/summary.csv",
				ETag:   "9b2cf535f27731c974343645a3985328",
				Size:   27,
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
