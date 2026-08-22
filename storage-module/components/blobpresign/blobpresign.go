// Package blobpresign mints a time-limited presigned URL for one object in an
// S3-compatible store, so something OUTSIDE the flow — a browser, Slack, a
// customer — can download (GET) or upload (PUT) the object directly, without
// credentials and without the bytes ever riding through the flow.
package blobpresign

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/modules/storage-module/internal/s3conn"
)

const (
	ComponentName = "blob_presign"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"

	MethodGet = "GET"
	MethodPut = "PUT"

	DefaultExpirySeconds = 900
	// MaxExpirySeconds is the S3 signature-v4 hard limit: 7 days.
	MaxExpirySeconds = 604800
)

type Context any

type Settings struct {
	s3conn.Conn

	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Emit failures on the error port as {context, error, retryable} instead of failing the flow — route them to a fallback or the retry component."`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context passed through to the response unchanged."`

	Bucket string `json:"bucket,omitempty" title:"Bucket" description:"Bucket holding (or receiving) the object. Empty falls back to the component's settings.bucket default."`

	Key string `json:"key" required:"true" minLength:"1" title:"Key" description:"Object key the URL grants access to, e.g. reports/2026-08/summary.csv."`

	Method string `json:"method" enum:"GET,PUT" enumTitles:"GET (download),PUT (upload)" default:"GET" title:"Method" description:"GET presigns a download of an existing object; PUT presigns an upload slot the holder can write once to this key."`

	ExpirySeconds int `json:"expirySeconds" default:"900" minimum:"1" maximum:"604800" title:"Expiry Seconds" description:"How long the URL stays valid. Default 900 (15 minutes); hard max 604800 (7 days — the S3 signing limit)."`
}

type Response struct {
	Context Context `json:"context,omitempty" title:"Context"`
	URL     string  `json:"url" title:"URL" description:"The presigned URL — hand it to anything that can speak plain HTTP; no credentials needed until it expires."`
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
		Description: "Blob Presign",
		Info: "Mints a time-limited presigned URL for one object in an S3-compatible store (AWS S3, MinIO, Cloudflare R2, GCS interop) and emits {url}. The URL carries its own signature: whoever holds it can GET (download) or PUT (upload) that one object over plain HTTP until it expires — no credentials, and the bytes never ride through the flow. Typical use: blob_put a report, blob_presign it, send the link to Slack or email. " +
			"Bucket precedence: request.bucket wins; empty falls back to settings.bucket. " +
			"Expiry defaults to 900s (15 min) and is capped at 604800s (7 days — the S3 signing limit; longer requests fail with a permanent error). " +
			"Signing happens locally; note that a GET URL for a missing key still signs fine — the holder gets a 404 when using it. For in-cluster MinIO endpoints the URL points at the internal host, reachable only where the cluster DNS resolves.",
		Tags: []string{"Storage", "S3", "MinIO", "Blob", "Presign", "Share"},
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
	return c.presign(ctx, handler, in)
}

func (c *Component) presign(ctx context.Context, handler module.Handler, in Request) module.Result {
	if in.Key == "" {
		return c.fail(ctx, handler, in.Context, module.Permanent(errors.New("key is required")))
	}

	bucket, err := c.settings.ResolveBucket(in.Bucket)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	method := strings.ToUpper(strings.TrimSpace(in.Method))
	if method == "" {
		method = MethodGet
	}
	if method != MethodGet && method != MethodPut {
		return c.fail(ctx, handler, in.Context, module.Permanent(fmt.Errorf("unknown method %q: use GET (download) or PUT (upload)", in.Method)))
	}

	expirySeconds := in.ExpirySeconds
	if expirySeconds <= 0 {
		expirySeconds = DefaultExpirySeconds
	}
	if expirySeconds > MaxExpirySeconds {
		return c.fail(ctx, handler, in.Context, module.Permanent(fmt.Errorf("expirySeconds=%d exceeds the S3 signing limit of %d (7 days)", expirySeconds, MaxExpirySeconds)))
	}
	expiry := time.Duration(expirySeconds) * time.Second

	client, err := s3conn.Client(c.settings.Conn)
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	// Signing is local, but when settings.region is empty the client may
	// probe the bucket location over the network first — hence Classify, so a
	// transient probe failure stays retryable.
	var u fmt.Stringer
	switch method {
	case MethodGet:
		u, err = client.PresignedGetObject(ctx, bucket, in.Key, expiry, nil)
	case MethodPut:
		u, err = client.PresignedPutObject(ctx, bucket, in.Key, expiry)
	}
	if err != nil {
		return c.fail(ctx, handler, in.Context, s3conn.Classify(err))
	}

	return handler(ctx, ResponsePort, Response{
		Context: in.Context,
		URL:     u.String(),
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
				Key:           "reports/2026-08/summary.csv",
				Method:        MethodGet,
				ExpirySeconds: DefaultExpirySeconds,
			},
		},
		{
			Name:     ResponsePort,
			Label:    "Response",
			Source:   true,
			Position: module.Right,
			// Concrete sample so an edge reading $.url is checkable at build
			// time instead of resolving to null at runtime.
			Configuration: Response{
				URL: "https://s3.amazonaws.com/reports/reports/2026-08/summary.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=900&X-Amz-Signature=examplesignature",
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
