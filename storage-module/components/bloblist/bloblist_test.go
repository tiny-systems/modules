package bloblist

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/storage-module/internal/s3conn"
)

type emitted struct {
	port string
	data any
}

func runList(t *testing.T, settings Settings, req Request) ([]emitted, module.Result) {
	t.Helper()
	comp := (&Component{}).Instance()
	if err := comp.(module.SettingsHandler).OnSettings(context.Background(), settings); err != nil {
		t.Fatalf("OnSettings: %v", err)
	}

	var outs []emitted
	handler := func(_ context.Context, port string, data any) module.Result {
		outs = append(outs, emitted{port: port, data: data})
		return module.Ok(nil)
	}
	res := comp.Handle(context.Background(), handler, RequestPort, req)
	return outs, res
}

// fakeBucket serves a ListObjectsV2 response with n objects in one page.
func fakeBucket(t *testing.T, n int) Settings {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
		b.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		b.WriteString(`<Name>reports</Name><Prefix></Prefix><KeyCount>`)
		fmt.Fprintf(&b, "%d", n)
		b.WriteString(`</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`)
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b,
				`<Contents><Key>reports/2026-08/file-%03d.csv</Key><LastModified>2026-08-01T10:00:00.000Z</LastModified><ETag>&quot;etag-%03d&quot;</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`,
				i, i, 100+i)
		}
		b.WriteString(`</ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(b.String()))
	}))
	t.Cleanup(srv.Close)

	return Settings{
		Conn: s3conn.Conn{
			Endpoint:  strings.TrimPrefix(srv.URL, "http://"),
			Region:    "us-east-1", // skip the bucket-location probe
			AccessKey: "test-access",
			SecretKey: "test-secret",
			UseSSL:    false,
			Bucket:    "reports",
		},
	}
}

func TestListItems(t *testing.T) {
	settings := fakeBucket(t, 2)

	outs, res := runList(t, settings, Request{Context: "c1", Prefix: "reports/"})
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if len(outs) != 1 || outs[0].port != ResponsePort {
		t.Fatalf("expected 1 response emission, got %+v", outs)
	}

	resp := outs[0].data.(Response)
	if resp.Truncated {
		t.Errorf("2 items under default max=100 must not be truncated")
	}
	if len(resp.Items) != 2 {
		t.Fatalf("items = %d, want 2", len(resp.Items))
	}
	first := resp.Items[0]
	if first.Key != "reports/2026-08/file-000.csv" {
		t.Errorf("key = %q", first.Key)
	}
	if first.Size != 100 {
		t.Errorf("size = %d", first.Size)
	}
	if first.ETag != "etag-000" {
		t.Errorf("etag = %q (quotes should be trimmed)", first.ETag)
	}
	if first.LastModified != "2026-08-01T10:00:00Z" {
		t.Errorf("lastModified = %q, want RFC 3339", first.LastModified)
	}
	if resp.Bucket != "reports" {
		t.Errorf("bucket fallback not applied: %q", resp.Bucket)
	}
	if resp.Context != "c1" {
		t.Errorf("context not passed through: %+v", resp.Context)
	}
}

func TestListTruncation(t *testing.T) {
	settings := fakeBucket(t, 3)

	outs, res := runList(t, settings, Request{Max: 2})
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	resp := outs[0].data.(Response)
	if len(resp.Items) != 2 {
		t.Errorf("items = %d, want 2 (max)", len(resp.Items))
	}
	if !resp.Truncated {
		t.Errorf("3 objects with max=2 must report truncated=true")
	}
}

func TestListEmptyIsAnArrayNotAnError(t *testing.T) {
	settings := fakeBucket(t, 0)

	outs, res := runList(t, settings, Request{Prefix: "nothing-here/"})
	if err := res.Err(); err != nil {
		t.Fatalf("empty listing must not error: %v", err)
	}
	resp := outs[0].data.(Response)
	if resp.Items == nil {
		t.Errorf("items must be an empty array, not null")
	}
	if len(resp.Items) != 0 || resp.Truncated {
		t.Errorf("unexpected result: %+v", resp)
	}
}

func TestCapMax(t *testing.T) {
	tests := []struct{ in, want int }{
		{0, DefaultMax},
		{-5, DefaultMax},
		{1, 1},
		{100, 100},
		{1000, 1000},
		{1001, MaxMax},
		{50000, MaxMax},
	}
	for _, tt := range tests {
		if got := capMax(tt.in); got != tt.want {
			t.Errorf("capMax(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestListValidation(t *testing.T) {
	outs, res := runList(t, Settings{Conn: s3conn.Conn{Endpoint: "minio:9000"}}, Request{})
	err := res.Err()
	if err == nil {
		t.Fatalf("missing bucket should fail, got %+v", outs)
	}
	if module.ShouldRetry(err) {
		t.Errorf("validation failure must be permanent: %v", err)
	}
	if !strings.Contains(err.Error(), "settings.bucket") {
		t.Errorf("error should name the settings fallback: %v", err)
	}
}

func TestListServerFailureRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	settings := Settings{
		Conn: s3conn.Conn{
			Endpoint:  strings.TrimPrefix(srv.URL, "http://"),
			Region:    "us-east-1",
			AccessKey: "a", SecretKey: "s", UseSSL: false, Bucket: "bkt",
		},
	}

	_, res := runList(t, settings, Request{})
	err := res.Err()
	if err == nil {
		t.Fatalf("expected error from 503 server")
	}
	if !module.ShouldRetry(err) {
		t.Errorf("503 during list must be retryable: %v", err)
	}
}
