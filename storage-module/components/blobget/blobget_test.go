package blobget

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/storage-module/internal/s3conn"
)

type emitted struct {
	port string
	data any
}

func runGet(t *testing.T, settings Settings, req Request) ([]emitted, module.Result) {
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

// fakeObject serves HEAD (stat) and GET for one object. gets counts how many
// GETs actually pulled the body — the size cap must refuse before any GET.
func fakeObject(t *testing.T, body []byte, contentType string, gets *atomic.Int32) Settings {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("ETag", `"abc123"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodHead {
			return
		}
		if gets != nil {
			gets.Add(1)
		}
		w.WriteHeader(http.StatusOK)
		w.Write(body)
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

func TestGetText(t *testing.T) {
	body := []byte("date,visits\n2026-08-01,42\n")
	settings := fakeObject(t, body, "text/csv", nil)

	outs, res := runGet(t, settings, Request{
		Context: map[string]any{"runId": "r1"},
		Key:     "reports/2026-08/summary.csv",
	})
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if len(outs) != 1 || outs[0].port != ResponsePort {
		t.Fatalf("expected 1 response emission, got %+v", outs)
	}

	resp := outs[0].data.(Response)
	if resp.Data != string(body) {
		t.Errorf("data = %q, want %q", resp.Data, body)
	}
	if resp.ContentType != "text/csv" {
		t.Errorf("contentType = %q", resp.ContentType)
	}
	if resp.Size != int64(len(body)) {
		t.Errorf("size = %d, want %d", resp.Size, len(body))
	}
	if resp.Bucket != "reports" {
		t.Errorf("bucket fallback not applied: %q", resp.Bucket)
	}
	ctxMap, ok := resp.Context.(map[string]any)
	if !ok || ctxMap["runId"] != "r1" {
		t.Errorf("context not passed through: %+v", resp.Context)
	}
}

func TestGetBinaryAsBase64(t *testing.T) {
	binary := make([]byte, 256)
	for i := range binary {
		binary[i] = byte(i)
	}
	settings := fakeObject(t, binary, "application/octet-stream", nil)

	outs, res := runGet(t, settings, Request{Key: "raw.bin", AsBase64: true})
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	resp := outs[0].data.(Response)
	decoded, err := base64.StdEncoding.DecodeString(resp.Data)
	if err != nil {
		t.Fatalf("data is not valid base64: %v", err)
	}
	if string(decoded) != string(binary) {
		t.Errorf("base64 round-trip lost bytes")
	}
	if resp.Size != 256 {
		t.Errorf("size = %d, want 256 (original bytes, not base64 length)", resp.Size)
	}
}

func TestGetRefusesOversizedBeforeDownload(t *testing.T) {
	var gets atomic.Int32
	body := make([]byte, 100)
	settings := fakeObject(t, body, "application/octet-stream", &gets)

	outs, res := runGet(t, settings, Request{Key: "big.bin", MaxBytes: 10})
	err := res.Err()
	if err == nil {
		t.Fatalf("expected refusal, got %+v", outs)
	}
	if module.ShouldRetry(err) {
		t.Errorf("size refusal must be permanent: %v", err)
	}
	if !strings.Contains(err.Error(), "maxBytes=10") || !strings.Contains(err.Error(), "100 bytes") {
		t.Errorf("refusal should name the cap and the actual size: %v", err)
	}
	if !strings.Contains(err.Error(), "request.maxBytes") {
		t.Errorf("refusal should tell the caller which knob to raise: %v", err)
	}
	if gets.Load() != 0 {
		t.Errorf("object body was downloaded despite the size refusal")
	}
}

func TestGetDefaultMaxBytesApplied(t *testing.T) {
	// An object comfortably under 10 MiB with maxBytes left zero must pass.
	body := []byte("small")
	settings := fakeObject(t, body, "text/plain", nil)

	outs, res := runGet(t, settings, Request{Key: "small.txt"})
	if err := res.Err(); err != nil {
		t.Fatalf("default maxBytes should admit a small object: %v", err)
	}
	if resp := outs[0].data.(Response); resp.Data != "small" {
		t.Errorf("data = %q", resp.Data)
	}
}

func TestGetValidation(t *testing.T) {
	outs, res := runGet(t, Settings{Conn: s3conn.Conn{Endpoint: "minio:9000", Bucket: "b"}}, Request{})
	err := res.Err()
	if err == nil {
		t.Fatalf("missing key should fail, got %+v", outs)
	}
	if module.ShouldRetry(err) {
		t.Errorf("validation failure must be permanent: %v", err)
	}

	_, res = runGet(t, Settings{Conn: s3conn.Conn{Endpoint: "minio:9000"}}, Request{Key: "k"})
	if err := res.Err(); err == nil || !strings.Contains(err.Error(), "settings.bucket") {
		t.Errorf("missing bucket should name the settings fallback: %v", err)
	}
}

func TestGetNotFoundPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	settings := Settings{
		Conn: s3conn.Conn{
			Endpoint:  strings.TrimPrefix(srv.URL, "http://"),
			Region:    "us-east-1",
			AccessKey: "a", SecretKey: "s", UseSSL: false, Bucket: "bkt",
		},
	}

	_, res := runGet(t, settings, Request{Key: "gone.txt"})
	err := res.Err()
	if err == nil {
		t.Fatalf("expected 404 error")
	}
	if module.ShouldRetry(err) {
		t.Errorf("404 must be permanent: %v", err)
	}
}

func TestGetErrorPortEmitsErrorMessage(t *testing.T) {
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
		EnableErrorPort: true,
	}

	outs, res := runGet(t, settings, Request{Context: "req-9", Key: "k.txt"})
	if err := res.Err(); err != nil {
		t.Fatalf("with error port enabled the hop should not fail: %v", err)
	}
	if len(outs) != 1 || outs[0].port != ErrorPort {
		t.Fatalf("expected 1 error-port emission, got %+v", outs)
	}
	msg := outs[0].data.(module.ErrorMessage)
	if !msg.Retryable {
		t.Errorf("503 should surface retryable=true on the error port")
	}
	if msg.Context != "req-9" {
		t.Errorf("context not passed through: %+v", msg.Context)
	}
}

// Guard against the constant drifting from the documented 10485760.
func TestDefaultMaxBytesValue(t *testing.T) {
	if DefaultMaxBytes != 10485760 {
		t.Fatalf("DefaultMaxBytes = %d, docs and schema say 10485760", DefaultMaxBytes)
	}
}
