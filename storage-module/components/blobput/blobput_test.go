package blobput

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/modules/storage-module/internal/s3conn"
)

type emitted struct {
	port string
	data any
}

func runPut(t *testing.T, settings Settings, req Request) ([]emitted, module.Result) {
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

// fakeS3 records the single PUT it receives and answers 200 with an ETag.
type fakeS3 struct {
	mu          sync.Mutex
	method      string
	path        string
	contentType string
	body        []byte
	status      int
}

func (f *fakeS3) settings(t *testing.T, srv *httptest.Server) Settings {
	t.Helper()
	return Settings{
		Conn: s3conn.Conn{
			Endpoint:  strings.TrimPrefix(srv.URL, "http://"),
			Region:    "us-east-1", // skip the bucket-location probe
			AccessKey: "test-access",
			SecretKey: "test-secret",
			UseSSL:    false,
		},
	}
}

// decodeAWSChunked strips the aws-chunked wire framing minio-go uses for
// streaming-signature uploads over plain HTTP: repeated
// "<hex-size>;chunk-signature=<sig>\r\n<data>\r\n" until a zero-size chunk.
func decodeAWSChunked(raw []byte) []byte {
	var out []byte
	rest := raw
	for {
		idx := strings.Index(string(rest), "\r\n")
		if idx < 0 {
			return out
		}
		header := string(rest[:idx])
		rest = rest[idx+2:]
		sizeHex, _, _ := strings.Cut(header, ";")
		size, err := strconv.ParseInt(sizeHex, 16, 64)
		if err != nil || size == 0 || int64(len(rest)) < size {
			return out
		}
		out = append(out, rest[:size]...)
		rest = rest[size:]
		if len(rest) >= 2 {
			rest = rest[2:] // trailing \r\n after the chunk data
		}
	}
}

func newFakeS3(t *testing.T) (*fakeS3, *httptest.Server) {
	t.Helper()
	f := &fakeS3{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
			body = decodeAWSChunked(body)
		}
		f.mu.Lock()
		f.method = r.Method
		f.path = r.URL.Path
		f.contentType = r.Header.Get("Content-Type")
		f.body = body
		status := f.status
		f.mu.Unlock()
		if status != http.StatusOK {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("ETag", `"9b2cf535f27731c974343645a3985328"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func TestPutStoresObject(t *testing.T) {
	f, srv := newFakeS3(t)
	settings := f.settings(t, srv)
	settings.Bucket = "reports"

	payload := "date,visits\n2026-08-01,42\n"
	outs, res := runPut(t, settings, Request{
		Context:     map[string]any{"runId": "r1"},
		Key:         "reports/2026-08/summary.csv",
		Data:        payload,
		ContentType: "text/csv",
	})
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if len(outs) != 1 || outs[0].port != ResponsePort {
		t.Fatalf("expected 1 response emission, got %+v", outs)
	}

	resp := outs[0].data.(Response)
	if resp.Bucket != "reports" || resp.Key != "reports/2026-08/summary.csv" {
		t.Errorf("bucket/key: %+v", resp)
	}
	if resp.ETag != "9b2cf535f27731c974343645a3985328" {
		t.Errorf("etag = %q", resp.ETag)
	}
	if resp.Size != int64(len(payload)) {
		t.Errorf("size = %d, want %d", resp.Size, len(payload))
	}
	ctxMap, ok := resp.Context.(map[string]any)
	if !ok || ctxMap["runId"] != "r1" {
		t.Errorf("context not passed through: %+v", resp.Context)
	}

	if f.method != http.MethodPut {
		t.Errorf("server saw %s, want PUT", f.method)
	}
	if f.path != "/reports/reports/2026-08/summary.csv" {
		t.Errorf("server saw path %q", f.path)
	}
	if f.contentType != "text/csv" {
		t.Errorf("server saw content-type %q", f.contentType)
	}
	if string(f.body) != payload {
		t.Errorf("server received body %q, want %q", f.body, payload)
	}
}

func TestPutBase64RoundTrip(t *testing.T) {
	f, srv := newFakeS3(t)
	settings := f.settings(t, srv)

	binary := make([]byte, 256)
	for i := range binary {
		binary[i] = byte(i)
	}
	outs, res := runPut(t, settings, Request{
		Bucket:     "blobs",
		Key:        "raw.bin",
		Data:       base64.StdEncoding.EncodeToString(binary),
		DataBase64: true,
	})
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	resp := outs[0].data.(Response)
	if resp.Size != 256 {
		t.Errorf("size = %d, want 256 (decoded bytes, not base64 length)", resp.Size)
	}
	if string(f.body) != string(binary) {
		t.Errorf("server did not receive the decoded bytes")
	}
	if f.contentType != DefaultContentType {
		t.Errorf("default content-type not applied: %q", f.contentType)
	}
}

func TestPutRequestBucketWinsOverSettings(t *testing.T) {
	f, srv := newFakeS3(t)
	settings := f.settings(t, srv)
	settings.Bucket = "default-bucket"

	_, res := runPut(t, settings, Request{Bucket: "explicit", Key: "k.txt", Data: "x"})
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if !strings.HasPrefix(f.path, "/explicit/") {
		t.Errorf("request bucket should win, server saw %q", f.path)
	}
}

func TestPutValidation(t *testing.T) {
	tests := []struct {
		name     string
		settings Settings
		req      Request
		wantIn   string
	}{
		{
			name:     "missing key",
			settings: Settings{Conn: s3conn.Conn{Endpoint: "minio:9000", Bucket: "b"}},
			req:      Request{Data: "x"},
			wantIn:   "key",
		},
		{
			name:     "no bucket anywhere",
			settings: Settings{Conn: s3conn.Conn{Endpoint: "minio:9000"}},
			req:      Request{Key: "k", Data: "x"},
			wantIn:   "settings.bucket",
		},
		{
			name:     "bad base64",
			settings: Settings{Conn: s3conn.Conn{Endpoint: "minio:9000", Bucket: "b"}},
			req:      Request{Key: "k", Data: "not-base64!!!", DataBase64: true},
			wantIn:   "base64",
		},
		{
			name:     "no endpoint",
			settings: Settings{Conn: s3conn.Conn{Bucket: "b"}},
			req:      Request{Key: "k", Data: "x"},
			wantIn:   "settings.endpoint",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outs, res := runPut(t, tt.settings, tt.req)
			err := res.Err()
			if err == nil {
				t.Fatalf("expected error, got emissions %+v", outs)
			}
			if len(outs) != 0 {
				t.Errorf("expected no emissions, got %+v", outs)
			}
			if module.ShouldRetry(err) {
				t.Errorf("validation failure must be permanent: %v", err)
			}
			if !strings.Contains(err.Error(), tt.wantIn) {
				t.Errorf("error %q should mention %q", err.Error(), tt.wantIn)
			}
		})
	}
}

func TestPutServerFailureRetryable(t *testing.T) {
	f, srv := newFakeS3(t)
	f.status = http.StatusServiceUnavailable
	settings := f.settings(t, srv)

	outs, res := runPut(t, settings, Request{Bucket: "bkt", Key: "k.txt", Data: "x"})
	err := res.Err()
	if err == nil {
		t.Fatalf("expected error, got %+v", outs)
	}
	if !module.ShouldRetry(err) {
		t.Errorf("503 during put must be retryable (PutObject is idempotent): %v", err)
	}
}

func TestPutErrorPortEmitsErrorMessage(t *testing.T) {
	f, srv := newFakeS3(t)
	f.status = http.StatusServiceUnavailable
	settings := f.settings(t, srv)
	settings.EnableErrorPort = true

	outs, res := runPut(t, settings, Request{Context: "req-7", Bucket: "bkt", Key: "k.txt", Data: "x"})
	if err := res.Err(); err != nil {
		t.Fatalf("with error port enabled the hop should not fail: %v", err)
	}
	if len(outs) != 1 || outs[0].port != ErrorPort {
		t.Fatalf("expected 1 error-port emission, got %+v", outs)
	}
	msg, ok := outs[0].data.(module.ErrorMessage)
	if !ok {
		t.Fatalf("emitted %T, want module.ErrorMessage", outs[0].data)
	}
	if !msg.Retryable {
		t.Errorf("503 should surface retryable=true on the error port")
	}
	if msg.Context != "req-7" {
		t.Errorf("context not passed through: %+v", msg.Context)
	}
}
