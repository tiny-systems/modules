package blobpresign

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/modules/storage-module/internal/s3conn"
)

type emitted struct {
	port string
	data any
}

// Presigning is pure signature computation — with settings.region set there is
// no bucket-location probe, so these tests never touch the network.
func testSettings() Settings {
	return Settings{
		Conn: s3conn.Conn{
			Endpoint:  "s3.example.com",
			Region:    "us-east-1",
			AccessKey: "AKIAEXAMPLE",
			SecretKey: "secret",
			UseSSL:    true,
			Bucket:    "reports",
		},
	}
}

func runPresign(t *testing.T, settings Settings, req Request) ([]emitted, module.Result) {
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

func presignedURL(t *testing.T, settings Settings, req Request) *url.URL {
	t.Helper()
	outs, res := runPresign(t, settings, req)
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if len(outs) != 1 || outs[0].port != ResponsePort {
		t.Fatalf("expected 1 response emission, got %+v", outs)
	}
	resp := outs[0].data.(Response)
	u, err := url.Parse(resp.URL)
	if err != nil {
		t.Fatalf("emitted URL does not parse: %q: %v", resp.URL, err)
	}
	return u
}

func TestPresignGetURLShape(t *testing.T) {
	u := presignedURL(t, testSettings(), Request{
		Key:           "reports/2026-08/summary.csv",
		Method:        MethodGet,
		ExpirySeconds: 900,
	})

	if u.Scheme != "https" {
		t.Errorf("scheme = %q, want https (UseSSL)", u.Scheme)
	}
	if u.Host != "s3.example.com" {
		t.Errorf("host = %q", u.Host)
	}
	if u.Path != "/reports/reports/2026-08/summary.csv" {
		t.Errorf("path = %q, want /<bucket>/<key>", u.Path)
	}
	q := u.Query()
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		t.Errorf("X-Amz-Algorithm = %q", q.Get("X-Amz-Algorithm"))
	}
	if q.Get("X-Amz-Expires") != "900" {
		t.Errorf("X-Amz-Expires = %q, want 900", q.Get("X-Amz-Expires"))
	}
	if q.Get("X-Amz-Signature") == "" {
		t.Errorf("X-Amz-Signature missing")
	}
	if !strings.HasPrefix(q.Get("X-Amz-Credential"), "AKIAEXAMPLE/") {
		t.Errorf("X-Amz-Credential = %q, should start with the access key", q.Get("X-Amz-Credential"))
	}
}

func TestPresignPut(t *testing.T) {
	u := presignedURL(t, testSettings(), Request{
		Bucket:        "uploads",
		Key:           "incoming/data.bin",
		Method:        MethodPut,
		ExpirySeconds: 3600,
	})
	if u.Path != "/uploads/incoming/data.bin" {
		t.Errorf("request bucket should win: path = %q", u.Path)
	}
	if got := u.Query().Get("X-Amz-Expires"); got != "3600" {
		t.Errorf("X-Amz-Expires = %q, want 3600", got)
	}
	if u.Query().Get("X-Amz-Signature") == "" {
		t.Errorf("X-Amz-Signature missing")
	}
}

func TestPresignDefaults(t *testing.T) {
	// Empty method → GET; zero expiry → 900; lowercase method tolerated.
	u := presignedURL(t, testSettings(), Request{Key: "k.txt"})
	if got := u.Query().Get("X-Amz-Expires"); got != "900" {
		t.Errorf("default expiry: X-Amz-Expires = %q, want 900", got)
	}

	u = presignedURL(t, testSettings(), Request{Key: "k.txt", Method: "put"})
	if u.Query().Get("X-Amz-Signature") == "" {
		t.Errorf("lowercase method should be accepted")
	}
}

func TestPresignContextPassesThrough(t *testing.T) {
	outs, res := runPresign(t, testSettings(), Request{Context: "c9", Key: "k.txt"})
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if resp := outs[0].data.(Response); resp.Context != "c9" {
		t.Errorf("context not passed through: %+v", resp.Context)
	}
}

func TestPresignValidation(t *testing.T) {
	tests := []struct {
		name   string
		req    Request
		wantIn string
	}{
		{"missing key", Request{}, "key"},
		{"unknown method", Request{Key: "k", Method: "DELETE"}, "GET"},
		{"expiry beyond the S3 limit", Request{Key: "k", ExpirySeconds: MaxExpirySeconds + 1}, "604800"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outs, res := runPresign(t, testSettings(), tt.req)
			err := res.Err()
			if err == nil {
				t.Fatalf("expected error, got %+v", outs)
			}
			if module.ShouldRetry(err) {
				t.Errorf("validation failure must be permanent: %v", err)
			}
			if !strings.Contains(err.Error(), tt.wantIn) {
				t.Errorf("error %q should mention %q", err.Error(), tt.wantIn)
			}
		})
	}

	settings := testSettings()
	settings.Bucket = ""
	_, res := runPresign(t, settings, Request{Key: "k"})
	if err := res.Err(); err == nil || !strings.Contains(err.Error(), "settings.bucket") {
		t.Errorf("missing bucket should name the settings fallback: %v", res.Err())
	}
}

func TestPresignErrorPortEmitsErrorMessage(t *testing.T) {
	settings := testSettings()
	settings.EnableErrorPort = true

	outs, res := runPresign(t, settings, Request{Context: "c3", Key: "k", Method: "DELETE"})
	if err := res.Err(); err != nil {
		t.Fatalf("with error port enabled the hop should not fail: %v", err)
	}
	if len(outs) != 1 || outs[0].port != ErrorPort {
		t.Fatalf("expected 1 error-port emission, got %+v", outs)
	}
	msg := outs[0].data.(module.ErrorMessage)
	if msg.Retryable {
		t.Errorf("validation failure should surface retryable=false")
	}
	if msg.Context != "c3" {
		t.Errorf("context not passed through: %+v", msg.Context)
	}
}
