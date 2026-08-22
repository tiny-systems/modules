package s3conn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/tiny-systems/module/module"
)

func TestClassifyConstructed(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantRetry bool
	}{
		{"nil", nil, false},
		{"429 throttle", minio.ErrorResponse{StatusCode: http.StatusTooManyRequests, Code: "SlowDown"}, true},
		{"500 internal", minio.ErrorResponse{StatusCode: http.StatusInternalServerError, Code: "InternalError"}, true},
		{"503 unavailable", minio.ErrorResponse{StatusCode: http.StatusServiceUnavailable, Code: "ServiceUnavailable"}, true},
		{"400 bad request", minio.ErrorResponse{StatusCode: http.StatusBadRequest, Code: "InvalidArgument"}, false},
		{"403 access denied", minio.ErrorResponse{StatusCode: http.StatusForbidden, Code: "AccessDenied"}, false},
		{"404 no such key", minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchKey"}, false},
		{"wrapped 503 stays retryable", fmt.Errorf("stat: %w", minio.ErrorResponse{StatusCode: http.StatusServiceUnavailable}), true},
		{"wrapped 404 stays permanent", fmt.Errorf("stat: %w", minio.ErrorResponse{StatusCode: http.StatusNotFound}), false},
		{"net.Error dial failure", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{"context deadline", context.DeadlineExceeded, true},
		{"plain error unmarked", errors.New("something else"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.err)
			if tt.err == nil {
				if got != nil {
					t.Fatalf("Classify(nil) = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("Classify swallowed the error")
			}
			if module.ShouldRetry(got) != tt.wantRetry {
				t.Errorf("ShouldRetry = %v, want %v (err: %v)", module.ShouldRetry(got), tt.wantRetry, got)
			}
		})
	}
}

// TestClassifyPermanent4xxIsMarked pins that a 4xx is explicitly Permanent,
// not merely unmarked: a Permanent marking survives even if a later layer
// wraps the error with Retryable (pkg/errors permanence wins in ShouldRetry).
func TestClassifyPermanent4xxIsMarked(t *testing.T) {
	got := Classify(minio.ErrorResponse{StatusCode: http.StatusNotFound, Code: "NoSuchKey"})
	var re module.RetryableError
	if !errors.As(got, &re) {
		t.Fatalf("4xx not wrapped as RetryableError: %v", got)
	}
	if re.Retryable() {
		t.Errorf("4xx marked retryable, want permanent")
	}
}

// TestClassifyPlainErrorPassesThrough pins that unrecognized errors come back
// unchanged — unmarked, so the SDK's never-retry default applies.
func TestClassifyPlainErrorPassesThrough(t *testing.T) {
	orig := errors.New("opaque")
	if got := Classify(orig); got != orig {
		t.Errorf("plain error was wrapped: %v", got)
	}
}

// liveErr drives a REAL minio-go request against an httptest server returning
// the given status, and hands back the error minio-go produced — proving
// Classify matches what the client library actually emits, not just what the
// test constructed.
func liveErr(t *testing.T, status int) error {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)

	client, err := Client(Conn{
		Endpoint:  strings.TrimPrefix(srv.URL, "http://"),
		Region:    "us-east-1", // skip the bucket-location probe
		AccessKey: "test-access",
		SecretKey: "test-secret",
		UseSSL:    false,
	})
	if err != nil {
		t.Fatalf("Client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = client.StatObject(ctx, "bucket", "missing.txt", minio.StatObjectOptions{})
	if err == nil {
		t.Fatalf("StatObject against %d server succeeded", status)
	}
	return err
}

func TestClassifyLiveMinioErrors(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		wantRetry bool
	}{
		{"404 from server is permanent", http.StatusNotFound, false},
		{"403 from server is permanent", http.StatusForbidden, false},
		{"503 from server is retryable", http.StatusServiceUnavailable, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := liveErr(t, tt.status)
			var resp minio.ErrorResponse
			if !errors.As(err, &resp) || resp.StatusCode != tt.status {
				t.Fatalf("expected minio.ErrorResponse with status %d, got %v", tt.status, err)
			}
			if got := module.ShouldRetry(Classify(err)); got != tt.wantRetry {
				t.Errorf("ShouldRetry = %v, want %v (err: %v)", got, tt.wantRetry, err)
			}
		})
	}
}

func TestClassifyLiveConnectionRefused(t *testing.T) {
	client, err := Client(Conn{
		Endpoint:  "127.0.0.1:1", // nothing listens on port 1
		Region:    "us-east-1",
		AccessKey: "a",
		SecretKey: "s",
		UseSSL:    false,
	})
	if err != nil {
		t.Fatalf("Client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = client.StatObject(ctx, "bucket", "key", minio.StatObjectOptions{})
	if err == nil {
		t.Fatalf("StatObject against closed port succeeded")
	}
	if !module.ShouldRetry(Classify(err)) {
		t.Errorf("connection-refused not retryable: %v", err)
	}
}

func TestResolveBucket(t *testing.T) {
	conn := Conn{Bucket: "default-bucket"}

	got, err := conn.ResolveBucket("request-bucket")
	if err != nil || got != "request-bucket" {
		t.Errorf("request bucket should win: got %q, err %v", got, err)
	}

	got, err = conn.ResolveBucket("")
	if err != nil || got != "default-bucket" {
		t.Errorf("settings bucket should fill in: got %q, err %v", got, err)
	}

	_, err = (Conn{}).ResolveBucket("")
	if err == nil {
		t.Fatalf("no bucket anywhere should error")
	}
	if module.ShouldRetry(err) {
		t.Errorf("missing bucket must be permanent, got retryable")
	}
	if !strings.Contains(err.Error(), "request.bucket") || !strings.Contains(err.Error(), "settings.bucket") {
		t.Errorf("error should name both knobs: %v", err)
	}
}

func TestClientEndpointHandling(t *testing.T) {
	if _, err := Client(Conn{}); err == nil {
		t.Errorf("empty endpoint should error")
	} else if module.ShouldRetry(err) {
		t.Errorf("empty endpoint must be permanent")
	}

	// Plain host:port.
	c, err := Client(Conn{Endpoint: "minio.storage.svc:9000", AccessKey: "a", SecretKey: "s"})
	if err != nil {
		t.Fatalf("host:port endpoint: %v", err)
	}
	if got := c.EndpointURL().Host; got != "minio.storage.svc:9000" {
		t.Errorf("endpoint host = %q", got)
	}

	// Scheme prefix tolerated; http:// forces SSL off, https:// forces it on.
	c, err = Client(Conn{Endpoint: "http://minio.storage.svc:9000", UseSSL: true, AccessKey: "a", SecretKey: "s"})
	if err != nil {
		t.Fatalf("http:// endpoint: %v", err)
	}
	if c.EndpointURL().Scheme != "http" {
		t.Errorf("http:// prefix should override UseSSL, got scheme %q", c.EndpointURL().Scheme)
	}

	c, err = Client(Conn{Endpoint: "https://s3.amazonaws.com", UseSSL: false, AccessKey: "a", SecretKey: "s"})
	if err != nil {
		t.Fatalf("https:// endpoint: %v", err)
	}
	if c.EndpointURL().Scheme != "https" {
		t.Errorf("https:// prefix should override UseSSL, got scheme %q", c.EndpointURL().Scheme)
	}
}
