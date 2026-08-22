package etc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"syscall"
	"testing"

	"github.com/tiny-systems/module/module"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestClassifyGoogleErr(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"nil", nil, false},
		{"googleapi 429", &googleapi.Error{Code: 429}, true},
		{"googleapi 500", &googleapi.Error{Code: 500}, true},
		{"googleapi 503 wrapped", fmt.Errorf("call: %w", &googleapi.Error{Code: 503}), true},
		{"googleapi 400", &googleapi.Error{Code: 400}, false},
		{"googleapi 401", &googleapi.Error{Code: 401}, false},
		{"googleapi 403", &googleapi.Error{Code: 403}, false},
		{"googleapi 404 wrapped", fmt.Errorf("call: %w", &googleapi.Error{Code: 404}), false},
		{"grpc unavailable", status.Error(codes.Unavailable, "unavailable"), true},
		{"grpc deadline", status.Error(codes.DeadlineExceeded, "deadline"), true},
		{"grpc resource exhausted", status.Error(codes.ResourceExhausted, "quota"), true},
		{"grpc aborted", status.Error(codes.Aborted, "aborted"), true},
		{"grpc internal wrapped", fmt.Errorf("snapshots next: %w", status.Error(codes.Internal, "internal")), true},
		{"grpc not found", status.Error(codes.NotFound, "missing doc"), false},
		{"grpc permission denied", status.Error(codes.PermissionDenied, "denied"), false},
		{"grpc invalid argument", status.Error(codes.InvalidArgument, "bad"), false},
		{"grpc failed precondition", status.Error(codes.FailedPrecondition, "precondition"), false},
		{"oauth 503", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 503}}, true},
		{"oauth invalid_grant 400", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 400}, ErrorCode: "invalid_grant"}, false},
		{"context deadline", context.DeadlineExceeded, true},
		{"context deadline wrapped", fmt.Errorf("req: %w", context.DeadlineExceeded), true},
		{"conn refused", fmt.Errorf("dial: %w", syscall.ECONNREFUSED), true},
		{"conn reset", fmt.Errorf("read: %w", syscall.ECONNRESET), true},
		{"net timeout", fmt.Errorf("request failed: %w", timeoutErr{}), true},
		{"plain error", errors.New("boom"), false},
		{"already marked permanent stays", module.Permanent(&googleapi.Error{Code: 503}), false},
		{"already marked retryable stays", module.Retryable(errors.New("x")), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyGoogleErr(tt.err)
			if module.IsRetryable(got) != tt.retryable {
				t.Fatalf("ClassifyGoogleErr(%v): retryable = %v, want %v", tt.err, module.IsRetryable(got), tt.retryable)
			}
			// message must pass through unchanged
			if tt.err != nil && got.Error() != tt.err.Error() {
				t.Fatalf("message changed: %q -> %q", tt.err.Error(), got.Error())
			}
			if tt.err == nil && got != nil {
				t.Fatalf("nil in must be nil out, got %v", got)
			}
		})
	}
}
