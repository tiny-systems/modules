package k8s

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/tiny-systems/module/module"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestClassifyError(t *testing.T) {
	gr := schema.GroupResource{Group: "apps", Resource: "deployments"}

	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"nil", nil, false},
		{"conflict", apierrors.NewConflict(gr, "web", errors.New("object modified")), true},
		{"server timeout", apierrors.NewServerTimeout(gr, "get", 1), true},
		{"timeout", apierrors.NewTimeoutError("too slow", 1), true},
		{"too many requests", apierrors.NewTooManyRequests("throttled", 1), true},
		{"service unavailable", apierrors.NewServiceUnavailable("down"), true},
		{"internal error", apierrors.NewInternalError(errors.New("boom")), true},
		{"connection refused", &net.OpError{Op: "dial", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}, true},
		{"deadline exceeded", fmt.Errorf("call failed: %w", context.DeadlineExceeded), true},
		{"not found", apierrors.NewNotFound(gr, "web"), false},
		{"forbidden", apierrors.NewForbidden(gr, "web", errors.New("rbac")), false},
		{"invalid", apierrors.NewInvalid(schema.GroupKind{Group: "apps", Kind: "Deployment"}, "web", nil), false},
		{"bad request", apierrors.NewBadRequest("nope"), false},
		{"unauthorized", apierrors.NewUnauthorized("who are you"), false},
		{"plain error", errors.New("validation failed"), false},
		{"context canceled", context.Canceled, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := module.IsRetryable(ClassifyError(tt.err))
			if got != tt.retryable {
				t.Errorf("ClassifyError(%v) retryable = %v, want %v", tt.err, got, tt.retryable)
			}
		})
	}

	t.Run("nil stays nil", func(t *testing.T) {
		if ClassifyError(nil) != nil {
			t.Error("ClassifyError(nil) should be nil")
		}
	})

	// The components wrap the classified error with fmt.Errorf("...: %w", ...)
	// before it reaches module.Fail — the marking must survive that wrap.
	t.Run("marking survives fmt.Errorf wrapping", func(t *testing.T) {
		conflict := apierrors.NewConflict(gr, "web", errors.New("object modified"))
		wrapped := fmt.Errorf("failed to update deployment: %w", ClassifyError(conflict))
		if !module.IsRetryable(wrapped) {
			t.Error("retryable marking lost through fmt.Errorf %w wrap")
		}
		if !apierrors.IsConflict(wrapped) {
			t.Error("conflict status lost through Retryable + fmt.Errorf wrap")
		}
	})

	// A conflict wrapped by a caller BEFORE classification must still classify.
	t.Run("classifies already-wrapped errors", func(t *testing.T) {
		conflict := apierrors.NewConflict(gr, "web", errors.New("object modified"))
		preWrapped := fmt.Errorf("failed to list pods: %w", conflict)
		if !module.IsRetryable(ClassifyError(preWrapped)) {
			t.Error("ClassifyError should see through fmt.Errorf %w wrapping")
		}
	})
}
