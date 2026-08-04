package k8s

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/tiny-systems/module/module"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// ParseLabelSelector parses a label selector string into a map
// Supports formats like "app=myapp" or "app=myapp,env=prod"
func ParseLabelSelector(selector string) (map[string]string, error) {
	if selector == "" {
		return nil, nil
	}

	labels := make(map[string]string)
	pairs := strings.Split(selector, ",")

	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid label selector: %s", pair)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		if key == "" {
			return nil, fmt.Errorf("empty label key in selector: %s", pair)
		}

		labels[key] = value
	}

	return labels, nil
}

// FormatAge formats a duration into a human-readable age string
// Similar to kubectl's age output (e.g., "5d", "3h", "45m", "30s")
func FormatAge(d time.Duration) string {
	if d < 0 {
		d = -d
	}

	days := int(d.Hours() / 24)
	if days > 0 {
		return fmt.Sprintf("%dd", days)
	}

	hours := int(d.Hours())
	if hours > 0 {
		return fmt.Sprintf("%dh", hours)
	}

	minutes := int(d.Minutes())
	if minutes > 0 {
		return fmt.Sprintf("%dm", minutes)
	}

	seconds := int(d.Seconds())
	return fmt.Sprintf("%ds", seconds)
}

// ClassifyError marks transient Kubernetes API failures with module.Retryable
// so the runtime may re-attempt the hop, and returns everything else unchanged
// — unmarked errors are never retried, which is the deliberate SDK default.
//
// Retryable: optimistic-concurrency conflicts (a Get→Update race), server-side
// timeouts and overload (ServerTimeout, Timeout, TooManyRequests,
// ServiceUnavailable, InternalError) and transport-level failures (connection
// refused/reset, a timed-out or dropped connection). Permanent: NotFound,
// Forbidden, Invalid, BadRequest and anything else a retry cannot fix.
func ClassifyError(err error) error {
	if err == nil {
		return nil
	}
	if apierrors.IsConflict(err) ||
		apierrors.IsServerTimeout(err) ||
		apierrors.IsTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(err) ||
		apierrors.IsInternalError(err) {
		return module.Retryable(err)
	}
	// Transport-level failures never carry an API status: connection
	// refused/reset and timeouts surface as net.Error (or as the API call's
	// own context.DeadlineExceeded).
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
		return module.Retryable(err)
	}
	return err
}
