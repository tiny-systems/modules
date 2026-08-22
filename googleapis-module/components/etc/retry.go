package etc

import (
	"context"
	"errors"
	"net"
	"syscall"

	"github.com/tiny-systems/module/module"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RetryableHTTPStatus reports whether an HTTP status code from a Google API
// indicates a transient failure that a backoff retry could clear.
func RetryableHTTPStatus(code int) bool {
	switch code {
	case 429, 500, 502, 503, 504:
		return true
	}
	return false
}

func retryableGRPCCode(c codes.Code) bool {
	switch c {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal:
		return true
	}
	return false
}

// ClassifyGoogleErr marks transient failures from external Google calls
// (googleapi HTTP errors, Firestore gRPC statuses, the OAuth token endpoint,
// plain network/timeout errors) with module.Retryable so the runtime's retry
// vocabulary picks them up — unmarked errors are never retried. Permanent
// failures (4xx, invalid credentials, malformed input, oauth invalid_grant)
// pass through unchanged. nil in, nil out.
func ClassifyGoogleErr(err error) error {
	if err == nil {
		return nil
	}
	// Already classified somewhere upstream — keep that decision.
	var re module.RetryableError
	if errors.As(err, &re) {
		return err
	}
	// Google API HTTP errors: retry on 429 and 5xx only.
	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		if RetryableHTTPStatus(gerr.Code) {
			return module.Retryable(err)
		}
		return err
	}
	// OAuth token endpoint: invalid_grant & co arrive as 4xx (permanent);
	// only transport-level 429/5xx are worth a retry.
	var oerr *oauth2.RetrieveError
	if errors.As(err, &oerr) {
		if oerr.Response != nil && RetryableHTTPStatus(oerr.Response.StatusCode) {
			return module.Retryable(err)
		}
		return err
	}
	// Firestore / gRPC status codes.
	if s, ok := status.FromError(err); ok && s != nil {
		if retryableGRPCCode(s.Code()) {
			return module.Retryable(err)
		}
		return err
	}
	// Plain network / timeout failures.
	if errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) {
		return module.Retryable(err)
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return module.Retryable(err)
	}
	return err
}
