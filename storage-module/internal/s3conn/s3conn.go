// Package s3conn holds what every storage-module component shares: the
// connection settings block (embedded into each component's Settings so all
// four settings forms stay identical), the client constructor, bucket
// precedence, and the one error-classification helper that decides
// retryability for the whole module.
package s3conn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/tiny-systems/module/module"
)

// Conn is the S3 connection settings block. Components embed it in their own
// Settings struct so each component carries a full, self-contained settings
// port — no cross-node wiring needed to configure credentials.
type Conn struct {
	Endpoint string `json:"endpoint" required:"true" title:"Endpoint" description:"S3 API endpoint as host[:port] — e.g. s3.amazonaws.com (AWS), minio.<namespace>.svc:9000 (in-cluster MinIO), <account>.r2.cloudflarestorage.com (Cloudflare R2), storage.googleapis.com (GCS interop). A scheme prefix (http:// or https://) is tolerated and also sets SSL on/off accordingly."`

	Region string `json:"region,omitempty" title:"Region" description:"Optional signing region (AWS: us-east-1 etc., R2: auto). Leave empty for MinIO. Setting it also skips the bucket-location probe the client would otherwise make on first use."`

	AccessKey string `json:"accessKey" required:"true" format:"password" title:"Access Key" description:"Access key ID (AWS access key, MinIO user, R2 token ID)."`

	SecretKey string `json:"secretKey" required:"true" format:"password" title:"Secret Key" description:"Secret access key matching the access key."`

	UseSSL bool `json:"useSSL" default:"true" title:"Use SSL" description:"Connect over HTTPS. Disable only for plain-HTTP in-cluster endpoints like minio.<namespace>.svc:9000."`

	Bucket string `json:"bucket,omitempty" title:"Default Bucket" description:"Bucket used when a request leaves its bucket field empty. A bucket set on the request always wins over this default."`
}

// ResolveBucket applies the module-wide precedence: the request's bucket wins,
// the settings default fills in, and neither configured is a permanent error —
// no retry conjures a bucket name.
func (c Conn) ResolveBucket(requestBucket string) (string, error) {
	if requestBucket != "" {
		return requestBucket, nil
	}
	if c.Bucket != "" {
		return c.Bucket, nil
	}
	return "", module.Permanent(errors.New("no bucket: set request.bucket or the component's settings.bucket default"))
}

// Client builds a minio client from the settings. All failures here are
// permanent — they are misconfigurations (missing endpoint, unparseable
// endpoint), not transient conditions.
//
// Endpoints pasted with a scheme ("https://s3.amazonaws.com") are tolerated:
// the scheme is stripped and overrides UseSSL, since the URL states the
// intent more directly than the checkbox.
func Client(c Conn) (*minio.Client, error) {
	endpoint := strings.TrimSpace(c.Endpoint)
	if endpoint == "" {
		return nil, module.Permanent(errors.New("endpoint not configured: set settings.endpoint (e.g. s3.amazonaws.com or minio.<namespace>.svc:9000)"))
	}

	secure := c.UseSSL
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" {
			return nil, module.Permanent(fmt.Errorf("invalid endpoint %q: use host[:port], e.g. s3.amazonaws.com or minio.<namespace>.svc:9000", endpoint))
		}
		endpoint = u.Host
		secure = u.Scheme != "http"
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: secure,
		Region: c.Region,
		// One attempt per hop: minio-go would otherwise retry 5xx/429
		// internally up to 10 times with backoff, hiding minutes of stalling
		// inside a single hop. The platform owns retries — Classify marks the
		// failure retryable and the edge/retry component re-attempts visibly.
		MaxRetries: 1,
	})
	if err != nil {
		return nil, module.Permanent(fmt.Errorf("s3 client for %q: %w", endpoint, err))
	}
	return client, nil
}

// Classify marks transient S3 failures with module.Retryable and definite
// client errors with module.Permanent, so the runtime's edge retry and the
// retry component both see one consistent verdict (module.ShouldRetry).
//
// Retryable: 429 (throttling / SlowDown) and 5xx from the server, plus
// transport-level failures that never carried a response — connection
// refused/reset, DNS failure, a timed-out or dropped connection. Permanent:
// any other 4xx (NoSuchKey, NoSuchBucket, AccessDenied, invalid arguments) —
// re-running the identical request fails identically. Anything unrecognized
// is returned unchanged, and unmarked errors are never retried — the
// deliberate SDK default.
//
// Note on writes: blob_put routes its PutObject errors through here even
// though a PUT is a write, because S3 PutObject is idempotent — a full-object
// replace of the same key with the same bytes. If a network failure hides
// whether the first attempt landed, re-running it converges on the same
// stored object, so marking those failures retryable is safe.
func Classify(err error) error {
	if err == nil {
		return nil
	}

	var resp minio.ErrorResponse
	if errors.As(err, &resp) {
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return module.Retryable(err)
		}
		if resp.StatusCode >= 400 {
			return module.Permanent(err)
		}
		return err
	}

	// Transport-level failures never carry an S3 error response: connection
	// refused/reset, DNS errors and timeouts surface as net.Error (or as the
	// call's own context.DeadlineExceeded).
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) {
		return module.Retryable(err)
	}
	return err
}
