// Package hash computes a message digest — plain or HMAC-keyed — over a
// string. One component covers dedup keys, content fingerprints, and signing
// outbound webhook payloads; its inbound counterpart is hmac_verify.
package hash

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	stdhash "hash"

	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "hash"
	RequestPort   = "request"
	ResponsePort  = "response"
)

// Algorithms and encodings. The request enums keep unknown values out; the
// constants exist so code and tests never re-type the strings.
const (
	AlgSHA256 = "sha256"
	AlgSHA512 = "sha512"
	AlgSHA1   = "sha1"
	AlgMD5    = "md5"

	EncodingHex    = "hex"
	EncodingBase64 = "base64"
)

type Context any

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context passed through to the response unchanged."`

	Data string `json:"data" required:"true" title:"Data" description:"The string to digest. Hashed byte-for-byte — for a canonical fingerprint of structured data, serialize it deterministically first."`

	Algorithm string `json:"algorithm" required:"true" title:"Algorithm" enum:"sha256,sha512,sha1,md5" enumTitles:"SHA-256,SHA-512,SHA-1 (legacy),MD5 (legacy — fingerprints only)" default:"sha256" description:"Digest algorithm. sha256 is the sensible default; sha1 and md5 exist for interop with systems that expect them — do not use them for anything security-sensitive."`

	Encoding string `json:"encoding" required:"true" title:"Encoding" enum:"hex,base64" enumTitles:"Hex (lowercase),Base64 (standard)" default:"hex" description:"How the digest bytes are rendered in the response."`

	HmacKey string `json:"hmacKey,omitempty" format:"password" title:"HMAC Key" description:"Optional. When set, the output is the HMAC-<algorithm> keyed digest of Data instead of the plain hash — use this to SIGN an outbound webhook payload (the receiver verifies with the same key). Carried per-request so it can come from a secret widget."`
}

type Response struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Digest  string  `json:"digest" title:"Digest" description:"The digest, rendered in the requested encoding."`
}

type Component struct{}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Hash",
		Info: "Computes a digest of a string: sha256 (default), sha512, sha1, or md5, rendered as hex (default) or base64. " +
			"Use it for dedup keys (hash the natural key of a record before store/compare), content fingerprints (detect that a document changed), and cache keys. " +
			"Set hmacKey to get the HMAC-<algorithm> keyed digest instead of the plain hash — that is how you SIGN an outbound webhook payload so the receiver can verify it; for verifying INBOUND webhooks use hmac_verify, which does the constant-time comparison and per-provider header parsing for you. " +
			"The data is digested byte-for-byte as given.",
		Tags: []string{"Hash", "HMAC", "Fingerprint", "Crypto"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	return c.handleRequest(ctx, handler, in)
}

func (c *Component) handleRequest(ctx context.Context, handler module.Handler, req Request) module.Result {
	digest, err := computeDigest(req)
	if err != nil {
		return module.Fail(err)
	}
	return handler(ctx, ResponsePort, Response{
		Context: req.Context,
		Digest:  digest,
	})
}

func computeDigest(req Request) (string, error) {
	newHash, err := hashConstructor(req.Algorithm)
	if err != nil {
		return "", err
	}

	var sum []byte
	if req.HmacKey != "" {
		mac := hmac.New(newHash, []byte(req.HmacKey))
		mac.Write([]byte(req.Data))
		sum = mac.Sum(nil)
	} else {
		h := newHash()
		h.Write([]byte(req.Data))
		sum = h.Sum(nil)
	}

	return encodeDigest(sum, req.Encoding)
}

// hashConstructor maps the algorithm enum to a constructor. Empty falls back
// to sha256 — the enum default — so a request built programmatically without
// the field behaves like the form.
func hashConstructor(algorithm string) (func() stdhash.Hash, error) {
	switch algorithm {
	case AlgSHA256, "":
		return sha256.New, nil
	case AlgSHA512:
		return sha512.New, nil
	case AlgSHA1:
		return sha1.New, nil
	case AlgMD5:
		return md5.New, nil
	}
	return nil, fmt.Errorf("unknown algorithm %q", algorithm)
}

func encodeDigest(sum []byte, encoding string) (string, error) {
	switch encoding {
	case EncodingHex, "":
		return hex.EncodeToString(sum), nil
	case EncodingBase64:
		return base64.StdEncoding.EncodeToString(sum), nil
	}
	return "", fmt.Errorf("unknown encoding %q", encoding)
}

func (c *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:     RequestPort,
			Label:    "Request",
			Position: module.Left,
			Configuration: Request{
				Data:      "hello world",
				Algorithm: AlgSHA256,
				Encoding:  EncodingHex,
			},
		},
		{
			Name:     ResponsePort,
			Label:    "Response",
			Source:   true,
			Position: module.Right,
			// Concrete sample so an edge reading $.digest is checkable at
			// build time instead of resolving to null at runtime.
			Configuration: Response{
				Digest: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
			},
		},
	}
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register((&Component{}).Instance())
}
