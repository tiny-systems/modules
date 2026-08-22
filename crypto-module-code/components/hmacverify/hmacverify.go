// Package hmacverify verifies inbound webhook signatures (GitHub, Stripe, or
// generic HMAC) so a flow can authenticate a webhook before acting on it.
//
// A signature that does not match is a NORMAL outcome, not an error: the
// component emits {valid:false, reason} on its response port so the flow
// author routes on it (typically to a 401 response). Errors — the error port
// or a failed hop — are reserved for configuration problems the sender cannot
// cause: an empty secret, an unparseable Stripe-Signature header, an unknown
// scheme. Nothing here is transient, so no failure is marked retryable.
package hmacverify

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "hmac_verify"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

// Signature schemes. Each one names the header convention it verifies.
const (
	SchemeGenericSHA256 = "generic-sha256"
	SchemeGenericSHA1   = "generic-sha1"
	SchemeGitHub        = "github"
	SchemeStripe        = "stripe"
)

const defaultToleranceSeconds = 300

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Emit configuration failures (empty secret, unparseable Stripe-Signature header, unknown scheme) on the error port instead of failing the flow. A signature that simply does not match is NOT an error — it arrives on Response with valid=false."`

	Scheme string `json:"scheme" required:"true" title:"Scheme" enum:"generic-sha256,generic-sha1,github,stripe" enumTitles:"Generic HMAC-SHA256 (hex or base64 digest),Generic HMAC-SHA1 (hex or base64 digest),GitHub (X-Hub-Signature-256 header),Stripe (Stripe-Signature header)" default:"generic-sha256" description:"How the signature was produced. generic-sha256 / generic-sha1: the signature is the bare HMAC digest of the body, hex or base64 encoded (e.g. an X-Signature header). github: the X-Hub-Signature-256 header value, 'sha256=<hex>' over the raw body. stripe: the Stripe-Signature header value, 't=<unix>,v1=<hex>' where the HMAC-SHA256 is computed over '<t>.<body>'."`

	ToleranceSeconds int `json:"toleranceSeconds" title:"Timestamp Tolerance (seconds)" default:"300" description:"Replay window for schemes that carry a timestamp (Stripe): the request is rejected with valid=false when |now - t| exceeds this. 0 disables the timestamp check entirely. Ignored by schemes without a timestamp."`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context passed through to the response unchanged."`

	Secret string `json:"secret" required:"true" format:"password" title:"Signing Secret" description:"The shared signing secret. Carried per-request so it can come from a secret widget or trigger form rather than being stored in the flow."`

	Payload string `json:"payload" required:"true" title:"Payload" description:"The EXACT raw request body as a string — byte-for-byte what the sender signed. Any re-serialization (parsed-then-re-encoded JSON, trimmed whitespace, normalized line endings) changes the bytes and the signature will never match."`

	Signature string `json:"signature" required:"true" title:"Signature" description:"The signature header value. github: the X-Hub-Signature-256 value ('sha256=<hex>'). stripe: the Stripe-Signature value ('t=...,v1=...'). generic: the bare hex or base64 digest."`
}

type Response struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Valid   bool    `json:"valid" title:"Valid" description:"True when the signature matches (and the timestamp is within tolerance, where the scheme carries one)."`
	Reason  string  `json:"reason" title:"Reason" description:"Why verification failed — e.g. 'signature mismatch', 'timestamp outside tolerance'. Empty when valid."`
}

type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	// now is swappable for tests; time.Now otherwise.
	now func() time.Time
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			Scheme:           SchemeGenericSHA256,
			ToleranceSeconds: defaultToleranceSeconds,
		},
		now: time.Now,
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "HMAC Verify",
		Info: "Verifies an inbound webhook signature and emits {valid, reason} for routing. Schemes: 'github' verifies the X-Hub-Signature-256 header value ('sha256=<hex>', HMAC-SHA256 over the raw body); 'stripe' verifies the Stripe-Signature header value ('t=<unix>,v1=<hex>', HMAC-SHA256 over '<t>.<body>', all v1 entries tried, timestamp checked against the tolerance setting — 0 disables the check); 'generic-sha256' / 'generic-sha1' verify a bare digest (hex or base64, both accepted) as sent in headers like X-Signature. " +
			"A signature that does not match is a NORMAL response with valid=false and a reason — route on $.valid (e.g. to a 401 branch); only configuration problems (empty secret, unparseable Stripe-Signature header, unknown scheme) reach the error port or fail the hop. " +
			"The payload MUST be the exact raw request body bytes as a string: pass the HTTP server's raw body straight through — parsing and re-serializing JSON, trimming, or re-encoding changes the bytes and verification will always fail. All comparisons are constant-time.",
		Tags: []string{"Webhook", "HMAC", "Signature", "Security", "Crypto"},
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if !knownScheme(in.Scheme) {
		return fmt.Errorf("unknown scheme %q", in.Scheme)
	}
	if in.ToleranceSeconds < 0 {
		return fmt.Errorf("toleranceSeconds must be >= 0")
	}
	c.settingsLock.Lock()
	c.settings = in
	c.settingsLock.Unlock()
	return nil
}

func knownScheme(s string) bool {
	switch s {
	case SchemeGenericSHA256, SchemeGenericSHA1, SchemeGitHub, SchemeStripe:
		return true
	}
	return false
}

func (c *Component) getSettings() Settings {
	c.settingsLock.RLock()
	defer c.settingsLock.RUnlock()
	return c.settings
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
	set := c.getSettings()

	if req.Secret == "" {
		return c.handleError(ctx, handler, req.Context, fmt.Errorf("secret is required"))
	}

	valid, reason, err := verify(set, req, c.clock())
	if err != nil {
		// Configuration problem — the flow author must fix it; the sender cannot.
		return c.handleError(ctx, handler, req.Context, err)
	}

	// valid=false is a normal response: the author routes on it.
	return handler(ctx, ResponsePort, Response{
		Context: req.Context,
		Valid:   valid,
		Reason:  reason,
	})
}

func (c *Component) clock() func() time.Time {
	if c.now != nil {
		return c.now
	}
	return time.Now
}

// verify dispatches per scheme. It returns (valid, reason, nil) for a normal
// verification outcome and a non-nil error only for configuration problems.
func verify(set Settings, req Request, now func() time.Time) (bool, string, error) {
	switch set.Scheme {
	case SchemeGenericSHA256:
		ok := verifyGeneric(sha256.New, req)
		return ok, reasonIfInvalid(ok), nil
	case SchemeGenericSHA1:
		ok := verifyGeneric(sha1.New, req)
		return ok, reasonIfInvalid(ok), nil
	case SchemeGitHub:
		ok := verifyGitHub(req)
		return ok, reasonIfInvalid(ok), nil
	case SchemeStripe:
		return verifyStripe(req, set.ToleranceSeconds, now)
	}
	return false, "", fmt.Errorf("unknown scheme %q", set.Scheme)
}

func reasonIfInvalid(valid bool) string {
	if valid {
		return ""
	}
	return "signature mismatch"
}

// verifyGeneric checks a bare HMAC digest. The wire encoding is not part of a
// "generic" contract, so both hex and base64 renderings of the digest are
// accepted — the digest bytes are compared, constant-time, whichever decodes.
func verifyGeneric(newHash func() hash.Hash, req Request) bool {
	mac := computeHMAC(newHash, []byte(req.Secret), []byte(req.Payload))
	sig := strings.TrimSpace(req.Signature)

	if decoded, err := hex.DecodeString(sig); err == nil && hmac.Equal(decoded, mac) {
		return true
	}
	if decoded, err := base64.StdEncoding.DecodeString(sig); err == nil && hmac.Equal(decoded, mac) {
		return true
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(sig); err == nil && hmac.Equal(decoded, mac) {
		return true
	}
	return false
}

// verifyGitHub checks an X-Hub-Signature-256 value: "sha256=<hex>" over the
// raw body. A value without the prefix or with malformed hex simply does not
// match — it is the sender's data, so it is a mismatch, not an error.
func verifyGitHub(req Request) bool {
	sig := strings.TrimSpace(req.Signature)
	rest, found := strings.CutPrefix(sig, "sha256=")
	if !found {
		return false
	}
	decoded, err := hex.DecodeString(rest)
	if err != nil {
		return false
	}
	mac := computeHMAC(sha256.New, []byte(req.Secret), []byte(req.Payload))
	return hmac.Equal(decoded, mac)
}

// verifyStripe checks a Stripe-Signature value: "t=<unix>,v1=<hex>[,v1=...]".
// The signed payload is "<t>.<body>". All v1 entries are tried — Stripe sends
// several during secret rotation. When toleranceSeconds > 0 the timestamp must
// be within that window of now; 0 skips the check.
func verifyStripe(req Request, toleranceSeconds int, now func() time.Time) (bool, string, error) {
	ts, v1s, err := parseStripeHeader(req.Signature)
	if err != nil {
		return false, "", err
	}

	if toleranceSeconds > 0 {
		age := now().Unix() - ts
		if age < 0 {
			age = -age
		}
		if age > int64(toleranceSeconds) {
			return false, "timestamp outside tolerance", nil
		}
	}

	signedPayload := strconv.FormatInt(ts, 10) + "." + req.Payload
	mac := computeHMAC(sha256.New, []byte(req.Secret), []byte(signedPayload))

	// No early exit: every candidate is compared so a match costs the same as
	// a miss regardless of position.
	matched := false
	for _, v1 := range v1s {
		decoded, err := hex.DecodeString(v1)
		if err != nil {
			continue
		}
		if hmac.Equal(decoded, mac) {
			matched = true
		}
	}
	if !matched {
		return false, "signature mismatch", nil
	}
	return true, "", nil
}

// parseStripeHeader extracts t and all v1 values from a Stripe-Signature
// header. A header missing either is unparseable — a wiring problem (wrong
// header mapped onto the signature field), reported as an error.
func parseStripeHeader(header string) (int64, []string, error) {
	var (
		tsRaw string
		v1s   []string
	)
	for _, part := range strings.Split(header, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			continue
		}
		switch key {
		case "t":
			tsRaw = value
		case "v1":
			v1s = append(v1s, value)
		}
		// Other keys (v0, future versions) are ignored, per Stripe's docs.
	}

	if tsRaw == "" || len(v1s) == 0 {
		return 0, nil, fmt.Errorf("unparseable Stripe-Signature header: expected 't=<unix>,v1=<hex>', got %q", header)
	}
	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("unparseable Stripe-Signature header: timestamp %q is not an integer", tsRaw)
	}
	return ts, v1s, nil
}

func computeHMAC(newHash func() hash.Hash, key, payload []byte) []byte {
	mac := hmac.New(newHash, key)
	mac.Write(payload)
	return mac.Sum(nil)
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, reqContext Context, err error) module.Result {
	if !c.getSettings().EnableErrorPort {
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, module.NewError(reqContext, err))
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: c.getSettings(),
		},
		{
			Name:     RequestPort,
			Label:    "Request",
			Position: module.Left,
			Configuration: Request{
				Payload:   `{"action":"opened"}`,
				Signature: "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
			},
		},
		{
			Name:     ResponsePort,
			Label:    "Response",
			Source:   true,
			Position: module.Right,
			// Concrete sample so an edge reading $.valid is checkable at build
			// time instead of resolving to null at runtime.
			Configuration: Response{Valid: true},
		},
	}
	if !c.getSettings().EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name:          ErrorPort,
		Label:         "Error",
		Source:        true,
		Position:      module.Bottom,
		Configuration: module.ErrorMessage{},
	})
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
