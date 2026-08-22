package hmacverify

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tiny-systems/module/module"
)

// fixedNow pins the clock so Stripe tolerance cases are deterministic.
var fixedNow = time.Unix(1700000000, 0)

type emitted struct {
	port string
	data any
}

// runVerify drives the component the way the runner would: settings first,
// then one message on the request port, capturing everything emitted.
func runVerify(t *testing.T, settings Settings, req Request) ([]emitted, module.Result) {
	t.Helper()

	comp, ok := (&Component{}).Instance().(*Component)
	if !ok {
		t.Fatal("Instance() did not return *Component")
	}
	comp.now = func() time.Time { return fixedNow }

	if err := comp.OnSettings(context.Background(), settings); err != nil {
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

// stripeSig builds a Stripe-Signature header for the given timestamp/payload,
// exactly as Stripe does: v1 = HMAC-SHA256(secret, "<t>.<payload>") hex.
func stripeSig(secret, payload string, ts int64) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.%s", ts, payload)
	return fmt.Sprintf("t=%d,v1=%s", ts, hex.EncodeToString(mac.Sum(nil)))
}

func TestVerify(t *testing.T) {
	const (
		stripeSecret  = "whsec_test_secret"
		stripePayload = `{"id":"evt_1","type":"payment_intent.succeeded"}`
	)
	freshTS := fixedNow.Unix() - 10  // inside a 300s window
	staleTS := fixedNow.Unix() - 900 // outside a 300s window

	tests := []struct {
		name       string
		settings   Settings
		req        Request
		wantValid  bool
		wantReason string
	}{
		// GitHub's documented example: secret "It's a Secret to Everybody",
		// payload "Hello, World!".
		{
			name:     "github docs example verifies",
			settings: Settings{Scheme: SchemeGitHub},
			req: Request{
				Secret:    "It's a Secret to Everybody",
				Payload:   "Hello, World!",
				Signature: "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
			},
			wantValid: true,
		},
		{
			name:     "github tampered payload mismatches",
			settings: Settings{Scheme: SchemeGitHub},
			req: Request{
				Secret:    "It's a Secret to Everybody",
				Payload:   "Hello, World?",
				Signature: "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
			},
			wantValid:  false,
			wantReason: "signature mismatch",
		},
		{
			name:     "github missing sha256= prefix mismatches",
			settings: Settings{Scheme: SchemeGitHub},
			req: Request{
				Secret:    "It's a Secret to Everybody",
				Payload:   "Hello, World!",
				Signature: "757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
			},
			wantValid:  false,
			wantReason: "signature mismatch",
		},

		// Generic: digest of {"hello":"world"} with key "topsecret",
		// accepted as hex or base64 of the same bytes.
		{
			name:     "generic sha256 hex verifies",
			settings: Settings{Scheme: SchemeGenericSHA256},
			req: Request{
				Secret:    "topsecret",
				Payload:   `{"hello":"world"}`,
				Signature: "afd00617ceb8f63e65ea5c310f06bf78c3901e7a713db532e25da26ad63c7236",
			},
			wantValid: true,
		},
		{
			name:     "generic sha256 uppercase hex verifies",
			settings: Settings{Scheme: SchemeGenericSHA256},
			req: Request{
				Secret:    "topsecret",
				Payload:   `{"hello":"world"}`,
				Signature: "AFD00617CEB8F63E65EA5C310F06BF78C3901E7A713DB532E25DA26AD63C7236",
			},
			wantValid: true,
		},
		{
			name:     "generic sha256 base64 verifies",
			settings: Settings{Scheme: SchemeGenericSHA256},
			req: Request{
				Secret:    "topsecret",
				Payload:   `{"hello":"world"}`,
				Signature: "r9AGF8649j5l6lwxDwa/eMOQHnpxPbUy4l2iatY8cjY=",
			},
			wantValid: true,
		},
		{
			name:     "generic sha256 wrong secret mismatches",
			settings: Settings{Scheme: SchemeGenericSHA256},
			req: Request{
				Secret:    "wrongsecret",
				Payload:   `{"hello":"world"}`,
				Signature: "afd00617ceb8f63e65ea5c310f06bf78c3901e7a713db532e25da26ad63c7236",
			},
			wantValid:  false,
			wantReason: "signature mismatch",
		},
		{
			name:     "generic sha1 hex verifies",
			settings: Settings{Scheme: SchemeGenericSHA1},
			req: Request{
				Secret:    "topsecret",
				Payload:   `{"hello":"world"}`,
				Signature: "7fff767f0d780626839194dd3d33a41ce6f216a6",
			},
			wantValid: true,
		},

		// Stripe: t inside/outside tolerance, multiple v1 entries, tolerance 0.
		{
			name:     "stripe fresh timestamp verifies",
			settings: Settings{Scheme: SchemeStripe, ToleranceSeconds: 300},
			req: Request{
				Secret:    stripeSecret,
				Payload:   stripePayload,
				Signature: stripeSig(stripeSecret, stripePayload, freshTS),
			},
			wantValid: true,
		},
		{
			name:     "stripe stale timestamp outside tolerance",
			settings: Settings{Scheme: SchemeStripe, ToleranceSeconds: 300},
			req: Request{
				Secret:    stripeSecret,
				Payload:   stripePayload,
				Signature: stripeSig(stripeSecret, stripePayload, staleTS),
			},
			wantValid:  false,
			wantReason: "timestamp outside tolerance",
		},
		{
			name:     "stripe tolerance zero skips timestamp check",
			settings: Settings{Scheme: SchemeStripe, ToleranceSeconds: 0},
			req: Request{
				Secret:    stripeSecret,
				Payload:   stripePayload,
				Signature: stripeSig(stripeSecret, stripePayload, staleTS),
			},
			wantValid: true,
		},
		{
			name:     "stripe second v1 entry matches during rotation",
			settings: Settings{Scheme: SchemeStripe, ToleranceSeconds: 300},
			req: Request{
				Secret:  stripeSecret,
				Payload: stripePayload,
				Signature: fmt.Sprintf("t=%d,v1=%s,%s",
					freshTS,
					strings.Repeat("ab", 32), // stale key's signature — no match
					strings.TrimPrefix(stripeSig(stripeSecret, stripePayload, freshTS), fmt.Sprintf("t=%d,", freshTS)),
				),
			},
			wantValid: true,
		},
		{
			name:     "stripe wrong secret mismatches",
			settings: Settings{Scheme: SchemeStripe, ToleranceSeconds: 300},
			req: Request{
				Secret:    "whsec_other",
				Payload:   stripePayload,
				Signature: stripeSig(stripeSecret, stripePayload, freshTS),
			},
			wantValid:  false,
			wantReason: "signature mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outs, res := runVerify(t, tt.settings, tt.req)
			if err := res.Err(); err != nil {
				t.Fatalf("Handle returned error: %v", err)
			}
			if len(outs) != 1 {
				t.Fatalf("expected 1 emission, got %d: %+v", len(outs), outs)
			}
			if outs[0].port != ResponsePort {
				t.Fatalf("emitted on port %q, want %q", outs[0].port, ResponsePort)
			}
			resp, ok := outs[0].data.(Response)
			if !ok {
				t.Fatalf("emitted %T, want Response", outs[0].data)
			}
			if resp.Valid != tt.wantValid {
				t.Errorf("valid: got %v, want %v (reason: %q)", resp.Valid, tt.wantValid, resp.Reason)
			}
			if resp.Reason != tt.wantReason {
				t.Errorf("reason: got %q, want %q", resp.Reason, tt.wantReason)
			}
		})
	}
}

// Config problems must fail the hop (or hit the error port), never emit a
// valid=false response the flow would route as "unauthorized sender".
func TestConfigErrors(t *testing.T) {
	tests := []struct {
		name     string
		settings Settings
		req      Request
	}{
		{
			name:     "empty secret",
			settings: Settings{Scheme: SchemeGenericSHA256},
			req:      Request{Payload: "body", Signature: "deadbeef"},
		},
		{
			name:     "stripe header without t",
			settings: Settings{Scheme: SchemeStripe, ToleranceSeconds: 300},
			req:      Request{Secret: "s", Payload: "body", Signature: "v1=deadbeef"},
		},
		{
			name:     "stripe header without v1",
			settings: Settings{Scheme: SchemeStripe, ToleranceSeconds: 300},
			req:      Request{Secret: "s", Payload: "body", Signature: "t=1700000000"},
		},
		{
			name:     "stripe header not a signature header at all",
			settings: Settings{Scheme: SchemeStripe, ToleranceSeconds: 300},
			req:      Request{Secret: "s", Payload: "body", Signature: "sha256=deadbeef"},
		},
		{
			name:     "stripe timestamp not an integer",
			settings: Settings{Scheme: SchemeStripe, ToleranceSeconds: 300},
			req:      Request{Secret: "s", Payload: "body", Signature: "t=yesterday,v1=deadbeef"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" fails hop", func(t *testing.T) {
			outs, res := runVerify(t, tt.settings, tt.req)
			if res.Err() == nil {
				t.Fatalf("expected failed result, got success with emissions %+v", outs)
			}
			if module.ShouldRetry(res.Err()) {
				t.Errorf("config error marked retryable; pure computation must not be")
			}
			if len(outs) != 0 {
				t.Errorf("expected no emissions, got %+v", outs)
			}
		})

		t.Run(tt.name+" routes to error port", func(t *testing.T) {
			settings := tt.settings
			settings.EnableErrorPort = true
			outs, res := runVerify(t, settings, tt.req)
			if err := res.Err(); err != nil {
				t.Fatalf("with error port enabled Handle should not fail: %v", err)
			}
			if len(outs) != 1 || outs[0].port != ErrorPort {
				t.Fatalf("expected 1 emission on %q, got %+v", ErrorPort, outs)
			}
			em, ok := outs[0].data.(module.ErrorMessage)
			if !ok {
				t.Fatalf("emitted %T, want module.ErrorMessage", outs[0].data)
			}
			if em.Error == "" {
				t.Error("error message is empty")
			}
			if em.Retryable {
				t.Error("config error marked retryable; pure computation must not be")
			}
		})
	}
}

func TestOnSettingsRejectsUnknownScheme(t *testing.T) {
	comp, _ := (&Component{}).Instance().(*Component)
	if err := comp.OnSettings(context.Background(), Settings{Scheme: "hmac-sha3"}); err == nil {
		t.Fatal("expected error for unknown scheme")
	}
}

func TestErrorPortOnlyListedWhenEnabled(t *testing.T) {
	comp, _ := (&Component{}).Instance().(*Component)

	hasError := func() bool {
		for _, p := range comp.Ports() {
			if p.Name == ErrorPort {
				return true
			}
		}
		return false
	}

	if hasError() {
		t.Error("error port listed with EnableErrorPort=false")
	}
	if err := comp.OnSettings(context.Background(), Settings{Scheme: SchemeGitHub, EnableErrorPort: true}); err != nil {
		t.Fatalf("OnSettings: %v", err)
	}
	if !hasError() {
		t.Error("error port missing with EnableErrorPort=true")
	}
}
