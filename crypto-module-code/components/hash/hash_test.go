package hash

import (
	"context"
	"testing"

	"github.com/tiny-systems/module/module"
)

type emitted struct {
	port string
	data any
}

func runHash(t *testing.T, req Request) ([]emitted, module.Result) {
	t.Helper()
	comp := (&Component{}).Instance()

	var outs []emitted
	handler := func(_ context.Context, port string, data any) module.Result {
		outs = append(outs, emitted{port: port, data: data})
		return module.Ok(nil)
	}
	res := comp.Handle(context.Background(), handler, RequestPort, req)
	return outs, res
}

func TestDigest(t *testing.T) {
	// Plain-hash vectors are the classic FIPS/NIST "abc" digests; keyed
	// vectors are RFC 4231 (SHA-2) and RFC 2202 (SHA-1, MD5) test case 2:
	// key "Jefe", data "what do ya want for nothing?".
	const (
		rfcKey  = "Jefe"
		rfcData = "what do ya want for nothing?"
	)

	tests := []struct {
		name string
		req  Request
		want string
	}{
		{
			name: "sha256 abc hex",
			req:  Request{Data: "abc", Algorithm: AlgSHA256, Encoding: EncodingHex},
			want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		},
		{
			name: "sha512 abc hex",
			req:  Request{Data: "abc", Algorithm: AlgSHA512, Encoding: EncodingHex},
			want: "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f",
		},
		{
			name: "sha1 abc hex",
			req:  Request{Data: "abc", Algorithm: AlgSHA1, Encoding: EncodingHex},
			want: "a9993e364706816aba3e25717850c26c9cd0d89d",
		},
		{
			name: "md5 abc hex",
			req:  Request{Data: "abc", Algorithm: AlgMD5, Encoding: EncodingHex},
			want: "900150983cd24fb0d6963f7d28e17f72",
		},
		{
			name: "sha256 abc base64",
			req:  Request{Data: "abc", Algorithm: AlgSHA256, Encoding: EncodingBase64},
			want: "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=",
		},
		{
			name: "empty algorithm and encoding default to sha256 hex",
			req:  Request{Data: "abc"},
			want: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		},
		{
			name: "hmac-sha256 rfc4231 case 2",
			req:  Request{Data: rfcData, Algorithm: AlgSHA256, Encoding: EncodingHex, HmacKey: rfcKey},
			want: "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
		},
		{
			name: "hmac-sha512 rfc4231 case 2",
			req:  Request{Data: rfcData, Algorithm: AlgSHA512, Encoding: EncodingHex, HmacKey: rfcKey},
			want: "164b7a7bfcf819e2e395fbe73b56e0a387bd64222e831fd610270cd7ea2505549758bf75c05a994a6d034f65f8f0e6fdcaeab1a34d4a6b4b636e070a38bce737",
		},
		{
			name: "hmac-sha1 rfc2202 case 2",
			req:  Request{Data: rfcData, Algorithm: AlgSHA1, Encoding: EncodingHex, HmacKey: rfcKey},
			want: "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79",
		},
		{
			name: "hmac-md5 rfc2202 case 2",
			req:  Request{Data: rfcData, Algorithm: AlgMD5, Encoding: EncodingHex, HmacKey: rfcKey},
			want: "750c783e6ab0b503eaa86e310a5db738",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outs, res := runHash(t, tt.req)
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
			if resp.Digest != tt.want {
				t.Errorf("digest:\n got  %s\n want %s", resp.Digest, tt.want)
			}
		})
	}
}

func TestContextPassesThrough(t *testing.T) {
	outs, res := runHash(t, Request{Context: map[string]any{"runId": "r1"}, Data: "abc"})
	if err := res.Err(); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	resp := outs[0].data.(Response)
	ctxMap, ok := resp.Context.(map[string]any)
	if !ok || ctxMap["runId"] != "r1" {
		t.Errorf("context not passed through: %+v", resp.Context)
	}
}

func TestUnknownAlgorithmFails(t *testing.T) {
	outs, res := runHash(t, Request{Data: "abc", Algorithm: "sha3-512"})
	if res.Err() == nil {
		t.Fatalf("expected error, got emissions %+v", outs)
	}
	if len(outs) != 0 {
		t.Errorf("expected no emissions, got %+v", outs)
	}
}

func TestUnknownEncodingFails(t *testing.T) {
	outs, res := runHash(t, Request{Data: "abc", Encoding: "base32"})
	if res.Err() == nil {
		t.Fatalf("expected error, got emissions %+v", outs)
	}
	if len(outs) != 0 {
		t.Errorf("expected no emissions, got %+v", outs)
	}
}
