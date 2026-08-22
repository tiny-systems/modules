package interaction

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/tiny-systems/module/module"
)

func TestParseInteraction(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantErr     bool
		errContains string
		check       func(t *testing.T, i Interaction)
	}{
		{
			name: "valid button click",
			body: `payload=` + urlEncode(`{"type":"block_actions","user":{"id":"U123","username":"johndoe"},"channel":{"id":"C456","name":"general"},"actions":[{"action_id":"logs","value":"my-pod|default","type":"button"}],"response_url":"https://hooks.slack.com/actions/T123/456/789","trigger_id":"123.456"}`),
			check: func(t *testing.T, i Interaction) {
				if i.ActionID != "logs" {
					t.Errorf("ActionID = %q, want %q", i.ActionID, "logs")
				}
				if i.ActionValue != "my-pod|default" {
					t.Errorf("ActionValue = %q, want %q", i.ActionValue, "my-pod|default")
				}
				if i.UserID != "U123" {
					t.Errorf("UserID = %q, want %q", i.UserID, "U123")
				}
				if i.UserName != "johndoe" {
					t.Errorf("UserName = %q, want %q", i.UserName, "johndoe")
				}
				if i.ChannelID != "C456" {
					t.Errorf("ChannelID = %q, want %q", i.ChannelID, "C456")
				}
				if i.ResponseURL != "https://hooks.slack.com/actions/T123/456/789" {
					t.Errorf("ResponseURL = %q", i.ResponseURL)
				}
				if i.TriggerID != "123.456" {
					t.Errorf("TriggerID = %q, want %q", i.TriggerID, "123.456")
				}
			},
		},
		{
			name: "restart button",
			body: `payload=` + urlEncode(`{"type":"block_actions","user":{"id":"U789","username":"admin"},"channel":{"id":"C111"},"actions":[{"action_id":"restart","value":"nginx|production","type":"button"}],"response_url":"https://hooks.slack.com/actions/T1/2/3","trigger_id":"t1"}`),
			check: func(t *testing.T, i Interaction) {
				if i.ActionID != "restart" {
					t.Errorf("ActionID = %q, want %q", i.ActionID, "restart")
				}
				if i.ActionValue != "nginx|production" {
					t.Errorf("ActionValue = %q, want %q", i.ActionValue, "nginx|production")
				}
			},
		},
		{
			name: "user name fallback",
			body: `payload=` + urlEncode(`{"type":"block_actions","user":{"id":"U123","name":"John Doe"},"channel":{"id":"C456"},"actions":[{"action_id":"test","value":"v"}],"response_url":"","trigger_id":""}`),
			check: func(t *testing.T, i Interaction) {
				if i.UserName != "John Doe" {
					t.Errorf("UserName = %q, want %q", i.UserName, "John Doe")
				}
			},
		},
		{
			name:        "no payload parameter",
			body:        "command=/k8s&text=pods",
			wantErr:     true,
			errContains: "no payload parameter",
		},
		{
			name:        "invalid JSON",
			body:        "payload=not-json",
			wantErr:     true,
			errContains: "failed to unmarshal",
		},
		{
			name:        "empty actions array",
			body:        `payload=` + urlEncode(`{"type":"block_actions","user":{"id":"U1"},"channel":{"id":"C1"},"actions":[]}`),
			wantErr:     true,
			errContains: "no actions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInteraction(tt.body)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, got)
			}
		})
	}
}

func TestVerifySignature(t *testing.T) {
	secret := "test-secret-123"
	body := `payload=%7B%22type%22%3A%22block_actions%22%7D`
	ts := fmt.Sprintf("%d", time.Now().Unix())

	// Compute valid signature
	sigBase := fmt.Sprintf("v0:%s:%s", ts, body)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(sigBase))
	sig := "v0=" + hex.EncodeToString(mac.Sum(nil))

	headers := []Header{
		{Key: "X-Slack-Request-Timestamp", Value: ts},
		{Key: "X-Slack-Signature", Value: sig},
	}

	t.Run("valid signature", func(t *testing.T) {
		err := verifySignature(secret, headers, body)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("wrong signature", func(t *testing.T) {
		badHeaders := []Header{
			{Key: "X-Slack-Request-Timestamp", Value: ts},
			{Key: "X-Slack-Signature", Value: "v0=bad"},
		}
		err := verifySignature(secret, badHeaders, body)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("missing headers", func(t *testing.T) {
		err := verifySignature(secret, nil, body)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("empty secret", func(t *testing.T) {
		err := verifySignature("", headers, body)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("old timestamp", func(t *testing.T) {
		oldTs := fmt.Sprintf("%d", time.Now().Unix()-600)
		oldSigBase := fmt.Sprintf("v0:%s:%s", oldTs, body)
		oldMac := hmac.New(sha256.New, []byte(secret))
		oldMac.Write([]byte(oldSigBase))
		oldSig := "v0=" + hex.EncodeToString(oldMac.Sum(nil))
		oldHeaders := []Header{
			{Key: "X-Slack-Request-Timestamp", Value: oldTs},
			{Key: "X-Slack-Signature", Value: oldSig},
		}
		err := verifySignature(secret, oldHeaders, body)
		if err == nil {
			t.Fatal("expected error for old timestamp")
		}
	})
}

func TestHandle(t *testing.T) {
	comp := &Component{}

	t.Run("settings port", func(t *testing.T) {
		// Settings is dispatched via the SettingsHandler capability, not Handle.
		err := comp.OnSettings(context.Background(), Settings{EnableErrorPort: true})
		if err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		if !comp.settings.EnableErrorPort {
			t.Fatal("settings not applied")
		}
	})

	t.Run("request port with skip verify", func(t *testing.T) {
		var gotPort string
		var gotMsg any
		handler := func(ctx context.Context, port string, msg any) module.Result {
			gotPort = port
			gotMsg = msg
			return module.Result{}
		}

		body := `payload=` + urlEncode(`{"type":"block_actions","user":{"id":"U1","username":"test"},"channel":{"id":"C1"},"actions":[{"action_id":"logs","value":"pod1|ns1","type":"button"}],"response_url":"https://example.com","trigger_id":"t1"}`)

		result := comp.Handle(context.Background(), handler, RequestPort, Request{
			SkipVerify: true,
			Body:       body,
			Context:    map[string]string{"key": "val"},
		})
		if err := result.Err(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if gotPort != InteractionPort {
			t.Errorf("port = %q, want %q", gotPort, InteractionPort)
		}
		interaction, ok := gotMsg.(Interaction)
		if !ok {
			t.Fatal("expected Interaction type")
		}
		if interaction.ActionID != "logs" {
			t.Errorf("ActionID = %q, want %q", interaction.ActionID, "logs")
		}
		if interaction.ActionValue != "pod1|ns1" {
			t.Errorf("ActionValue = %q, want %q", interaction.ActionValue, "pod1|ns1")
		}
	})

	t.Run("error port enabled", func(t *testing.T) {
		comp.settings.EnableErrorPort = true
		var gotPort string
		handler := func(ctx context.Context, port string, msg any) module.Result {
			gotPort = port
			return module.Result{}
		}

		result := comp.Handle(context.Background(), handler, RequestPort, Request{
			SkipVerify: true,
			Body:       "no-payload-here",
		})
		if err := result.Err(); err != nil {
			t.Fatalf("expected no error (handler returned ok), got %v", err)
		}
		if gotPort != ErrorPort {
			t.Errorf("port = %q, want %q", gotPort, ErrorPort)
		}
	})

	t.Run("error port disabled returns error", func(t *testing.T) {
		comp.settings.EnableErrorPort = false

		result := comp.Handle(context.Background(), nil, RequestPort, Request{
			SkipVerify: true,
			Body:       "no-payload-here",
		})
		if result.Err() == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("unknown port", func(t *testing.T) {
		result := comp.Handle(context.Background(), nil, "unknown", nil)
		if result.Err() == nil {
			t.Fatal("expected error for unknown port")
		}
	})
}

func TestPorts(t *testing.T) {
	comp := &Component{}
	ports := comp.Ports()
	if len(ports) != 3 {
		t.Errorf("expected 3 ports, got %d", len(ports))
	}

	comp.settings.EnableErrorPort = true
	ports = comp.Ports()
	if len(ports) != 4 {
		t.Errorf("expected 4 ports with error enabled, got %d", len(ports))
	}
}

func TestInstance(t *testing.T) {
	comp := &Component{}
	inst := comp.Instance()
	if inst == nil {
		t.Fatal("Instance() returned nil")
	}
	if inst == comp {
		t.Fatal("Instance() should return new instance")
	}
}

// helpers

func urlEncode(s string) string {
	var result []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			result = append(result, c)
		} else {
			result = append(result, '%')
			result = append(result, "0123456789ABCDEF"[c>>4])
			result = append(result, "0123456789ABCDEF"[c&0x0f])
		}
	}
	return string(result)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
