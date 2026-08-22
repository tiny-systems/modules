package debug

import (
	"context"
	"testing"

	"github.com/tiny-systems/module/pkg/redact"
)

// TestHandleMasksCredentials pins the leak path: whatever Handle stores
// becomes node state — rendered on the dashboard, written to the TinyNode
// CR, and carried into project exports. An API key reached a public
// solution export exactly this way.
func TestHandleMasksCredentials(t *testing.T) {
	c := &Component{}
	msg := InMessage{
		"context": map[string]interface{}{
			"apiKey":   "sk-ant-secret-value",
			"question": "is anything unhealthy?",
		},
	}

	c.Handle(context.Background(), nil, InPort, msg)

	stored, ok := c.settings.Context.(map[string]interface{})
	if !ok {
		t.Fatalf("settings context is %T, want map", c.settings.Context)
	}
	ctx, _ := stored["context"].(map[string]interface{})
	if ctx["apiKey"] != redact.Value {
		t.Errorf("apiKey stored as %v — a credential must never become node state", ctx["apiKey"])
	}
	if ctx["question"] != "is anything unhealthy?" {
		t.Errorf("non-secret field lost: %v", ctx["question"])
	}
}
