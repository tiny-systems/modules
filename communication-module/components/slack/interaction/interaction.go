package interaction

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName   = "slack_interaction"
	InteractionPort = "interaction"
	ErrorPort       = "error"
	RequestPort     = "request"
)

// Context type alias for schema generation
type Context any

// Settings configures the component - only port flags
type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

// Header matches HTTP Server's header format
type Header struct {
	Key   string `json:"key" required:"true" title:"Key"`
	Value string `json:"value" required:"true" title:"Value"`
}

// Request is compatible with HTTP Server's Request output
type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through to output"`

	// Credentials - from edge config
	SigningSecret string `json:"signingSecret" configurable:"true" title:"Signing Secret" description:"Slack app signing secret for request verification"`
	SkipVerify   bool   `json:"skipVerify,omitempty" configurable:"true" title:"Skip Verify" description:"Skip signature verification (for testing)"`

	// HTTP request data - matches HTTP Server Request format
	RequestURI string   `json:"requestURI,omitempty" title:"Request URI"`
	Method     string   `json:"method,omitempty" title:"Method"`
	Headers    []Header `json:"headers,omitempty" title:"Headers" description:"HTTP headers from the request"`
	Body       string   `json:"body" required:"true" title:"Body" description:"Raw request body"`
}

// Interaction is the parsed Block Kit interaction payload
type Interaction struct {
	Context     Context `json:"context,omitempty" configurable:"true" title:"Context"`
	ActionID    string  `json:"actionId" title:"Action ID" description:"Button action identifier (e.g. logs, restart)"`
	ActionValue string  `json:"actionValue" title:"Action Value" description:"Value embedded in the button"`
	UserID      string  `json:"userId" title:"User ID" description:"Slack user ID who clicked"`
	UserName    string  `json:"userName" title:"User Name" description:"Slack username who clicked"`
	ChannelID   string  `json:"channelId" title:"Channel ID" description:"Channel where interaction happened"`
	ResponseURL string  `json:"responseUrl" format:"uri" title:"Response URL" description:"URL to send follow-up messages"`
	TriggerID   string  `json:"triggerId" title:"Trigger ID" description:"ID for opening modals"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the Slack interaction receiver
type Component struct {
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Slack Interaction",
		Info:        "Receives and parses Slack Block Kit interaction payloads (button clicks, menu selections). Connect to HTTP Server to receive webhooks. Verifies request signature and outputs parsed interaction for routing.",
		Tags:        []string{"Slack", "ChatOps", "Webhook", "Block Kit"},
	}
}

// OnSettings stores the component settings.
func (c *Component) OnSettings(_ context.Context, msg any) error {

	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settings = in
	return nil
}

// Handle dispatches the RequestPort. System ports go through capabilities.
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
	// Verify signature unless skipped
	if !req.SkipVerify {
		if err := verifySignature(req.SigningSecret, req.Headers, req.Body); err != nil {
			return c.handleError(ctx, handler, req, fmt.Sprintf("signature verification failed: %v", err))
		}
	}

	// Parse the interaction payload
	interaction, err := parseInteraction(req.Body)
	if err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to parse interaction: %v", err))
	}

	// Pass context through
	interaction.Context = req.Context

	return handler(ctx, InteractionPort, interaction)
}

// interactionPayload represents the Slack interaction callback JSON structure
type interactionPayload struct {
	Type    string `json:"type"`
	User    struct {
		ID       string `json:"id"`
		Username string `json:"username"`
		Name     string `json:"name"`
	} `json:"user"`
	Channel struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"channel"`
	Actions []struct {
		ActionID string `json:"action_id"`
		Value    string `json:"value"`
		Type     string `json:"type"`
	} `json:"actions"`
	ResponseURL string `json:"response_url"`
	TriggerID   string `json:"trigger_id"`
}

func parseInteraction(body string) (Interaction, error) {
	// Body is URL-encoded: payload=<URL-encoded JSON>
	payloadJSON := ""
	for _, pair := range strings.Split(body, "&") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 && parts[0] == "payload" {
			payloadJSON = urlDecode(parts[1])
			break
		}
	}

	if payloadJSON == "" {
		return Interaction{}, fmt.Errorf("no payload parameter found in body")
	}

	var payload interactionPayload
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return Interaction{}, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	if len(payload.Actions) == 0 {
		return Interaction{}, fmt.Errorf("no actions in interaction payload")
	}

	action := payload.Actions[0]

	// Use username, fall back to name
	userName := payload.User.Username
	if userName == "" {
		userName = payload.User.Name
	}

	return Interaction{
		ActionID:    action.ActionID,
		ActionValue: action.Value,
		UserID:      payload.User.ID,
		UserName:    userName,
		ChannelID:   payload.Channel.ID,
		ResponseURL: payload.ResponseURL,
		TriggerID:   payload.TriggerID,
	}, nil
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, req Request, errMsg string) module.Result {
	if c.settings.EnableErrorPort {
		return handler(ctx, ErrorPort, Error{
			Context: req.Context,
			Error:   errMsg,
		})
	}
	return module.Fail(errors.New(errMsg))
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: Settings{},
		},
		{
			Name:  RequestPort,
			Label: "Request",
			Configuration: Request{
				Headers: []Header{
					{Key: "X-Slack-Request-Timestamp", Value: "1234567890"},
					{Key: "X-Slack-Signature", Value: "v0=..."},
				},
				Body: `payload=%7B%22type%22%3A%22block_actions%22%2C%22actions%22%3A%5B%7B%22action_id%22%3A%22logs%22%2C%22value%22%3A%22my-pod%7Cdefault%22%7D%5D%7D`,
			},
			Position: module.Left,
		},
		{
			Name:   InteractionPort,
			Label:  "Interaction",
			Source: true,
			Configuration: Interaction{
				ActionID:    "logs",
				ActionValue: "my-pod|default",
				UserID:      "U123",
				UserName:    "johndoe",
				ChannelID:   "C456",
				ResponseURL: "https://hooks.slack.com/actions/T123/456/789",
				TriggerID:   "123.456",
			},
			Position: module.Right,
		},
	}

	if c.settings.EnableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Configuration: Error{},
			Position:      module.Bottom,
		})
	}

	return ports
}

// Helper functions (same as slack_command)

func verifySignature(signingSecret string, headers []Header, body string) error {
	if signingSecret == "" {
		return fmt.Errorf("signing secret not provided")
	}

	timestamp := ""
	signature := ""
	for _, h := range headers {
		lower := strings.ToLower(h.Key)
		if lower == "x-slack-request-timestamp" {
			timestamp = h.Value
		} else if lower == "x-slack-signature" {
			signature = h.Value
		}
	}

	if timestamp == "" || signature == "" {
		return fmt.Errorf("missing Slack signature headers")
	}

	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp")
	}
	if abs(time.Now().Unix()-ts) > 300 {
		return fmt.Errorf("timestamp too old")
	}

	sigBaseString := fmt.Sprintf("v0:%s:%s", timestamp, body)
	mac := hmac.New(sha256.New, []byte(signingSecret))
	mac.Write([]byte(sigBaseString))
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}

func abs(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

func urlDecode(s string) string {
	s = strings.ReplaceAll(s, "+", " ")
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' && i+2 < len(s) {
			if b, err := hex.DecodeString(s[i+1 : i+3]); err == nil {
				result = append(result, b...)
				i += 2
				continue
			}
		}
		result = append(result, s[i])
	}
	return string(result)
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
