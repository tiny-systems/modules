package command

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

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "slack_command"
	CommandPort   = "command"
	ErrorPort     = "error"
	RequestPort   = "request"
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

	// Credentials - from edge config (e.g., inject component)
	SigningSecret string `json:"signingSecret" configurable:"true" title:"Signing Secret" description:"Slack app signing secret for request verification"`
	SkipVerify    bool   `json:"skipVerify,omitempty" configurable:"true" title:"Skip Verify" description:"Skip signature verification (for testing)"`

	// HTTP request data - matches HTTP Server Request format
	RequestURI string   `json:"requestURI,omitempty" title:"Request URI"`
	Method     string   `json:"method,omitempty" title:"Method"`
	Headers    []Header `json:"headers,omitempty" title:"Headers" description:"HTTP headers from the request"`
	Body       string   `json:"body" required:"true" title:"Body" description:"Raw request body"`
}

// Command is the parsed slash command
type Command struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`

	// Original request context
	ResponseURL string `json:"responseUrl" format:"uri" title:"Response URL" description:"URL to send delayed responses"`
	TriggerID   string `json:"triggerId" title:"Trigger ID" description:"ID for opening modals"`

	// Command details
	Command string   `json:"command" title:"Command" description:"The slash command used (e.g., /deploy)"`
	Text    string   `json:"text" title:"Text" description:"Text after the command"`
	Args    []string `json:"args" title:"Args" description:"Text split into arguments"`

	// User info
	UserID   string `json:"userId" title:"User ID" description:"Slack user ID"`
	UserName string `json:"userName" title:"User Name" description:"Slack username"`

	// Channel info
	ChannelID   string `json:"channelId" title:"Channel ID" description:"Channel where command was invoked"`
	ChannelName string `json:"channelName" title:"Channel Name" description:"Channel name"`

	// Team info
	TeamID     string `json:"teamId" title:"Team ID" description:"Slack workspace ID"`
	TeamDomain string `json:"teamDomain" title:"Team Domain" description:"Slack workspace domain"`

	// For routing
	Action    string   `json:"action" title:"Action" description:"First argument (e.g., 'status' from '/k8s status app')"`
	Target    string   `json:"target" title:"Target" description:"Second argument (e.g., 'app' from '/k8s status app')"`
	ExtraArgs []string `json:"extraArgs" title:"Extra Args" description:"Remaining arguments after action and target"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
	Request Request `json:"request" title:"Request"`
}

// Component implements the Slack command receiver
type Component struct {
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Slack Command",
		Info:        "Receives and parses Slack slash commands. Connect to HTTP Server to receive webhooks. Verifies request signature and outputs parsed command for routing.",
		Tags:        []string{"Slack", "ChatOps", "Webhook"},
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

	// Parse the command
	cmd, err := parseCommand(req.Body)
	if err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to parse command: %v", err))
	}

	// Pass context through
	cmd.Context = req.Context

	// Emit the parsed command and return the response (for http-server)
	return handler(ctx, CommandPort, cmd)
}

func verifySignature(signingSecret string, headers []Header, body string) error {
	if signingSecret == "" {
		return fmt.Errorf("signing secret not provided")
	}

	// Get headers (case-insensitive)
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

	// Check timestamp to prevent replay attacks (5 minute window)
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid timestamp")
	}
	if abs(time.Now().Unix()-ts) > 300 {
		return fmt.Errorf("timestamp too old")
	}

	// Compute expected signature
	sigBaseString := fmt.Sprintf("v0:%s:%s", timestamp, body)
	mac := hmac.New(sha256.New, []byte(signingSecret))
	mac.Write([]byte(sigBaseString))
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}

func parseCommand(body string) (Command, error) {
	// Parse URL-encoded form data
	params := make(map[string]string)
	for _, pair := range strings.Split(body, "&") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			key := parts[0]
			value := urlDecode(parts[1])
			params[key] = value
		}
	}

	text := params["text"]
	args := parseArgs(text)

	cmd := Command{
		ResponseURL: params["response_url"],
		TriggerID:   params["trigger_id"],
		Command:     params["command"],
		Text:        text,
		Args:        args,
		UserID:      params["user_id"],
		UserName:    params["user_name"],
		ChannelID:   params["channel_id"],
		ChannelName: params["channel_name"],
		TeamID:      params["team_id"],
		TeamDomain:  params["team_domain"],
	}

	// Extract action and target for easy routing
	// Always initialize ExtraArgs to empty slice (not nil) so expressions like length($.extraArgs) work
	cmd.ExtraArgs = []string{}
	if len(args) > 0 {
		cmd.Action = args[0]
	}
	if len(args) > 1 {
		cmd.Target = args[1]
	}
	if len(args) > 2 {
		cmd.ExtraArgs = args[2:]
	}

	return cmd, nil
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, req Request, errMsg string) module.Result {
	if c.settings.EnableErrorPort {
		// IMPORTANT: Return the handler result to propagate responses back through the call chain.
		// This is critical for blocking I/O patterns like HTTP Server which expects responses
		// to flow back through the same handler chain that sent the request.
		return handler(ctx, ErrorPort, Error{
			Context: req.Context,
			Error:   errMsg,
			Request: req,
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
				Body: "command=/k8s&text=status+myapp&user_id=U123&channel_id=C456",
			},
			Position: module.Left,
		},
		{
			Name:   CommandPort,
			Label:  "Command",
			Source: true,
			Configuration: Command{
				Command: "/k8s",
				Action:  "status",
				Target:  "myapp",
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

// Helper functions

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

func parseArgs(text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	return strings.Fields(text)
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
