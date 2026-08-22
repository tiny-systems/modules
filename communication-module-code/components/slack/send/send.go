package send

import (
	"context"
	"errors"
	"fmt"

	"github.com/slack-go/slack"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "slack_send"
	ResponsePort  = "response"
	ErrorPort     = "error"
	RequestPort   = "request"
)

type Settings struct {
	EnableSuccessPort bool `json:"enableSuccessPort" required:"true" title:"Enable Success port" description:""`
	EnableErrorPort   bool `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"If error happen during send, error port will emit an error message"`
}

type Context any

type Message struct {
	ChannelID  string `json:"channelID" required:"true" minLength:"1" title:"ChannelID" description:""`
	SlackToken string `json:"slackToken" required:"true" minLength:"1" format:"password" title:"Slack token" description:"Bot User OAuth Token"`
	Text       string `json:"text" required:"true" minLength:"1" title:"Message text" format:"textarea"`
	ThreadTs   string `json:"threadTs,omitempty" title:"Thread ts" description:"Timestamp (ts) of a parent message — when set, the message is posted as a reply in that thread"`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Message Message `json:"slack_message" required:"true" title:"Slack Message"`
}

type Response struct {
	Request Request `json:"request"`
	Sent    Message `json:"sent"`
	Channel string  `json:"channel" title:"Channel" description:"Channel ID the message was posted to"`
	Ts      string  `json:"ts" title:"Ts" description:"Message timestamp — pass as thread_ts to reply in a thread"`
}

type Error struct {
	Context Context `json:"context"`
	Error   string  `json:"error"`
}

type Component struct {
	settings Settings
}

func (t *Component) Instance() module.Component {
	return &Component{
		settings: Settings{},
	}
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Slack Channel Sender",
		Info:        "Sends messages to slack channel",
		Tags:        []string{"Slack", "IM"},
	}
}

// OnSettings stores the component settings.
func (t *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	t.settings = in
	return nil
}

// Handle dispatches the RequestPort. System ports go through capabilities.
func (t *Component) Handle(ctx context.Context, responseHandler module.Handler, port string, msg interface{}) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}

	client := slack.New(in.Message.SlackToken)
	opts := []slack.MsgOption{slack.MsgOptionText(in.Message.Text, true)}
	if in.Message.ThreadTs != "" {
		opts = append(opts, slack.MsgOptionTS(in.Message.ThreadTs))
	}
	channelID, ts, _, err := client.SendMessageContext(ctx, in.Message.ChannelID, opts...)

	if err != nil {
		var rateLimited *slack.RateLimitedError
		if errors.As(err, &rateLimited) {
			// Slack asked us to back off — a retry can clear it.
			err = module.Retryable(err)
		}
		if !t.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return responseHandler(ctx, ErrorPort, Error{
			Context: in.Context,
			Error:   err.Error(),
		})
	}

	if t.settings.EnableSuccessPort {
		return responseHandler(ctx, ResponsePort, Response{
			Request: in,
			Sent:    in.Message,
			Channel: channelID,
			Ts:      ts,
		})
	}
	return module.Result{}
}

func (t *Component) Ports() []module.Port {
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
				Message: Message{
					Text: "Message to send",
				},
			},
			Position: module.Left,
		},
	}
	if t.settings.EnableSuccessPort {
		ports = append(ports, module.Port{
			Position:      module.Right,
			Name:          ResponsePort,
			Label:         "Response",
			Source:        true,
			Configuration: Response{},
		})
	}

	if !t.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Position:      module.Bottom,
		Name:          ErrorPort,
		Label:         "Error",
		Source:        true,
		Configuration: Error{},
	})
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
