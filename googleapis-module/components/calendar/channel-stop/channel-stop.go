package channel_stop

import (
	"context"
	"fmt"
	"github.com/tiny-systems/modules/googleapis-module/components/etc"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"google.golang.org/api/calendar/v3"
	"google.golang.org/api/option"
)

const (
	ComponentName = "calendar_watch_stop"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Channel struct {
	ID         string `json:"id" required:"true" title:"ID" description:"A UUID or similar unique string that identifies this channel."`
	ResourceId string `json:"resourceId" title:"ResourceID" description:"An opaque ID that identifies the resource being stop on this channel. Stable across different API versions."`
	Token      string `json:"token" title:"Auth Token" description:"An arbitrary string delivered to the target address with each notification delivered over this channel."`
}

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"If request may fail, error port will emit an error message"`
}

type Context any

type Request struct {
	Context Context          `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary message to be send further"`
	Token   *etc.Token       `json:"token,omitempty" title:"Access token"`
	Channel Channel          `json:"channel" required:"true" title:"Channel to stop"`
	Config  etc.ClientConfig `json:"config" required:"true" title:"Client credentials"`
}

type Response struct {
	Context Context `json:"context"`
}

type Error struct {
	Context Context `json:"context"`
	Error   string  `json:"error"`
}

type Component struct {
	settings Settings
}

func (h *Component) Instance() module.Component {
	return &Component{
		settings: Settings{},
	}
}

func (h *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Stop Calendar Channel",
		Info:        "Stop calendar watcher",
		Tags:        []string{"Google", "Calendar", "Watch", "Stop"},
	}
}

// OnSettings stores the component settings.
func (h *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	h.settings = in
	return nil
}

// Handle dispatches business ports. System ports go through capabilities.
func (h *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port %s", port))
	}

	req, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}

	err := h.stop(ctx, req)
	if err != nil {
		if !h.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return handler(ctx, ErrorPort, Error{
			Context: req.Context,
			Error:   err.Error(),
		})
	}
	return handler(ctx, ResponsePort, Response{
		Context: req.Context,
	})

}

func (h *Component) stop(ctx context.Context, req Request) error {
	client, err := etc.NewGoogleHTTPClient(ctx, req.Config, req.Token)
	if err != nil {
		return fmt.Errorf("unable to create google client: %v", err)
	}

	srv, err := calendar.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return fmt.Errorf("unable to retrieve calendar client: %v", err)
	}
	return etc.ClassifyGoogleErr(srv.Channels.Stop(&calendar.Channel{
		Token:      req.Channel.Token,
		Id:         req.Channel.ID,
		ResourceId: req.Channel.ResourceId,
	}).Do())
}

func (h *Component) Ports() []module.Port {
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
				Channel: Channel{},
				Token: &etc.Token{
					TokenType: "Bearer",
				},
			},
			Position: module.Left,
		},
		{
			Name:          ResponsePort,
			Label:         "Response",
			Source:        true,
			Position:      module.Right,
			Configuration: Response{},
		},
	}
	if !h.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name:          ErrorPort,
		Label:         "Error",
		Source:        true,
		Position:      module.Bottom,
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
