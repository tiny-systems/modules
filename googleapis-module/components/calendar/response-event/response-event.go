package response_event

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
	ComponentName = "calendar_event_respond"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable Error Port" description:"If request may fail, error port will emit an error message"`
}

type Component struct {
	settings Settings
}

type Request struct {
	Context            Context          `json:"context" title:"Context" configurable:"true"`
	Config             etc.ClientConfig `json:"config" title:"Config"  required:"true" description:"Client Config"`
	Token              *etc.Token       `json:"token,omitempty" title:"Auth Token"`
	CalendarID         string           `json:"calendarID" title:"Calendar ID" required:"true"`
	EventID            string           `json:"eventID" title:"Event ID" required:"true"`
	EventAttendeeEmail string           `json:"eventAttendeeEmail" title:"Event Attendee Email" required:"true"`
	ResponseStatus     string           `json:"responseStatus" title:"Response Status" required:"true" enum:"accepted,declined,tentative"`
}

type Response struct {
	Context Context `json:"context"`
}

type Error struct {
	Context Context `json:"context"`
	Error   string  `json:"error"`
}

func (g *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Response to event",
		Info:        "Response to calendar event",
		Tags:        []string{"google", "calendar", "auth"},
	}
}

// OnSettings stores the component settings.
func (g *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	g.settings = in
	return nil
}

// Handle dispatches business ports. System ports go through capabilities.
func (g *Component) Handle(ctx context.Context, output module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port %s", port))
	}

	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid input message"))
	}

	err := g.responseEvent(ctx, in)
	if err != nil {
		// check err port
		if !g.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return output(ctx, ErrorPort, Error{
			Context: in.Context,
			Error:   err.Error(),
		})
	}

	return output(ctx, ResponsePort, Response{
		Context: in.Context,
	})

}

func (c *Component) responseEvent(ctx context.Context, req Request) error {

	client, err := etc.NewGoogleHTTPClient(ctx, req.Config, req.Token)
	if err != nil {
		return fmt.Errorf("unable to create google client: %v", err)
	}

	srv, err := calendar.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return fmt.Errorf("unable to retrieve calendar client: %v", err)
	}

	event, err := srv.Events.Get(req.CalendarID, req.EventID).Context(ctx).Do()
	if err != nil {
		return etc.ClassifyGoogleErr(fmt.Errorf("unable to retrieve event: %w", err))
	}
	//

	for _, a := range event.Attendees {
		if a.Email != req.EventAttendeeEmail {
			continue
		}
		a.ResponseStatus = req.ResponseStatus
	}

	_, err = srv.Events.Update(req.CalendarID, req.EventID, event).Context(ctx).Do()

	return etc.ClassifyGoogleErr(err)
}

func (g *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: Settings{},
		},
		{
			Name:          RequestPort,
			Label:         "Request",
			Position:      module.Left,
			Configuration: Request{},
		},
		{
			Source:        true,
			Name:          ResponsePort,
			Label:         "Response",
			Position:      module.Right,
			Configuration: Response{},
		},
	}
	if !g.settings.EnableErrorPort {
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

func (g *Component) Instance() module.Component {
	return &Component{}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
