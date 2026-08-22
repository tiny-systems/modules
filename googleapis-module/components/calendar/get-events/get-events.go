package get_events

import (
	"context"
	"fmt"
	"github.com/tiny-systems/googleapis-module/components/etc"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"google.golang.org/api/calendar/v3"
	"google.golang.org/api/option"
	"time"
)

const (
	ComponentName = "calendar_events_get"
	RequestPort   = "request"
	ResponsePort  = "response"
	ErrorPort     = "error"
)

type Context any

type Request struct {
	Context      Context          `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary message to be send further"`
	Config       etc.ClientConfig `json:"config" required:"true" title:"Client credentials"`
	CalendarId   string           `json:"calendarId" required:"true" default:"primary" minLength:"1" title:"Calendar ID"`
	StartDate    time.Time        `json:"startDate" title:"Start date" description:"2012-10-01T09:45:00.000+02:00"`
	EndDate      time.Time        `json:"endDate" title:"End date" description:"2012-10-01T09:45:00.000+02:00"`
	Token        *etc.Token       `json:"token,omitempty" title:"Auth Token"`
	SyncToken    *string          `json:"syncToken,omitempty" title:"Sync Token" description:"To proceed syncing from previous position"`
	PageToken    *string          `json:"pageToken,omitempty" title:"Page Token" description:"Token used to retrieve the page."`
	ShowDeleted  bool             `json:"showDeleted,omitempty" title:"Show deleted events" default:"true"`
	SingleEvents bool             `json:"singleEvents" title:"Single events" description:"Whether to expand recurring events into instances and only return single one-off events and instances of recurring events"`
	MaxResults   int64            `json:"maxResults" title:"Max results" description:"Maximum number of events returned on one result page." max:"2500" default:"250"`
}

type Error struct {
	Context Context `json:"context"`
	Error   string  `json:"error"`
}

type Response struct {
	Context Context         `json:"context"`
	Results calendar.Events `json:"results"`
}

type Component struct {
	settings Settings
}

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" default:"false" required:"true" title:"Enable Error Port" description:"If request may fail, error port will emit an error message"`
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Calendar Get Events",
		Info:        "Calendar Get Events",
		Tags:        []string{"Google", "Calendar"},
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

// Handle dispatches business ports. System ports go through capabilities.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port %s", port))
	}

	req, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}
	events, err := c.getEvents(ctx, req)
	if err != nil {
		if !c.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return handler(ctx, ErrorPort, Error{
			Context: req.Context,
			Error:   err.Error(),
		})
	}

	return handler(ctx, ResponsePort, Response{
		Context: req.Context,
		Results: *events,
	})

}

func (c *Component) getEvents(ctx context.Context, req Request) (*calendar.Events, error) {

	client, err := etc.NewGoogleHTTPClient(ctx, req.Config, req.Token)
	if err != nil {
		return nil, fmt.Errorf("unable to create google client: %v", err)
	}

	srv, err := calendar.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve calendar client: %v", err)
	}

	call := srv.Events.List(req.CalendarId).ShowDeleted(req.ShowDeleted).SingleEvents(req.SingleEvents)

	if req.PageToken != nil {
		call.PageToken(*req.PageToken)
	}

	if req.SyncToken != nil {
		call.SyncToken(*req.SyncToken)
	} else {

		if !req.StartDate.IsZero() {
			call.TimeMin(req.StartDate.Format(time.RFC3339))
		}
		if !req.EndDate.IsZero() {
			call.TimeMax(req.EndDate.Format(time.RFC3339))
		}
	}

	maxResults := req.MaxResults
	if maxResults == 0 {
		maxResults = 250
	}
	call.MaxResults(maxResults)

	events, err := call.Do()
	if err != nil {
		return nil, etc.ClassifyGoogleErr(fmt.Errorf("unable to retrieve user's events: %w", err))
	}

	return events, nil
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
				Config: etc.ClientConfig{
					Scopes: []string{"https://www.googleapis.com/auth/calendar.events.readonly"},
				},
				CalendarId: "SomeID",
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
		}}

	if !c.settings.EnableErrorPort {
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

func (c *Component) Instance() module.Component {
	return &Component{}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
