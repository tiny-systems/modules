package create

import (
	"context"
	firebase "firebase.google.com/go"
	"fmt"
	"github.com/tiny-systems/googleapis-module/components/etc"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"google.golang.org/api/option"
)

const (
	ComponentName = "firestore_delete_doc"
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
	Context    Context          `json:"context,omitempty" title:"Context" configurable:"true"`
	Config     etc.ClientConfig `json:"config" title:"Config"  required:"true" description:"Client Config"`
	Collection string           `json:"collection" title:"Collection" required:"true"`
	RefID      string           `json:"refID" title:"Ref ID" required:"true"`
}

type Response struct {
	Context Context `json:"context" title:"Context"`
}

type Error struct {
	Context Context `json:"context" title:"Context"`
	Error   string  `json:"error"`
}

func (g *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Firestore Delete Document",
		Info:        "Deletes document from a collection",
		Tags:        []string{"google", "firestore", "db"},
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
	var err error

	req, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}

	app, err := firebase.NewApp(ctx, nil, option.WithCredentialsJSON([]byte(req.Config.Credentials)), option.WithScopes(req.Config.Scopes...))
	if err != nil {
		// check err port
		if !g.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return output(ctx, ErrorPort, Error{
			Context: req.Context,
			Error:   err.Error(),
		})
	}

	db, err := app.Firestore(ctx)

	if err != nil {
		// check err port
		if !g.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return output(ctx, ErrorPort, Error{
			Error: err.Error(),
		})
	}

	ref := db.Collection(req.Collection)

	_, err = ref.Doc(req.RefID).Delete(ctx)
	err = etc.ClassifyGoogleErr(err)
	if err != nil {
		// check err port
		if !g.settings.EnableErrorPort {
			return module.Fail(err)
		}
		return output(ctx, ErrorPort, Error{
			Error: err.Error(),
		})
	}

	return output(ctx, ResponsePort, Response{
		Context: req.Context,
	})

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
	//

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
