package listen_collection

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	firebase "firebase.google.com/go"
	"fmt"
	"github.com/tiny-systems/modules/googleapis-module/components/etc"
	"github.com/tiny-systems/modules/googleapis-module/components/firestore/utils"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

const (
	ComponentName = "firestore_listen_collection"
	ResponsePort  = "response"
	StartPort     = "start"
	StopPort      = "stop"
	ErrorPort     = "error"
)

type Context any

type StartControl struct {
	Status string `json:"status" title:"Status" readonly:"true"`
}

type StopControl struct {
	Stop   bool   `json:"stop" format:"button" title:"Stop" required:"true" description:"Stop listening"`
	Status string `json:"status" title:"Status" readonly:"true"`
}

type Stop struct {
}

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" required:"true" title:"Enable error port" description:"If request may fail, error port will emit an error message"`
	EnableStopPort  bool `json:"enableStopPort" required:"true" title:"Enable stop port" description:"Stop port allows you to stop listener"`
}

type Component struct {
	settings Settings

	startSettings  Start
	cancelFunc     context.CancelFunc
	cancelFuncLock *sync.Mutex

	runLock *sync.Mutex
}

type Start struct {
	Context    Context          `json:"context,omitempty" title:"Context" configurable:"true"`
	Config     etc.ClientConfig `json:"config" title:"Config"  required:"true" description:"Client Config"`
	Collection string           `json:"collection" title:"Collection" required:"true"`
	Wheres     []utils.Where    `json:"wheres,omitempty" title:"Where" description:"Where to filter. Leave empty if you want to listen the entire collection."`
}

type Response struct {
	Context  Context                `json:"context" title:"Context"`
	RefID    string                 `json:"refID"`
	Document map[string]interface{} `json:"document" title:"Document" description:"Document that changed"`
	Action   string                 `json:"action" title:"Action" enum:"added,modified,removed"`
}

type Error struct {
	Context Context `json:"context"`
	Error   string  `json:"error"`
}

func (g *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Firestore Listen Collection",
		Info:        "Listens to changes of the collection",
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

// OnControl handles the Stop button on the dashboard.
func (g *Component) OnControl(_ context.Context, msg any) error {
	if msg == nil {
		return nil
	}
	if _, ok := msg.(StopControl); ok {
		return g.stop()
	}
	return nil
}

// Handle dispatches the StartPort. System ports go through capabilities.
func (g *Component) Handle(ctx context.Context, handler module.Handler, port string, msg interface{}) module.Result {
	if port != StartPort {
		return module.Fail(fmt.Errorf("invalid port"))
	}
	req, ok := msg.(Start)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	g.startSettings = req
	return module.Fail(g.start(ctx, handler))
}

func (g *Component) start(ctx context.Context, handler module.Handler) error {

	g.runLock.Lock()
	defer g.runLock.Unlock()

	listenCtx, listenCancel := context.WithCancel(ctx)
	defer listenCancel()

	g.setCancelFunc(listenCancel)
	// reconcile so show we are listening
	_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)

	defer func() {
		g.setCancelFunc(nil)
		_ = handler(context.Background(), v1alpha1.ReconcilePort, nil)
	}()

	app, err := firebase.NewApp(listenCtx, nil,
		option.WithCredentialsJSON([]byte(g.startSettings.Config.Credentials)),
		option.WithScopes(g.startSettings.Config.Scopes...),
	)
	if err != nil {
		// check err port
		return err
	}

	db, err := app.Firestore(listenCtx)
	if err != nil {
		// check err port
		return err
	}

	defer db.Close()

	q := db.Collection(g.startSettings.Collection).Query
	if len(g.startSettings.Wheres) > 0 {
		for _, w := range g.startSettings.Wheres {
			q = q.Where(w.Path, w.Operation, w.Value)
		}
	}

	iter := q.Snapshots(listenCtx)
	for {
		snap, err := iter.Next()
		// DeadlineExceeded will be returned when ctx is cancelled.
		if status.Code(err) == codes.DeadlineExceeded {
			return nil
		}
		if errors.Is(listenCtx.Err(), context.Canceled) {
			return nil
		}
		if err != nil {
			return etc.ClassifyGoogleErr(fmt.Errorf("snapshots next: %w", err))
		}
		if snap == nil {
			continue
		}

		for _, change := range snap.Changes {

			var action string
			switch change.Kind {
			case firestore.DocumentAdded:
				action = "added"
			case firestore.DocumentModified:
				action = "modified"
			case firestore.DocumentRemoved:
				action = "removed"
			}

			resp := Response{
				Context: g.startSettings.Context,
				Action:  action,
			}
			if change.Doc != nil {
				resp.Document = change.Doc.Data()
				if change.Doc.Ref != nil {
					resp.RefID = change.Doc.Ref.ID
				}
			}
			_ = handler(trace.ContextWithSpanContext(listenCtx, trace.NewSpanContext(trace.SpanContextConfig{})), ResponsePort, resp)
		}
	}
}

func (g *Component) stop() error {
	g.cancelFuncLock.Lock()
	defer g.cancelFuncLock.Unlock()
	if g.cancelFunc == nil {
		return nil
	}
	g.cancelFunc()

	return nil
}

func (g *Component) setCancelFunc(f func()) {
	g.cancelFuncLock.Lock()
	defer g.cancelFuncLock.Unlock()
	g.cancelFunc = f
}

func (g *Component) isListening() bool {
	g.cancelFuncLock.Lock()
	defer g.cancelFuncLock.Unlock()

	return g.cancelFunc != nil
}

func (g *Component) getControl() interface{} {
	if g.isListening() {
		return StopControl{
			Status: "Listening",
		}
	}
	return StartControl{
		Status: "Not listening",
	}
}

func (g *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: Settings{},
		},
		{
			Name:          StartPort,
			Label:         "Start",
			Position:      module.Left,
			Configuration: g.startSettings,
		},
		{
			Name:          v1alpha1.ControlPort,
			Label:         "Dashboard",
			Source:        true,
			Configuration: g.getControl(),
		},
		{
			Source:        true,
			Name:          ResponsePort,
			Label:         "Response",
			Position:      module.Right,
			Configuration: Response{},
		},
	}

	// programmatically stop server
	if g.settings.EnableStopPort {
		ports = append(ports, module.Port{
			Position:      module.Left,
			Name:          StopPort,
			Label:         "Stop",
			Configuration: Stop{},
		})
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
	return &Component{
		cancelFuncLock: &sync.Mutex{},
		runLock:        &sync.Mutex{},
		startSettings:  Start{},
	}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.ControlHandler  = (*Component)(nil)
)

func init() {
	registry.Register(&Component{
		cancelFuncLock: &sync.Mutex{},
		runLock:        &sync.Mutex{},
	})
}
