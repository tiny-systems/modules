package ticker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
	"go.opentelemetry.io/otel/trace"
)

const (
	ComponentName        = "ticker"
	OutPort       string = "out"

	metadataKeyRunning = "ticker-running"
	metadataKeyConfig  = "ticker-config"
)

type Context any

type Settings struct {
	Context Context `json:"context" configurable:"true" title:"Context" description:"Arbitrary message to be send each period of time"`
	Delay   int     `json:"delay" required:"true" title:"Delay (ms)" description:"Delay between signals" minimum:"0" default:"1000"`
}

type Component struct {
	module.Base

	settings Settings

	cancelFunc     context.CancelFunc
	cancelFuncLock *sync.Mutex

	runLock *sync.Mutex

	// settingsFromPort tracks whether OnSettings or OnControl has provided
	// fresh values since the runner started. Reconcile uses this to decide
	// whether stale metadata should restore in-memory state.
	settingsFromPort bool
}

func (t *Component) Instance() module.Component {
	return &Component{
		cancelFuncLock: &sync.Mutex{},
		runLock:        &sync.Mutex{},
		settings: Settings{
			Delay: 1000,
		},
	}
}

// ControlStopped is the _control port schema when the ticker is not running
type ControlStopped struct {
	Context Context `json:"context" required:"true" title:"Context"`
	Status  string  `json:"status" title:"Status" readonly:"true"`
	Start   bool    `json:"start" format:"button" title:"Start" required:"true"`
}

// ControlRunning is the _control port schema when the ticker is running
type ControlRunning struct {
	Context Context `json:"context" required:"true" title:"Context" readonly:"true"`
	Status  string  `json:"status" title:"Status" readonly:"true"`
	Stop    bool    `json:"stop" format:"button" title:"Stop" required:"true"`
}

func (t *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Ticker",
		Info:        "Periodic emitter. Click Start to begin emitting context on Out. Waits for Out port to complete, then waits [delay] ms before next emit. Click Stop to pause. Survives pod restarts via metadata persistence.",
		Tags:        []string{"SDK"},
	}
}

// OnSettings receives settings from the SettingsPort. Marks settingsFromPort
// so a later reconcile won't restore stale metadata over fresh values.
// Preserves the existing in-memory Context if a control click set it before
// the CRD-driven settings catch up.
func (t *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if t.settingsFromPort {
		in.Context = t.settings.Context
	}
	t.settings = in
	t.settingsFromPort = true
	if t.isRunning() {
		t.persistMetadata()
	}
	return nil
}

// OnReconcile restores running state from node metadata if a previous
// instance was emitting before pod restart.
func (t *Component) OnReconcile(ctx context.Context, node v1alpha1.TinyNode) error {
	if node.Status.Metadata == nil {
		return nil
	}
	if _, running := node.Status.Metadata[metadataKeyRunning]; !running {
		return nil
	}
	if t.isRunning() {
		return nil
	}
	if !utils.IsLeader(ctx) {
		return nil
	}

	if !t.settingsFromPort {
		if configStr, ok := node.Status.Metadata[metadataKeyConfig]; ok {
			var cfg Settings
			if err := json.Unmarshal([]byte(configStr), &cfg); err == nil {
				t.settings = cfg
				t.settingsFromPort = true
			}
		}
	}

	log.Info().Interface("settings", t.settings).Msg("ticker: restoring from metadata")
	go t.run(ctx)
	return nil
}

// OnControl handles Start/Stop dashboard clicks.
func (t *Component) OnControl(ctx context.Context, msg any) error {
	if msg == nil {
		return nil
	}
	if !utils.IsLeader(ctx) {
		return nil
	}

	switch ctrl := msg.(type) {
	case ControlRunning:
		if ctrl.Stop {
			t.settingsFromPort = false
			t.clearMetadata()
			return t.stop()
		}
	case ControlStopped:
		t.settings.Context = ctrl.Context
		t.settingsFromPort = true
		t.persistMetadata()

		if t.isRunning() {
			return nil
		}
		go t.run(context.Background())
	}
	return nil
}

func (t *Component) run(ctx context.Context) error {
	t.runLock.Lock()
	defer t.runLock.Unlock()

	// Use Background context - long-running and shouldn't inherit caller's deadline
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	// Bridge: cancel run when parent context is done (e.g., runner shutdown)
	go func() {
		select {
		case <-ctx.Done():
			runCancel()
		case <-runCtx.Done():
		}
	}()

	t.setCancelFunc(runCancel)
	t.Emit(context.Background(), v1alpha1.ControlPort, t.getControl())

	defer func() {
		t.setCancelFunc(nil)
		t.Emit(context.Background(), v1alpha1.ControlPort, t.getControl())
	}()

	timer := time.NewTimer(0) // first tick fires immediately
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			emitCtx := trace.ContextWithSpanContext(runCtx, trace.NewSpanContext(trace.SpanContextConfig{}))
			if err := t.Emit(emitCtx, OutPort, t.settings.Context).Err(); err != nil {
				log.Warn().Err(err).Msg("ticker: downstream error on out port")
			}
			timer.Reset(time.Duration(t.settings.Delay) * time.Millisecond)

		case <-runCtx.Done():
			err := runCtx.Err()
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}
}

func (t *Component) persistMetadata() {
	configBytes, _ := json.Marshal(t.settings)
	t.Emit(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[metadataKeyRunning] = "true"
		n.Status.Metadata[metadataKeyConfig] = string(configBytes)
		return nil
	})
}

func (t *Component) clearMetadata() {
	t.Emit(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			return nil
		}
		delete(n.Status.Metadata, metadataKeyRunning)
		delete(n.Status.Metadata, metadataKeyConfig)
		return nil
	})
}

func (t *Component) setCancelFunc(f func()) {
	t.cancelFuncLock.Lock()
	defer t.cancelFuncLock.Unlock()
	t.cancelFunc = f
}

func (t *Component) isRunning() bool {
	t.cancelFuncLock.Lock()
	defer t.cancelFuncLock.Unlock()
	return t.cancelFunc != nil
}

func (t *Component) stop() error {
	t.cancelFuncLock.Lock()
	defer t.cancelFuncLock.Unlock()

	if t.cancelFunc == nil {
		return nil
	}
	t.cancelFunc()
	return nil
}

func (t *Component) Ports() []module.Port {
	return []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: t.settings,
		},
		{
			Name:  v1alpha1.ReconcilePort,
			Label: "Reconcile",
		},
		{
			Name:          OutPort,
			Label:         "Out",
			Source:        true,
			Position:      module.Right,
			Configuration: new(Context),
		},
		{
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Source:        true,
			Configuration: t.getControl(),
		},
	}
}

func (t *Component) getControl() interface{} {
	if t.isRunning() {
		return ControlRunning{
			Status:  "Running",
			Context: t.settings.Context,
			Stop:    true,
		}
	}
	return ControlStopped{
		Context: t.settings.Context,
		Status:  "Not running",
		Start:   true,
	}
}

// Handle is unreachable for ticker — every port the component declares is
// either system (dispatched via capabilities) or source (emit-only). The
// stub guards against accidental routing.
func (t *Component) Handle(_ context.Context, _ module.Handler, port string, _ any) module.Result {
	return module.Fail(fmt.Errorf("ticker has no business-port input: got %q", port))
}

var (
	_ module.Component        = (*Component)(nil)
	_ module.SettingsHandler  = (*Component)(nil)
	_ module.ReconcileHandler = (*Component)(nil)
	_ module.ControlHandler   = (*Component)(nil)
	_ module.Destroyer        = (*Component)(nil)
)

func (t *Component) OnDestroy(_ map[string]string) {
	t.stop()
}

func init() {
	registry.Register((&Component{}).Instance())
}
