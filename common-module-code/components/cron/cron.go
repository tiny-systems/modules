package cron

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/robfig/cron/v3"
	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "cron"
	OutPort       = "out"
)

const (
	metadataKeyRunning  = "cron-running"
	metadataKeySchedule = "cron-schedule"
	metadataKeyContext  = "cron-context"
	metadataKeyError    = "cron-error"
)

type Context any

type Settings struct {
	Context  Context `json:"context" configurable:"true" title:"Context" description:"Arbitrary message to send on each scheduled execution"`
	Schedule string  `json:"schedule" required:"true" title:"Schedule" description:"Cron expression (e.g., '*/5 * * * *' for every 5 minutes, '0 9 * * 1-5' for 9 AM on weekdays)" default:"*/1 * * * *"`
}

// ControlStopped is the _control port schema when the cron is not running
type ControlStopped struct {
	Context  Context `json:"context" required:"true" title:"Context"`
	Schedule string  `json:"schedule" required:"true" title:"Schedule" description:"Cron expression"`
	Status   string  `json:"status" title:"Status" readonly:"true"`
	Start    bool    `json:"start" format:"button" title:"Start" required:"true"`
}

// ControlRunning is the _control port schema when the cron is running
type ControlRunning struct {
	Context  Context `json:"context" required:"true" title:"Context" readonly:"true"`
	Schedule string  `json:"schedule" title:"Schedule" readonly:"true"`
	NextRun  string  `json:"nextRun" title:"Next Run" readonly:"true"`
	Status   string  `json:"status" title:"Status" readonly:"true"`
	Stop     bool    `json:"stop" format:"button" title:"Stop" required:"true"`
}

type Component struct {
	module.Base

	mu       sync.Mutex
	settings Settings
	cancel   context.CancelFunc
	nextTick time.Time

	// settingsFromPort tracks whether OnSettings or OnControl has provided
	// fresh values since the runner started. Reconcile uses this to decide
	// whether stale metadata should be allowed to overwrite in-memory state.
	//
	// Why: framework only guarantees Reconcile-then-Settings ordering inside
	// a single Update cycle. Settings can also arrive via a standalone port
	// signal between cycles; a subsequent reconcile would otherwise restore
	// from stale metadata and undo the fresh user input.
	settingsFromPort bool

	lastError string
	runMu     sync.Mutex
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{Schedule: "*/1 * * * *"},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Cron",
		Info:        "Scheduled emitter using cron expressions. Click Start to begin emitting context on Out port according to the schedule. Supports standard cron syntax (minute hour day-of-month month day-of-week). Examples: '*/5 * * * *' (every 5 min), '0 */2 * * *' (every 2 hours), '0 9 * * 1-5' (9 AM weekdays). Click Stop to pause. Cron survives pod restarts and leadership changes.",
		Tags:        []string{"SDK"},
	}
}

// OnSettings receives settings from SettingsPort. Marks settingsFromPort
// so any later reconcile won't restore stale metadata over fresh values.
func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.mu.Lock()
	c.settings = in
	c.settingsFromPort = true
	isRunning := c.cancel != nil
	c.mu.Unlock()
	if isRunning {
		c.persistRunningState()
	}
	return nil
}

// OnReconcile restores schedule/context/error from metadata and resumes
// running state if the cron was previously active.
func (c *Component) OnReconcile(ctx context.Context, node v1alpha1.TinyNode) error {
	c.restoreSettingsFromMetadata(node.Status.Metadata)
	c.handleOrphanedRunningState(ctx, node.Status.Metadata)
	return nil
}

// OnControl handles Start/Stop dashboard clicks.
func (c *Component) OnControl(ctx context.Context, msg any) error {
	if msg == nil {
		return nil
	}
	if !utils.IsLeader(ctx) {
		return nil
	}
	return c.handleControl(msg)
}

func (c *Component) handleControl(msg interface{}) error {
	switch ctrl := msg.(type) {
	case ControlRunning:
		if ctrl.Stop {
			return c.stop()
		}
		// A running cron used to ignore everything except Stop. The message
		// that reaches it while running carries the context but not the Start
		// flag — the port's schema differs by state — so a caller supplying
		// fresh settings, typically the credential a user just typed, got
		// silence: no error, no effect, and the next scheduled run still used
		// the old context. Apply the change instead, which is what the widget
		// being "the settings form" is supposed to mean.
		if !c.contextMatches(ctrl.Context) {
			if err := c.stop(); err != nil {
				return err
			}
			c.mu.Lock()
			c.settings.Context = ctrl.Context
			c.settingsFromPort = true
			c.mu.Unlock()
			c.persistRunningState()
			go c.run(context.Background())
		}
	case ControlStopped:
		// Validate schedule before starting
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		if _, err := parser.Parse(ctrl.Schedule); err != nil {
			errMsg := fmt.Sprintf("invalid schedule %q: %v", ctrl.Schedule, err)
			c.mu.Lock()
			c.lastError = errMsg
			c.mu.Unlock()
			c.persistError(errMsg)
			return nil
		}

		c.mu.Lock()
		c.settings.Context = ctrl.Context
		c.settings.Schedule = ctrl.Schedule
		c.settingsFromPort = true
		c.lastError = ""
		c.mu.Unlock()
		c.clearError()

		c.persistRunningState()
		go c.run(context.Background())
	}
	return nil
}

// contextMatches reports whether a delivered context is the one already in
// force. Compared by encoded form rather than identity: the context is
// arbitrary user data, and a redelivery of the same values must not restart
// the schedule.
func (c *Component) contextMatches(incoming Context) bool {
	c.mu.Lock()
	current := c.settings.Context
	c.mu.Unlock()

	a, errA := json.Marshal(current)
	b, errB := json.Marshal(incoming)
	if errA != nil || errB != nil {
		return false
	}
	return string(a) == string(b)
}

func (c *Component) restoreSettingsFromMetadata(metadata map[string]string) {
	if metadata == nil {
		return
	}

	c.mu.Lock()
	if c.settingsFromPort {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	if schedule, ok := metadata[metadataKeySchedule]; ok && schedule != "" {
		c.mu.Lock()
		c.settings.Schedule = schedule
		c.mu.Unlock()
	}

	if ctxStr, ok := metadata[metadataKeyContext]; ok && ctxStr != "" {
		var savedCtx Context
		if err := json.Unmarshal([]byte(ctxStr), &savedCtx); err == nil {
			c.mu.Lock()
			c.settings.Context = savedCtx
			c.mu.Unlock()
		}
	}

	if errMsg, ok := metadata[metadataKeyError]; ok {
		c.mu.Lock()
		c.lastError = errMsg
		c.mu.Unlock()
	}
}

func (c *Component) handleOrphanedRunningState(ctx context.Context, metadata map[string]string) {
	if metadata == nil {
		return
	}
	if _, hasRunning := metadata[metadataKeyRunning]; !hasRunning {
		return
	}

	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	if !utils.IsLeader(ctx) {
		return
	}

	log.Info().Msg("cron component: resuming after pod restart or leadership change")
	go c.run(context.Background())
}

func (c *Component) run(ctx context.Context) error {
	c.runMu.Lock()
	defer c.runMu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.mu.Lock()
	c.cancel = cancel
	schedule := c.settings.Schedule
	c.mu.Unlock()

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse(schedule)
	if err != nil {
		c.mu.Lock()
		c.cancel = nil
		c.mu.Unlock()
		c.clearRunningMetadata()
		return fmt.Errorf("invalid cron expression %q: %w", schedule, err)
	}

	c.mu.Lock()
	c.nextTick = sched.Next(time.Now())
	c.mu.Unlock()

	c.Emit(context.Background(), v1alpha1.ReconcilePort, nil)

	defer func() {
		c.mu.Lock()
		c.cancel = nil
		c.nextTick = time.Time{}
		c.mu.Unlock()
		c.Emit(context.Background(), v1alpha1.ReconcilePort, nil)
	}()

	log.Info().Str("schedule", schedule).Time("nextTick", c.nextTick).Msg("cron: started")

	for {
		c.mu.Lock()
		nextTick := c.nextTick
		c.mu.Unlock()

		if err := c.waitUntil(ctx, nextTick); err != nil {
			return nil
		}

		c.mu.Lock()
		data := c.settings.Context
		c.mu.Unlock()

		if err := c.Emit(ctx, OutPort, data).Err(); err != nil {
			log.Warn().Err(err).Msg("cron: downstream error on out port")
		}

		if ctx.Err() != nil {
			return nil
		}

		c.mu.Lock()
		c.nextTick = sched.Next(time.Now())
		c.mu.Unlock()

		c.Emit(context.Background(), v1alpha1.ReconcilePort, nil)

		log.Debug().Time("nextTick", c.nextTick).Msg("cron: scheduled next tick")
	}
}

func (c *Component) waitUntil(ctx context.Context, t time.Time) error {
	wait := time.Until(t)
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Component) stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}
	c.clearRunningMetadata()
	return nil
}

func (c *Component) persistRunningState() {
	c.mu.Lock()
	schedule := c.settings.Schedule
	cronCtx := c.settings.Context
	c.mu.Unlock()

	ctxBytes, _ := json.Marshal(cronCtx)
	c.Emit(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[metadataKeyRunning] = "true"
		n.Status.Metadata[metadataKeySchedule] = schedule
		n.Status.Metadata[metadataKeyContext] = string(ctxBytes)
		return nil
	})
}

func (c *Component) clearRunningMetadata() {
	c.Emit(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata != nil {
			delete(n.Status.Metadata, metadataKeyRunning)
			delete(n.Status.Metadata, metadataKeySchedule)
			delete(n.Status.Metadata, metadataKeyContext)
			delete(n.Status.Metadata, metadataKeyError)
		}
		return nil
	})
}

func (c *Component) persistError(errMsg string) {
	c.Emit(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata == nil {
			n.Status.Metadata = make(map[string]string)
		}
		n.Status.Metadata[metadataKeyError] = errMsg
		return nil
	})
}

func (c *Component) clearError() {
	c.Emit(context.Background(), v1alpha1.ReconcilePort, func(n *v1alpha1.TinyNode) error {
		if n.Status.Metadata != nil {
			delete(n.Status.Metadata, metadataKeyError)
		}
		return nil
	})
}

func (c *Component) Ports() []module.Port {
	c.mu.Lock()
	defer c.mu.Unlock()

	return []module.Port{
		{Name: v1alpha1.ReconcilePort},
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: OutPort, Label: "Out", Source: true, Position: module.Right, Configuration: new(Context)},
		{Name: v1alpha1.ControlPort, Label: "Control", Source: true, Configuration: c.control()},
	}
}

func (c *Component) control() interface{} {
	if c.cancel != nil {
		nextRun := ""
		if !c.nextTick.IsZero() {
			nextRun = c.nextTick.Format(time.RFC3339)
		}
		return ControlRunning{
			Status:   "Running",
			Context:  c.settings.Context,
			Schedule: c.settings.Schedule,
			NextRun:  nextRun,
			Stop:     true,
		}
	}

	status := "Not running"
	if c.lastError != "" {
		status = c.lastError
	}

	return ControlStopped{
		Context:  c.settings.Context,
		Schedule: c.settings.Schedule,
		Status:   status,
		Start:    true,
	}
}

// Handle is unreachable: every port is system or source.
func (c *Component) Handle(_ context.Context, _ module.Handler, port string, _ any) module.Result {
	return module.Fail(fmt.Errorf("cron has no business-port input: got %q", port))
}

var (
	_ module.Component        = (*Component)(nil)
	_ module.SettingsHandler  = (*Component)(nil)
	_ module.ReconcileHandler = (*Component)(nil)
	_ module.ControlHandler   = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
