package eventwatcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "event_watch"

	StartPort = "start"
	EventPort = "event"
	ErrorPort = "error"
)

// Control shows the current watcher status
type Control struct {
	Status    string `json:"status" title:"Status" readonly:"true"`
	Namespace string `json:"namespace,omitempty" title:"Namespace" readonly:"true"`
}

// Context type alias for schema generation
type Context any

// Settings configures the watcher behavior
type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

// Start initiates watching with the given configuration
type Start struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through to events"`

	// Scope
	Namespace string `json:"namespace" required:"true" title:"Namespace" description:"Namespace to watch"`

	// Filtering
	FieldSelector    string   `json:"fieldSelector,omitempty" title:"Field Selector" description:"Filter events (e.g., type=Warning, involvedObject.kind=Pod)"`
	IgnoreNamespaces []string `json:"ignoreNamespaces,omitempty" title:"Ignore Namespaces" description:"Skip events from these namespaces (e.g., kube-system)"`

	// Event type filter
	WarningsOnly bool     `json:"warningsOnly,omitempty" title:"Warnings Only" description:"Only emit Warning events (not Normal)"`
	WatchActions []string `json:"watchActions,omitempty" title:"Watch Actions" description:"Only emit these actions (ADDED, MODIFIED, DELETED). Empty means all."`
	Reasons      []string `json:"reasons,omitempty" title:"Reasons" description:"Only emit events with these reasons (e.g., Failed, BackOff, Unhealthy). Empty means all."`

	// Deduplication
	CooldownSeconds int `json:"cooldownSeconds,omitempty" title:"Cooldown Seconds" description:"Don't emit same event more than once within this period (0 = no cooldown)"`
}

// InvolvedObject identifies what the event is about
type InvolvedObject struct {
	Kind      string `json:"kind" title:"Kind" description:"Kind of object (Pod, Deployment, etc.)"`
	Name      string `json:"name" title:"Name" description:"Name of the object"`
	Namespace string `json:"namespace,omitempty" title:"Namespace"`
	UID       string `json:"uid,omitempty" title:"UID"`
}

// Event is emitted for each Kubernetes event
type Event struct {
	Context Context `json:"context,omitempty" title:"Context" description:"Context passed from start message"`

	// Event metadata
	WatchAction string `json:"watchAction" title:"Watch Action" description:"ADDED, MODIFIED, or DELETED"`
	Timestamp   string `json:"timestamp" title:"Timestamp" description:"When the event was received"`

	// Kubernetes Event fields
	Type           string         `json:"type" title:"Type" description:"Event type: Warning or Normal"`
	Reason         string         `json:"reason" title:"Reason" description:"Short reason for the event (Failed, BackOff, Unhealthy, etc.)"`
	Message        string         `json:"message" title:"Message" description:"Human-readable description"`
	InvolvedObject InvolvedObject `json:"involvedObject" title:"Involved Object" description:"What this event is about"`
	Count          int32          `json:"count" title:"Count" description:"Number of times this event occurred"`
	FirstTimestamp string         `json:"firstTimestamp,omitempty" title:"First Timestamp"`
	LastTimestamp  string         `json:"lastTimestamp,omitempty" title:"Last Timestamp"`
	Source         string         `json:"source,omitempty" title:"Source" description:"Component that generated the event"`
}

// Error output for error port
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error" description:"Error message"`
}

// Component implements the EventWatcher
type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	k8sClient     client.WithWatch
	k8sClientLock sync.RWMutex

	control     Control
	controlLock sync.RWMutex

	cancelFunc     context.CancelFunc
	cancelFuncLock sync.Mutex

	watchDone     chan struct{}
	watchDoneLock sync.Mutex

	// cooldown tracking: event key -> last emit time
	lastEmitTime     map[string]time.Time
	lastEmitTimeLock sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{},
		control:  Control{Status: "Not watching"},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Event Watcher",
		Info:        "Watch Kubernetes Events for warnings and errors. Use to detect ImagePullBackOff, CrashLoopBackOff, scheduling failures, and other cluster issues.",
		Tags:        []string{"Kubernetes", "Events", "Alerts", "Monitoring"},
	}
}

// OnClient stashes the K8s client.
func (c *Component) OnClient(k8sClient module.K8sClient) {
	if k8sClient == nil {
		return
	}
	c.k8sClientLock.Lock()
	c.k8sClient = k8sClient.GetK8sClient()
	c.k8sClientLock.Unlock()
	log.Debug().Msg("K8s client received")
}

// OnSettings stores the component settings.
func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings message")
	}
	c.settingsLock.Lock()
	c.settings = in
	c.settingsLock.Unlock()
	return nil
}

// Handle dispatches the StartPort. System ports go through capabilities.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != StartPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	if msg == nil {
		log.Info().Msg("event_watch: StartPort received nil, stopping")
		return module.Fail(c.stop())
	}

	in, ok := msg.(Start)
	if !ok {
		return module.Fail(fmt.Errorf("invalid start message"))
	}

	if done := c.getWatchDone(); done != nil {
		log.Info().Msg("event_watch: already running, waiting for watcher to stop")
		select {
		case <-done:
			return module.Result{}
		case <-ctx.Done():
			c.stop()
			return module.Result{}
		}
	}

	return module.Fail(c.runWatch(ctx, handler, in))
}

func (c *Component) getWatchDone() chan struct{} {
	c.watchDoneLock.Lock()
	defer c.watchDoneLock.Unlock()
	return c.watchDone
}

func (c *Component) setWatchDone(done chan struct{}) {
	c.watchDoneLock.Lock()
	defer c.watchDoneLock.Unlock()
	c.watchDone = done
}

func (c *Component) setCancelFunc(fn context.CancelFunc) {
	c.cancelFuncLock.Lock()
	defer c.cancelFuncLock.Unlock()
	c.cancelFunc = fn
}

func (c *Component) isRunning() bool {
	c.cancelFuncLock.Lock()
	defer c.cancelFuncLock.Unlock()
	return c.cancelFunc != nil
}

func (c *Component) stop() error {
	c.cancelFuncLock.Lock()
	defer c.cancelFuncLock.Unlock()

	if c.cancelFunc == nil {
		return nil
	}

	log.Info().Msg("event_watch: stopping watcher")
	c.cancelFunc()
	return nil
}

func (c *Component) updateControl(ctx context.Context, handler module.Handler, ctrl Control) {
	c.controlLock.Lock()
	c.control = ctrl
	c.controlLock.Unlock()

	handler(ctx, v1alpha1.ControlPort, ctrl)
}

func (c *Component) runWatch(ctx context.Context, handler module.Handler, start Start) error {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	c.k8sClientLock.RUnlock()

	if k8sClient == nil {
		c.updateControl(ctx, handler, Control{Status: "Error: K8s client not available"})
		return fmt.Errorf("K8s client not available")
	}

	// Build list options
	listOpts := &client.ListOptions{
		Namespace: start.Namespace,
	}

	// Add field selector for filtering
	fieldSelector := start.FieldSelector
	if start.WarningsOnly {
		if fieldSelector != "" {
			fieldSelector = fieldSelector + ",type=Warning"
		} else {
			fieldSelector = "type=Warning"
		}
	}

	if fieldSelector != "" {
		listOpts.Raw = &metav1.ListOptions{
			FieldSelector: fieldSelector,
		}
	}

	// Create done channel
	done := make(chan struct{})
	c.setWatchDone(done)
	defer func() {
		c.setWatchDone(nil)
		close(done)
	}()

	// Use Background context - watch is long-running and shouldn't inherit caller's deadline
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()

	// Bridge: cancel watcher when parent context is done.
	go func() {
		select {
		case <-ctx.Done():
			watchCancel()
		case <-watchCtx.Done():
		}
	}()

	c.setCancelFunc(watchCancel)
	defer c.setCancelFunc(nil)

	// Create event list for watching
	eventList := &corev1.EventList{}

	// Start the watch
	watcher, err := k8sClient.Watch(watchCtx, eventList, listOpts)
	if err != nil {
		c.updateControl(ctx, handler, Control{Status: fmt.Sprintf("Error: %v", err)})
		return fmt.Errorf("failed to start watch: %w", k8s.ClassifyError(err))
	}

	// Start watch loop in goroutine
	go c.watchLoop(watchCtx, handler, start, watcher, eventList, listOpts)

	// Update control to show watching status
	c.updateControl(ctx, handler, c.watchingControl(start))

	log.Info().Str("namespace", start.Namespace).Msg("Event watcher started")

	// Block until context done
	<-watchCtx.Done()

	watcher.Stop()
	c.updateControl(ctx, handler, Control{Status: "Stopped"})

	log.Info().Msg("Event watcher stopped")
	return watchCtx.Err()
}

func (c *Component) watchLoop(ctx context.Context, handler module.Handler, start Start, watcher watch.Interface, eventList *corev1.EventList, listOpts *client.ListOptions) {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	c.k8sClientLock.RUnlock()

	// Last resourceVersion seen. Re-watches resume from here so the apiserver
	// does not replay every existing event as a synthetic ADDED on each
	// reconnect (watches are closed server-side every few minutes by default).
	var lastResourceVersion string

	for {
		select {
		case <-ctx.Done():
			watcher.Stop()
			return
		case watchEvent, ok := <-watcher.ResultChan():
			if !ok {
				log.Debug().Str("resourceVersion", lastResourceVersion).Msg("Event watch channel closed, restarting...")
				watcher = c.rewatch(ctx, handler, k8sClient, eventList, listOpts, start, &lastResourceVersion)
				if watcher == nil {
					return
				}
				continue
			}

			if watchEvent.Type == watch.Error {
				err := apierrors.FromObject(watchEvent.Object)
				if apierrors.IsGone(err) || apierrors.IsResourceExpired(err) {
					// Stored resourceVersion was compacted away — must re-watch fresh.
					lastResourceVersion = ""
				}
				log.Warn().Err(err).Msg("Event watch error event, restarting watch")
				watcher.Stop()
				watcher = c.rewatch(ctx, handler, k8sClient, eventList, listOpts, start, &lastResourceVersion)
				if watcher == nil {
					return
				}
				continue
			}

			k8sEvent, ok := watchEvent.Object.(*corev1.Event)
			if !ok {
				continue
			}

			// Track progress for reconnects (also from Bookmark events, which
			// carry only a resourceVersion and are skipped below).
			if rv := k8sEvent.ResourceVersion; rv != "" {
				lastResourceVersion = rv
			}

			// Determine watch action first (needed for filtering)
			var watchAction string
			switch watchEvent.Type {
			case watch.Added:
				watchAction = "ADDED"
			case watch.Modified:
				watchAction = "MODIFIED"
			case watch.Deleted:
				watchAction = "DELETED"
			default:
				continue
			}

			// Filter: ignore namespaces
			if len(start.IgnoreNamespaces) > 0 {
				skip := false
				for _, ns := range start.IgnoreNamespaces {
					if k8sEvent.InvolvedObject.Namespace == ns {
						skip = true
						break
					}
				}
				if skip {
					continue
				}
			}

			// Filter: watch actions
			if len(start.WatchActions) > 0 {
				allowed := false
				for _, action := range start.WatchActions {
					if action == watchAction {
						allowed = true
						break
					}
				}
				if !allowed {
					continue
				}
			}

			// Filter: warnings only
			if start.WarningsOnly && k8sEvent.Type != "Warning" {
				continue
			}

			// Filter: specific reasons
			if len(start.Reasons) > 0 {
				allowed := false
				for _, reason := range start.Reasons {
					if k8sEvent.Reason == reason {
						allowed = true
						break
					}
				}
				if !allowed {
					continue
				}
			}

			// Filter: cooldown
			if start.CooldownSeconds > 0 {
				// Use event UID as key for deduplication
				eventKey := string(k8sEvent.UID)
				if eventKey == "" {
					eventKey = k8sEvent.Namespace + "/" + k8sEvent.Name
				}

				c.lastEmitTimeLock.RLock()
				lastEmit, exists := c.lastEmitTime[eventKey]
				c.lastEmitTimeLock.RUnlock()

				if exists && time.Since(lastEmit) < time.Duration(start.CooldownSeconds)*time.Second {
					continue
				}

				c.lastEmitTimeLock.Lock()
				if c.lastEmitTime == nil {
					c.lastEmitTime = make(map[string]time.Time)
				}
				c.lastEmitTime[eventKey] = time.Now()
				// Sweep stale entries to prevent unbounded map growth
				cooldown := time.Duration(start.CooldownSeconds) * time.Second
				for k, t := range c.lastEmitTime {
					if time.Since(t) >= cooldown {
						delete(c.lastEmitTime, k)
					}
				}
				c.lastEmitTimeLock.Unlock()
			}

			c.emitEvent(ctx, handler, start.Context, watchAction, k8sEvent)
		}
	}
}

// watchingControl builds the Control state shown while the watch is healthy.
func (c *Component) watchingControl(start Start) Control {
	return Control{
		Status:    "Watching",
		Namespace: start.Namespace,
	}
}

// rewatch re-establishes a closed watch, resuming from the last seen
// resourceVersion so existing events are not replayed as synthetic ADDED.
// On "410 Gone / resourceVersion too old" it falls back to one fresh watch
// without a resourceVersion; the replayed ADDED events are emitted (this
// component keeps no seen-set) but the recovery is flagged in the log.
// Failures retry with exponential backoff (1s doubling to 30s), surfacing the
// error on the Control port, until the watch is back or ctx is done. Returns
// nil only when ctx is done.
func (c *Component) rewatch(ctx context.Context, handler module.Handler, k8sClient client.WithWatch, eventList *corev1.EventList, listOpts *client.ListOptions, start Start, lastResourceVersion *string) watch.Interface {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	reportedError := false

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Copy the list options so retries don't mutate the caller's struct;
		// preserve any Raw options (field selector) and resume from the last
		// seen resourceVersion when we have one.
		opts := *listOpts
		raw := metav1.ListOptions{}
		if listOpts.Raw != nil {
			raw = *listOpts.Raw
		}
		raw.ResourceVersion = *lastResourceVersion
		opts.Raw = &raw

		watcher, err := k8sClient.Watch(ctx, eventList, &opts)
		if err == nil {
			if reportedError {
				log.Info().Msg("Event watch re-established")
				c.updateControl(ctx, handler, c.watchingControl(start))
			}
			return watcher
		}

		if *lastResourceVersion != "" && (apierrors.IsGone(err) || apierrors.IsResourceExpired(err)) {
			// The stored resourceVersion was compacted away. Recover with a
			// fresh watch; the apiserver replays current events as ADDED.
			log.Warn().Err(err).Msg("Event watch resourceVersion expired; falling back to fresh watch — existing events will replay as ADDED")
			*lastResourceVersion = ""
			continue
		}

		log.Error().Err(err).Dur("backoff", backoff).Msg("Failed to restart event watch, retrying")
		ctrl := c.watchingControl(start)
		ctrl.Status = fmt.Sprintf("Watch failed: %v, retrying", err)
		c.updateControl(ctx, handler, ctrl)
		reportedError = true

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (c *Component) emitEvent(ctx context.Context, handler module.Handler, userCtx Context, watchAction string, k8sEvent *corev1.Event) {
	event := Event{
		Context:     userCtx,
		WatchAction: watchAction,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Type:        k8sEvent.Type,
		Reason:      k8sEvent.Reason,
		Message:     k8sEvent.Message,
		InvolvedObject: InvolvedObject{
			Kind:      k8sEvent.InvolvedObject.Kind,
			Name:      k8sEvent.InvolvedObject.Name,
			Namespace: k8sEvent.InvolvedObject.Namespace,
			UID:       string(k8sEvent.InvolvedObject.UID),
		},
		Count:  k8sEvent.Count,
		Source: k8sEvent.Source.Component,
	}

	if !k8sEvent.FirstTimestamp.IsZero() {
		event.FirstTimestamp = k8sEvent.FirstTimestamp.Format(time.RFC3339)
	}
	if !k8sEvent.LastTimestamp.IsZero() {
		event.LastTimestamp = k8sEvent.LastTimestamp.Format(time.RFC3339)
	}

	if err := handler(ctx, EventPort, event).Err(); err != nil {
		log.Error().Err(err).Str("reason", k8sEvent.Reason).Msg("Failed to emit event")
	}
}

func (c *Component) Ports() []module.Port {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	c.controlLock.RLock()
	control := c.control
	c.controlLock.RUnlock()

	ports := []module.Port{
		{
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Configuration: control,
		},
		{
			Name:     v1alpha1.ClientPort,
			Label:    "Client",
			Position: module.Left,
		},
		{
			Name:  v1alpha1.ReconcilePort,
			Label: "Reconcile",
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: Settings{},
		},
		{
			Name:  StartPort,
			Label: "Start",
			Configuration: Start{
				WarningsOnly: true,
			},
			Position: module.Left,
		},
		{
			Name:   EventPort,
			Label:  "Event",
			Source: true,
			Configuration: Event{
				WatchAction: "ADDED",
				Type:        "Warning",
				Reason:      "Failed",
				Message:     "Error: ImagePullBackOff",
				InvolvedObject: InvolvedObject{
					Kind:      "Pod",
					Name:      "myapp-abc123",
					Namespace: "default",
				},
				Count: 5,
			},
			Position: module.Right,
		},
	}

	if enableErrorPort {
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

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.ClientAware     = (*Component)(nil)
)
var _ module.Destroyer = (*Component)(nil)

func (c *Component) OnDestroy(_ map[string]string) {
	c.stop()
}

func init() {
	registry.Register(&Component{})
}
