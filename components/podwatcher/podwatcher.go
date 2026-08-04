package podwatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "pod_watch"

	StartPort = "start"
	EventPort = "event"
	ErrorPort = "error"
)

// Control shows the current watcher status
type Control struct {
	Status           string   `json:"status" title:"Status" readonly:"true"`
	Namespace        string   `json:"namespace,omitempty" title:"Namespace" readonly:"true"`
	LabelSelector    string   `json:"labelSelector,omitempty" title:"Label Selector" readonly:"true"`
	IgnoreNamespaces []string `json:"ignoreNamespaces,omitempty" title:"Ignore Namespaces" readonly:"true"`
	ProblemsOnly     bool     `json:"problemsOnly,omitempty" title:"Problems Only" readonly:"true"`
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
	LabelSelector    string   `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by labels (e.g., app=nginx,env=prod)"`
	IgnoreNamespaces []string `json:"ignoreNamespaces,omitempty" title:"Ignore Namespaces" description:"Skip events from these namespaces (e.g., kube-system, kube-public)"`

	// Event filters
	ProblemsOnly    bool     `json:"problemsOnly,omitempty" title:"Problems Only" description:"Only emit events for pods with issues (not Running/Succeeded)"`
	WatchActions    []string `json:"watchActions,omitempty" title:"Watch Actions" description:"Only emit these actions (ADDED, MODIFIED, DELETED). Empty means all."`
	MinRestartCount int32    `json:"minRestartCount,omitempty" title:"Min Restart Count" description:"Only emit if any container has at least this many restarts"`

	// Deduplication
	CooldownSeconds int `json:"cooldownSeconds,omitempty" title:"Cooldown Seconds" description:"Don't emit same pod more than once within this period (0 = no cooldown)"`
}

// ContainerState represents the state of a container
type ContainerState struct {
	State   string `json:"state" title:"State" description:"waiting, running, or terminated"`
	Reason  string `json:"reason,omitempty" title:"Reason" description:"Reason for current state (e.g., ImagePullBackOff, CrashLoopBackOff)"`
	Message string `json:"message,omitempty" title:"Message" description:"Details about the state"`
}

// ContainerStatus represents a container's status
type ContainerStatus struct {
	Name         string         `json:"name" title:"Name"`
	Ready        bool           `json:"ready" title:"Ready"`
	RestartCount int32          `json:"restartCount" title:"Restart Count"`
	State        ContainerState `json:"state" title:"State"`
}

// PodInfo contains typed pod information
type PodInfo struct {
	Name              string            `json:"name" title:"Name"`
	Namespace         string            `json:"namespace" title:"Namespace"`
	Phase             string            `json:"phase" title:"Phase" description:"Pending, Running, Succeeded, Failed, Unknown"`
	Ready             bool              `json:"ready" title:"Ready" description:"All containers ready"`
	Message           string            `json:"message,omitempty" title:"Message" description:"Human readable status message"`
	Reason            string            `json:"reason,omitempty" title:"Reason" description:"Brief reason for current status"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty" title:"Container Statuses"`
	Labels            map[string]string `json:"labels,omitempty" title:"Labels"`
	NodeName          string            `json:"nodeName,omitempty" title:"Node Name"`
	PodIP             string            `json:"podIP,omitempty" title:"Pod IP"`
	StartTime         string            `json:"startTime,omitempty" title:"Start Time"`
}

// Event is emitted for each pod change
type Event struct {
	Context Context `json:"context,omitempty" title:"Context" description:"Context passed from start message"`

	// Event metadata
	WatchAction string `json:"watchAction" title:"Watch Action" description:"ADDED, MODIFIED, or DELETED"`
	Timestamp   string `json:"timestamp" title:"Timestamp" description:"When the event was received"`

	// Pod information
	Pod PodInfo `json:"pod" title:"Pod" description:"Pod details"`

	// Quick access fields
	HasProblem    bool   `json:"hasProblem" title:"Has Problem" description:"True if pod has issues"`
	ProblemReason string `json:"problemReason,omitempty" title:"Problem Reason" description:"Brief description of the issue"`
}

// Error output for error port
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error" description:"Error message"`
}

// Component implements the PodWatcher
type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	k8sClient     client.WithWatch
	k8sClientLock sync.RWMutex

	control     Control
	controlLock sync.RWMutex

	cancelFunc     context.CancelFunc
	cancelFuncLock sync.Mutex

	// watchDone is closed when the watcher stops, allowing waiters to unblock
	watchDone     chan struct{}
	watchDoneLock sync.Mutex

	// cooldown tracking: pod key -> last emit time
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
		Description: "Pod Watcher",
		Info:        "Watch Kubernetes Pods for status changes. Emits typed events with pod phase, container states, and problem detection.",
		Tags:        []string{"Kubernetes", "Pods", "Watch", "Monitoring"},
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
		log.Info().Msg("pod_watch: StartPort received nil, stopping")
		return module.Fail(c.stop())
	}

	in, ok := msg.(Start)
	if !ok {
		return module.Fail(fmt.Errorf("invalid start message"))
	}

	if done := c.getWatchDone(); done != nil {
		log.Info().Msg("pod_watch: already running, waiting for watcher to stop")
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

	log.Info().Msg("pod_watch: stopping watcher")
	c.cancelFunc()
	return nil
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

	if start.LabelSelector != "" {
		selector, err := metav1.ParseToLabelSelector(start.LabelSelector)
		if err != nil {
			c.updateControl(ctx, handler, Control{Status: fmt.Sprintf("Error: %v", err)})
			return fmt.Errorf("invalid label selector: %v", err)
		}
		labelSelector, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			c.updateControl(ctx, handler, Control{Status: fmt.Sprintf("Error: %v", err)})
			return fmt.Errorf("invalid label selector: %v", err)
		}
		listOpts.LabelSelector = labelSelector
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

	// Create pod list for watching
	podList := &corev1.PodList{}

	// Start the watch
	watcher, err := k8sClient.Watch(watchCtx, podList, listOpts)
	if err != nil {
		c.updateControl(ctx, handler, Control{Status: fmt.Sprintf("Error: %v", err)})
		return fmt.Errorf("failed to start watch: %w", k8s.ClassifyError(err))
	}

	// Start watch loop in goroutine
	go c.watchLoop(watchCtx, handler, start, watcher, podList, listOpts)

	// Update control to show watching status
	c.updateControl(ctx, handler, Control{
		Status:           "Watching",
		Namespace:        start.Namespace,
		LabelSelector:    start.LabelSelector,
		IgnoreNamespaces: start.IgnoreNamespaces,
		ProblemsOnly:     start.ProblemsOnly,
	})

	log.Info().Str("namespace", start.Namespace).Str("labelSelector", start.LabelSelector).Msg("Pod watcher started")

	// Block until context done
	<-watchCtx.Done()

	watcher.Stop()
	c.updateControl(ctx, handler, Control{Status: "Stopped"})

	log.Info().Msg("Pod watcher stopped")
	return watchCtx.Err()
}

func (c *Component) watchLoop(ctx context.Context, handler module.Handler, start Start, watcher watch.Interface, podList *corev1.PodList, listOpts *client.ListOptions) {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	c.k8sClientLock.RUnlock()

	for {
		select {
		case <-ctx.Done():
			return
		case watchEvent, ok := <-watcher.ResultChan():
			if !ok {
				log.Debug().Msg("Pod watch channel closed, restarting...")
				time.Sleep(time.Second)
				var err error
				watcher, err = k8sClient.Watch(ctx, podList, listOpts)
				if err != nil {
					log.Error().Err(err).Msg("Failed to restart pod watch")
					return
				}
				continue
			}

			pod, ok := watchEvent.Object.(*corev1.Pod)
			if !ok {
				continue
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
					if pod.Namespace == ns {
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

			hasProblem, problemReason := c.detectProblem(pod)

			// Filter: problems only
			if start.ProblemsOnly && !hasProblem {
				continue
			}

			// Filter: min restart count
			if start.MinRestartCount > 0 {
				maxRestarts := int32(0)
				for _, cs := range pod.Status.ContainerStatuses {
					if cs.RestartCount > maxRestarts {
						maxRestarts = cs.RestartCount
					}
				}
				if maxRestarts < start.MinRestartCount {
					continue
				}
			}

			// Filter: cooldown
			if start.CooldownSeconds > 0 {
				podKey := pod.Namespace + "/" + pod.Name
				c.lastEmitTimeLock.RLock()
				lastEmit, exists := c.lastEmitTime[podKey]
				c.lastEmitTimeLock.RUnlock()

				if exists && time.Since(lastEmit) < time.Duration(start.CooldownSeconds)*time.Second {
					continue
				}

				c.lastEmitTimeLock.Lock()
				if c.lastEmitTime == nil {
					c.lastEmitTime = make(map[string]time.Time)
				}
				c.lastEmitTime[podKey] = time.Now()
				// Sweep stale entries to prevent unbounded map growth
				cooldown := time.Duration(start.CooldownSeconds) * time.Second
				for k, t := range c.lastEmitTime {
					if time.Since(t) >= cooldown {
						delete(c.lastEmitTime, k)
					}
				}
				c.lastEmitTimeLock.Unlock()
			}

			c.emitEvent(ctx, handler, start.Context, watchAction, pod, hasProblem, problemReason)
		}
	}
}

func (c *Component) detectProblem(pod *corev1.Pod) (bool, string) {
	// Check pod phase
	switch pod.Status.Phase {
	case corev1.PodFailed:
		reason := pod.Status.Reason
		if reason == "" {
			reason = "Pod failed"
		}
		return true, reason
	case corev1.PodPending:
		// Check for scheduling issues
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionFalse {
				return true, cond.Reason
			}
		}
	}

	// Check container statuses for problems
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			reason := cs.State.Waiting.Reason
			if reason == "ImagePullBackOff" || reason == "ErrImagePull" ||
				reason == "CrashLoopBackOff" || reason == "CreateContainerError" ||
				reason == "CreateContainerConfigError" {
				return true, reason
			}
		}
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			reason := cs.State.Terminated.Reason
			if reason == "" {
				reason = fmt.Sprintf("Container exited with code %d", cs.State.Terminated.ExitCode)
			}
			return true, reason
		}
	}

	// Check init container statuses
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.State.Waiting != nil {
			reason := cs.State.Waiting.Reason
			if reason == "ImagePullBackOff" || reason == "ErrImagePull" ||
				reason == "CrashLoopBackOff" {
				return true, "Init: " + reason
			}
		}
	}

	return false, ""
}

func (c *Component) emitEvent(ctx context.Context, handler module.Handler, userCtx Context, watchAction string, pod *corev1.Pod, hasProblem bool, problemReason string) {
	podInfo := PodInfo{
		Name:      pod.Name,
		Namespace: pod.Namespace,
		Phase:     string(pod.Status.Phase),
		Ready:     c.isPodReady(pod),
		Message:   pod.Status.Message,
		Reason:    pod.Status.Reason,
		Labels:    pod.Labels,
		NodeName:  pod.Spec.NodeName,
		PodIP:     pod.Status.PodIP,
	}

	if pod.Status.StartTime != nil {
		podInfo.StartTime = pod.Status.StartTime.Format(time.RFC3339)
	}

	// Convert container statuses
	for _, cs := range pod.Status.ContainerStatuses {
		status := ContainerStatus{
			Name:         cs.Name,
			Ready:        cs.Ready,
			RestartCount: cs.RestartCount,
		}

		if cs.State.Running != nil {
			status.State = ContainerState{State: "running"}
		} else if cs.State.Waiting != nil {
			status.State = ContainerState{
				State:   "waiting",
				Reason:  cs.State.Waiting.Reason,
				Message: cs.State.Waiting.Message,
			}
		} else if cs.State.Terminated != nil {
			status.State = ContainerState{
				State:   "terminated",
				Reason:  cs.State.Terminated.Reason,
				Message: cs.State.Terminated.Message,
			}
		}

		podInfo.ContainerStatuses = append(podInfo.ContainerStatuses, status)
	}

	event := Event{
		Context:       userCtx,
		WatchAction:   watchAction,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		Pod:           podInfo,
		HasProblem:    hasProblem,
		ProblemReason: problemReason,
	}

	if err := handler(ctx, EventPort, event).Err(); err != nil {
		log.Error().Err(err).Str("pod", pod.Name).Msg("Failed to emit pod event")
	}
}

func (c *Component) isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (c *Component) updateControl(ctx context.Context, handler module.Handler, ctrl Control) {
	c.controlLock.Lock()
	c.control = ctrl
	c.controlLock.Unlock()

	handler(ctx, v1alpha1.ControlPort, ctrl)
}

func (c *Component) handleError(handler module.Handler, userCtx Context, errMsg string) module.Result {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		return handler(context.Background(), ErrorPort, Error{
			Context: userCtx,
			Error:   errMsg,
		})
	}
	return module.Fail(errors.New(errMsg))
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
				LabelSelector: "app=myapp",
			},
			Position: module.Left,
		},
		{
			Name:   EventPort,
			Label:  "Event",
			Source: true,
			Configuration: Event{
				WatchAction: "MODIFIED",
				Pod: PodInfo{
					Name:      "myapp-abc123",
					Namespace: "default",
					Phase:     "Running",
					Ready:     true,
					ContainerStatuses: []ContainerStatus{
						{
							Name:         "main",
							Ready:        true,
							RestartCount: 0,
							State:        ContainerState{State: "running"},
						},
					},
				},
				HasProblem: false,
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
