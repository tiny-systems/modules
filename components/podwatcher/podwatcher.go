package podwatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
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
	Status        string `json:"status" title:"Status" readonly:"true"`
	Namespace     string `json:"namespace,omitempty" title:"Namespace" readonly:"true"`
	LabelSelector string `json:"labelSelector,omitempty" title:"Label Selector" readonly:"true"`
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
	LabelSelector string `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by labels (e.g., app=nginx,env=prod)"`

	// Options
	ProblemsOnly bool `json:"problemsOnly,omitempty" title:"Problems Only" description:"Only emit events for pods with issues (not Running/Succeeded)"`
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

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	switch port {
	case v1alpha1.ClientPort:
		if k8sProvider, ok := msg.(module.K8sClient); ok {
			c.k8sClientLock.Lock()
			c.k8sClient = k8sProvider.GetK8sClient()
			c.k8sClientLock.Unlock()
			log.Debug().Msg("K8s client received")
		}
		return nil

	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings message")
		}
		c.settingsLock.Lock()
		c.settings = in
		c.settingsLock.Unlock()
		return nil

	case StartPort:
		in, ok := msg.(Start)
		if !ok {
			return fmt.Errorf("invalid start message")
		}

		if !utils.IsLeader(ctx) {
			return nil
		}

		// If already running, wait for it to stop
		if done := c.getWatchDone(); done != nil {
			<-done
			return nil
		}

		return c.runWatch(ctx, handler, in)
	}

	return fmt.Errorf("unknown port: %s", port)
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

	// Create watch context
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	c.setCancelFunc(watchCancel)
	defer c.setCancelFunc(nil)

	// Create pod list for watching
	podList := &corev1.PodList{}

	// Start the watch
	watcher, err := k8sClient.Watch(watchCtx, podList, listOpts)
	if err != nil {
		c.updateControl(ctx, handler, Control{Status: fmt.Sprintf("Error: %v", err)})
		return fmt.Errorf("failed to start watch: %v", err)
	}

	// Start watch loop in goroutine
	go c.watchLoop(watchCtx, handler, start, watcher, podList, listOpts)

	// Update control to show watching status
	c.updateControl(ctx, handler, Control{
		Status:        "Watching",
		Namespace:     start.Namespace,
		LabelSelector: start.LabelSelector,
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

			hasProblem, problemReason := c.detectProblem(pod)

			// Filter to problems only if requested
			if start.ProblemsOnly && !hasProblem {
				continue
			}

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

	if result := handler(ctx, EventPort, event); result != nil {
		if err, ok := result.(error); ok {
			log.Error().Err(err).Str("pod", pod.Name).Msg("Failed to emit pod event")
		}
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

	_ = handler(ctx, v1alpha1.ControlPort, ctrl)
}

func (c *Component) handleError(handler module.Handler, userCtx Context, errMsg string) any {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		return handler(context.Background(), ErrorPort, Error{
			Context: userCtx,
			Error:   errMsg,
		})
	}
	return errors.New(errMsg)
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

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
