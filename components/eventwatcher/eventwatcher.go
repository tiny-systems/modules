package eventwatcher

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
	ComponentName = "event_watch"

	StartPort = "start"
	EventPort = "event"
	ErrorPort = "error"
)

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
	Namespace     string `json:"namespace,omitempty" title:"Namespace" description:"Namespace to watch. Leave empty for all namespaces"`
	AllNamespaces bool   `json:"allNamespaces,omitempty" title:"All Namespaces" description:"Watch across all namespaces"`

	// Filtering
	FieldSelector string `json:"fieldSelector,omitempty" title:"Field Selector" description:"Filter events (e.g., type=Warning, involvedObject.kind=Pod)"`

	// Event type filter
	WarningsOnly bool `json:"warningsOnly,omitempty" title:"Warnings Only" description:"Only emit Warning events (not Normal)"`
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
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{},
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

		return c.startWatching(ctx, handler, in)
	}

	return fmt.Errorf("unknown port: %s", port)
}

func (c *Component) startWatching(ctx context.Context, handler module.Handler, start Start) any {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	c.k8sClientLock.RUnlock()

	if k8sClient == nil {
		return c.handleError(handler, start.Context, "K8s client not available")
	}

	// Build list options
	listOpts := &client.ListOptions{}
	if start.Namespace != "" && !start.AllNamespaces {
		listOpts.Namespace = start.Namespace
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

	// Create event list for watching
	eventList := &corev1.EventList{}

	// Start the watch
	watcher, err := k8sClient.Watch(ctx, eventList, listOpts)
	if err != nil {
		return c.handleError(handler, start.Context, fmt.Sprintf("failed to start watch: %v", err))
	}
	defer watcher.Stop()

	log.Info().Str("namespace", start.Namespace).Bool("allNamespaces", start.AllNamespaces).Msg("Event watcher started")

	// Process watch events
	for {
		select {
		case <-ctx.Done():
			log.Debug().Msg("Event watch context cancelled")
			return ctx.Err()
		case watchEvent, ok := <-watcher.ResultChan():
			if !ok {
				log.Debug().Msg("Event watch channel closed, restarting...")
				time.Sleep(time.Second)
				watcher, err = k8sClient.Watch(ctx, eventList, listOpts)
				if err != nil {
					log.Error().Err(err).Msg("Failed to restart event watch")
					return err
				}
				continue
			}

			k8sEvent, ok := watchEvent.Object.(*corev1.Event)
			if !ok {
				continue
			}

			// Filter warnings if requested
			if start.WarningsOnly && k8sEvent.Type != "Warning" {
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

			c.emitEvent(ctx, handler, start.Context, watchAction, k8sEvent)
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
		Count: k8sEvent.Count,
		Source: k8sEvent.Source.Component,
	}

	if !k8sEvent.FirstTimestamp.IsZero() {
		event.FirstTimestamp = k8sEvent.FirstTimestamp.Format(time.RFC3339)
	}
	if !k8sEvent.LastTimestamp.IsZero() {
		event.LastTimestamp = k8sEvent.LastTimestamp.Format(time.RFC3339)
	}

	if result := handler(ctx, EventPort, event); result != nil {
		if err, ok := result.(error); ok {
			log.Error().Err(err).Str("reason", k8sEvent.Reason).Msg("Failed to emit event")
		}
	}
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

	ports := []module.Port{
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
				AllNamespaces: true,
				WarningsOnly:  true,
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

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
