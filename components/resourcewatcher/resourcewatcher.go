package resourcewatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "resource_watcher"

	StartPort  = "start"
	EventPort  = "event"
	ErrorPort  = "error"
	StatusPort = "status"
)

// EventType represents the type of watch event
type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
)

// Settings configures the watcher behavior
type Settings struct {
	// EnableErrorPort enables output of errors to error port instead of returning them
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
	// EnableStatusPort enables output of status updates
	EnableStatusPort bool `json:"enableStatusPort" title:"Enable Status Port" description:"Output status updates when watching starts/stops"`
}

// Start initiates watching with the given configuration
type Start struct {
	Context any `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through to events"`

	// Resource identification
	APIVersion string `json:"apiVersion" required:"true" title:"API Version" description:"API version of the resource (e.g., v1, apps/v1, networking.k8s.io/v1)"`
	Kind       string `json:"kind" required:"true" title:"Kind" description:"Kind of the resource (e.g., Pod, Deployment, Service)"`

	// Scope
	Namespace     string `json:"namespace,omitempty" title:"Namespace" description:"Namespace to watch. Leave empty for all namespaces or cluster-scoped resources"`
	AllNamespaces bool   `json:"allNamespaces,omitempty" title:"All Namespaces" description:"Watch across all namespaces"`

	// Filtering
	LabelSelector string `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by labels (e.g., app=nginx,env=prod)"`
	FieldSelector string `json:"fieldSelector,omitempty" title:"Field Selector" description:"Filter by fields (e.g., metadata.name=my-pod)"`

	// Event filtering
	EventTypes []EventType `json:"eventTypes,omitempty" title:"Event Types" description:"Filter to specific event types. Empty means all types (ADDED, MODIFIED, DELETED)"`

	// Behavior
	IncludeInitialList bool `json:"includeInitialList,omitempty" title:"Include Initial List" description:"Emit ADDED events for existing resources when starting"`
}

// Event is emitted for each resource change
type Event struct {
	Context any `json:"context,omitempty" title:"Context" description:"Context passed from start message"`

	// Event metadata
	EventType EventType `json:"eventType" title:"Event Type" description:"Type of event: ADDED, MODIFIED, DELETED"`
	Timestamp string    `json:"timestamp" title:"Timestamp" description:"When the event occurred"`

	// Resource identification
	APIVersion string `json:"apiVersion" title:"API Version"`
	Kind       string `json:"kind" title:"Kind"`
	Name       string `json:"name" title:"Name"`
	Namespace  string `json:"namespace,omitempty" title:"Namespace"`
	UID        string `json:"uid" title:"UID" description:"Unique identifier of the resource"`

	// Resource data
	Resource map[string]any `json:"resource" title:"Resource" description:"Full resource object"`

	// For MODIFIED events, include what changed
	OldResource map[string]any `json:"oldResource,omitempty" title:"Old Resource" description:"Previous resource state (for MODIFIED events)"`
}

// Status provides the current watcher status
type Status struct {
	Watching   bool   `json:"watching" title:"Watching" description:"Whether the watcher is active"`
	APIVersion string `json:"apiVersion,omitempty" title:"API Version"`
	Kind       string `json:"kind,omitempty" title:"Kind"`
	Namespace  string `json:"namespace,omitempty" title:"Namespace"`
	Error      string `json:"error,omitempty" title:"Error" description:"Last error if any"`
}

// Error output for error port
type Error struct {
	Context any    `json:"context,omitempty" title:"Context"`
	Error   string `json:"error" title:"Error" description:"Error message"`
}

// Component implements the ResourceWatcher
type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	k8sClient     client.WithWatch
	k8sNamespace  string
	k8sClientLock sync.RWMutex

	// Store last known state for detecting changes
	resourceCache     map[string]map[string]any
	resourceCacheLock sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings:      Settings{},
		resourceCache: make(map[string]map[string]any),
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Resource Watcher",
		Info:        "Watch Kubernetes resources for changes. Emits events when resources are created, modified, or deleted.",
		Tags:        []string{"Kubernetes", "Watch", "Events", "Trigger"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	switch port {
	case v1alpha1.ClientPort:
		// Receive K8s client from runtime
		if k8sProvider, ok := msg.(module.K8sClient); ok {
			c.k8sClientLock.Lock()
			c.k8sClient = k8sProvider.GetK8sClient()
			c.k8sNamespace = k8sProvider.GetNamespace()
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
		return c.startWatching(ctx, handler, in)
	}

	return fmt.Errorf("unknown port: %s", port)
}

func (c *Component) startWatching(ctx context.Context, handler module.Handler, start Start) error {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	c.k8sClientLock.RUnlock()

	if k8sClient == nil {
		return c.handleError(handler, start.Context, "K8s client not available")
	}

	// Parse GVK
	gv, err := schema.ParseGroupVersion(start.APIVersion)
	if err != nil {
		return c.handleError(handler, start.Context, fmt.Sprintf("invalid API version: %v", err))
	}
	gvk := gv.WithKind(start.Kind)

	// Build list options
	listOpts := &client.ListOptions{}
	if start.Namespace != "" && !start.AllNamespaces {
		listOpts.Namespace = start.Namespace
	}
	if start.LabelSelector != "" {
		selector, err := metav1.ParseToLabelSelector(start.LabelSelector)
		if err != nil {
			return c.handleError(handler, start.Context, fmt.Sprintf("invalid label selector: %v", err))
		}
		labelSelector, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			return c.handleError(handler, start.Context, fmt.Sprintf("invalid label selector: %v", err))
		}
		listOpts.LabelSelector = labelSelector
	}

	// Create unstructured list for the resource type
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk)

	// Emit status if enabled
	c.emitStatus(handler, ctx, Status{
		Watching:   true,
		APIVersion: start.APIVersion,
		Kind:       start.Kind,
		Namespace:  start.Namespace,
	})

	// Start watching in goroutine - will stop when ctx is cancelled
	go func() {
		defer func() {
			// Emit stopped status if enabled
			c.emitStatus(handler, context.Background(), Status{
				Watching: false,
			})
		}()

		// Build event type filter map
		eventFilter := make(map[EventType]bool)
		if len(start.EventTypes) > 0 {
			for _, et := range start.EventTypes {
				eventFilter[et] = true
			}
		}

		// Start the watch
		watcher, err := k8sClient.Watch(ctx, list, listOpts)
		if err != nil {
			log.Error().Err(err).Msg("Failed to start watch")
			_ = c.handleError(handler, start.Context, fmt.Sprintf("failed to start watch: %v", err))
			return
		}
		defer watcher.Stop()

		// If includeInitialList, first list existing resources
		if start.IncludeInitialList {
			existingList := &unstructured.UnstructuredList{}
			existingList.SetGroupVersionKind(gvk)
			if err := k8sClient.List(ctx, existingList, listOpts); err != nil {
				log.Error().Err(err).Msg("Failed to list existing resources")
			} else {
				for _, item := range existingList.Items {
					if shouldEmitEvent(EventAdded, eventFilter) {
						c.emitEvent(handler, start.Context, EventAdded, &item, nil)
					}
					// Cache the resource
					c.cacheResource(&item)
				}
			}
		}

		// Process watch events
		for {
			select {
			case <-ctx.Done():
				log.Debug().Msg("Watch context cancelled")
				return
			case event, ok := <-watcher.ResultChan():
				if !ok {
					log.Debug().Msg("Watch channel closed, restarting...")
					// Channel closed, try to restart
					time.Sleep(time.Second)
					watcher, err = k8sClient.Watch(ctx, list, listOpts)
					if err != nil {
						log.Error().Err(err).Msg("Failed to restart watch")
						return
					}
					continue
				}

				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}

				var eventType EventType
				switch event.Type {
				case watch.Added:
					eventType = EventAdded
				case watch.Modified:
					eventType = EventModified
				case watch.Deleted:
					eventType = EventDeleted
				default:
					continue
				}

				if shouldEmitEvent(eventType, eventFilter) {
					// Get old resource from cache for MODIFIED events
					var oldResource map[string]any
					if eventType == EventModified {
						oldResource = c.getCachedResource(obj)
					}

					c.emitEvent(handler, start.Context, eventType, obj, oldResource)
				}

				// Update cache
				if eventType == EventDeleted {
					c.removeCachedResource(obj)
				} else {
					c.cacheResource(obj)
				}
			}
		}
	}()

	return nil
}

func (c *Component) emitEvent(handler module.Handler, userCtx any, eventType EventType, obj *unstructured.Unstructured, oldResource map[string]any) {
	event := Event{
		Context:     userCtx,
		EventType:   eventType,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		APIVersion:  obj.GetAPIVersion(),
		Kind:        obj.GetKind(),
		Name:        obj.GetName(),
		Namespace:   obj.GetNamespace(),
		UID:         string(obj.GetUID()),
		Resource:    obj.Object,
		OldResource: oldResource,
	}

	if result := handler(context.Background(), EventPort, event); result != nil {
		if err, ok := result.(error); ok {
			log.Error().Err(err).Str("name", obj.GetName()).Msg("Failed to emit event")
		}
	}
}

func (c *Component) handleError(handler module.Handler, userCtx any, errMsg string) error {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		_ = handler(context.Background(), ErrorPort, Error{
			Context: userCtx,
			Error:   errMsg,
		})
		return nil
	}
	return errors.New(errMsg)
}

func (c *Component) emitStatus(handler module.Handler, ctx context.Context, status Status) {
	c.settingsLock.RLock()
	enableStatusPort := c.settings.EnableStatusPort
	c.settingsLock.RUnlock()

	if enableStatusPort {
		_ = handler(ctx, StatusPort, status)
	}
}

func (c *Component) cacheResource(obj *unstructured.Unstructured) {
	key := resourceKey(obj)
	c.resourceCacheLock.Lock()
	defer c.resourceCacheLock.Unlock()

	// Deep copy the object
	c.resourceCache[key] = obj.DeepCopy().Object
}

func (c *Component) getCachedResource(obj *unstructured.Unstructured) map[string]any {
	key := resourceKey(obj)
	c.resourceCacheLock.RLock()
	defer c.resourceCacheLock.RUnlock()

	if cached, ok := c.resourceCache[key]; ok {
		return cached
	}
	return nil
}

func (c *Component) removeCachedResource(obj *unstructured.Unstructured) {
	key := resourceKey(obj)
	c.resourceCacheLock.Lock()
	defer c.resourceCacheLock.Unlock()

	delete(c.resourceCache, key)
}

func resourceKey(obj *unstructured.Unstructured) string {
	return fmt.Sprintf("%s/%s/%s/%s", obj.GetAPIVersion(), obj.GetKind(), obj.GetNamespace(), obj.GetName())
}

func shouldEmitEvent(eventType EventType, filter map[EventType]bool) bool {
	if len(filter) == 0 {
		return true
	}
	return filter[eventType]
}

func (c *Component) Ports() []module.Port {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	enableStatusPort := c.settings.EnableStatusPort
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
				APIVersion:         "v1",
				Kind:               "Pod",
				IncludeInitialList: true,
			},
			Position: module.Left,
		},
		{
			Name:          EventPort,
			Label:         "Event",
			Source:        true,
			Configuration: Event{},
			Position:      module.Right,
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

	if enableStatusPort {
		ports = append(ports, module.Port{
			Name:          StatusPort,
			Label:         "Status",
			Source:        true,
			Configuration: Status{},
			Position:      module.Right,
		})
	}

	return ports
}

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
