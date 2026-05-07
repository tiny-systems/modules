package configmappatch

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "configmap_patch"
	RequestPort   = "request"
	ResultPort    = "result"
	ErrorPort     = "error"
)

// Context type alias for schema generation
type Context any

// Settings configures the component
type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

// Request is the input to patch a ConfigMap key
type Request struct {
	Context   Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace string  `json:"namespace" required:"true" title:"Namespace" description:"ConfigMap namespace"`
	Name      string  `json:"name" required:"true" title:"Name" description:"ConfigMap name"`
	Operation string  `json:"operation" required:"true" title:"Operation" description:"Operation to perform" enum:"set,remove" enumTitles:"Set,Remove"`
	Key       string  `json:"key" required:"true" title:"Key" description:"Data key to set or remove"`
	Value     string  `json:"value,omitempty" title:"Value" description:"Value to set (ignored for remove)"`
}

// Result is the output after patching
type Result struct {
	Context   Context `json:"context,omitempty" title:"Context"`
	Namespace string  `json:"namespace" title:"Namespace"`
	Name      string  `json:"name" title:"Name"`
	Key       string  `json:"key" title:"Key"`
	Operation string  `json:"operation" title:"Operation"`
	Success   bool    `json:"success" title:"Success"`
	Message   string  `json:"message" title:"Message"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the configmap patch
type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	k8sClient     client.Client
	k8sClientLock sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "ConfigMap Patch",
		Info:        "Set or remove a key in a Kubernetes ConfigMap. On set: creates the ConfigMap if it doesn't exist, or upserts the key. On remove: deletes the key from the ConfigMap data.",
		Tags:        []string{"Kubernetes", "ConfigMap", "Configuration"},
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
}

// OnSettings stores the component settings.
func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settingsLock.Lock()
	c.settings = in
	c.settingsLock.Unlock()
	return nil
}

// Handle dispatches business ports. System ports go through capabilities.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	if port != RequestPort {
		return fmt.Errorf("unknown port: %s", port)
	}
	in, ok := msg.(Request)
	if !ok {
		return fmt.Errorf("invalid request")
	}
	return c.handleRequest(ctx, handler, in)
}

func (c *Component) handleRequest(ctx context.Context, handler module.Handler, req Request) any {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	c.k8sClientLock.RUnlock()

	if k8sClient == nil {
		return c.handleError(ctx, handler, req, "K8s client not available")
	}

	switch req.Operation {
	case "set":
		return c.handleSet(ctx, handler, k8sClient, req)
	case "remove":
		return c.handleRemove(ctx, handler, k8sClient, req)
	default:
		return c.handleError(ctx, handler, req, fmt.Sprintf("unknown operation: %s", req.Operation))
	}
}

func (c *Component) handleSet(ctx context.Context, handler module.Handler, k8sClient client.Client, req Request) any {
	cm := &corev1.ConfigMap{}
	err := k8sClient.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, cm)

	if k8serrors.IsNotFound(err) {
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: req.Namespace,
				Name:      req.Name,
			},
			Data: map[string]string{req.Key: req.Value},
		}
		if err := k8sClient.Create(ctx, cm); err != nil {
			return c.handleError(ctx, handler, req, fmt.Sprintf("failed to create configmap: %v", err))
		}
		return handler(ctx, ResultPort, Result{
			Context:   req.Context,
			Namespace: req.Namespace,
			Name:      req.Name,
			Key:       req.Key,
			Operation: req.Operation,
			Success:   true,
			Message:   fmt.Sprintf("ConfigMap %s/%s created with key %s", req.Namespace, req.Name, req.Key),
		})
	}

	if err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to get configmap: %v", err))
	}

	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}
	cm.Data[req.Key] = req.Value

	if err := k8sClient.Update(ctx, cm); err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to update configmap: %v", err))
	}

	return handler(ctx, ResultPort, Result{
		Context:   req.Context,
		Namespace: req.Namespace,
		Name:      req.Name,
		Key:       req.Key,
		Operation: req.Operation,
		Success:   true,
		Message:   fmt.Sprintf("Key %s set in ConfigMap %s/%s", req.Key, req.Namespace, req.Name),
	})
}

func (c *Component) handleRemove(ctx context.Context, handler module.Handler, k8sClient client.Client, req Request) any {
	cm := &corev1.ConfigMap{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, cm); err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to get configmap: %v", err))
	}

	if cm.Data == nil {
		return handler(ctx, ResultPort, Result{
			Context:   req.Context,
			Namespace: req.Namespace,
			Name:      req.Name,
			Key:       req.Key,
			Operation: req.Operation,
			Success:   true,
			Message:   fmt.Sprintf("Key %s not found in ConfigMap %s/%s (no-op)", req.Key, req.Namespace, req.Name),
		})
	}

	delete(cm.Data, req.Key)

	if err := k8sClient.Update(ctx, cm); err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to update configmap: %v", err))
	}

	return handler(ctx, ResultPort, Result{
		Context:   req.Context,
		Namespace: req.Namespace,
		Name:      req.Name,
		Key:       req.Key,
		Operation: req.Operation,
		Success:   true,
		Message:   fmt.Sprintf("Key %s removed from ConfigMap %s/%s", req.Key, req.Namespace, req.Name),
	})
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, req Request, errMsg string) any {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		return handler(ctx, ErrorPort, Error{
			Context: req.Context,
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
			Name:  RequestPort,
			Label: "Request",
			Configuration: Request{
				Namespace: "default",
				Name:      "my-configmap",
				Operation: "set",
				Key:       "my-key",
				Value:     "my-value",
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Namespace: "default",
				Name:      "my-configmap",
				Key:       "my-key",
				Operation: "set",
				Success:   true,
				Message:   "Key my-key set in ConfigMap default/my-configmap",
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

func init() {
	registry.Register(&Component{})
}
