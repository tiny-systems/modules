package poddelete

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "pod_delete"
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

// Request is the input to delete a pod
type Request struct {
	Context       Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace     string  `json:"namespace" required:"true" title:"Namespace" description:"Pod namespace"`
	Name          string  `json:"name" required:"true" title:"Name" description:"Pod name"`
	GracePeriod   int64   `json:"gracePeriod,omitempty" title:"Grace Period" description:"Seconds to wait before force kill (0 = immediate)"`
}

// Result is the output after pod deletion
type Result struct {
	Context   Context `json:"context,omitempty" title:"Context"`
	Name      string  `json:"name" title:"Name" description:"Deleted pod name"`
	Namespace string  `json:"namespace" title:"Namespace"`
	Success   bool    `json:"success" title:"Success"`
	Message   string  `json:"message" title:"Message"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the pod delete
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
		Description: "Pod Delete",
		Info:        "Delete a Kubernetes Pod by name. Useful for forcing pod restart or cleanup.",
		Tags:        []string{"Kubernetes", "Pods", "Delete"},
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
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	return c.handleRequest(ctx, handler, in)
}

func (c *Component) handleRequest(ctx context.Context, handler module.Handler, req Request) module.Result {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	c.k8sClientLock.RUnlock()

	if k8sClient == nil {
		return c.handleError(ctx, handler, req, module.Retryable(errors.New("K8s client not available")))
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: req.Namespace,
		},
	}

	deleteOpts := []client.DeleteOption{}
	if req.GracePeriod >= 0 {
		deleteOpts = append(deleteOpts, client.GracePeriodSeconds(req.GracePeriod))
	}

	if err := k8sClient.Delete(ctx, pod, deleteOpts...); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to delete pod: %w", k8s.ClassifyError(err)))
	}

	return handler(ctx, ResultPort, Result{
		Context:   req.Context,
		Name:      req.Name,
		Namespace: req.Namespace,
		Success:   true,
		Message:   fmt.Sprintf("Pod %s/%s deleted", req.Namespace, req.Name),
	})
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, req Request, err error) module.Result {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		return handler(ctx, ErrorPort, Error{
			Context: req.Context,
			Error:   err.Error(),
		})
	}
	return module.Fail(err)
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
				Name:      "my-pod",
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Name:      "my-pod",
				Namespace: "default",
				Success:   true,
				Message:   "Pod default/my-pod deleted",
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
