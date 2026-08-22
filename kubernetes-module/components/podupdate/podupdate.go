package podupdate

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "pod_update"
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

// Request is the input to update a pod
type Request struct {
	Context     Context           `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace   string            `json:"namespace" required:"true" title:"Namespace" description:"Pod namespace"`
	Name        string            `json:"name" required:"true" title:"Name" description:"Pod name"`
	Labels      map[string]string `json:"labels,omitempty" configurable:"true" title:"Labels" description:"Labels to set (merged with existing)"`
	Annotations map[string]string `json:"annotations,omitempty" configurable:"true" title:"Annotations" description:"Annotations to set (merged with existing)"`
}

// Result is the output after pod update
type Result struct {
	Context     Context           `json:"context,omitempty" title:"Context"`
	Name        string            `json:"name" title:"Name"`
	Namespace   string            `json:"namespace" title:"Namespace"`
	Labels      map[string]string `json:"labels,omitempty" title:"Labels" description:"Final labels"`
	Annotations map[string]string `json:"annotations,omitempty" title:"Annotations" description:"Final annotations"`
	Success     bool              `json:"success" title:"Success"`
	Message     string            `json:"message" title:"Message"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the pod update
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
		Description: "Pod Update",
		Info:        "Update a Kubernetes Pod's labels and annotations. Note: Pod spec cannot be modified after creation.",
		Tags:        []string{"Kubernetes", "Pods", "Update"},
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

	// Get current pod
	pod := &corev1.Pod{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, pod); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("pod not found: %w", k8s.ClassifyError(err)))
	}

	// Merge labels
	if len(req.Labels) > 0 {
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		for k, v := range req.Labels {
			pod.Labels[k] = v
		}
	}

	// Merge annotations
	if len(req.Annotations) > 0 {
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		for k, v := range req.Annotations {
			pod.Annotations[k] = v
		}
	}

	// Update pod
	if err := k8sClient.Update(ctx, pod); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to update pod: %w", k8s.ClassifyError(err)))
	}

	return handler(ctx, ResultPort, Result{
		Context:     req.Context,
		Name:        pod.Name,
		Namespace:   pod.Namespace,
		Labels:      pod.Labels,
		Annotations: pod.Annotations,
		Success:     true,
		Message:     fmt.Sprintf("Pod %s/%s updated", req.Namespace, req.Name),
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
				Labels:    map[string]string{"env": "test"},
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
				Labels:    map[string]string{"app": "myapp", "env": "test"},
				Success:   true,
				Message:   "Pod default/my-pod updated",
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
