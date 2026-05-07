package podstatus

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "pod_status_get"
	RequestPort   = "request"
	StatusPort    = "status"
	ErrorPort     = "error"
)

// Context type alias for schema generation
type Context any

// Settings configures the component
type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

// Request is the input to get pod status
type Request struct {
	Context       Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace     string  `json:"namespace,omitempty" title:"Namespace" description:"Kubernetes namespace. Leave empty to search all namespaces."`
	LabelSelector string  `json:"labelSelector" required:"true" minLength:"3" title:"Label Selector" description:"Filter pods by labels (e.g., app=myapp). Required to avoid listing all pods."`
}

// Status is the output with pod information
type Status struct {
	Context Context `json:"context,omitempty" title:"Context"`
	k8s.PodStatus
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the pod status checker
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
		Description: "Pod Status",
		Info:        "Get status of pods matching a label selector. Returns pod count, health summary, and individual pod details.",
		Tags:        []string{"Kubernetes", "ChatOps", "Pods", "Status"},
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

	// Empty namespace = all namespaces
	podStatus, err := k8s.GetPodStatus(ctx, k8sClient, req.Namespace, req.LabelSelector)
	if err != nil {
		return c.handleError(ctx, handler, req, err.Error())
	}

	return handler(ctx, StatusPort, Status{
		Context:   req.Context,
		PodStatus: *podStatus,
	})
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, req Request, errMsg string) any {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		// Return handler result to propagate responses back through the call chain
		// (critical for blocking I/O patterns like HTTP Server)
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
				LabelSelector: "app=myapp",
			},
			Position: module.Left,
		},
		{
			Name:   StatusPort,
			Label:  "Status",
			Source: true,
			Configuration: Status{
				PodStatus: k8s.PodStatus{
					Total:   2,
					Running: 2,
					Healthy: true,
					Summary: "2/2 pods running",
				},
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
