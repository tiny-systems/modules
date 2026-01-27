package restartdeployment

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
	ComponentName = "deployment_restart"
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

// Request is the input to restart a deployment
type Request struct {
	Context    Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace  string  `json:"namespace" required:"true" title:"Namespace" description:"Kubernetes namespace"`
	Deployment string  `json:"deployment" required:"true" title:"Deployment" description:"Deployment name or app label"`
}

// Result is the output with restart result
type Result struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	k8s.RestartResult
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the deployment restarter
type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	k8sClient     client.Client
	k8sNamespace  string
	k8sClientLock sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Restart Deployment",
		Info:        "Performs a rollout restart on a deployment. Can look up deployments by name or app label.",
		Tags:        []string{"Kubernetes", "ChatOps", "Deployment", "Restart"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	switch port {
	case v1alpha1.ClientPort:
		if k8sProvider, ok := msg.(module.K8sClient); ok {
			c.k8sClientLock.Lock()
			c.k8sClient = k8sProvider.GetK8sClient()
			c.k8sNamespace = k8sProvider.GetNamespace()
			c.k8sClientLock.Unlock()
		}
		return nil

	case v1alpha1.SettingsPort:
		in, ok := msg.(Settings)
		if !ok {
			return fmt.Errorf("invalid settings")
		}
		c.settingsLock.Lock()
		c.settings = in
		c.settingsLock.Unlock()
		return nil

	case RequestPort:
		in, ok := msg.(Request)
		if !ok {
			return fmt.Errorf("invalid request")
		}
		return c.handleRequest(ctx, handler, in)
	}

	return fmt.Errorf("unknown port: %s", port)
}

func (c *Component) handleRequest(ctx context.Context, handler module.Handler, req Request) any {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	defaultNS := c.k8sNamespace
	c.k8sClientLock.RUnlock()

	if k8sClient == nil {
		return c.handleError(ctx, handler, req, "K8s client not available")
	}

	namespace := req.Namespace
	if namespace == "" {
		namespace = defaultNS
	}

	restartResult, err := k8s.RestartDeployment(ctx, k8sClient, namespace, req.Deployment)
	if err != nil {
		return c.handleError(ctx, handler, req, err.Error())
	}

	return handler(ctx, ResultPort, Result{
		Context:       req.Context,
		RestartResult: *restartResult,
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
				Deployment: "myapp",
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				RestartResult: k8s.RestartResult{
					Name:    "myapp",
					Success: true,
					Message: "Deployment myapp restarted",
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

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
