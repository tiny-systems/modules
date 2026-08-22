package deploymentscale

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "deployment_scale"
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

// Request is the input to scale a deployment
type Request struct {
	Context   Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace string  `json:"namespace" required:"true" title:"Namespace" description:"Deployment namespace"`
	Name      string  `json:"name" required:"true" title:"Name" description:"Deployment name"`
	Replicas  int32   `json:"replicas" required:"true" title:"Replicas" description:"Desired replica count" minimum:"0"`
}

// Result is the output after scaling
type Result struct {
	Context          Context `json:"context,omitempty" title:"Context"`
	Name             string  `json:"name" title:"Name"`
	Namespace        string  `json:"namespace" title:"Namespace"`
	PreviousReplicas int32   `json:"previousReplicas" title:"Previous Replicas"`
	DesiredReplicas  int32   `json:"desiredReplicas" title:"Desired Replicas"`
	Success          bool    `json:"success" title:"Success"`
	Message          string  `json:"message" title:"Message"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the deployment scale
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
		Description: "Deployment Scale",
		Info:        "Scale a Kubernetes Deployment to a specified replica count.",
		Tags:        []string{"Kubernetes", "Deployments", "Scale"},
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

	// Get current deployment
	deployment := &appsv1.Deployment{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, deployment); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("deployment not found: %w", k8s.ClassifyError(err)))
	}

	previousReplicas := *deployment.Spec.Replicas

	// Scale
	deployment.Spec.Replicas = &req.Replicas
	if err := k8sClient.Update(ctx, deployment); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to scale deployment: %w", k8s.ClassifyError(err)))
	}

	return handler(ctx, ResultPort, Result{
		Context:          req.Context,
		Name:             req.Name,
		Namespace:        req.Namespace,
		PreviousReplicas: previousReplicas,
		DesiredReplicas:  req.Replicas,
		Success:          true,
		Message:          fmt.Sprintf("Deployment %s/%s scaled from %d to %d", req.Namespace, req.Name, previousReplicas, req.Replicas),
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
				Name:      "my-deployment",
				Replicas:  3,
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Name:             "my-deployment",
				Namespace:        "default",
				PreviousReplicas: 1,
				DesiredReplicas:  3,
				Success:          true,
				Message:          "Deployment default/my-deployment scaled from 1 to 3",
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
