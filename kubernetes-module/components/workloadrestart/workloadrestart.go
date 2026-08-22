package workloadrestart

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "workload_restart"
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

// Request is the input to restart a workload
type Request struct {
	Context   Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace string  `json:"namespace" required:"true" title:"Namespace" description:"Kubernetes namespace"`
	Name      string  `json:"name" required:"true" title:"Name" description:"Resource name"`
	Kind      string  `json:"kind" required:"true" title:"Kind" description:"Resource kind: Deployment, StatefulSet, or DaemonSet" enum:"Deployment,StatefulSet,DaemonSet"`
}

// Result is the output with restart result
type Result struct {
	Context Context `json:"context,omitempty" title:"Context"`
	k8s.RestartResult
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the workload restarter
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
		Description: "Workload Restart",
		Info:        "Performs a rollout restart on a Deployment, StatefulSet, or DaemonSet by name. Triggers a rolling update by setting the restart annotation.",
		Tags:        []string{"Kubernetes", "Restart"},
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

	restartAnnotation := map[string]string{
		"kubectl.kubernetes.io/restartedAt": time.Now().Format(time.RFC3339),
	}

	var result *k8s.RestartResult
	var err error

	switch req.Kind {
	case "Deployment":
		result, err = c.restartDeployment(ctx, k8sClient, req.Namespace, req.Name, restartAnnotation)
	case "StatefulSet":
		result, err = c.restartStatefulSet(ctx, k8sClient, req.Namespace, req.Name, restartAnnotation)
	case "DaemonSet":
		result, err = c.restartDaemonSet(ctx, k8sClient, req.Namespace, req.Name, restartAnnotation)
	default:
		return c.handleError(ctx, handler, req, fmt.Errorf("unsupported kind: %s", req.Kind))
	}

	if err != nil {
		return c.handleError(ctx, handler, req, err)
	}

	return handler(ctx, ResultPort, Result{
		Context:       req.Context,
		RestartResult: *result,
	})
}

func (c *Component) restartDeployment(ctx context.Context, k8sClient client.Client, namespace, name string, annotation map[string]string) (*k8s.RestartResult, error) {
	deployment := &appsv1.Deployment{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, deployment); err != nil {
		return nil, fmt.Errorf("deployment not found: %w", k8s.ClassifyError(err))
	}

	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = make(map[string]string)
	}
	for k, v := range annotation {
		deployment.Spec.Template.Annotations[k] = v
	}

	if err := k8sClient.Update(ctx, deployment); err != nil {
		return nil, fmt.Errorf("failed to restart deployment: %w", k8s.ClassifyError(err))
	}

	return &k8s.RestartResult{
		Name:      name,
		Namespace: namespace,
		Success:   true,
		Message:   fmt.Sprintf("Deployment %s restarted", name),
	}, nil
}

func (c *Component) restartStatefulSet(ctx context.Context, k8sClient client.Client, namespace, name string, annotation map[string]string) (*k8s.RestartResult, error) {
	sts := &appsv1.StatefulSet{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, sts); err != nil {
		return nil, fmt.Errorf("statefulset not found: %w", k8s.ClassifyError(err))
	}

	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = make(map[string]string)
	}
	for k, v := range annotation {
		sts.Spec.Template.Annotations[k] = v
	}

	if err := k8sClient.Update(ctx, sts); err != nil {
		return nil, fmt.Errorf("failed to restart statefulset: %w", k8s.ClassifyError(err))
	}

	return &k8s.RestartResult{
		Name:      name,
		Namespace: namespace,
		Success:   true,
		Message:   fmt.Sprintf("StatefulSet %s restarted", name),
	}, nil
}

func (c *Component) restartDaemonSet(ctx context.Context, k8sClient client.Client, namespace, name string, annotation map[string]string) (*k8s.RestartResult, error) {
	ds := &appsv1.DaemonSet{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, ds); err != nil {
		return nil, fmt.Errorf("daemonset not found: %w", k8s.ClassifyError(err))
	}

	if ds.Spec.Template.Annotations == nil {
		ds.Spec.Template.Annotations = make(map[string]string)
	}
	for k, v := range annotation {
		ds.Spec.Template.Annotations[k] = v
	}

	if err := k8sClient.Update(ctx, ds); err != nil {
		return nil, fmt.Errorf("failed to restart daemonset: %w", k8s.ClassifyError(err))
	}

	return &k8s.RestartResult{
		Name:      name,
		Namespace: namespace,
		Success:   true,
		Message:   fmt.Sprintf("DaemonSet %s restarted", name),
	}, nil
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
				Kind: "Deployment",
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

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.ClientAware     = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
