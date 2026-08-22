package deploymentlist

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tiny-systems/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "deployment_list"
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

// Request is the input to list deployments
type Request struct {
	Context       Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace     string  `json:"namespace,omitempty" title:"Namespace" description:"Filter to specific namespace (empty = all namespaces)"`
	LabelSelector string  `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by labels (e.g., app=myapp)"`
}

// DeploymentCondition represents a deployment condition
type DeploymentCondition struct {
	Type    string `json:"type" title:"Type"`
	Status  string `json:"status" title:"Status"`
	Reason  string `json:"reason,omitempty" title:"Reason"`
	Message string `json:"message,omitempty" title:"Message"`
}

// ContainerInfo contains container name and image
type ContainerInfo struct {
	Name  string `json:"name" title:"Name" description:"Container name"`
	Image string `json:"image" title:"Image" description:"Container image"`
}

// DeploymentInfo contains typed deployment information
type DeploymentInfo struct {
	Name              string                `json:"name" title:"Name"`
	Namespace         string                `json:"namespace" title:"Namespace"`
	Labels            map[string]string     `json:"labels,omitempty" title:"Labels"`
	Replicas          int32                 `json:"replicas" title:"Replicas" description:"Desired replicas"`
	ReadyReplicas     int32                 `json:"readyReplicas" title:"Ready Replicas"`
	AvailableReplicas int32                 `json:"availableReplicas" title:"Available Replicas"`
	UpdatedReplicas   int32                 `json:"updatedReplicas" title:"Updated Replicas"`
	Image             string                `json:"image,omitempty" title:"Image" description:"First container image"`
	Containers        []ContainerInfo       `json:"containers,omitempty" title:"Containers" description:"All containers with names and images"`
	ImagePullSecrets  []string              `json:"imagePullSecrets,omitempty" title:"Image Pull Secrets" description:"Names of secrets used to pull images"`
	Strategy          string                `json:"strategy" title:"Strategy" description:"RollingUpdate or Recreate"`
	Conditions        []DeploymentCondition `json:"conditions,omitempty" title:"Conditions"`
	Age               string                `json:"age" title:"Age"`
	Ready             bool                  `json:"ready" title:"Ready" description:"All replicas ready"`
}

// Result is the output with deployment list
type Result struct {
	Context     Context          `json:"context,omitempty" title:"Context"`
	Deployments []DeploymentInfo `json:"deployments" title:"Deployments"`
	Count       int              `json:"count" title:"Count"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the deployment list fetcher
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
		Description: "Deployment List",
		Info:        "Lists Kubernetes Deployments with replica status, conditions, and health information.",
		Tags:        []string{"Kubernetes", "Deployments", "List"},
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

	listOpts := []client.ListOption{}
	if req.Namespace != "" {
		listOpts = append(listOpts, client.InNamespace(req.Namespace))
	}
	if req.LabelSelector != "" {
		selector, err := metav1.ParseToLabelSelector(req.LabelSelector)
		if err != nil {
			return c.handleError(ctx, handler, req, fmt.Errorf("invalid label selector: %v", err))
		}
		labelSelector, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			return c.handleError(ctx, handler, req, fmt.Errorf("invalid label selector: %v", err))
		}
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: labelSelector})
	}

	deploymentList := &appsv1.DeploymentList{}
	if err := k8sClient.List(ctx, deploymentList, listOpts...); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to list deployments: %w", k8s.ClassifyError(err)))
	}

	deployments := make([]DeploymentInfo, 0, len(deploymentList.Items))
	for _, d := range deploymentList.Items {
		info := DeploymentInfo{
			Name:              d.Name,
			Namespace:         d.Namespace,
			Labels:            d.Labels,
			Replicas:          *d.Spec.Replicas,
			ReadyReplicas:     d.Status.ReadyReplicas,
			AvailableReplicas: d.Status.AvailableReplicas,
			UpdatedReplicas:   d.Status.UpdatedReplicas,
			Strategy:          string(d.Spec.Strategy.Type),
			Age:               time.Since(d.CreationTimestamp.Time).Round(time.Second).String(),
			Ready:             d.Status.ReadyReplicas == *d.Spec.Replicas,
		}

		for _, c := range d.Spec.Template.Spec.Containers {
			info.Containers = append(info.Containers, ContainerInfo{
				Name:  c.Name,
				Image: c.Image,
			})
		}
		if len(info.Containers) > 0 {
			info.Image = info.Containers[0].Image
		}

		for _, s := range d.Spec.Template.Spec.ImagePullSecrets {
			info.ImagePullSecrets = append(info.ImagePullSecrets, s.Name)
		}

		for _, cond := range d.Status.Conditions {
			info.Conditions = append(info.Conditions, DeploymentCondition{
				Type:    string(cond.Type),
				Status:  string(cond.Status),
				Reason:  cond.Reason,
				Message: cond.Message,
			})
		}

		deployments = append(deployments, info)
	}

	return handler(ctx, ResultPort, Result{
		Context:     req.Context,
		Deployments: deployments,
		Count:       len(deployments),
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
			Name:          RequestPort,
			Label:         "Request",
			Configuration: Request{},
			Position:      module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Deployments: []DeploymentInfo{
					{
						Name:              "myapp-api",
						Namespace:         "production",
						Labels:            map[string]string{"app": "myapp"},
						Replicas:          3,
						ReadyReplicas:     3,
						AvailableReplicas: 3,
						UpdatedReplicas:   3,
						Image:             "myapp:v1.0.0",
						Containers:        []ContainerInfo{{Name: "myapp-api", Image: "myapp:v1.0.0"}},
					ImagePullSecrets:  []string{"regcred"},
						Strategy:          "RollingUpdate",
						Age:               "24h",
						Ready:             true,
					},
				},
				Count: 1,
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
