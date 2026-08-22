package workloadlist

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "workload_list"
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

// Request is the input to list workloads
type Request struct {
	Context       Context  `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Kinds         []string `json:"kinds,omitempty" title:"Kinds" description:"Optional. Resource kinds to list: Deployment, StatefulSet, DaemonSet. Leave empty to list all three."`
	Namespace     string   `json:"namespace,omitempty" title:"Namespace" description:"Optional. Filter to specific namespace. Leave empty to list across all namespaces."`
	LabelSelector string   `json:"labelSelector,omitempty" title:"Label Selector" description:"Optional. Filter by label expression. Supports: key=value, key!=value, key in (a,b), key notin (a,b), key, !key"`
}

// ResourceInfo represents a workload resource
type ResourceInfo struct {
	Name      string            `json:"name" title:"Name"`
	Namespace string            `json:"namespace" title:"Namespace"`
	Kind      string            `json:"kind" title:"Kind"`
	Labels    map[string]string `json:"labels,omitempty" title:"Labels"`
	Replicas  int32             `json:"replicas" title:"Replicas"`
	Ready     int32             `json:"ready" title:"Ready"`
	Image     string            `json:"image,omitempty" title:"Image"`
	Age       string            `json:"age,omitempty" title:"Age"`
}

// Result is the output after listing workloads
type Result struct {
	Context   Context        `json:"context,omitempty" title:"Context"`
	Resources []ResourceInfo `json:"resources" title:"Resources"`
	Count     int            `json:"count" title:"Count"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the workload list
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
		Description: "Workload List",
		Info:        "Lists Kubernetes workload resources (Deployments, StatefulSets, DaemonSets) with optional filtering by namespace and label selector. Returns resource details including name, namespace, kind, labels, replicas, and status.",
		Tags:        []string{"Kubernetes", "Workloads", "List"},
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

	// Determine which kinds to list
	kinds := req.Kinds
	if len(kinds) == 0 {
		kinds = []string{"Deployment", "StatefulSet", "DaemonSet"}
	}

	// Build list options
	listOpts := []client.ListOption{}
	if req.Namespace != "" {
		listOpts = append(listOpts, client.InNamespace(req.Namespace))
	}
	if req.LabelSelector != "" {
		selector, err := labels.Parse(req.LabelSelector)
		if err != nil {
			return c.handleError(ctx, handler, req, fmt.Errorf("invalid label selector: %v", err))
		}
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
	}

	// Initialize to empty slice (not nil) so it serializes as [] not null
	resources := make([]ResourceInfo, 0)

	for _, kind := range kinds {
		switch kind {
		case "Deployment":
			deployments := &appsv1.DeploymentList{}
			if err := k8sClient.List(ctx, deployments, listOpts...); err != nil {
				return c.handleError(ctx, handler, req, fmt.Errorf("failed to list deployments: %w", k8s.ClassifyError(err)))
			}
			for _, d := range deployments.Items {
				image := ""
				if len(d.Spec.Template.Spec.Containers) > 0 {
					image = d.Spec.Template.Spec.Containers[0].Image
				}
				replicas := int32(0)
				if d.Spec.Replicas != nil {
					replicas = *d.Spec.Replicas
				}
				resources = append(resources, ResourceInfo{
					Name:      d.Name,
					Namespace: d.Namespace,
					Kind:      "Deployment",
					Labels:    d.Labels,
					Replicas:  replicas,
					Ready:     d.Status.ReadyReplicas,
					Image:     image,
					Age:       formatAge(d.CreationTimestamp.Time),
				})
			}

		case "StatefulSet":
			statefulsets := &appsv1.StatefulSetList{}
			if err := k8sClient.List(ctx, statefulsets, listOpts...); err != nil {
				return c.handleError(ctx, handler, req, fmt.Errorf("failed to list statefulsets: %w", k8s.ClassifyError(err)))
			}
			for _, s := range statefulsets.Items {
				image := ""
				if len(s.Spec.Template.Spec.Containers) > 0 {
					image = s.Spec.Template.Spec.Containers[0].Image
				}
				replicas := int32(0)
				if s.Spec.Replicas != nil {
					replicas = *s.Spec.Replicas
				}
				resources = append(resources, ResourceInfo{
					Name:      s.Name,
					Namespace: s.Namespace,
					Kind:      "StatefulSet",
					Labels:    s.Labels,
					Replicas:  replicas,
					Ready:     s.Status.ReadyReplicas,
					Image:     image,
					Age:       formatAge(s.CreationTimestamp.Time),
				})
			}

		case "DaemonSet":
			daemonsets := &appsv1.DaemonSetList{}
			if err := k8sClient.List(ctx, daemonsets, listOpts...); err != nil {
				return c.handleError(ctx, handler, req, fmt.Errorf("failed to list daemonsets: %w", k8s.ClassifyError(err)))
			}
			for _, d := range daemonsets.Items {
				image := ""
				if len(d.Spec.Template.Spec.Containers) > 0 {
					image = d.Spec.Template.Spec.Containers[0].Image
				}
				resources = append(resources, ResourceInfo{
					Name:      d.Name,
					Namespace: d.Namespace,
					Kind:      "DaemonSet",
					Labels:    d.Labels,
					Replicas:  d.Status.DesiredNumberScheduled,
					Ready:     d.Status.NumberReady,
					Image:     image,
					Age:       formatAge(d.CreationTimestamp.Time),
				})
			}
		}
	}

	return handler(ctx, ResultPort, Result{
		Context:   req.Context,
		Resources: resources,
		Count:     len(resources),
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

func formatAge(t time.Time) string {
	d := time.Since(t)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
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
				Kinds: []string{"Deployment", "StatefulSet", "DaemonSet"},
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Resources: []ResourceInfo{
					{
						Name:      "myapp-api",
						Namespace: "production",
						Kind:      "Deployment",
						Labels:    map[string]string{"app": "myapp", "environment": "prod"},
						Replicas:  3,
						Ready:     3,
						Image:     "myapp:v1.0.0",
						Age:       "5d",
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
