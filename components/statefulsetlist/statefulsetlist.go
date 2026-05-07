package statefulsetlist

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "statefulset_list"
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

// Request is the input to list statefulsets
type Request struct {
	Context       Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace     string  `json:"namespace,omitempty" title:"Namespace" description:"Filter to specific namespace (empty = all namespaces)"`
	LabelSelector string  `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by labels (e.g., app=myapp)"`
}

// StatefulSetInfo contains typed statefulset information
type StatefulSetInfo struct {
	Name            string            `json:"name" title:"Name"`
	Namespace       string            `json:"namespace" title:"Namespace"`
	Labels          map[string]string `json:"labels,omitempty" title:"Labels"`
	Replicas        int32             `json:"replicas" title:"Replicas" description:"Desired replicas"`
	ReadyReplicas   int32             `json:"readyReplicas" title:"Ready Replicas"`
	CurrentReplicas int32             `json:"currentReplicas" title:"Current Replicas"`
	UpdatedReplicas int32             `json:"updatedReplicas" title:"Updated Replicas"`
	CurrentRevision string            `json:"currentRevision,omitempty" title:"Current Revision"`
	UpdateRevision  string            `json:"updateRevision,omitempty" title:"Update Revision"`
	Image           string            `json:"image,omitempty" title:"Image" description:"First container image"`
	ServiceName     string            `json:"serviceName" title:"Service Name" description:"Governing service name"`
	Age             string            `json:"age" title:"Age"`
	Ready           bool              `json:"ready" title:"Ready" description:"All replicas ready"`
}

// Result is the output with statefulset list
type Result struct {
	Context      Context           `json:"context,omitempty" title:"Context"`
	StatefulSets []StatefulSetInfo `json:"statefulSets" title:"StatefulSets"`
	Count        int               `json:"count" title:"Count"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the statefulset list fetcher
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
		Description: "StatefulSet List",
		Info:        "Lists Kubernetes StatefulSets with replica status, revisions, and health information.",
		Tags:        []string{"Kubernetes", "StatefulSets", "List"},
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

	listOpts := []client.ListOption{}
	if req.Namespace != "" {
		listOpts = append(listOpts, client.InNamespace(req.Namespace))
	}
	if req.LabelSelector != "" {
		selector, err := metav1.ParseToLabelSelector(req.LabelSelector)
		if err != nil {
			return c.handleError(ctx, handler, req, fmt.Sprintf("invalid label selector: %v", err))
		}
		labelSelector, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			return c.handleError(ctx, handler, req, fmt.Sprintf("invalid label selector: %v", err))
		}
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: labelSelector})
	}

	stsList := &appsv1.StatefulSetList{}
	if err := k8sClient.List(ctx, stsList, listOpts...); err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to list statefulsets: %v", err))
	}

	statefulSets := make([]StatefulSetInfo, 0, len(stsList.Items))
	for _, s := range stsList.Items {
		info := StatefulSetInfo{
			Name:            s.Name,
			Namespace:       s.Namespace,
			Labels:          s.Labels,
			Replicas:        *s.Spec.Replicas,
			ReadyReplicas:   s.Status.ReadyReplicas,
			CurrentReplicas: s.Status.CurrentReplicas,
			UpdatedReplicas: s.Status.UpdatedReplicas,
			CurrentRevision: s.Status.CurrentRevision,
			UpdateRevision:  s.Status.UpdateRevision,
			ServiceName:     s.Spec.ServiceName,
			Age:             time.Since(s.CreationTimestamp.Time).Round(time.Second).String(),
			Ready:           s.Status.ReadyReplicas == *s.Spec.Replicas,
		}

		if len(s.Spec.Template.Spec.Containers) > 0 {
			info.Image = s.Spec.Template.Spec.Containers[0].Image
		}

		statefulSets = append(statefulSets, info)
	}

	return handler(ctx, ResultPort, Result{
		Context:      req.Context,
		StatefulSets: statefulSets,
		Count:        len(statefulSets),
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
				StatefulSets: []StatefulSetInfo{
					{
						Name:            "mydb",
						Namespace:       "production",
						Labels:          map[string]string{"app": "mydb"},
						Replicas:        3,
						ReadyReplicas:   3,
						CurrentReplicas: 3,
						UpdatedReplicas: 3,
						ServiceName:     "mydb-headless",
						Age:             "24h",
						Ready:           true,
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
