package daemonsetlist

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "daemonset_list"
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

// Request is the input to list daemonsets
type Request struct {
	Context       Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace     string  `json:"namespace,omitempty" title:"Namespace" description:"Filter to specific namespace (empty = all namespaces)"`
	LabelSelector string  `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by labels (e.g., app=myapp)"`
}

// DaemonSetInfo contains typed daemonset information
type DaemonSetInfo struct {
	Name                   string            `json:"name" title:"Name"`
	Namespace              string            `json:"namespace" title:"Namespace"`
	Labels                 map[string]string `json:"labels,omitempty" title:"Labels"`
	DesiredNumberScheduled int32             `json:"desiredNumberScheduled" title:"Desired Scheduled" description:"Nodes that should run the pod"`
	CurrentNumberScheduled int32             `json:"currentNumberScheduled" title:"Current Scheduled" description:"Nodes currently running the pod"`
	NumberReady            int32             `json:"numberReady" title:"Number Ready"`
	NumberAvailable        int32             `json:"numberAvailable" title:"Number Available"`
	NumberMisscheduled     int32             `json:"numberMisscheduled" title:"Number Misscheduled" description:"Nodes running pod but shouldn't"`
	UpdatedNumberScheduled int32             `json:"updatedNumberScheduled" title:"Updated Scheduled"`
	Image                  string            `json:"image,omitempty" title:"Image" description:"First container image"`
	Age                    string            `json:"age" title:"Age"`
	Ready                  bool              `json:"ready" title:"Ready" description:"All desired nodes ready"`
}

// Result is the output with daemonset list
type Result struct {
	Context    Context         `json:"context,omitempty" title:"Context"`
	DaemonSets []DaemonSetInfo `json:"daemonSets" title:"DaemonSets"`
	Count      int             `json:"count" title:"Count"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the daemonset list fetcher
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
		Description: "DaemonSet List",
		Info:        "Lists Kubernetes DaemonSets with scheduling status and health information.",
		Tags:        []string{"Kubernetes", "DaemonSets", "List"},
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

	dsList := &appsv1.DaemonSetList{}
	if err := k8sClient.List(ctx, dsList, listOpts...); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to list daemonsets: %w", k8s.ClassifyError(err)))
	}

	daemonSets := make([]DaemonSetInfo, 0, len(dsList.Items))
	for _, d := range dsList.Items {
		info := DaemonSetInfo{
			Name:                   d.Name,
			Namespace:              d.Namespace,
			Labels:                 d.Labels,
			DesiredNumberScheduled: d.Status.DesiredNumberScheduled,
			CurrentNumberScheduled: d.Status.CurrentNumberScheduled,
			NumberReady:            d.Status.NumberReady,
			NumberAvailable:        d.Status.NumberAvailable,
			NumberMisscheduled:     d.Status.NumberMisscheduled,
			UpdatedNumberScheduled: d.Status.UpdatedNumberScheduled,
			Age:                    time.Since(d.CreationTimestamp.Time).Round(time.Second).String(),
			Ready:                  d.Status.NumberReady == d.Status.DesiredNumberScheduled,
		}

		if len(d.Spec.Template.Spec.Containers) > 0 {
			info.Image = d.Spec.Template.Spec.Containers[0].Image
		}

		daemonSets = append(daemonSets, info)
	}

	return handler(ctx, ResultPort, Result{
		Context:    req.Context,
		DaemonSets: daemonSets,
		Count:      len(daemonSets),
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
				DaemonSets: []DaemonSetInfo{
					{
						Name:                   "fluentd",
						Namespace:              "kube-system",
						Labels:                 map[string]string{"app": "fluentd"},
						DesiredNumberScheduled: 3,
						CurrentNumberScheduled: 3,
						NumberReady:            3,
						NumberAvailable:        3,
						Age:                    "24h",
						Ready:                  true,
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
