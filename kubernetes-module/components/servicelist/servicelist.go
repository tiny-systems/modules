package servicelist

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "service_list"
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

// Request is the input to list services
type Request struct {
	Context       Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace     string  `json:"namespace,omitempty" title:"Namespace" description:"Filter to specific namespace (empty = all namespaces)"`
	LabelSelector string  `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by labels (e.g., app=myapp)"`
}

// ServicePort represents a service port
type ServicePort struct {
	Name       string `json:"name,omitempty" title:"Name"`
	Protocol   string `json:"protocol" title:"Protocol"`
	Port       int32  `json:"port" title:"Port"`
	TargetPort string `json:"targetPort" title:"Target Port"`
	NodePort   int32  `json:"nodePort,omitempty" title:"Node Port"`
}

// ServiceInfo contains typed service information
type ServiceInfo struct {
	Name           string            `json:"name" title:"Name"`
	Namespace      string            `json:"namespace" title:"Namespace"`
	Labels         map[string]string `json:"labels,omitempty" title:"Labels"`
	Type           string            `json:"type" title:"Type" description:"ClusterIP, NodePort, LoadBalancer, ExternalName"`
	ClusterIP      string            `json:"clusterIP,omitempty" title:"Cluster IP"`
	ExternalIPs    []string          `json:"externalIPs,omitempty" title:"External IPs"`
	LoadBalancerIP string            `json:"loadBalancerIP,omitempty" title:"LoadBalancer IP"`
	Ports          []ServicePort     `json:"ports,omitempty" title:"Ports"`
	Selector       map[string]string `json:"selector,omitempty" title:"Selector" description:"Pod selector"`
	Age            string            `json:"age" title:"Age"`
}

// Result is the output with service list
type Result struct {
	Context  Context       `json:"context,omitempty" title:"Context"`
	Services []ServiceInfo `json:"services" title:"Services"`
	Count    int           `json:"count" title:"Count"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the service list fetcher
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
		Description: "Service List",
		Info:        "Lists Kubernetes Services with ports, selectors, and endpoint information.",
		Tags:        []string{"Kubernetes", "Services", "List"},
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

	svcList := &corev1.ServiceList{}
	if err := k8sClient.List(ctx, svcList, listOpts...); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to list services: %w", k8s.ClassifyError(err)))
	}

	services := make([]ServiceInfo, 0, len(svcList.Items))
	for _, s := range svcList.Items {
		info := ServiceInfo{
			Name:        s.Name,
			Namespace:   s.Namespace,
			Labels:      s.Labels,
			Type:        string(s.Spec.Type),
			ClusterIP:   s.Spec.ClusterIP,
			ExternalIPs: s.Spec.ExternalIPs,
			Selector:    s.Spec.Selector,
			Age:         time.Since(s.CreationTimestamp.Time).Round(time.Second).String(),
		}

		if s.Spec.LoadBalancerIP != "" {
			info.LoadBalancerIP = s.Spec.LoadBalancerIP
		} else if len(s.Status.LoadBalancer.Ingress) > 0 {
			if s.Status.LoadBalancer.Ingress[0].IP != "" {
				info.LoadBalancerIP = s.Status.LoadBalancer.Ingress[0].IP
			} else {
				info.LoadBalancerIP = s.Status.LoadBalancer.Ingress[0].Hostname
			}
		}

		for _, p := range s.Spec.Ports {
			info.Ports = append(info.Ports, ServicePort{
				Name:       p.Name,
				Protocol:   string(p.Protocol),
				Port:       p.Port,
				TargetPort: p.TargetPort.String(),
				NodePort:   p.NodePort,
			})
		}

		services = append(services, info)
	}

	return handler(ctx, ResultPort, Result{
		Context:  req.Context,
		Services: services,
		Count:    len(services),
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
				Services: []ServiceInfo{
					{
						Name:      "myapp-service",
						Namespace: "production",
						Labels:    map[string]string{"app": "myapp"},
						Type:      "ClusterIP",
						ClusterIP: "10.96.0.100",
						Ports: []ServicePort{
							{
								Name:       "http",
								Protocol:   "TCP",
								Port:       80,
								TargetPort: "8080",
							},
						},
						Selector: map[string]string{"app": "myapp"},
						Age:      "24h",
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
