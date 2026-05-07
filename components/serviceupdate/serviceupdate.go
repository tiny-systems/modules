package serviceupdate

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "service_update"
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

// ServicePort represents a service port to update
type ServicePort struct {
	Name       string `json:"name,omitempty" title:"Name" description:"Port name"`
	Port       int32  `json:"port" required:"true" title:"Port" description:"Service port"`
	TargetPort int32  `json:"targetPort,omitempty" title:"Target Port" description:"Container port (defaults to Port)"`
	Protocol   string `json:"protocol,omitempty" title:"Protocol" description:"TCP or UDP (defaults to TCP)"`
}

// Request is the input to update a service
type Request struct {
	Context     Context           `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace   string            `json:"namespace" required:"true" title:"Namespace" description:"Service namespace"`
	Name        string            `json:"name" required:"true" title:"Name" description:"Service name"`
	Selector    map[string]string `json:"selector,omitempty" configurable:"true" title:"Selector" description:"Pod selector (replaces existing)"`
	Ports       []ServicePort     `json:"ports,omitempty" title:"Ports" description:"Service ports (replaces existing if provided)"`
	Labels      map[string]string `json:"labels,omitempty" configurable:"true" title:"Labels" description:"Labels to merge"`
	Annotations map[string]string `json:"annotations,omitempty" configurable:"true" title:"Annotations" description:"Annotations to merge"`
}

// ResultPort represents a service port in the result
type ResultServicePort struct {
	Name       string `json:"name,omitempty" title:"Name"`
	Port       int32  `json:"port" title:"Port"`
	TargetPort string `json:"targetPort" title:"Target Port"`
	Protocol   string `json:"protocol" title:"Protocol"`
}

// Result is the output after service update
type Result struct {
	Context     Context             `json:"context,omitempty" title:"Context"`
	Name        string              `json:"name" title:"Name"`
	Namespace   string              `json:"namespace" title:"Namespace"`
	Type        string              `json:"type" title:"Type"`
	ClusterIP   string              `json:"clusterIP" title:"Cluster IP"`
	Selector    map[string]string   `json:"selector,omitempty" title:"Selector"`
	Ports       []ResultServicePort `json:"ports,omitempty" title:"Ports"`
	Success     bool                `json:"success" title:"Success"`
	Message     string              `json:"message" title:"Message"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the service update
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
		Description: "Service Update",
		Info:        "Update a Kubernetes Service's selector, ports, labels, or annotations.",
		Tags:        []string{"Kubernetes", "Services", "Update"},
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

	// Get current service
	svc := &corev1.Service{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, svc); err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("service not found: %v", err))
	}

	// Update selector if provided
	if len(req.Selector) > 0 {
		svc.Spec.Selector = req.Selector
	}

	// Update ports if provided
	if len(req.Ports) > 0 {
		svc.Spec.Ports = make([]corev1.ServicePort, 0, len(req.Ports))
		for _, p := range req.Ports {
			protocol := corev1.ProtocolTCP
			if p.Protocol == "UDP" {
				protocol = corev1.ProtocolUDP
			}

			targetPort := p.TargetPort
			if targetPort == 0 {
				targetPort = p.Port
			}

			svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{
				Name:       p.Name,
				Port:       p.Port,
				TargetPort: intstr.FromInt32(targetPort),
				Protocol:   protocol,
			})
		}
	}

	// Merge labels
	if len(req.Labels) > 0 {
		if svc.Labels == nil {
			svc.Labels = make(map[string]string)
		}
		for k, v := range req.Labels {
			svc.Labels[k] = v
		}
	}

	// Merge annotations
	if len(req.Annotations) > 0 {
		if svc.Annotations == nil {
			svc.Annotations = make(map[string]string)
		}
		for k, v := range req.Annotations {
			svc.Annotations[k] = v
		}
	}

	// Update service
	if err := k8sClient.Update(ctx, svc); err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to update service: %v", err))
	}

	// Build result
	result := Result{
		Context:   req.Context,
		Name:      svc.Name,
		Namespace: svc.Namespace,
		Type:      string(svc.Spec.Type),
		ClusterIP: svc.Spec.ClusterIP,
		Selector:  svc.Spec.Selector,
		Success:   true,
		Message:   fmt.Sprintf("Service %s/%s updated", req.Namespace, req.Name),
	}

	for _, p := range svc.Spec.Ports {
		result.Ports = append(result.Ports, ResultServicePort{
			Name:       p.Name,
			Port:       p.Port,
			TargetPort: p.TargetPort.String(),
			Protocol:   string(p.Protocol),
		})
	}

	return handler(ctx, ResultPort, result)
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
			Name:  RequestPort,
			Label: "Request",
			Configuration: Request{
				Namespace: "default",
				Name:      "my-service",
				Selector:  map[string]string{"app": "myapp"},
				Ports: []ServicePort{
					{Name: "http", Port: 80, TargetPort: 8080},
				},
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Name:      "my-service",
				Namespace: "default",
				Type:      "ClusterIP",
				ClusterIP: "10.96.0.100",
				Selector:  map[string]string{"app": "myapp"},
				Ports: []ResultServicePort{
					{Name: "http", Port: 80, TargetPort: "8080", Protocol: "TCP"},
				},
				Success: true,
				Message: "Service default/my-service updated",
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
