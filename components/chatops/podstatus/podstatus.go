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
	ComponentName = "pod_status"
	RequestPort   = "request"
	StatusPort    = "status"
	ErrorPort     = "error"
)

// Settings configures the component
type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

// Request is the input to get pod status
type Request struct {
	Context       any    `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace     string `json:"namespace" required:"true" title:"Namespace" description:"Kubernetes namespace"`
	LabelSelector string `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter pods by labels (e.g., app=myapp)"`
}

// Status is the output with pod information
type Status struct {
	Context any `json:"context,omitempty" title:"Context"`
	k8s.PodStatus
}

// Error output
type Error struct {
	Context any    `json:"context,omitempty" title:"Context"`
	Error   string `json:"error" title:"Error"`
	Request Request `json:"request" title:"Request"`
}

// Component implements the pod status checker
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
		Description: "Pod Status",
		Info:        "Get status of pods matching a label selector. Returns pod count, health summary, and individual pod details.",
		Tags:        []string{"Kubernetes", "ChatOps", "Pods", "Status"},
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

func (c *Component) handleRequest(ctx context.Context, handler module.Handler, req Request) error {
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

	podStatus, err := k8s.GetPodStatus(ctx, k8sClient, namespace, req.LabelSelector)
	if err != nil {
		return c.handleError(ctx, handler, req, err.Error())
	}

	result := handler(ctx, StatusPort, Status{
		Context:   req.Context,
		PodStatus: *podStatus,
	})
	if result != nil {
		if err, ok := result.(error); ok {
			return err
		}
	}
	return nil
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, req Request, errMsg string) error {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		_ = handler(ctx, ErrorPort, Error{
			Context: req.Context,
			Error:   errMsg,
			Request: req,
		})
		return nil
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

var _ module.Component = (*Component)(nil)

func init() {
	registry.Register(&Component{})
}
