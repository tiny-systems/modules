package resourcelist

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
	ComponentName = "resource_list"
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

// Request is the input to list resources
type Request struct {
	Context       Context  `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Kinds         []string `json:"kinds,omitempty" title:"Kinds" description:"Resource kinds to list: Deployment, StatefulSet, DaemonSet (empty = all)"`
	Namespace     string   `json:"namespace,omitempty" title:"Namespace" description:"Filter to specific namespace (empty = all namespaces)"`
	LabelSelector string   `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by label expression (e.g., app=myapp, environment in (prod,staging))"`
}

// Result is the output with resource list
type Result struct {
	Context Context `json:"context,omitempty" title:"Context"`
	k8s.ResourceList
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the resource list fetcher
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
		Description: "Resource List",
		Info:        "Lists Kubernetes workload resources (Deployments, StatefulSets, DaemonSets) with optional filtering by namespace and label selector. Returns resource details including name, namespace, kind, labels, replicas, and status.",
		Tags:        []string{"Kubernetes", "Resources"},
	}
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) any {
	switch port {
	case v1alpha1.ClientPort:
		if k8sProvider, ok := msg.(module.K8sClient); ok {
			c.k8sClientLock.Lock()
			c.k8sClient = k8sProvider.GetK8sClient()
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
	c.k8sClientLock.RUnlock()

	if k8sClient == nil {
		return c.handleError(ctx, handler, req, "K8s client not available")
	}

	listReq := k8s.ListResourcesRequest{
		Kinds:         req.Kinds,
		Namespace:     req.Namespace,
		LabelSelector: req.LabelSelector,
	}

	resourceList, err := k8s.ListResources(ctx, k8sClient, listReq)
	if err != nil {
		return c.handleError(ctx, handler, req, err.Error())
	}

	return handler(ctx, ResultPort, Result{
		Context:      req.Context,
		ResourceList: *resourceList,
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
				ResourceList: k8s.ResourceList{
					Resources: []k8s.ResourceInfo{
						{
							Name:      "myapp-api",
							Namespace: "production",
							Kind:      "Deployment",
							Labels:    map[string]string{"app": "myapp", "environment": "prod"},
							Replicas:  3,
							Ready:     3,
						},
					},
					Count: 1,
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
