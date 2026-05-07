package secretget

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "secret_get"
	RequestPort   = "request"
	ResultPort    = "result"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

type Request struct {
	Context   Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace string  `json:"namespace" required:"true" title:"Namespace" description:"Secret namespace"`
	Name      string  `json:"name" required:"true" title:"Name" description:"Secret name (e.g. regcred)"`
	Key       string  `json:"key,omitempty" title:"Key" description:"Specific data key to return. Leave empty to return all keys."`
}

type Result struct {
	Context   Context           `json:"context,omitempty" title:"Context"`
	Namespace string            `json:"namespace" title:"Namespace"`
	Name      string            `json:"name" title:"Name"`
	Type      string            `json:"type" title:"Type" description:"Secret type (e.g. kubernetes.io/dockerconfigjson)"`
	Data      map[string]string `json:"data" title:"Data" description:"Secret data (base64-decoded string values)"`
	Value     string            `json:"value,omitempty" title:"Value" description:"Single key value (when key is specified)"`
}

type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

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
		Description: "Secret Get",
		Info:        "Read a Kubernetes Secret by name. Returns decoded string data. Use to read docker-registry secrets (regcred), TLS certs, or any opaque secret. Specify key to get a single value.",
		Tags:        []string{"Kubernetes", "Secret", "Configuration"},
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

	if req.Name == "" {
		// No secret requested — pass through with empty value
		return handler(ctx, ResultPort, Result{
			Context:   req.Context,
			Namespace: req.Namespace,
		})
	}

	if req.Namespace == "" {
		return c.handleError(ctx, handler, req, "namespace is required")
	}

	secret := &corev1.Secret{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, secret); err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to get secret: %v", err))
	}

	data := make(map[string]string, len(secret.Data))
	for k, v := range secret.Data {
		data[k] = string(v)
	}

	result := Result{
		Context:   req.Context,
		Namespace: req.Namespace,
		Name:      req.Name,
		Type:      string(secret.Type),
		Data:      data,
	}

	if req.Key != "" {
		val, ok := data[req.Key]
		if !ok {
			return c.handleError(ctx, handler, req, fmt.Sprintf("key %q not found in secret %s/%s", req.Key, req.Namespace, req.Name))
		}
		result.Value = val
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
				Name:      "regcred",
				Key:       ".dockerconfigjson",
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Namespace: "default",
				Name:      "regcred",
				Type:      "kubernetes.io/dockerconfigjson",
				Data:      map[string]string{".dockerconfigjson": "{\"auths\":{}}"},
				Value:     "{\"auths\":{}}",
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
