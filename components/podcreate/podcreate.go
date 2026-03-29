package podcreate

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "pod_create"
	RequestPort   = "request"
	ResultPort    = "result"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

type ImagePullSecret struct {
	Name string `json:"name" title:"Name" description:"Secret name"`
}

type Request struct {
	Context          Context           `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace        string            `json:"namespace" required:"true" title:"Namespace" description:"Pod namespace"`
	Name             string            `json:"name" required:"true" title:"Name" description:"Pod name"`
	Image            string            `json:"image" required:"true" title:"Image" description:"Container image (e.g. zot.local:5000/nginx:latest)"`
	Command          []string          `json:"command,omitempty" title:"Command" description:"Override container command. Defaults to ['true'] for test pods."`
	ImagePullSecrets []ImagePullSecret `json:"imagePullSecrets,omitempty" title:"Image Pull Secrets" description:"Secrets for pulling the image"`
	RestartPolicy    string            `json:"restartPolicy,omitempty" title:"Restart Policy" description:"Never, OnFailure, or Always. Defaults to Never." enum:"Never,OnFailure,Always"`
	Labels           map[string]string `json:"labels,omitempty" configurable:"true" title:"Labels" description:"Pod labels"`
}

type Result struct {
	Context   Context `json:"context,omitempty" title:"Context"`
	Name      string  `json:"name" title:"Name"`
	Namespace string  `json:"namespace" title:"Namespace"`
	Success   bool    `json:"success" title:"Success"`
	Message   string  `json:"message" title:"Message"`
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
		Description: "Pod Create",
		Info:        "Create a Kubernetes Pod. Use to run one-off tasks, test image pulls, or spawn workers. Set command to ['true'] and restartPolicy to Never for a quick image pull test.",
		Tags:        []string{"Kubernetes", "Pods", "Create"},
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

	if req.Name == "" || req.Namespace == "" || req.Image == "" {
		return c.handleError(ctx, handler, req, "namespace, name, and image are required")
	}

	command := req.Command
	if len(command) == 0 {
		command = []string{"true"}
	}

	restartPolicy := corev1.RestartPolicyNever
	switch req.RestartPolicy {
	case "OnFailure":
		restartPolicy = corev1.RestartPolicyOnFailure
	case "Always":
		restartPolicy = corev1.RestartPolicyAlways
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: req.Namespace,
			Labels:    req.Labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "main",
					Image:   req.Image,
					Command: command,
				},
			},
			RestartPolicy: restartPolicy,
		},
	}

	for _, s := range req.ImagePullSecrets {
		pod.Spec.ImagePullSecrets = append(pod.Spec.ImagePullSecrets, corev1.LocalObjectReference{Name: s.Name})
	}

	if err := k8sClient.Create(ctx, pod); err != nil {
		return c.handleError(ctx, handler, req, fmt.Sprintf("failed to create pod: %v", err))
	}

	return handler(ctx, ResultPort, Result{
		Context:   req.Context,
		Name:      req.Name,
		Namespace: req.Namespace,
		Success:   true,
		Message:   fmt.Sprintf("Pod %s/%s created", req.Namespace, req.Name),
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
				Namespace:     "default",
				Name:          "test-pull",
				Image:         "nginx:latest",
				RestartPolicy: "Never",
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Name:      "test-pull",
				Namespace: "default",
				Success:   true,
				Message:   "Pod default/test-pull created",
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
