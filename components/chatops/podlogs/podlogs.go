package podlogs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "pod_logs"
	RequestPort   = "request"
	LogsPort      = "logs"
	ErrorPort     = "error"
)

// Settings configures the component
type Settings struct {
	EnableErrorPort bool  `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
	DefaultLines    int64 `json:"defaultLines" title:"Default Lines" description:"Default number of log lines to return (default: 50)"`
}

// Request is the input to get pod logs
type Request struct {
	Context   any    `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace string `json:"namespace" required:"true" title:"Namespace" description:"Kubernetes namespace"`
	Pod       string `json:"pod" required:"true" title:"Pod" description:"Pod name or app label"`
	Container string `json:"container,omitempty" title:"Container" description:"Container name (optional, defaults to first container)"`
	Lines     int64  `json:"lines,omitempty" title:"Lines" description:"Number of log lines to return"`
}

// Logs is the output with pod logs
type Logs struct {
	Context any `json:"context,omitempty" title:"Context"`
	k8s.PodLogs
}

// Error output
type Error struct {
	Context any     `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
	Request Request `json:"request" title:"Request"`
}

// Component implements the pod logs fetcher
type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	k8sClient     client.Client
	k8sNamespace  string
	k8sClientLock sync.RWMutex

	logsClient     *k8s.LogsClient
	logsClientLock sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Pod Logs",
		Info:        "Get logs from a pod. Can look up pods by name or app label.",
		Tags:        []string{"Kubernetes", "ChatOps", "Pods", "Logs"},
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

			// Initialize logs client
			c.initLogsClient()
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

func (c *Component) initLogsClient() {
	c.logsClientLock.Lock()
	defer c.logsClientLock.Unlock()

	if c.logsClient != nil {
		return
	}

	logsClient, err := k8s.NewLogsClient()
	if err != nil {
		log.Error().Err(err).Msg("Failed to create logs client")
		return
	}
	c.logsClient = logsClient
}

func (c *Component) handleRequest(ctx context.Context, handler module.Handler, req Request) error {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	defaultNS := c.k8sNamespace
	c.k8sClientLock.RUnlock()

	c.logsClientLock.RLock()
	logsClient := c.logsClient
	c.logsClientLock.RUnlock()

	if k8sClient == nil {
		return c.handleError(ctx, handler, req, "K8s client not available")
	}

	if logsClient == nil {
		return c.handleError(ctx, handler, req, "Logs client not available")
	}

	namespace := req.Namespace
	if namespace == "" {
		namespace = defaultNS
	}

	lines := req.Lines
	if lines <= 0 {
		c.settingsLock.RLock()
		lines = c.settings.DefaultLines
		c.settingsLock.RUnlock()
	}

	podLogs, err := k8s.GetPodLogs(ctx, k8sClient, logsClient, namespace, req.Pod, req.Container, lines)
	if err != nil {
		return c.handleError(ctx, handler, req, err.Error())
	}

	result := handler(ctx, LogsPort, Logs{
		Context: req.Context,
		PodLogs: *podLogs,
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
			Configuration: Settings{DefaultLines: 50},
		},
		{
			Name:  RequestPort,
			Label: "Request",
			Configuration: Request{
				Pod:   "myapp",
				Lines: 50,
			},
			Position: module.Left,
		},
		{
			Name:   LogsPort,
			Label:  "Logs",
			Source: true,
			Configuration: Logs{
				PodLogs: k8s.PodLogs{
					PodName: "myapp-abc123",
					Logs:    "[logs output here]",
					Lines:   50,
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
