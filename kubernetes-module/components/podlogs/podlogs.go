package podlogs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "pod_logs_get"
	RequestPort   = "request"
	LogsPort      = "logs"
	ErrorPort     = "error"
)

// Context type alias for schema generation
type Context any

// Settings configures the component
type Settings struct {
	EnableErrorPort bool  `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
	DefaultLines    int64 `json:"defaultLines" title:"Default Lines" description:"Default number of log lines to return (default: 50)"`
}

// Request is the input to get pod logs
type Request struct {
	Context   Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace string  `json:"namespace" required:"true" title:"Namespace" description:"Kubernetes namespace"`
	Pod       string  `json:"pod" required:"true" title:"Pod" description:"Exact pod name"`
	Container string  `json:"container,omitempty" title:"Container" description:"Container name (optional, defaults to first container)"`
	Lines     int64   `json:"lines,omitempty" title:"Lines" description:"Number of log lines to return"`
}

// Logs is the output with pod logs
type Logs struct {
	Context Context `json:"context,omitempty" title:"Context"`
	k8s.PodLogs
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
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
		Info:        "Get logs from a specific pod by exact name. Use pod_list to find pods first if needed.",
		Tags:        []string{"Kubernetes", "Pods", "Logs"},
	}
}

// OnClient stashes the K8s client and namespace, then initializes the
// streaming logs client.
func (c *Component) OnClient(k8sClient module.K8sClient) {
	if k8sClient == nil {
		return
	}
	c.k8sClientLock.Lock()
	c.k8sClient = k8sClient.GetK8sClient()
	c.k8sNamespace = k8sClient.GetNamespace()
	c.k8sClientLock.Unlock()
	c.initLogsClient()
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

func (c *Component) handleRequest(ctx context.Context, handler module.Handler, req Request) module.Result {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	c.k8sClientLock.RUnlock()

	c.logsClientLock.RLock()
	logsClient := c.logsClient
	c.logsClientLock.RUnlock()

	if k8sClient == nil {
		return c.handleError(ctx, handler, req, module.Retryable(errors.New("K8s client not available")))
	}

	if logsClient == nil {
		return c.handleError(ctx, handler, req, module.Retryable(errors.New("Logs client not available")))
	}

	lines := req.Lines
	if lines <= 0 {
		c.settingsLock.RLock()
		lines = c.settings.DefaultLines
		c.settingsLock.RUnlock()
	}

	podLogs, err := k8s.GetPodLogs(ctx, k8sClient, logsClient, req.Namespace, req.Pod, req.Container, lines)
	if err != nil {
		return c.handleError(ctx, handler, req, k8s.ClassifyError(err))
	}

	return handler(ctx, LogsPort, Logs{
		Context: req.Context,
		PodLogs: *podLogs,
	})
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, req Request, err error) module.Result {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		// Return handler result to propagate responses back through the call chain
		// (critical for blocking I/O patterns like HTTP Server)
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

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.ClientAware     = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
