package podlist

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
	ComponentName = "pod_list"
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

// Request is the input to list pods
type Request struct {
	Context       Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace     string  `json:"namespace,omitempty" title:"Namespace" description:"Filter to specific namespace (empty = all namespaces)"`
	LabelSelector string  `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by labels (e.g., app=myapp)"`
}

// ContainerState represents the state of a container
type ContainerState struct {
	State   string `json:"state" title:"State" description:"waiting, running, or terminated"`
	Reason  string `json:"reason,omitempty" title:"Reason"`
	Message string `json:"message,omitempty" title:"Message"`
}

// ContainerStatus represents a container's status
type ContainerStatus struct {
	Name         string         `json:"name" title:"Name"`
	Ready        bool           `json:"ready" title:"Ready"`
	RestartCount int32          `json:"restartCount" title:"Restart Count"`
	State        ContainerState `json:"state" title:"State"`
	Image        string         `json:"image" title:"Image"`
}

// PodInfo contains typed pod information
type PodInfo struct {
	Name              string            `json:"name" title:"Name"`
	Namespace         string            `json:"namespace" title:"Namespace"`
	Labels            map[string]string `json:"labels,omitempty" title:"Labels"`
	Phase             string            `json:"phase" title:"Phase" description:"Pending, Running, Succeeded, Failed, Unknown"`
	Ready             bool              `json:"ready" title:"Ready" description:"All containers ready"`
	PodIP             string            `json:"podIP,omitempty" title:"Pod IP"`
	NodeName          string            `json:"nodeName,omitempty" title:"Node Name"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty" title:"Container Statuses"`
	Age               string            `json:"age" title:"Age"`
	Restarts          int32             `json:"restarts" title:"Restarts" description:"Total restart count"`
	HasProblem        bool              `json:"hasProblem" title:"Has Problem"`
	ProblemReason     string            `json:"problemReason,omitempty" title:"Problem Reason"`
}

// Result is the output with pod list
type Result struct {
	Context Context   `json:"context,omitempty" title:"Context"`
	Pods    []PodInfo `json:"pods" title:"Pods"`
	Count   int       `json:"count" title:"Count"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the pod list fetcher
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
		Description: "Pod List",
		Info:        "Lists Kubernetes Pods with container statuses, phases, and problem detection.",
		Tags:        []string{"Kubernetes", "Pods", "List"},
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

	podList := &corev1.PodList{}
	if err := k8sClient.List(ctx, podList, listOpts...); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to list pods: %w", k8s.ClassifyError(err)))
	}

	pods := make([]PodInfo, 0, len(podList.Items))
	for _, p := range podList.Items {
		hasProblem, problemReason := c.detectProblem(&p)

		info := PodInfo{
			Name:          p.Name,
			Namespace:     p.Namespace,
			Labels:        p.Labels,
			Phase:         string(p.Status.Phase),
			Ready:         c.isPodReady(&p),
			PodIP:         p.Status.PodIP,
			NodeName:      p.Spec.NodeName,
			Age:           time.Since(p.CreationTimestamp.Time).Round(time.Second).String(),
			HasProblem:    hasProblem,
			ProblemReason: problemReason,
		}

		var totalRestarts int32
		for _, cs := range p.Status.ContainerStatuses {
			totalRestarts += cs.RestartCount

			status := ContainerStatus{
				Name:         cs.Name,
				Ready:        cs.Ready,
				RestartCount: cs.RestartCount,
				Image:        cs.Image,
			}

			if cs.State.Running != nil {
				status.State = ContainerState{State: "running"}
			} else if cs.State.Waiting != nil {
				status.State = ContainerState{
					State:   "waiting",
					Reason:  cs.State.Waiting.Reason,
					Message: cs.State.Waiting.Message,
				}
			} else if cs.State.Terminated != nil {
				status.State = ContainerState{
					State:   "terminated",
					Reason:  cs.State.Terminated.Reason,
					Message: cs.State.Terminated.Message,
				}
			}

			info.ContainerStatuses = append(info.ContainerStatuses, status)
		}
		info.Restarts = totalRestarts

		pods = append(pods, info)
	}

	return handler(ctx, ResultPort, Result{
		Context: req.Context,
		Pods:    pods,
		Count:   len(pods),
	})
}

func (c *Component) detectProblem(pod *corev1.Pod) (bool, string) {
	if pod.Status.Phase == corev1.PodFailed {
		reason := pod.Status.Reason
		if reason == "" {
			reason = "Pod failed"
		}
		return true, reason
	}

	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			reason := cs.State.Waiting.Reason
			if reason == "ImagePullBackOff" || reason == "ErrImagePull" ||
				reason == "CrashLoopBackOff" || reason == "CreateContainerError" {
				return true, reason
			}
		}
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return true, fmt.Sprintf("Exit code %d", cs.State.Terminated.ExitCode)
		}
	}

	return false, ""
}

func (c *Component) isPodReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
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
				Pods: []PodInfo{
					{
						Name:      "myapp-abc123",
						Namespace: "production",
						Labels:    map[string]string{"app": "myapp"},
						Phase:     "Running",
						Ready:     true,
						PodIP:     "10.0.0.5",
						NodeName:  "node-1",
						ContainerStatuses: []ContainerStatus{
							{
								Name:         "main",
								Ready:        true,
								RestartCount: 0,
								State:        ContainerState{State: "running"},
								Image:        "myapp:v1.0.0",
							},
						},
						Age:        "1h",
						Restarts:   0,
						HasProblem: false,
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
