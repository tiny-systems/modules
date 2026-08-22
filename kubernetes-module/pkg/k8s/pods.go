package k8s

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PodInfo contains info about a single pod
type PodInfo struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Phase     string `json:"phase"`
	Ready     bool   `json:"ready"`
	Restarts  int32  `json:"restarts"`
	Age       string `json:"age"`
	Node      string `json:"node"`
	IP        string `json:"ip"`
	Message   string `json:"message,omitempty"`
}

// PodStatus is the aggregated status of pods
type PodStatus struct {
	Total     int       `json:"total"`
	Running   int       `json:"running"`
	Pending   int       `json:"pending"`
	Failed    int       `json:"failed"`
	Succeeded int       `json:"succeeded"`
	Healthy   bool      `json:"healthy"`
	Summary   string    `json:"summary"`
	Pods      []PodInfo `json:"pods"`
}

// PodLogs contains logs from a pod
type PodLogs struct {
	PodName   string `json:"podName"`
	Container string `json:"container"`
	Logs      string `json:"logs"`
	Lines     int    `json:"lines"`
}

// GetPodStatus returns the status of pods matching the selector
func GetPodStatus(ctx context.Context, k8sClient client.Client, namespace, labelSelector string) (*PodStatus, error) {
	pods, err := listPods(ctx, k8sClient, namespace, labelSelector)
	if err != nil {
		return nil, err
	}

	status := &PodStatus{
		Pods: make([]PodInfo, 0, len(pods)),
	}

	for _, pod := range pods {
		info := podToInfo(pod)
		status.Total++

		switch pod.Status.Phase {
		case corev1.PodRunning:
			if info.Ready {
				status.Running++
			} else {
				status.Pending++
			}
		case corev1.PodPending:
			status.Pending++
		case corev1.PodFailed:
			status.Failed++
		case corev1.PodSucceeded:
			status.Succeeded++
		}

		status.Pods = append(status.Pods, info)
	}

	status.Healthy = status.Total > 0 && status.Running == status.Total && status.Failed == 0
	status.Summary = buildSummary(status)

	return status, nil
}

// GetPodLogs returns logs from a pod by exact name
func GetPodLogs(ctx context.Context, k8sClient client.Client, restClient RESTClient, namespace, podName, container string, lines int64) (*PodLogs, error) {
	if lines <= 0 {
		lines = 50
	}

	if podName == "" {
		return nil, fmt.Errorf("pod name is required")
	}

	// Get logs using REST client
	opts := &corev1.PodLogOptions{
		TailLines: &lines,
	}
	if container != "" {
		opts.Container = container
	}

	logs, err := restClient.GetLogs(ctx, namespace, podName, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}

	return &PodLogs{
		PodName:   podName,
		Container: container,
		Logs:      logs,
		Lines:     int(lines),
	}, nil
}

// FindPod finds a single pod by exact name
func FindPod(ctx context.Context, k8sClient client.Client, namespace, name string) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pod)
	if err != nil {
		return nil, fmt.Errorf("pod not found: %s", name)
	}
	return pod, nil
}

// FindPodBySelector finds the first pod matching a label selector
func FindPodBySelector(ctx context.Context, k8sClient client.Client, namespace, labelSelector string) (*corev1.Pod, error) {
	pods, err := listPods(ctx, k8sClient, namespace, labelSelector)
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 {
		return nil, fmt.Errorf("no pods found matching %s", labelSelector)
	}
	return &pods[0], nil
}

// listPods lists pods by label selector
// If namespace is empty, lists across all namespaces
func listPods(ctx context.Context, k8sClient client.Client, namespace, labelSelector string) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}

	labels, err := ParseLabelSelector(labelSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid label selector: %w", err)
	}

	opts := []client.ListOption{}
	if namespace != "" {
		opts = append(opts, client.InNamespace(namespace))
	}
	if len(labels) > 0 {
		opts = append(opts, client.MatchingLabels(labels))
	}

	if err := k8sClient.List(ctx, podList, opts...); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	return podList.Items, nil
}

func podToInfo(pod corev1.Pod) PodInfo {
	info := PodInfo{
		Name:      pod.Name,
		Namespace: pod.Namespace,
		Phase:     string(pod.Status.Phase),
		Node:      pod.Spec.NodeName,
		IP:        pod.Status.PodIP,
	}

	// Calculate restarts and ready status
	allReady := true
	for _, cs := range pod.Status.ContainerStatuses {
		info.Restarts += cs.RestartCount
		if !cs.Ready {
			allReady = false
		}
	}
	info.Ready = allReady && len(pod.Status.ContainerStatuses) > 0

	// Calculate age
	if !pod.CreationTimestamp.IsZero() {
		info.Age = FormatAge(time.Since(pod.CreationTimestamp.Time))
	}

	// Get status message
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status != corev1.ConditionTrue {
			info.Message = cond.Message
			break
		}
	}

	return info
}

func buildSummary(status *PodStatus) string {
	if status.Total == 0 {
		return "No pods found"
	}
	if status.Healthy {
		return fmt.Sprintf("%d/%d pods running", status.Running, status.Total)
	}

	parts := []string{}
	if status.Running > 0 {
		parts = append(parts, fmt.Sprintf("%d running", status.Running))
	}
	if status.Pending > 0 {
		parts = append(parts, fmt.Sprintf("%d pending", status.Pending))
	}
	if status.Failed > 0 {
		parts = append(parts, fmt.Sprintf("%d failed", status.Failed))
	}
	return fmt.Sprintf("%d pods: %s", status.Total, strings.Join(parts, ", "))
}

// RESTClient interface for getting pod logs
type RESTClient interface {
	GetLogs(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (string, error)
}

// DefaultRESTClient implements RESTClient using kubernetes clientset
type DefaultRESTClient struct {
	Clientset interface {
		CoreV1() interface {
			Pods(namespace string) interface {
				GetLogs(name string, opts *corev1.PodLogOptions) interface {
					Stream(ctx context.Context) (io.ReadCloser, error)
				}
			}
		}
	}
}

func (c *DefaultRESTClient) GetLogs(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (string, error) {
	stream, err := c.Clientset.CoreV1().Pods(namespace).GetLogs(podName, opts).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()

	logs, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	return string(logs), nil
}
