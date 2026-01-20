package k8s

import (
	"context"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// LogsClient wraps a kubernetes clientset for pod logs operations
type LogsClient struct {
	clientset kubernetes.Interface
}

// NewLogsClient creates a new LogsClient using in-cluster or kubeconfig
func NewLogsClient() (*LogsClient, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	return NewLogsClientFromConfig(cfg)
}

// NewLogsClientFromConfig creates a LogsClient from an existing rest.Config
func NewLogsClientFromConfig(cfg *rest.Config) (*LogsClient, error) {
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &LogsClient{clientset: clientset}, nil
}

// GetLogs implements RESTClient interface
func (c *LogsClient) GetLogs(ctx context.Context, namespace, podName string, opts *corev1.PodLogOptions) (string, error) {
	stream, err := c.clientset.CoreV1().Pods(namespace).GetLogs(podName, opts).Stream(ctx)
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

// Ensure LogsClient implements RESTClient
var _ RESTClient = (*LogsClient)(nil)
