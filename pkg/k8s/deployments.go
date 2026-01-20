package k8s

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DeploymentInfo contains info about a deployment
type DeploymentInfo struct {
	Name              string `json:"name"`
	Namespace         string `json:"namespace"`
	Replicas          int32  `json:"replicas"`
	ReadyReplicas     int32  `json:"readyReplicas"`
	UpdatedReplicas   int32  `json:"updatedReplicas"`
	AvailableReplicas int32  `json:"availableReplicas"`
	Age               string `json:"age"`
	Image             string `json:"image"`
}

// RestartResult contains the result of a restart operation
type RestartResult struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Success   bool   `json:"success"`
	Message   string `json:"message"`
}

// RestartDeployment performs a rollout restart on a deployment
func RestartDeployment(ctx context.Context, k8sClient client.Client, namespace, name string) (*RestartResult, error) {
	deployment, err := findDeployment(ctx, k8sClient, namespace, name)
	if err != nil {
		return nil, err
	}

	// Perform rollout restart by updating the pod template annotation
	if deployment.Spec.Template.Annotations == nil {
		deployment.Spec.Template.Annotations = make(map[string]string)
	}
	deployment.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().Format(time.RFC3339)

	if err := k8sClient.Update(ctx, deployment); err != nil {
		return nil, fmt.Errorf("failed to restart deployment: %w", err)
	}

	return &RestartResult{
		Name:      deployment.Name,
		Namespace: deployment.Namespace,
		Success:   true,
		Message:   fmt.Sprintf("Deployment %s restarted", deployment.Name),
	}, nil
}

// GetDeploymentStatus returns the status of a deployment
func GetDeploymentStatus(ctx context.Context, k8sClient client.Client, namespace, name string) (*DeploymentInfo, error) {
	deployment, err := findDeployment(ctx, k8sClient, namespace, name)
	if err != nil {
		return nil, err
	}

	return deploymentToInfo(deployment), nil
}

// ScaleDeployment scales a deployment to the specified replicas
func ScaleDeployment(ctx context.Context, k8sClient client.Client, namespace, name string, replicas int32) (*RestartResult, error) {
	deployment, err := findDeployment(ctx, k8sClient, namespace, name)
	if err != nil {
		return nil, err
	}

	oldReplicas := *deployment.Spec.Replicas
	deployment.Spec.Replicas = &replicas

	if err := k8sClient.Update(ctx, deployment); err != nil {
		return nil, fmt.Errorf("failed to scale deployment: %w", err)
	}

	return &RestartResult{
		Name:      deployment.Name,
		Namespace: deployment.Namespace,
		Success:   true,
		Message:   fmt.Sprintf("Deployment %s scaled from %d to %d", deployment.Name, oldReplicas, replicas),
	}, nil
}

// findDeployment finds a deployment by name or app label
func findDeployment(ctx context.Context, k8sClient client.Client, namespace, nameOrApp string) (*appsv1.Deployment, error) {
	// Try direct lookup first
	deployment := &appsv1.Deployment{}
	err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: nameOrApp}, deployment)
	if err == nil {
		return deployment, nil
	}

	// Try by app label
	deploymentList := &appsv1.DeploymentList{}
	labels := map[string]string{"app": nameOrApp}
	if err := k8sClient.List(ctx, deploymentList, client.InNamespace(namespace), client.MatchingLabels(labels)); err != nil {
		return nil, fmt.Errorf("failed to list deployments: %w", err)
	}
	if len(deploymentList.Items) == 0 {
		return nil, fmt.Errorf("deployment not found: %s", nameOrApp)
	}
	return &deploymentList.Items[0], nil
}

func deploymentToInfo(d *appsv1.Deployment) *DeploymentInfo {
	info := &DeploymentInfo{
		Name:              d.Name,
		Namespace:         d.Namespace,
		ReadyReplicas:     d.Status.ReadyReplicas,
		UpdatedReplicas:   d.Status.UpdatedReplicas,
		AvailableReplicas: d.Status.AvailableReplicas,
	}

	if d.Spec.Replicas != nil {
		info.Replicas = *d.Spec.Replicas
	}

	if !d.CreationTimestamp.IsZero() {
		info.Age = FormatAge(time.Since(d.CreationTimestamp.Time))
	}

	// Get first container image
	if len(d.Spec.Template.Spec.Containers) > 0 {
		info.Image = d.Spec.Template.Spec.Containers[0].Image
	}

	return info
}
