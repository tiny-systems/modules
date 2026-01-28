package k8s

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ResourceInfo contains info about a Kubernetes workload resource
type ResourceInfo struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Kind      string            `json:"kind"` // Deployment, StatefulSet, DaemonSet
	Labels    map[string]string `json:"labels"`
	Replicas  int32             `json:"replicas"` // desired
	Ready     int32             `json:"ready"`    // ready count
	Image     string            `json:"image"`    // first container image
	Age       string            `json:"age"`
}

// ResourceList contains a list of resources
type ResourceList struct {
	Resources []ResourceInfo `json:"resources"`
	Count     int            `json:"count"`
}

// ListResourcesRequest contains options for listing resources
type ListResourcesRequest struct {
	Kinds         []string // Deployment, StatefulSet, DaemonSet
	Namespace     string   // empty = all namespaces
	LabelSelector string   // label expression (optional)
}

// ListResources lists workload resources across namespaces
func ListResources(ctx context.Context, k8sClient client.Client, req ListResourcesRequest) (*ResourceList, error) {
	resources := make([]ResourceInfo, 0)

	// Default to all workload types if none specified
	kinds := req.Kinds
	if len(kinds) == 0 {
		kinds = []string{"Deployment", "StatefulSet", "DaemonSet"}
	}

	// Parse label selector
	var labelMap map[string]string
	var err error
	if req.LabelSelector != "" {
		labelMap, err = ParseLabelSelector(req.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("invalid label selector: %w", err)
		}
	}

	// Build list options
	opts := []client.ListOption{}
	if req.Namespace != "" {
		opts = append(opts, client.InNamespace(req.Namespace))
	}
	if len(labelMap) > 0 {
		opts = append(opts, client.MatchingLabels(labelMap))
	}

	for _, kind := range kinds {
		switch kind {
		case "Deployment":
			items, err := listDeployments(ctx, k8sClient, opts)
			if err != nil {
				return nil, err
			}
			resources = append(resources, items...)

		case "StatefulSet":
			items, err := listStatefulSets(ctx, k8sClient, opts)
			if err != nil {
				return nil, err
			}
			resources = append(resources, items...)

		case "DaemonSet":
			items, err := listDaemonSets(ctx, k8sClient, opts)
			if err != nil {
				return nil, err
			}
			resources = append(resources, items...)
		}
	}

	// Sort by namespace, then kind, then name
	sort.Slice(resources, func(i, j int) bool {
		if resources[i].Namespace != resources[j].Namespace {
			return resources[i].Namespace < resources[j].Namespace
		}
		if resources[i].Kind != resources[j].Kind {
			return resources[i].Kind < resources[j].Kind
		}
		return resources[i].Name < resources[j].Name
	})

	return &ResourceList{
		Resources: resources,
		Count:     len(resources),
	}, nil
}

func listDeployments(ctx context.Context, k8sClient client.Client, opts []client.ListOption) ([]ResourceInfo, error) {
	list := &appsv1.DeploymentList{}
	if err := k8sClient.List(ctx, list, opts...); err != nil {
		return nil, fmt.Errorf("failed to list deployments: %w", err)
	}

	resources := make([]ResourceInfo, 0, len(list.Items))
	for _, d := range list.Items {
		info := ResourceInfo{
			Name:      d.Name,
			Namespace: d.Namespace,
			Kind:      "Deployment",
			Labels:    d.Labels,
			Ready:     d.Status.ReadyReplicas,
		}
		if d.Spec.Replicas != nil {
			info.Replicas = *d.Spec.Replicas
		}
		if len(d.Spec.Template.Spec.Containers) > 0 {
			info.Image = d.Spec.Template.Spec.Containers[0].Image
		}
		if !d.CreationTimestamp.IsZero() {
			info.Age = FormatAge(time.Since(d.CreationTimestamp.Time))
		}
		resources = append(resources, info)
	}
	return resources, nil
}

func listStatefulSets(ctx context.Context, k8sClient client.Client, opts []client.ListOption) ([]ResourceInfo, error) {
	list := &appsv1.StatefulSetList{}
	if err := k8sClient.List(ctx, list, opts...); err != nil {
		return nil, fmt.Errorf("failed to list statefulsets: %w", err)
	}

	resources := make([]ResourceInfo, 0, len(list.Items))
	for _, s := range list.Items {
		info := ResourceInfo{
			Name:      s.Name,
			Namespace: s.Namespace,
			Kind:      "StatefulSet",
			Labels:    s.Labels,
			Ready:     s.Status.ReadyReplicas,
		}
		if s.Spec.Replicas != nil {
			info.Replicas = *s.Spec.Replicas
		}
		if len(s.Spec.Template.Spec.Containers) > 0 {
			info.Image = s.Spec.Template.Spec.Containers[0].Image
		}
		if !s.CreationTimestamp.IsZero() {
			info.Age = FormatAge(time.Since(s.CreationTimestamp.Time))
		}
		resources = append(resources, info)
	}
	return resources, nil
}

func listDaemonSets(ctx context.Context, k8sClient client.Client, opts []client.ListOption) ([]ResourceInfo, error) {
	list := &appsv1.DaemonSetList{}
	if err := k8sClient.List(ctx, list, opts...); err != nil {
		return nil, fmt.Errorf("failed to list daemonsets: %w", err)
	}

	resources := make([]ResourceInfo, 0, len(list.Items))
	for _, d := range list.Items {
		info := ResourceInfo{
			Name:      d.Name,
			Namespace: d.Namespace,
			Kind:      "DaemonSet",
			Labels:    d.Labels,
			Replicas:  d.Status.DesiredNumberScheduled,
			Ready:     d.Status.NumberReady,
		}
		if len(d.Spec.Template.Spec.Containers) > 0 {
			info.Image = d.Spec.Template.Spec.Containers[0].Image
		}
		if !d.CreationTimestamp.IsZero() {
			info.Age = FormatAge(time.Since(d.CreationTimestamp.Time))
		}
		resources = append(resources, info)
	}
	return resources, nil
}
