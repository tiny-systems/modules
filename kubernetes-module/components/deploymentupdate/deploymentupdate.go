package deploymentupdate

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "deployment_update"
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

// ContainerImage specifies image update for a container
type ContainerImage struct {
	Name  string `json:"name" required:"true" title:"Container Name" description:"Name of the container to update"`
	Image string `json:"image" required:"true" title:"Image" description:"New image (e.g., nginx:1.21)"`
}

// EnvVar represents an environment variable
type EnvVar struct {
	Name  string `json:"name" required:"true" title:"Name" description:"Environment variable name"`
	Value string `json:"value" title:"Value" description:"Environment variable value"`
}

// ContainerEnv specifies env vars update for a container
type ContainerEnv struct {
	Name    string   `json:"name" required:"true" title:"Container Name" description:"Name of the container"`
	EnvVars []EnvVar `json:"envVars" title:"Environment Variables" description:"Environment variables to set (merged with existing)"`
}

// ResourceRequirements specifies resource limits/requests
type ResourceRequirements struct {
	CPURequest    string `json:"cpuRequest,omitempty" title:"CPU Request" description:"CPU request (e.g., 100m)"`
	CPULimit      string `json:"cpuLimit,omitempty" title:"CPU Limit" description:"CPU limit (e.g., 500m)"`
	MemoryRequest string `json:"memoryRequest,omitempty" title:"Memory Request" description:"Memory request (e.g., 128Mi)"`
	MemoryLimit   string `json:"memoryLimit,omitempty" title:"Memory Limit" description:"Memory limit (e.g., 512Mi)"`
}

// ContainerResources specifies resource update for a container
type ContainerResources struct {
	Name      string               `json:"name" required:"true" title:"Container Name" description:"Name of the container"`
	Resources ResourceRequirements `json:"resources" title:"Resources" description:"Resource requirements"`
}

// Request is the input to update a deployment
type Request struct {
	Context        Context              `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Namespace      string               `json:"namespace" required:"true" title:"Namespace" description:"Deployment namespace"`
	Name           string               `json:"name" required:"true" title:"Name" description:"Deployment name"`
	Replicas       *int32               `json:"replicas,omitempty" title:"Replicas" description:"Desired replica count"`
	Images         []ContainerImage     `json:"images,omitempty" title:"Images" description:"Container images to update"`
	EnvUpdates     []ContainerEnv       `json:"envUpdates,omitempty" title:"Environment Updates" description:"Container environment variables to update"`
	Resources      []ContainerResources `json:"resources,omitempty" title:"Resources" description:"Container resources to update"`
	Labels         map[string]string    `json:"labels,omitempty" configurable:"true" title:"Labels" description:"Labels to merge into deployment metadata"`
	Annotations    map[string]string    `json:"annotations,omitempty" configurable:"true" title:"Annotations" description:"Annotations to merge into deployment metadata"`
	PodLabels      map[string]string    `json:"podLabels,omitempty" configurable:"true" title:"Pod Labels" description:"Labels to merge into pod template"`
	PodAnnotations map[string]string    `json:"podAnnotations,omitempty" configurable:"true" title:"Pod Annotations" description:"Annotations to merge into pod template"`
}

// ContainerInfo represents container info in result
type ContainerInfo struct {
	Name  string `json:"name" title:"Name"`
	Image string `json:"image" title:"Image"`
}

// Result is the output after deployment update
type Result struct {
	Context       Context         `json:"context,omitempty" title:"Context"`
	Name          string          `json:"name" title:"Name"`
	Namespace     string          `json:"namespace" title:"Namespace"`
	Replicas      int32           `json:"replicas" title:"Replicas"`
	ReadyReplicas int32           `json:"readyReplicas" title:"Ready Replicas"`
	Containers    []ContainerInfo `json:"containers" title:"Containers"`
	Success       bool            `json:"success" title:"Success"`
	Message       string          `json:"message" title:"Message"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the deployment update
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
		Description: "Deployment Update",
		Info:        "Update a Kubernetes Deployment's images, environment variables, resources, replicas, and metadata.",
		Tags:        []string{"Kubernetes", "Deployments", "Update"},
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

	// Get current deployment
	deployment := &appsv1.Deployment{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: req.Namespace, Name: req.Name}, deployment); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("deployment not found: %w", k8s.ClassifyError(err)))
	}

	// Update replicas
	if req.Replicas != nil {
		deployment.Spec.Replicas = req.Replicas
	}

	// Update container images
	for _, imgUpdate := range req.Images {
		found := false
		for i := range deployment.Spec.Template.Spec.Containers {
			if deployment.Spec.Template.Spec.Containers[i].Name == imgUpdate.Name {
				deployment.Spec.Template.Spec.Containers[i].Image = imgUpdate.Image
				found = true
				break
			}
		}
		if !found {
			return c.handleError(ctx, handler, req, fmt.Errorf("container %q not found", imgUpdate.Name))
		}
	}

	// Update container environment variables
	for _, envUpdate := range req.EnvUpdates {
		found := false
		for i := range deployment.Spec.Template.Spec.Containers {
			if deployment.Spec.Template.Spec.Containers[i].Name == envUpdate.Name {
				found = true
				container := &deployment.Spec.Template.Spec.Containers[i]

				// Merge env vars
				for _, newEnv := range envUpdate.EnvVars {
					updated := false
					for j := range container.Env {
						if container.Env[j].Name == newEnv.Name {
							container.Env[j].Value = newEnv.Value
							updated = true
							break
						}
					}
					if !updated {
						container.Env = append(container.Env, corev1.EnvVar{
							Name:  newEnv.Name,
							Value: newEnv.Value,
						})
					}
				}
				break
			}
		}
		if !found {
			return c.handleError(ctx, handler, req, fmt.Errorf("container %q not found for env update", envUpdate.Name))
		}
	}

	// Update container resources
	for _, resUpdate := range req.Resources {
		found := false
		for i := range deployment.Spec.Template.Spec.Containers {
			if deployment.Spec.Template.Spec.Containers[i].Name == resUpdate.Name {
				found = true
				container := &deployment.Spec.Template.Spec.Containers[i]

				if container.Resources.Requests == nil {
					container.Resources.Requests = corev1.ResourceList{}
				}
				if container.Resources.Limits == nil {
					container.Resources.Limits = corev1.ResourceList{}
				}

				if resUpdate.Resources.CPURequest != "" {
					if q, err := resource.ParseQuantity(resUpdate.Resources.CPURequest); err == nil {
						container.Resources.Requests[corev1.ResourceCPU] = q
					}
				}
				if resUpdate.Resources.CPULimit != "" {
					if q, err := resource.ParseQuantity(resUpdate.Resources.CPULimit); err == nil {
						container.Resources.Limits[corev1.ResourceCPU] = q
					}
				}
				if resUpdate.Resources.MemoryRequest != "" {
					if q, err := resource.ParseQuantity(resUpdate.Resources.MemoryRequest); err == nil {
						container.Resources.Requests[corev1.ResourceMemory] = q
					}
				}
				if resUpdate.Resources.MemoryLimit != "" {
					if q, err := resource.ParseQuantity(resUpdate.Resources.MemoryLimit); err == nil {
						container.Resources.Limits[corev1.ResourceMemory] = q
					}
				}
				break
			}
		}
		if !found {
			return c.handleError(ctx, handler, req, fmt.Errorf("container %q not found for resource update", resUpdate.Name))
		}
	}

	// Merge deployment labels
	if len(req.Labels) > 0 {
		if deployment.Labels == nil {
			deployment.Labels = make(map[string]string)
		}
		for k, v := range req.Labels {
			deployment.Labels[k] = v
		}
	}

	// Merge deployment annotations
	if len(req.Annotations) > 0 {
		if deployment.Annotations == nil {
			deployment.Annotations = make(map[string]string)
		}
		for k, v := range req.Annotations {
			deployment.Annotations[k] = v
		}
	}

	// Merge pod template labels
	if len(req.PodLabels) > 0 {
		if deployment.Spec.Template.Labels == nil {
			deployment.Spec.Template.Labels = make(map[string]string)
		}
		for k, v := range req.PodLabels {
			deployment.Spec.Template.Labels[k] = v
		}
	}

	// Merge pod template annotations
	if len(req.PodAnnotations) > 0 {
		if deployment.Spec.Template.Annotations == nil {
			deployment.Spec.Template.Annotations = make(map[string]string)
		}
		for k, v := range req.PodAnnotations {
			deployment.Spec.Template.Annotations[k] = v
		}
	}

	// Update deployment
	if err := k8sClient.Update(ctx, deployment); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to update deployment: %w", k8s.ClassifyError(err)))
	}

	// Build result
	result := Result{
		Context:       req.Context,
		Name:          deployment.Name,
		Namespace:     deployment.Namespace,
		Replicas:      *deployment.Spec.Replicas,
		ReadyReplicas: deployment.Status.ReadyReplicas,
		Success:       true,
		Message:       fmt.Sprintf("Deployment %s/%s updated", req.Namespace, req.Name),
	}

	for _, c := range deployment.Spec.Template.Spec.Containers {
		result.Containers = append(result.Containers, ContainerInfo{
			Name:  c.Name,
			Image: c.Image,
		})
	}

	return handler(ctx, ResultPort, result)
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
			Name:  RequestPort,
			Label: "Request",
			Configuration: Request{
				Namespace: "default",
				Name:      "my-deployment",
				Images: []ContainerImage{
					{Name: "app", Image: "myapp:v2"},
				},
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Name:          "my-deployment",
				Namespace:     "default",
				Replicas:      3,
				ReadyReplicas: 3,
				Containers: []ContainerInfo{
					{Name: "app", Image: "myapp:v2"},
				},
				Success: true,
				Message: "Deployment default/my-deployment updated",
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
