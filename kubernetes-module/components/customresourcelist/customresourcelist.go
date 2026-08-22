package customresourcelist

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "custom_resource_list"
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

// Request is the input to list custom resources
type Request struct {
	Context       Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	APIVersion    string  `json:"apiVersion" required:"true" title:"API Version" description:"API version (e.g., cert-manager.io/v1, v1, apps/v1)"`
	Kind          string  `json:"kind" required:"true" title:"Kind" description:"Resource kind (e.g., Certificate, ConfigMap, Ingress)"`
	Namespace     string  `json:"namespace,omitempty" title:"Namespace" description:"Filter to specific namespace (empty = all namespaces)"`
	LabelSelector string  `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter by labels (e.g., app=myapp)"`
}

// ResourceItem represents a single custom resource
type ResourceItem struct {
	Name              string            `json:"name" title:"Name"`
	Namespace         string            `json:"namespace" title:"Namespace"`
	Kind              string            `json:"kind" title:"Kind"`
	APIVersion        string            `json:"apiVersion" title:"API Version"`
	Labels            map[string]string `json:"labels,omitempty" title:"Labels"`
	Annotations       map[string]string `json:"annotations,omitempty" title:"Annotations"`
	CreationTimestamp string            `json:"creationTimestamp" title:"Created"`
	Age               string            `json:"age" title:"Age"`
	Spec              map[string]any    `json:"spec,omitempty" title:"Spec"`
	Status            map[string]any    `json:"status,omitempty" title:"Status"`
}

// Result is the output with resource list
type Result struct {
	Context Context        `json:"context,omitempty" title:"Context"`
	Items   []ResourceItem `json:"items" title:"Items"`
	Count   int            `json:"count" title:"Count"`
}

// Error output
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Component implements the custom resource list fetcher
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
		Description: "Custom Resource List",
		Info:        "Lists any Kubernetes resource by API version and kind. Works with built-in resources (Pods, Deployments) and custom resources (Certificates, VirtualServices). Returns name, namespace, labels, spec, and status for each item. CRDs outside the module's built-in RBAC must be granted at install time via the chart's rbac.extraRules values; a 403 from this component includes the exact helm command.",
		Tags:        []string{"Kubernetes", "CRD", "Custom Resource", "List"},
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

	if req.APIVersion == "" || req.Kind == "" {
		return c.handleError(ctx, handler, req, errors.New("apiVersion and kind are required"))
	}

	gvk := schema.FromAPIVersionAndKind(req.APIVersion, req.Kind)

	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind + "List",
	})

	listOpts := []client.ListOption{}
	if req.Namespace != "" {
		listOpts = append(listOpts, client.InNamespace(req.Namespace))
	}
	if req.LabelSelector != "" {
		selector, err := labels.Parse(req.LabelSelector)
		if err != nil {
			return c.handleError(ctx, handler, req, fmt.Errorf("invalid label selector: %v", err))
		}
		listOpts = append(listOpts, client.MatchingLabelsSelector{Selector: selector})
	}

	if err := k8sClient.List(ctx, list, listOpts...); err != nil {
		if apierrors.IsForbidden(err) {
			return c.handleError(ctx, handler, req, forbiddenHint(gvk, err))
		}
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to list %s.%s: %w", gvk.Kind, gvk.Group, k8s.ClassifyError(err)))
	}

	items := make([]ResourceItem, 0, len(list.Items))
	for _, u := range list.Items {
		content := u.UnstructuredContent()

		spec, _ := content["spec"].(map[string]any)
		status, _ := content["status"].(map[string]any)

		items = append(items, ResourceItem{
			Name:              u.GetName(),
			Namespace:         u.GetNamespace(),
			Kind:              u.GetKind(),
			APIVersion:        u.GetAPIVersion(),
			Labels:            u.GetLabels(),
			Annotations:       u.GetAnnotations(),
			CreationTimestamp: u.GetCreationTimestamp().Format(time.RFC3339),
			Age:               formatAge(u.GetCreationTimestamp().Time),
			Spec:              spec,
			Status:            status,
		})
	}

	return handler(ctx, ResultPort, Result{
		Context: req.Context,
		Items:   items,
		Count:   len(items),
	})
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

// forbiddenHint turns an RBAC 403 into an actionable operator message. The
// module's ClusterRole deliberately carries no cluster-wide read wildcard, so
// resources outside the built-in rules must be granted at install time via the
// chart's rbac.extraRules values — the returned error spells out the command.
func forbiddenHint(gvk schema.GroupVersionKind, err error) error {
	group := gvk.Group
	groupDisplay := group
	if groupDisplay == "" {
		groupDisplay = "core"
	}
	// helm --set list syntax: core group is the empty string.
	groupValue := group
	if groupValue == "" {
		groupValue = `""`
	}
	plural := resourcePlural(gvk, err)
	return fmt.Errorf(
		"listing %s/%s is not covered by this module's RBAC — grant it at install time: "+
			"helm upgrade <release> tinysystems/tinysystems-operator --reuse-values "+
			"--set 'rbac.extraRules[0].apiGroups={%s}' "+
			"--set 'rbac.extraRules[0].resources={%s}' "+
			"--set 'rbac.extraRules[0].verbs={get,list,watch}'",
		groupDisplay, gvk.Kind, groupValue, plural)
}

// resourcePlural extracts the resource plural from the apiserver's Forbidden
// status details (the RBAC filter reports the resource it denied), falling
// back to naive lowercase pluralization when the details are absent.
func resourcePlural(gvk schema.GroupVersionKind, err error) string {
	var statusErr *apierrors.StatusError
	if errors.As(err, &statusErr) {
		if details := statusErr.Status().Details; details != nil && details.Kind != "" {
			return details.Kind
		}
	}
	return strings.ToLower(gvk.Kind) + "s"
}

func formatAge(t time.Time) string {
	d := time.Since(t)
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
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
				APIVersion: "cert-manager.io/v1",
				Kind:       "Certificate",
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Items: []ResourceItem{
					{
						Name:              "my-certificate",
						Namespace:         "default",
						Kind:              "Certificate",
						APIVersion:        "cert-manager.io/v1",
						Labels:            map[string]string{"app": "myapp"},
						CreationTimestamp: "2025-01-01T00:00:00Z",
						Age:               "30d",
						Spec: map[string]any{
							"secretName": "my-cert-tls",
							"dnsNames":   []any{"example.com"},
						},
						Status: map[string]any{
							"notAfter": "2025-04-01T00:00:00Z",
							"conditions": []any{
								map[string]any{"type": "Ready", "status": "True"},
							},
						},
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
