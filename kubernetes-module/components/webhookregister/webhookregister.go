package webhookregister

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "webhook_register"
	RequestPort   = "request"
	ResultPort    = "result"
	ErrorPort     = "error"
)

type Context any

type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

type Rule struct {
	Operations  []string `json:"operations" title:"Operations" description:"CREATE, UPDATE, DELETE" required:"true"`
	APIGroups   []string `json:"apiGroups" title:"API Groups" description:"e.g. apps, '' (core)"`
	APIVersions []string `json:"apiVersions" title:"API Versions" description:"e.g. v1"`
	Resources   []string `json:"resources" title:"Resources" description:"e.g. deployments, pods"`
}

type Request struct {
	Context           Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through"`
	Name              string  `json:"name" required:"true" title:"Name" description:"Webhook configuration name" colSpan:"col-span-6"`
	Operation         string  `json:"operation,omitempty" title:"Operation" description:"register or delete (default: register)" enum:"register,delete" enumTitles:"Register,Delete" colSpan:"col-span-6"`
	ServiceName       string  `json:"serviceName" required:"true" title:"Service Name" description:"Target K8s service that handles webhook requests" colSpan:"col-span-6"`
	ServicePort       int     `json:"servicePort,omitempty" title:"Service Port" description:"Port on the target service (default: 443)" colSpan:"col-span-6"`
	ServiceNamespace  string  `json:"serviceNamespace" required:"true" title:"Service Namespace" description:"Namespace of the target service" colSpan:"col-span-6"`
	ServicePath       string  `json:"servicePath,omitempty" title:"Service Path" description:"URL path on the service (default: /)" colSpan:"col-span-6"`
	CABundle          string  `json:"caBundle,omitempty" title:"CA Bundle" description:"Base64-encoded CA certificate for self-signed TLS. Leave empty for public CAs." format:"textarea"`
	NamespaceSelector string  `json:"namespaceSelector,omitempty" title:"Namespace Label" description:"Only intercept namespaces with this label (e.g. image-policy=enabled)"`
	FailurePolicy     string  `json:"failurePolicy,omitempty" title:"Failure Policy" description:"Ignore or Fail (default: Ignore)" enum:"Ignore,Fail" enumTitles:"Ignore (safe),Fail (strict)"`
	Rules             []Rule  `json:"rules" required:"true" title:"Rules" description:"What to intercept"`
}

type Result struct {
	Context   Context `json:"context,omitempty" title:"Context"`
	Name      string  `json:"name" title:"Name"`
	Operation string  `json:"operation" title:"Operation"`
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
		Description: "Webhook Register",
		Info:        "Register or delete a Kubernetes MutatingWebhookConfiguration. Use to subscribe to cluster events at the admission level — intercept resource creation/updates before they happen.",
		Tags:        []string{"Kubernetes", "Webhook", "Admission", "Policy"},
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

	op := req.Operation
	if op == "" {
		op = "register"
	}

	switch op {
	case "register":
		return c.handleRegister(ctx, handler, k8sClient, req)
	case "delete":
		return c.handleDelete(ctx, handler, k8sClient, req)
	default:
		return c.handleError(ctx, handler, req, fmt.Errorf("unknown operation: %s", op))
	}
}

func (c *Component) handleRegister(ctx context.Context, handler module.Handler, k8sClient client.Client, req Request) module.Result {
	path := req.ServicePath
	if path == "" {
		path = "/"
	}

	failurePolicy := admissionregistrationv1.Ignore
	if req.FailurePolicy == "Fail" {
		failurePolicy = admissionregistrationv1.Fail
	}

	sideEffects := admissionregistrationv1.SideEffectClassNoneOnDryRun

	var rules []admissionregistrationv1.RuleWithOperations
	for _, r := range req.Rules {
		var ops []admissionregistrationv1.OperationType
		for _, o := range r.Operations {
			ops = append(ops, admissionregistrationv1.OperationType(o))
		}
		rules = append(rules, admissionregistrationv1.RuleWithOperations{
			Operations: ops,
			Rule: admissionregistrationv1.Rule{
				APIGroups:   r.APIGroups,
				APIVersions: r.APIVersions,
				Resources:   r.Resources,
			},
		})
	}

	servicePort := int32(443)
	if req.ServicePort > 0 {
		servicePort = int32(req.ServicePort)
	}

	webhook := admissionregistrationv1.MutatingWebhook{
		Name:                    req.Name + ".tinysystems.io",
		AdmissionReviewVersions: []string{"v1"},
		ClientConfig: admissionregistrationv1.WebhookClientConfig{
			Service: &admissionregistrationv1.ServiceReference{
				Name:      req.ServiceName,
				Namespace: req.ServiceNamespace,
				Path:      &path,
				Port:      &servicePort,
			},
		},
		Rules:         rules,
		FailurePolicy: &failurePolicy,
		SideEffects:   &sideEffects,
	}

	if req.CABundle != "" {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(req.CABundle))
		if err == nil {
			webhook.ClientConfig.CABundle = decoded
		} else {
			// Treat as raw PEM
			webhook.ClientConfig.CABundle = []byte(req.CABundle)
		}
	}

	if req.NamespaceSelector != "" {
		parts := splitLabel(req.NamespaceSelector)
		if len(parts) == 2 {
			webhook.NamespaceSelector = &metav1.LabelSelector{
				MatchLabels: map[string]string{parts[0]: parts[1]},
			}
		}
	}

	config := &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: req.Name,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "tinysystems",
			},
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{webhook},
	}

	existing := &admissionregistrationv1.MutatingWebhookConfiguration{}
	err := k8sClient.Get(ctx, client.ObjectKey{Name: req.Name}, existing)

	if k8serrors.IsNotFound(err) {
		if err := k8sClient.Create(ctx, config); err != nil {
			return c.handleError(ctx, handler, req, fmt.Errorf("failed to create webhook: %w", k8s.ClassifyError(err)))
		}
		return handler(ctx, ResultPort, Result{
			Context:   req.Context,
			Name:      req.Name,
			Operation: "register",
			Success:   true,
			Message:   fmt.Sprintf("MutatingWebhookConfiguration %s created", req.Name),
		})
	}

	if err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to get webhook: %w", k8s.ClassifyError(err)))
	}

	existing.Webhooks = config.Webhooks
	existing.Labels = config.Labels
	if err := k8sClient.Update(ctx, existing); err != nil {
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to update webhook: %w", k8s.ClassifyError(err)))
	}

	return handler(ctx, ResultPort, Result{
		Context:   req.Context,
		Name:      req.Name,
		Operation: "register",
		Success:   true,
		Message:   fmt.Sprintf("MutatingWebhookConfiguration %s updated", req.Name),
	})
}

func (c *Component) handleDelete(ctx context.Context, handler module.Handler, k8sClient client.Client, req Request) module.Result {
	config := &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: req.Name},
	}

	if err := k8sClient.Delete(ctx, config); err != nil {
		if k8serrors.IsNotFound(err) {
			return handler(ctx, ResultPort, Result{
				Context:   req.Context,
				Name:      req.Name,
				Operation: "delete",
				Success:   true,
				Message:   fmt.Sprintf("MutatingWebhookConfiguration %s not found (no-op)", req.Name),
			})
		}
		return c.handleError(ctx, handler, req, fmt.Errorf("failed to delete webhook: %w", k8s.ClassifyError(err)))
	}

	return handler(ctx, ResultPort, Result{
		Context:   req.Context,
		Name:      req.Name,
		Operation: "delete",
		Success:   true,
		Message:   fmt.Sprintf("MutatingWebhookConfiguration %s deleted", req.Name),
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
			Name:  RequestPort,
			Label: "Request",
			Configuration: Request{
				Name:             "image-policy",
				ServiceName:      "tinysystems-http-module-v0-manager-service",
				ServicePort:      37077,
				ServiceNamespace: "tinysystems-tinysystems",
				ServicePath:      "/",
				FailurePolicy:    "Ignore",
				Rules: []Rule{
					{
						Operations:  []string{"CREATE", "UPDATE"},
						APIGroups:   []string{"apps"},
						APIVersions: []string{"v1"},
						Resources:   []string{"deployments"},
					},
				},
			},
			Position: module.Left,
		},
		{
			Name:   ResultPort,
			Label:  "Result",
			Source: true,
			Configuration: Result{
				Name:      "image-policy",
				Operation: "register",
				Success:   true,
				Message:   "MutatingWebhookConfiguration image-policy created",
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

func splitLabel(s string) []string {
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return []string{s[:i], s[i+1:]}
		}
	}
	return []string{s}
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.ClientAware     = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
