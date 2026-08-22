package testharness

import (
	"context"
	"sync"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/state"
	"github.com/tiny-systems/module/pkg/utils"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// PortMsg captures a single output sent by a component via handler()
type PortMsg struct {
	Port string
	Data any
}

// nodeName/Namespace used by the harness's fake client. Tests don't need
// to care; State scopes to this constant pair internally.
const (
	harnessNodeName  = "test-node"
	harnessNamespace = "default"
)

// Harness wraps a component for testing.
// Captures all port outputs and manages metadata persistence.
type Harness struct {
	component module.Component
	mu        sync.Mutex
	Metadata  map[string]string
	Outputs   []PortMsg

	// k8sClient backs the State primitive. Reads of TinyNode go through it;
	// updates to status.metadata are mirrored from h.Handler's reconcile
	// updaters into the fake client.
	k8sClient client.Client
}

// New creates a harness for the given component instance. The harness
// mirrors the scheduler's capability injection so components written
// against the new lifecycle interfaces (OnSettings, OnReconcile, OnControl,
// OnEmitter, OnState, etc.) work in tests the same way they do in the runner.
func New(c module.Component) *Harness {
	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	node := &v1alpha1.TinyNode{}
	node.Name = harnessNodeName
	node.Namespace = harnessNamespace
	node.Status.Metadata = map[string]string{}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithStatusSubresource(&v1alpha1.TinyNode{}).
		Build()

	h := &Harness{
		component: c,
		Metadata:  map[string]string{},
		k8sClient: cl,
	}
	if e, ok := c.(module.EmitterAware); ok {
		e.OnEmitter(h.Handler)
	}
	if s, ok := c.(module.Stateful); ok {
		s.OnState(state.NewMetadataState(h.k8sClient,
			types.NamespacedName{Name: harnessNodeName, Namespace: harnessNamespace},
			h.Handler,
		))
	}
	return h
}

// Handler implements module.Handler, capturing outputs and metadata writes.
// Reconcile-port updaters are applied to both h.Metadata (legacy hand-rolled
// access) and the fake K8s client (so State.Get sees them).
func (h *Harness) Handler(ctx context.Context, port string, data any) module.Result {
	if port == v1alpha1.ReconcilePort {
		if fn, ok := data.(func(*v1alpha1.TinyNode) error); ok {
			h.mu.Lock()
			node := &v1alpha1.TinyNode{
				Status: v1alpha1.TinyNodeStatus{Metadata: h.Metadata},
			}
			if err := fn(node); err != nil {
				h.mu.Unlock()
				return module.Fail(err)
			}
			h.Metadata = node.Status.Metadata
			h.mu.Unlock()

			// Mirror into the fake client so State reads see it.
			if h.k8sClient != nil {
				key := types.NamespacedName{Name: harnessNodeName, Namespace: harnessNamespace}
				var live v1alpha1.TinyNode
				if err := h.k8sClient.Get(ctx, key, &live); err == nil {
					live.Status.Metadata = h.Metadata
					_ = h.k8sClient.Status().Update(ctx, &live)
				}
			}
		}
		return module.Result{}
	}
	h.mu.Lock()
	h.Outputs = append(h.Outputs, PortMsg{Port: port, Data: data})
	h.mu.Unlock()
	return module.Result{}
}

// LeaderHandler returns a handler with IsLeader=true in context.
func (h *Harness) LeaderHandler(ctx context.Context, port string, data any) module.Result {
	return h.Handler(utils.WithLeader(ctx, true), port, data)
}

// Handle sends a message to the component on the given port. For system
// ports, dispatches through the matching capability interface when the
// component implements one (mirroring scheduler.Update's Phase 1). For
// other ports, falls through to Component.Handle.
func (h *Harness) Handle(ctx context.Context, port string, msg any) module.Result {
	return h.dispatch(ctx, port, msg)
}

// HandleAsLeader sends a message with IsLeader=true in context.
func (h *Harness) HandleAsLeader(ctx context.Context, port string, msg any) module.Result {
	return h.dispatch(utils.WithLeader(ctx, true), port, msg)
}

// Reconcile simulates a reconcile event with current metadata.
func (h *Harness) Reconcile(ctx context.Context) module.Result {
	h.mu.Lock()
	meta := copyMap(h.Metadata)
	h.mu.Unlock()
	return h.dispatch(ctx, v1alpha1.ReconcilePort, v1alpha1.TinyNode{
		Status: v1alpha1.TinyNodeStatus{Metadata: meta},
	})
}

// ReconcileAsLeader simulates a reconcile with IsLeader=true.
func (h *Harness) ReconcileAsLeader(ctx context.Context) module.Result {
	h.mu.Lock()
	meta := copyMap(h.Metadata)
	h.mu.Unlock()
	return h.dispatch(utils.WithLeader(ctx, true), v1alpha1.ReconcilePort, v1alpha1.TinyNode{
		Status: v1alpha1.TinyNodeStatus{Metadata: meta},
	})
}

// dispatch routes the call to a capability interface for system ports when
// the component implements one, or to Component.Handle otherwise. This
// mirrors what scheduler.Update + dispatchCapability do at runtime.
func (h *Harness) dispatch(ctx context.Context, port string, msg any) module.Result {
	switch port {
	case v1alpha1.SettingsPort:
		if c, ok := h.component.(module.SettingsHandler); ok {
			if err := c.OnSettings(ctx, msg); err != nil {
				return module.Fail(err)
			}
			return module.Result{}
		}
	case v1alpha1.ReconcilePort:
		if c, ok := h.component.(module.ReconcileHandler); ok {
			node, _ := msg.(v1alpha1.TinyNode)
			if err := c.OnReconcile(ctx, node); err != nil {
				return module.Fail(err)
			}
			return module.Result{}
		}
	case v1alpha1.ControlPort:
		if c, ok := h.component.(module.ControlHandler); ok {
			if err := c.OnControl(ctx, msg); err != nil {
				return module.Fail(err)
			}
			return module.Result{}
		}
	case v1alpha1.IdentityPort:
		if c, ok := h.component.(module.IdentityAware); ok {
			id, _ := msg.(v1alpha1.NodeIdentity)
			c.OnIdentity(id)
			return module.Result{}
		}
	case v1alpha1.ClientPort:
		if c, ok := h.component.(module.ClientAware); ok {
			client, _ := msg.(module.K8sClient)
			c.OnClient(client)
			return module.Result{}
		}
	}
	return h.component.Handle(ctx, h.Handler, port, msg)
}

// NewPod simulates a pod restart: fresh component instance, same metadata.
// Reinjects all SDK capabilities on the new instance so it behaves like a
// freshly scheduled runner.
func (h *Harness) NewPod() *Harness {
	h.mu.Lock()
	meta := copyMap(h.Metadata)
	h.mu.Unlock()

	scheme := runtime.NewScheme()
	_ = v1alpha1.AddToScheme(scheme)
	node := &v1alpha1.TinyNode{}
	node.Name = harnessNodeName
	node.Namespace = harnessNamespace
	node.Status.Metadata = copyMap(meta)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node).
		WithStatusSubresource(&v1alpha1.TinyNode{}).
		Build()

	pod := &Harness{
		component: h.component.Instance(),
		Metadata:  meta,
		k8sClient: cl,
	}
	if e, ok := pod.component.(module.EmitterAware); ok {
		e.OnEmitter(pod.Handler)
	}
	if s, ok := pod.component.(module.Stateful); ok {
		s.OnState(state.NewMetadataState(pod.k8sClient,
			types.NamespacedName{Name: harnessNodeName, Namespace: harnessNamespace},
			pod.Handler,
		))
	}
	return pod
}

// PortOutputs returns all data sent to the given port.
func (h *Harness) PortOutputs(port string) []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	var result []any
	for _, o := range h.Outputs {
		if o.Port == port {
			result = append(result, o.Data)
		}
	}
	return result
}

// Reset clears captured outputs (but keeps metadata).
func (h *Harness) Reset() {
	h.mu.Lock()
	h.Outputs = nil
	h.mu.Unlock()
}

// Ports returns the component's current port definitions.
func (h *Harness) Ports() []module.Port {
	return h.component.Ports()
}

func copyMap(m map[string]string) map[string]string {
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
