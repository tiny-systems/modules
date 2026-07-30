# Tiny Systems Kubernetes Module

Kubernetes resource management components for cluster automation workflows.

## Components

| Component | Description |
|-----------|-------------|
| Pod Create | Create a pod for one-off tasks or image pull tests |
| Pod Watcher | Watch pod events in real time |
| Pod Status | Get current status of a pod |
| Pod Logs | Stream or fetch pod log output |
| Pod List | List pods with label/field selectors |
| Pod Delete | Delete a pod by name |
| Pod Update | Update pod spec or metadata |
| Deployment List | List deployments in a namespace |
| Deployment Scale | Scale a deployment's replica count |
| Deployment Update | Update deployment spec or metadata |
| Workload List | List workloads across resource types |
| Workload Restart | Trigger a rolling restart of a workload |
| StatefulSet List | List statefulsets in a namespace |
| DaemonSet List | List daemonsets in a namespace |
| Service List | List services in a namespace |
| Service Update | Update service spec or metadata |
| ConfigMap Patch | Patch individual keys in a ConfigMap |
| Secret Get | Read a Kubernetes Secret by name (regcred, TLS, opaque) |
| Event Watcher | Watch Kubernetes events in real time |
| Custom Resource List | List any Kubernetes resource by API version and kind (built-in or CRDs) |
| Sandbox Run | Runs a script in a throwaway Job and returns its output and exit code. Built for code an agent wrote: non-root, read-only root filesystem, no service-account token, dropped capabilities, CPU/memory limits, deleted when it finishes. |

## Notes

**`sandbox_run` does not restrict network egress.** The container is otherwise
locked down, but a script can reach whatever the cluster can. Run it in a
namespace with a restrictive NetworkPolicy when that matters — one is not
created for you.

**A non-zero exit is a normal result, not a failure.** It arrives on `result`
with its exit code; only infrastructure problems reach the error port. The wait
is capped at 240s, because a hop with no progress for six minutes is re-driven
and would run the script a second time.

**RBAC grew in 0.8.11.** `pods: create,delete`, `secrets: get,list`,
`events: watch`, `configmaps: get`, `statefulsets`, and `batch/jobs`. Most were
already being called by existing components and failing with a 403; the module
now declares what it uses. Review before upgrading if your cluster is strict.

## Installation

```shell
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install kubernetes-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=ghcr.io/tiny-systems/kubernetes-module
```

## Run locally

```shell
go run cmd/main.go run --name=kubernetes-module --namespace=tinysystems --version=1.0.0
```

## Part of Tiny Systems

This module is part of the [Tiny Systems](https://github.com/tiny-systems) platform -- a visual flow-based automation engine running on Kubernetes.

## License

This module's source code is MIT-licensed. It depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
