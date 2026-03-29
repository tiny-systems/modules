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
