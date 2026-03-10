# TinySystems Git Module

Git integration components for TinySystems. This module is in early development.

## Components

| Component | Description |
|-----------|-------------|
| Echo | Placeholder component for testing and development |

## Installation

```shell
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install git-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=ghcr.io/tiny-systems/git-module
```

## Run locally

```shell
go run cmd/main.go run --name=git-module --namespace=tinysystems --version=1.0.0
```

## Part of TinySystems

This module is part of the [TinySystems](https://github.com/tiny-systems) platform -- a visual flow-based automation engine running on Kubernetes.

## License

This module's source code is MIT-licensed. It depends on the [TinySystems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
