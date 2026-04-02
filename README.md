# Tiny Systems Crypto Module

Cryptographic operations for Tiny Systems flows. Generate TLS certificates, sign data, and manage keys.

## Components

| Component | Description |
|-----------|-------------|
| cert_generate | Generate self-signed TLS certificates with SANs. Use for K8s admission webhooks, internal HTTPS servers, or any TLS endpoint. |

## Installation

```shell
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install crypto-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=ghcr.io/tiny-systems/crypto-module
```

## Run locally

```shell
go run cmd/main.go run --name=tinysystems/crypto-module --namespace=tinysystems --version=1.0.0
```

## Part of Tiny Systems

This module is part of the [Tiny Systems](https://github.com/tiny-systems) platform — a visual flow-based programming engine for Kubernetes.

## License

This module's source code is MIT-licensed. It depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
