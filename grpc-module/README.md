# Tiny Systems gRPC Module

gRPC client component with reflection-based service discovery.

## Components

| Component | Description |
|-----------|-------------|
| gRPC Call | Call unary gRPC methods, using server reflection to discover services, methods, and message schemas automatically |

### gRPC Call

- **Discovery** — services, methods and message schemas are read from the server via the gRPC reflection service (which must be enabled on the server); no proto files needed.
- **TLS by default** — connections use TLS with the system root certificates. Tick **Insecure mode** in the Connect settings to talk to plaintext (non-TLS) servers.
- **Auth & metadata** — each request can carry a **Bearer Token** (sent as `authorization: Bearer <token>` call metadata) and arbitrary key/value **Metadata Headers**. An explicit `authorization` header takes precedence over the bearer token. Metadata applies to method calls; the reflection/discovery connection is unauthenticated.
- **Keep Alive** — optional HTTP/2 keepalive pings (every 30s, 10s timeout, also while idle) for long-lived connections.
- **Unary only** — client, server and bidirectional streaming methods are not supported.

## Installation

```shell
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install grpc-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=ghcr.io/tiny-systems/grpc-module
```

## Run locally

```shell
go run cmd/main.go run --name=grpc-module --namespace=tinysystems --version=1.0.0
```

## Part of Tiny Systems

This module is part of the [Tiny Systems](https://github.com/tiny-systems) platform -- a visual flow-based automation engine running on Kubernetes.

## License

This module's source code is MIT-licensed. It depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
