# Tiny Systems Encoding Module

Data encoding, decoding, and templating components.

## Components

| Component | Description |
|-----------|-------------|
| JSON Encode | Serialize data to JSON |
| JSON Decode | Parse JSON string into structured data |
| XML Encode | Serialize data to XML |
| JWT Encoder | Create signed JSON Web Tokens |
| JWT Decoder | Verify and decode JSON Web Tokens |
| Go Template Engine | Render output using Go `text/template` syntax |

## Installation

```shell
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install encoding-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=ghcr.io/tiny-systems/encoding-module
```

## Run locally

```shell
go run cmd/main.go run --name=encoding-module --namespace=tinysystems --version=1.0.0
```

## Part of Tiny Systems

This module is part of the [Tiny Systems](https://github.com/tiny-systems) platform -- a visual flow-based automation engine running on Kubernetes.

## License

This module's source code is MIT-licensed. It depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
