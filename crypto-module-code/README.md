# Tiny Systems Crypto Module

Cryptographic operations for Tiny Systems flows: self-signed TLS certificates, hash/HMAC digests, and inbound webhook signature verification.

## Components

| Component | Description |
|-----------|-------------|
| cert_generate | Generate self-signed TLS certificates with SANs. Use for K8s admission webhooks, internal HTTPS servers, or any TLS endpoint. |
| hmac_verify | Verify inbound webhook signatures — GitHub (X-Hub-Signature-256), Stripe (Stripe-Signature with replay window), or generic HMAC-SHA256/SHA1 (hex or base64). Emits {valid, reason} for routing; constant-time comparison. |
| hash | Compute sha256/sha512/sha1/md5 digests (hex or base64), plain or HMAC-keyed. Use for dedup keys, content fingerprints, and signing outbound webhook payloads. |

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
