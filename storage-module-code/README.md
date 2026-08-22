# Tiny Systems Storage Module

Blob transport for Tiny Systems flows: put, get, list, and presign objects in any S3-compatible store — AWS S3, MinIO, Cloudflare R2, GCS interop. This is how a flow attaches the CSV, stores the report, or reads the dump.

## Components

| Component | Description |
|-----------|-------------|
| blob_put | Upload an object — {bucket?, key, data, dataBase64?, contentType?} → {etag, size}. Text goes in as-is; binary via base64. Network failures are retryable: S3 PUT is idempotent for the same key + bytes. |
| blob_get | Download an object into the flow — {bucket?, key, maxBytes?, asBase64?} → {data, contentType, size}. Objects over maxBytes (default 10 MiB) are refused with a permanent error before any bytes move; binary comes back safely as base64. |
| blob_list | List objects by prefix — {bucket?, prefix?, max?} → {items: [{key, size, lastModified, etag}], truncated}. Default 100 items, hard cap 1000; truncated=true means narrow the prefix. |
| blob_presign | Mint a time-limited URL for one object — {bucket?, key, method GET/PUT, expirySeconds?} → {url}. Whoever holds the URL can download or upload that object without credentials; default 15 min, max 7 days. |

Every component carries the same settings port: endpoint, region, access key, secret key, SSL, and a default bucket. A bucket set on a request always wins over the settings default.

## Worked example: publish a report as a Slack link

Fetch a report, store it, and share a download link — the report bytes ride the flow once and never pass through Slack:

1. **http_call** fetches the report (`GET https://internal-api/reports/weekly.csv`).
2. **blob_put** stores it: `{key: "reports/2026-08/weekly.csv", data: $.body, contentType: "text/csv"}` → `{etag, size}`.
3. **blob_presign** mints the link: `{key: "reports/2026-08/weekly.csv", method: "GET", expirySeconds: 86400}` → `{url}`.
4. **slack_send** posts it: `"Weekly report ready (24h link): " + $.url`.

The reverse direction works the same way: **blob_list** the `incoming/` prefix, **blob_get** each key (with `asBase64: true` for binary), and process the contents in-flow.

## Installation

```shell
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install storage-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=ghcr.io/tiny-systems/storage-module
```

## Run locally

```shell
go run cmd/main.go run --name=tinysystems/storage-module --namespace=tinysystems --version=1.0.0
```

## Part of Tiny Systems

This module is part of the [Tiny Systems](https://github.com/tiny-systems) platform — a visual flow-based programming engine for Kubernetes.

## License

This module's source code is MIT-licensed. It depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
