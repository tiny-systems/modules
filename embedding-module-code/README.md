# Tiny Systems Embedding Module

In-cluster text embeddings for Tiny Systems flows. Wraps HuggingFace text-embeddings-inference (TEI) and ships it as a curated bundle, so installing this module also provisions the TEI service in the same namespace — no external API, no separate helm install.

## Components

### embed_text

Takes a string, returns its dense vector embedding.

- **Input:** `{ context: any, text: string }`
- **Output:** `{ context: any, embedding: float32[], dims: int }`
- **Error:** `{ context: any, error: string }` when enabled

The component reads `TEI_URL` from env. The platform's install flow wires this automatically when the TEI bundle is enabled. To point at an external embedding endpoint, set `baseURL` in the node settings instead.

## Install

The module declares the `tei` bundle with `DefaultEnabled: true`, so a plain `helm upgrade --install` of the operator chart pulls TEI in as a subchart and starts a Deployment + Service alongside the module.

Default model: `BAAI/bge-small-en-v1.5` — 384-dim, CPU-friendly, decent quality for English RAG. Override via `--set bundles.tei.image.tag=cpu-1.5 --set bundles.tei.modelId=intfloat/multilingual-e5-large` if you need a different model.

The TEI Service lands at `<release-name>-tei:80` inside the namespace; the install flow sets `TEI_URL` on the module pod's env to match.

## Pairs with

- `database-module` vector_search / vector_upsert components (also via a bundle: `pgvector`) — embed + store + retrieve as a three-component RAG slice.
