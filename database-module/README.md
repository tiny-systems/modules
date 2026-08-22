# Tiny Systems Database Module

Postgres and Redis components for Tiny Systems flows.

## Components

| Name | Purpose |
|---|---|
| `postgres_exec` | Run INSERT/UPDATE/DELETE with positional parameters; emits `rowsAffected`. |
| `postgres_query` | Run SELECT; emits `rows[]` keyed by column name with a configurable row shape. |
| `vector_upsert` | Write an embedding row into a pgvector table; `INSERT ... ON CONFLICT (id) DO UPDATE`. Pair with `embedding-module` to build a RAG store. |
| `vector_search` | kNN over a pgvector column; returns top-K rows with normalised similarity scores and an optional JSONB metadata filter. Cosine / L2 / inner-product. |
| `redis_dedup` | Atomic "first seen" check via `SET NX EX`; routes new IDs to **out_new** and duplicates to **out_seen**. |
| `redis_set` | Set a key, with optional TTL and NX. |
| `redis_get` | Get a key, returns `found=false` for missing keys without raising an error. |

All components take their connection string (`dsn` for Postgres, `url` for Redis) **per message**, so a single deployed module can talk to many databases. Connections are pooled by DSN/URL across calls.

## Patterns

### Dedup-then-route

```
ticker → http_request(api) → json_decode → array_split → redis_dedup
                                                            ├── out_new  → … process and store ──→ postgres_exec
                                                            └── out_seen → drop
```

Use `redis_dedup` for "have I already processed this ID?" checks. `keyPrefix` + `id` form the composite key. TTL determines how long Redis remembers — set it longer than the polling cycle plus margin.

### Insert with parameters

```
redis_dedup:out_new → postgres_exec
                       sql: INSERT INTO matched_posts (id, source, title, score) VALUES ($1, $2, $3, $4)
                       params: ["{{$.id}}", "reddit", "{{$.context.title}}", "{{$.context.score}}"]
```

### Query with configurable row shape

In `postgres_query` settings, define the expected row shape:

```json
{
  "row": { "id": "abc", "title": "title", "score": 0 }
}
```

Downstream edges can then navigate `$.rows[0].title`, `$.count`, etc.

### RAG store: embed → upsert → search → chat

The two `vector_*` components assume the pgvector extension is installed and the target table already exists. A minimal schema for 384-dim embeddings (BGE-small):

```sql
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE memories (
  id        TEXT PRIMARY KEY,
  embedding VECTOR(384),
  metadata  JSONB
);

CREATE INDEX ON memories USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
```

**Ingest flow** — convert text to a vector and store it:

```
signal → embedding(embed_text) → vector_upsert
                                    table: memories
                                    id:    "{{$.context.docId}}"
                                    embedding: "{{$.embedding}}"
                                    metadata: { source: "{{$.context.source}}", text: "{{$.context.text}}" }
```

**Query flow** — embed the question, retrieve the closest rows, hand them to the chat model:

```
signal → embedding(query) → vector_search
                              table: memories
                              topK:  5
                              metric: cosine
                              metadataFilter: { source: "docs" }   # optional
        → llm_chat
              messages: [{ role: "user", content: "Context: {{$.results}}\n\nQuestion: {{$.context.question}}" }]
```

Distance metrics: `cosine` (default — best for semantic search), `l2` (Euclidean), `ip` (negative inner product, useful when embeddings are already normalised and you want pure dot-product ranking). The score is normalised to `[0, 1]` regardless of metric so flows can threshold without knowing which metric was chosen.

## Run Locally

```shell
go run cmd/main.go run \
  --name=tiny-systems/database-module-v0 \
  --namespace=tinysystems \
  --version=0.1.0
```

## License

MIT for this module's source. Depends on [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1).
