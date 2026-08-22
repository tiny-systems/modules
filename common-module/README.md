# Tiny Systems Common Module

Core building blocks for flow-based automations on the Tiny Systems platform.

## Components

| Component | Description |
|-----------|-------------|
| Router | Conditional message routing based on expressions |
| Debug | Inspect and log messages passing through a flow |
| Cron | Schedule-based flow trigger (cron expressions) |
| Signal | Manual flow trigger with configurable context |
| Delay | Pause execution for a specified duration |
| Array Get | Extract elements from an array by index |
| Ticker | Interval-based recurring trigger |
| Group By | Group incoming messages by key |
| Async | Non-blocking message passthrough (fire-and-forget) |
| Split Array | Split an array into individual messages, each carrying `index` and `total` for downstream fan-in |
| Collect | Fan-in counterpart to `array_split` -- buffers items per group key in the node's State and emits the reassembled, index-ordered array once all `total` items arrive; incomplete groups time out onto the error port |
| Inject | Data enrichment -- merge additional data into messages |
| `transform` | Modify -- transform and reshape message data |
| Key-Value Store | State-backed key-value storage for flow state -- records persist in the node's State and are multi-replica safe |
| `run_start` | Start a durable run and reply immediately with the run id while the work continues |
| `run_status` | Query a run's status by id -- complete, failed, or steps still pending |
| Retry | Explicit retry supervisor with bounded attempts and configurable backoff. Wire to the error port of `llm_*` / `http_request` / database components; routes back to the original input until success or attempt limit. |
| Ask a human | Presents a form and waits for a person to answer. The form is a JSON Schema you author, so the same component asks "approve this restart?" or "how many replicas?". Concurrent questions queue FIFO and persist in the node's State across restarts; unanswered questions can time out onto the error port. Put it in front of anything destructive. |
| Budget Guard | Bounds an agent loop by iterations, tokens and cost. Emits on `proceed` while within budget, `exceeded` once past it. |
| Flow Telemetry | Reads the platform's own execution traces, so a flow can inspect how flows are running — list runs in a window, or fetch one run's hops to find where it broke. |

## Notes

**`ask` does not park the run.** A request publishes the form and returns; the
answer arrives later as a separate hop and starts the downstream branch itself.
Continuity is the published form, not a held message.

**`ask` queues and persists questions.** The control widget renders one form
per node, so concurrent requests queue FIFO: the oldest question is presented
first and answering it reveals the next (the form notes how many wait behind
it). Pending questions live in the node's State — same backing as `kv` and
`collect` — so they survive pod restarts and are multi-replica safe. With
`timeoutSeconds` set (0 = wait forever, the default), unanswered questions
expire onto the error port as `{context, error}`. Expiry is passive, checked
on traffic and on reconcile ticks — a fully idle node holds an expired
question until the next event. Enable the error port when a timeout is set;
without it expired questions are dropped with only a log line.

**`ask` renders through the generic JSON editor.** It is functional rather than
pretty — a dedicated form widget is not built yet. Because `build_flow`
validates against the component catalog rather than a node's live settings,
edges reading a custom form's own fields need a follow-up `configure_edge` once
the node has reconciled.

**`collect` expires passively.** Group timeouts are checked when messages
arrive, not on a background timer — an idle node holds stragglers until the
next message on any group. Map a group key that is unique per fan-out (e.g.
`$.context.runId`) and enable the error port in production; without it,
timed-out groups are dropped with only a log line.

**`budget_guard` only counts if you thread its counters back.** Map
`iteration` / `spentTokens` / `spentUSD` from `proceed` into the guard on each
pass. Miss that and it reports iteration 1 forever — a guard that guards
nothing.

**`flow_telemetry` needs the otel-collector** the operator installs, and reads
it over its service address. Its scan is bounded: check `truncated` before
reading an empty `errorsOnly` result as "nothing failed".

## Installation

```shell
helm repo add tinysystems https://tiny-systems.github.io/module/
helm install common-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=ghcr.io/tiny-systems/common-module
```

## Run locally

```shell
go run cmd/main.go run --name=common-module --namespace=tinysystems --version=1.0.0
```

## Part of Tiny Systems

This module is part of the [Tiny Systems](https://github.com/tiny-systems) platform -- a visual flow-based automation engine running on Kubernetes.

## License

This module's source code is MIT-licensed. It depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1). See [LICENSE](LICENSE) for details.
