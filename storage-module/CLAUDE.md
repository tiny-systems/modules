# Claude Code Rules for Storage Module

## Code Style

- Early returns, no nested ifs
- Extract logic into small, focused functions
- Flat structure over deep nesting
- Idiomatic Go - if err != nil { return } pattern

## CRITICAL: Handler Response Propagation

**NEVER ignore the return value of handler() calls. ALWAYS return it.**

```go
// WRONG - breaks blocking I/O, causes timeouts
_ = handler(ctx, ErrorPort, Error{...})
return nil

// CORRECT - propagates response back through call chain
return handler(ctx, ErrorPort, Error{...})
```

Exception: `_reconcile` port calls can ignore returns (internal system port).

## Declaring a Blocking Component: the SyncRPC Capability

If a component **blocks holding a live connection** — emits on a source port and synchronously waits for the downstream chain to deliver a result back within the same request — it MUST declare the `module.SyncRPC` capability:

```go
func (c *Component) SyncRPC() module.SyncRPCInfo { return module.SyncRPCInfo{} }
```

`module build` auto-tags the component `sync_rpc`; the platform keeps the subgraph containing it on blocking request/reply delivery, while trigger-driven subgraphs run durable (fire-and-forget) — modes are derived, never configured. Forgetting this is fatal for a blocking component: durable hops return nothing to their sender, so the awaited response never comes back and the connection times out even with perfect handler-return discipline. Canonical implementer: `http_server` (live socket). Components that merely sit in its subgraph — a Slack command handler, a router — declare nothing; they inherit classic delivery automatically.

## Component Design

- Handle() switch cases should be minimal - delegate to functions
- No JSON parsing in components - SDK handles deserialization
- Components should be flat in `components/` folder

## Context Pattern for Schema Generation

Define a type alias for Context and use it in structs:

```go
type Context any

type Request struct {
    Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
}

type Output struct {
    Context Context `json:"context,omitempty" title:"Context"`
}
```

## Releasing

After making changes, bump the version:

```bash
./release.sh patch   # For bug fixes and small changes
./release.sh minor   # For new features
./release.sh major   # For breaking changes
```
