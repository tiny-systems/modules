# Claude Code Rules for Crypto Module

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
