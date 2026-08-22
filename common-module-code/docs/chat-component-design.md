# `chat` component — contract draft

Date: 2026-08-15. Merges `prompt` (human asks flow) and `ask` (flow asks human) into one
human-touchpoint component with a conversation surface. Owner decision: the two components
are mirror twins from the form era; at chat paradigm the split is artificial. This restores
the "conversation contract" that fire-and-forget dropped: correlation, working state,
completion, expiry — explicitly, on top of async transport (no return to blocking).

Status: DRAFT for owner review. No code yet.

## One tile, one conversation

The widget renders a message thread, not a form:

- human types in a composer → message bubble (right)
- flow replies → markdown bubble (left), matched to the message it answers
- flow asks → question bubble with fields/buttons inline (ask's approve/deny lives INSIDE the thread)
- unanswered message → "working…" under it (composer stays locked per-message, not per-widget)
- expiry/errors → small system notes in-thread

A flow with no free chat (pure approval gate) still works: thread is just question cards.
A flow with no questions is pure chat. Same component, degenerate cases.

## Ports

| Port      | Dir    | Payload | Semantics |
|-----------|--------|---------|-----------|
| `say`     | in     | `{requestId?, role?, text, context?}` | Flow → human content. `role`: `assistant` (default) \| `system` \| `error`. If `requestId` matches a pending human message, that message is marked answered (prompt's In). Unknown/stale `requestId` → appended as unsolicited message, NOT dropped (change from prompt: a conversation may receive pushes). |
| `ask`     | in     | `{context, form?}` | Flow → human structured question (ask's Request). Form snapshotted at ask time: `form` override if given, else Settings form, else default Approve/Deny. FIFO queue, durable, `_qid` correlation as today. Question renders as in-thread card. |
| `message` | source | `{requestId, text, context?}` | Human free input from the composer. Flow answers via `say` carrying `requestId`. |
| `answer`  | source | `{questionId, values, context}` | Human's reply to an `ask` question (ask's Reply + id). |
| `error`   | source | `{context, error}` | Expired questions (ask's ErrorMessage), gated by `enableErrorPort`. |
| Control   | source | see below | The widget. |
| Settings  | in     | see below | |

Separate `message`/`answer` sources on purpose: free text routes to the LLM path,
answers route to gated actions — merging them forces every consumer to branch.

## Settings

```
form            string  JSON Schema default question form (ask's semantics; empty = Approve/Deny)
timeoutSeconds  int     question expiry; 0 = forever (passive check, as ask today)
enableErrorPort bool    expired questions → error port
historyLimit    int     default 50 — messages kept for DISPLAY
placeholder     string  composer placeholder text
```

## State (TinyNode status metadata — small by design)

```
thread:  [{id, kind: message|reply|question|answer|note, role?, text?, values?, qid?, at, pending?}]  // capped at historyLimit
queue:   [pendingQuestion]  // ask's FIFO, unchanged semantics (snapshot form, survive restarts, multi-replica)
```

The thread is a DISPLAY buffer, not memory. Full conversation memory stays a flow concern
(store-module), exactly like the prompt-demo flow does with `conversation`. Cap enforced on
every append; State backing is CR status — must stay small.

## Control port (the widget)

- Schema: `{format: "chat"}` at root — the editor dispatch key. Carries the composer field,
  and the head question's form schema nested when one is pending.
- Data: `{thread: [...], pendingQuestion: {qid, form, context} | null}`.
- Human submits through the existing control channel (`runAction` on `_control`), payload
  tagged: `{_kind: "message", text}` or `{_kind: "answer", _qid, values}`.

## Leader/replica rules (lessons already paid for)

- `Handle` (say/ask): NOT leader-gated — answers route to any replica; State converges
  (prompt's leader-gate bug, fixed once, stays fixed).
- `OnControl`, `OnReconcile`: leader-gated, run once.
- Reconcile republishes control ONLY when thread/queue actually changed (ask's `rehydrated`
  flag pattern) — no re-render under a typing human. Widget key on data, not wall-clock.

## Editor side (separate repo, same feature)

- `Widget.vue` dispatch: control schema `format == "chat"` → `ChatWidget.vue`; anything else →
  JSONEditor form as today (old modules unaffected).
- `ChatWidget.vue`: thread list (markdown via existing sanitized MarkdownView), question cards
  render their form schema with the existing JSONEditor *inside* the bubble (reuse, not rewrite),
  composer pinned bottom, per-message working state, system/error notes styled small.
- No dashboard chrome changes. One widget stops looking like a form.

## Migration

- `prompt` and `ask` are DELETED (owner call: lived under a week, nobody used them).
- prompt-demo `chat` flow moves to the new component as the acceptance case:
  browser test = send message → working → answer bubble; wire an ask into the same
  thread → question card → approve → downstream fires.

## Non-goals

- Not an LLM component: it renders/relays; thinking happens in the flow.
- Not conversation memory (store-module's job).
- Not credentials entry (separate setup-request track).
- No streaming tokens in v1 — `say` is message-granular; streaming is a later `say` extension.
