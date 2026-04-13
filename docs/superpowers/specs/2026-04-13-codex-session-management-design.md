# Codex Plugin Session Management Design

## Overview

Add multi-session management to `custom_plugin_codex`, enabling wasm components to maintain per-context current session state and manage session lifecycle through dedicated WASI interface functions.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Session scope | Per-component + context-key | One component can serve multiple users/contexts concurrently |
| Key source | Wasm explicitly passes context-key | Simple, wasm has full control over session grouping |
| Interface structure | Merge session + session-manager into one `session` interface | Related operations belong together; fewer imports |
| Task submission | Modify existing `execute` to add context-key with auto-resume | Minimal API surface, one-step operation |
| Delete current session | Clear current session pointer | Simpler, next execute auto-creates new session |

## WIT Interface Changes

### Modified `executor` interface

```wit
interface executor {
    use types.{codex-error};
    use types.{exec-stream};

    execute: func(context-key: string, prompt: string) -> result<exec-stream, codex-error>;
}
```

Behavior: if context-key has a current session, resume it with prompt; otherwise create new session, set as current, return stream.

### Merged `session` interface

```wit
interface session {
    use types.{codex-error};
    use types.{exec-stream};
    use types.{token-usage};
    use types.{session-info};

    // Existing
    get-usage: func(session-id: string) -> result<token-usage, codex-error>;
    resume: func(session-id: string, prompt: string) -> result<exec-stream, codex-error>;

    // New session management
    new-session: func(context-key: string, prompt: string) -> result<exec-stream, codex-error>;
    change-session: func(context-key: string, session-id: string) -> result<_, codex-error>;
    delete-session: func(session-id: string) -> result<_, codex-error>;
    list-sessions: func() -> result<list<session-info>, codex-error>;
}
```

### New type in `types` interface

```wit
record session-info {
    session-id: string,
    thread-id: string,
    created-at: string,
    token-usage: option<token-usage>,
}
```

### world.wit

```wit
world codex {
    import custom:codex/executor@0.1.0;
    import custom:codex/session@0.1.0;
}
```

## Host-Side State Management

### New state fields

```rust
struct CodexComponentState {
    // Existing
    sessions: HashMap<String, CodexSessionState>,      // session-id -> state
    session_key_map: HashMap<String, String>,           // thread-id -> session-id

    // New
    current_sessions: HashMap<String, String>,           // context-key -> session-id
    session_metadata: HashMap<String, SessionMetadata>,  // session-id -> metadata
}

struct SessionMetadata {
    session_id: String,
    thread_id: String,
    context_key: String,
    created_at: String,  // ISO 8601
}
```

### Function state transitions

| Function | State change |
|----------|-------------|
| `execute(ctx-key, prompt)` | Has current -> resume; No current -> create, set current, write metadata |
| `new-session(ctx-key, prompt)` | Create session, set current, write metadata |
| `resume(session-id, prompt)` | No state change (existing behavior) |
| `change-session(ctx-key, session-id)` | Validate session exists, update current_sessions[ctx-key] |
| `delete-session(session-id)` | Remove from sessions, session_metadata, session_key_map; clear current_sessions entries pointing to this session |
| `list-sessions()` | Iterate session_metadata, compose session-info with token-usage |
| `get-usage(session-id)` | No state change (existing behavior) |

### Error handling

- `change-session`: session-id not found -> `not-found`
- `delete-session`: session-id not found -> `not-found`
- `execute`/`new-session`: codex process failure -> `process` (existing logic)

## Host-Side Implementation

### `execute(context-key, prompt)` core logic

```rust
async fn execute(&mut self, context_key: String, prompt: String) -> Result<ExecStream> {
    if let Some(current_id) = self.current_sessions.get(&context_key) {
        self.resume_internal(current_id, prompt).await
    } else {
        let (session_id, stream) = self.create_session(&context_key, prompt).await?;
        self.current_sessions.insert(context_key, session_id);
        Ok(stream)
    }
}
```

### `new-session(context-key, prompt)` core logic

```rust
async fn new_session(&mut self, context_key: String, prompt: String) -> Result<ExecStream> {
    let (session_id, stream) = self.create_session(&context_key, prompt).await?;
    self.current_sessions.insert(context_key, session_id);
    Ok(stream)
}
```

### `change-session` / `delete-session` / `list-sessions`

- `change-session`: lookup session_metadata for validation, update current_sessions
- `delete-session`: remove from sessions, session_metadata, session_key_map; scan current_sessions to clear references
- `list-sessions`: iterate session_metadata, combine with token-usage from session state

### Component unbind cleanup

Extend existing `unbind_component` to also clean up `current_sessions` and `session_metadata`.

## Unchanged

- `codex_process.rs` - subprocess management and JSONL parsing fully reused
- `resume` / `get-usage` host implementations
- Configuration structure (api_token, model, base_url, etc.)
