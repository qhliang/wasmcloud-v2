# Codex Session Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add multi-session management to `custom_plugin_codex` with per-context current session tracking, enabling wasm components to manage session lifecycle through WASI interface functions.

**Architecture:** Extend the existing WIT types/session interfaces and host-side `ComponentData` state. Modify `executor.execute` to accept `context-key` with auto-resume logic. Add `new-session`, `change-session`, `delete-session`, `list-sessions` to the existing `session` WIT interface.

**Tech Stack:** Rust, Wasmtime Component Model, WIT (WebAssembly Interface Types), tokio async runtime

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `crates/custom_plugin_codex/wit/deps/custom-codex.wit` | Modify | Add `session-info` type, `execute(context-key)`, session management functions |
| `crates/custom_plugin_codex/wit/world.wit` | Modify | No change needed (already imports executor + session) |
| `crates/custom_plugin_codex/src/lib.rs` | Modify | Add `SessionMetadata`, `current_sessions`, implement new host methods, refactor `execute` |
| `crates/custom_plugin_codex/src/codex_process.rs` | No change | Subprocess management fully reused |

---

### Task 1: Update WIT definitions

**Files:**
- Modify: `crates/custom_plugin_codex/wit/deps/custom-codex.wit`

- [ ] **Step 1: Update the WIT file with new types and modified interfaces**

Replace the entire content of `crates/custom_plugin_codex/wit/deps/custom-codex.wit` with:

```wit
package custom:codex@0.1.0;

interface types {
    variant codex-error {
        internal(string),
        config-error(string),
        process-error(string),
        not-found(string),
        timeout(string),
    }

    record token-usage {
        input-tokens: u64,
        cached-input-tokens: u64,
        output-tokens: u64,
    }

    record codex-event {
        event-type: string,
        item-id: option<string>,
        item-type: option<string>,
        text-content: option<string>,
        command: option<string>,
        status: option<string>,
        raw-json: string,
    }

    variant exec-stream-event {
        event(codex-event),
        usage(token-usage),
        done(string),
        error(string),
    }

    record session-info {
        session-id: string,
        thread-id: string,
        created-at: string,
        token-usage: option<token-usage>,
    }
}

interface executor {
    use types.{codex-error, exec-stream-event};

    resource exec-stream {
        next: func() -> result<tuple<list<exec-stream-event>, bool>, codex-error>;
    }

    execute: func(context-key: string, prompt: string) -> result<exec-stream, codex-error>;
}

interface session {
    use types.{codex-error, token-usage, exec-stream-event, session-info};
    use executor.{exec-stream};

    // Existing
    get-usage: func(session-id: string) -> result<token-usage, codex-error>;
    resume: func(session-id: string, prompt: string) -> result<exec-stream, codex-error>;

    // New session management
    new-session: func(context-key: string, prompt: string) -> result<exec-stream, codex-error>;
    change-session: func(context-key: string, session-id: string) -> result<_, codex-error>;
    delete-session: func(session-id: string) -> result<_, codex-error>;
    list-sessions: func() -> result<list<session-info>, codex-error>;
}

world codex {
    import executor;
    import session;
}
```

- [ ] **Step 2: Verify WIT compiles**

Run: `cargo build -p custom_plugin_codex 2>&1 | head -50`
Expected: Compile error because host implementations don't match new signatures yet — this is expected. The WIT file itself should parse without syntax errors.

- [ ] **Step 3: Commit WIT changes**

```bash
git add crates/custom_plugin_codex/wit/deps/custom-codex.wit
git commit -m "feat(codex): update WIT with session-info type and session management functions"
```

---

### Task 2: Add SessionMetadata and extend ComponentData

**Files:**
- Modify: `crates/custom_plugin_codex/src/lib.rs`

- [ ] **Step 1: Add SessionMetadata struct**

Add after the `CodexSessionState` struct (after line ~94 in `lib.rs`):

```rust
/// Metadata tracked for each session to support list/change/delete
struct SessionMetadata {
    session_id: String,
    thread_id: String,
    context_key: String,
    created_at: String,
}
```

- [ ] **Step 2: Extend ComponentData with new fields**

Replace the `ComponentData` struct with:

```rust
struct ComponentData {
    /// Token that cancels ALL background tasks for this component
    cancel_token: tokio_util::sync::CancellationToken,
    /// Codex spawn configuration
    config: CodexConfig,
    /// Active codex sessions: internal key -> session state
    sessions: HashMap<String, CodexSessionState>,
    /// Reverse map: codex thread_id -> internal session key
    session_id_map: HashMap<String, String>,
    /// Current session per context-key: context-key -> internal session key
    current_sessions: HashMap<String, String>,
    /// Metadata for all sessions: internal session key -> metadata
    session_metadata: HashMap<String, SessionMetadata>,
}
```

- [ ] **Step 3: Update ComponentData construction in on_workload_item_bind**

In the `on_workload_item_bind` method, update the `ComponentData` construction to include the new fields:

```rust
        self.tracker.write().await.add_component(
            component_handle,
            ComponentData {
                cancel_token: tokio_util::sync::CancellationToken::new(),
                config,
                sessions: HashMap::new(),
                session_id_map: HashMap::new(),
                current_sessions: HashMap::new(),
                session_metadata: HashMap::new(),
            },
        );
```

- [ ] **Step 4: Commit state changes**

```bash
git add crates/custom_plugin_codex/src/lib.rs
git commit -m "feat(codex): add SessionMetadata and extend ComponentData for session management"
```

---

### Task 3: Refactor execute() to accept context-key with auto-resume

**Files:**
- Modify: `crates/custom_plugin_codex/src/lib.rs`

- [ ] **Step 1: Extract a shared helper for creating sessions**

Add a private helper method to create a new codex session and store its metadata. Add this as a free function before the `impl bindings::custom::codex::executor::Host` block:

```rust
/// Helper: spawn a new codex session, store it in tracker, return (internal_key, stream resource).
async fn create_new_session(
    ctx: &mut ActiveCtx<'_>,
    plugin: &Codex,
    component_id: &str,
    context_key: &str,
    prompt: &str,
) -> wasmtime::Result<Result<(String, Resource<ExecStreamHandle>), CodexError>> {
    // Get config
    let config = {
        let lock = plugin.tracker.read().await;
        match lock.get_component_data(component_id) {
            Some(data) => data.config.clone(),
            None => return Ok(Err(CodexError::Internal("component not tracked".to_string()))),
        }
    };

    // Ensure binary exists
    if let Err(e) = ensure_codex_binary(&config.binary_path).await {
        return Ok(Err(CodexError::ProcessError(format!(
            "codex binary not available: {e}"
        ))));
    }

    // Spawn codex subprocess
    let spawn_config = CodexSpawnConfig {
        binary_path: config.binary_path.clone(),
        model: config.model.clone(),
        api_token: config.api_token.clone(),
        base_url: config.base_url.clone(),
        project_dir: config.project_dir.clone(),
    };

    let mut child = match spawn_codex_exec(&spawn_config, prompt, None) {
        Ok(c) => c,
        Err(e) => {
            return Ok(Err(CodexError::ProcessError(format!(
                "failed to spawn codex: {e}"
            ))));
        }
    };

    let stdout = match child.stdout.take() {
        Some(s) => s,
        None => {
            return Ok(Err(CodexError::ProcessError(
                "codex process has no stdout".to_string(),
            )));
        }
    };

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let cancel_token = {
        let lock = plugin.tracker.read().await;
        match lock.get_component_data(component_id) {
            Some(data) => data.cancel_token.clone(),
            None => {
                return Ok(Err(CodexError::Internal(
                    "component not tracked".to_string(),
                )));
            }
        }
    };

    let task_token = cancel_token.child_token();
    tokio::spawn(async move {
        tokio::select! {
            _ = task_token.cancelled() => {
                debug!("Codex JSONL reader cancelled");
            }
            _ = read_jsonl_output(stdout, tx) => {}
        }
    });

    let session_key = new_session_key();
    let now = chrono::Utc::now().to_rfc3339();

    let session = CodexSessionState {
        event_rx: rx,
        pending_events: Vec::new(),
        ended: false,
        usage: TokenUsageAccum::default(),
        session_id: None,
    };

    {
        let mut lock = plugin.tracker.write().await;
        if let Some(data) = lock.get_component_data_mut(component_id) {
            data.sessions.insert(session_key.clone(), session);
            data.session_metadata.insert(
                session_key.clone(),
                SessionMetadata {
                    session_id: String::new(), // Will be filled when thread.started arrives
                    thread_id: String::new(),
                    context_key: context_key.to_string(),
                    created_at: now,
                },
            );
        }
    }

    debug!(
        component_id = %component_id,
        session_key = %session_key,
        "Codex exec session started"
    );

    let handle = ExecStreamHandle {
        session_key: session_key.clone(),
        ended: false,
    };

    let resource = ctx.table.push(handle)?;
    Ok(Ok((session_key, resource)))
}
```

- [ ] **Step 2: Rewrite executor::Host::execute to use context-key**

Replace the entire `execute` method in `impl<'a> bindings::custom::codex::executor::Host for ActiveCtx<'a>`:

```rust
    #[instrument(skip_all)]
    async fn execute(
        &mut self,
        context_key: String,
        prompt: String,
    ) -> wasmtime::Result<Result<Resource<ExecStreamHandle>, CodexError>> {
        let Some(plugin) = self.get_plugin::<Codex>(PLUGIN_ID) else {
            return Ok(Err(CodexError::Internal(
                "codex plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        // Check if context-key has a current session
        let current_session_key = {
            let lock = plugin.tracker.read().await;
            lock.get_component_data(&component_id)
                .and_then(|data| data.current_sessions.get(&context_key).cloned())
        };

        if let Some(ref internal_key) = current_session_key {
            // Resume existing current session
            // Get session_id (codex thread_id) for this internal key
            let session_id = {
                let lock = plugin.tracker.read().await;
                lock.get_component_data(&component_id)
                    .and_then(|data| {
                        data.sessions
                            .get(internal_key)
                            .and_then(|s| s.session_id.clone())
                    })
            };

            match session_id {
                Some(sid) => {
                    // Use existing resume logic
                    return self.resume(sid, prompt).await;
                }
                None => {
                    // Session exists but no thread_id yet — treat as no current session
                    // Fall through to create new session
                }
            }
        }

        // No current session — create new
        let (internal_key, resource) = match create_new_session(
            self,
            &plugin,
            &component_id,
            &context_key,
            &prompt,
        )
        .await
        {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => return Ok(Err(e)),
            Err(e) => return Err(e),
        };

        // Set as current session for this context-key
        {
            let mut lock = plugin.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(&component_id) {
                data.current_sessions
                    .insert(context_key, internal_key);
            }
        }

        Ok(Ok(resource))
    }
```

- [ ] **Step 3: Commit execute refactor**

```bash
git add crates/custom_plugin_codex/src/lib.rs
git commit -m "feat(codex): refactor execute() to accept context-key with auto-resume"
```

---

### Task 4: Add chrono dependency and update session metadata on thread.started

**Files:**
- Modify: `crates/custom_plugin_codex/Cargo.toml`
- Modify: `crates/custom_plugin_codex/src/lib.rs`

- [ ] **Step 1: Add chrono to Cargo.toml**

Add under `[dependencies]`:

```toml
chrono = { workspace = true }
```

- [ ] **Step 2: Update drain_channel to populate session metadata**

In the `drain_channel` function, after the line `session.session_id = Some(thread_id.clone());`, add metadata update logic. Since `drain_channel` only has access to `CodexSessionState`, we need to update the `HostExecStream::next` method instead where we already have tracker access.

In the `HostExecStream::next` method, after the existing `session_id_map` update block (around line 504-510), add:

```rust
        // Update session_metadata with thread_id if it became available
        if let Some(data) = lock.get_component_data_mut(&component_id)
            && let Some(session) = data.sessions.get(&session_key)
            && let Some(ref sid) = session.session_id
        {
            if let Some(meta) = data.session_metadata.get_mut(&session_key) {
                if meta.session_id.is_empty() {
                    meta.session_id = sid.clone();
                    meta.thread_id = sid.clone();
                }
            }
        }
```

- [ ] **Step 3: Commit**

```bash
git add crates/custom_plugin_codex/Cargo.toml crates/custom_plugin_codex/src/lib.rs
git commit -m "feat(codex): populate session metadata on thread.started event"
```

---

### Task 5: Implement new-session host method

**Files:**
- Modify: `crates/custom_plugin_codex/src/lib.rs`

- [ ] **Step 1: Add new-session to session::Host implementation**

Add the `new_session` method to `impl<'a> bindings::custom::codex::session::Host for ActiveCtx<'a>`, after the existing `resume` method:

```rust
    #[instrument(skip_all)]
    async fn new_session(
        &mut self,
        context_key: String,
        prompt: String,
    ) -> wasmtime::Result<Result<Resource<ExecStreamHandle>, CodexError>> {
        let Some(plugin) = self.get_plugin::<Codex>(PLUGIN_ID) else {
            return Ok(Err(CodexError::Internal(
                "codex plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        // Always create a new session
        let (internal_key, resource) = match create_new_session(
            self,
            &plugin,
            &component_id,
            &context_key,
            &prompt,
        )
        .await
        {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => return Ok(Err(e)),
            Err(e) => return Err(e),
        };

        // Set as current session for this context-key
        {
            let mut lock = plugin.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(&component_id) {
                data.current_sessions.insert(context_key, internal_key);
            }
        }

        Ok(Ok(resource))
    }
```

- [ ] **Step 2: Commit**

```bash
git add crates/custom_plugin_codex/src/lib.rs
git commit -m "feat(codex): implement new-session host method"
```

---

### Task 6: Implement change-session host method

**Files:**
- Modify: `crates/custom_plugin_codex/src/lib.rs`

- [ ] **Step 1: Add change-session to session::Host implementation**

Add after `new_session`:

```rust
    #[instrument(skip_all)]
    async fn change_session(
        &mut self,
        context_key: String,
        session_id: String,
    ) -> wasmtime::Result<Result<(), CodexError>> {
        let Some(plugin) = self.get_plugin::<Codex>(PLUGIN_ID) else {
            return Ok(Err(CodexError::Internal(
                "codex plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let mut lock = plugin.tracker.write().await;
        match lock.get_component_data_mut(&component_id) {
            Some(data) => {
                // Look up internal key by session_id (thread_id)
                let internal_key = match data.session_id_map.get(&session_id) {
                    Some(k) => k.clone(),
                    None => {
                        return Ok(Err(CodexError::NotFound(format!(
                            "session '{session_id}' not found"
                        ))));
                    }
                };

                // Verify session still exists
                if !data.sessions.contains_key(&internal_key) {
                    return Ok(Err(CodexError::NotFound(format!(
                        "session '{session_id}' not found"
                    ))));
                }

                data.current_sessions.insert(context_key, internal_key);
                Ok(Ok(()))
            }
            None => Ok(Err(CodexError::NotFound(
                "component not tracked".to_string(),
            ))),
        }
    }
```

- [ ] **Step 2: Commit**

```bash
git add crates/custom_plugin_codex/src/lib.rs
git commit -m "feat(codex): implement change-session host method"
```

---

### Task 7: Implement delete-session host method

**Files:**
- Modify: `crates/custom_plugin_codex/src/lib.rs`

- [ ] **Step 1: Add delete-session to session::Host implementation**

Add after `change_session`:

```rust
    #[instrument(skip_all)]
    async fn delete_session(
        &mut self,
        session_id: String,
    ) -> wasmtime::Result<Result<(), CodexError>> {
        let Some(plugin) = self.get_plugin::<Codex>(PLUGIN_ID) else {
            return Ok(Err(CodexError::Internal(
                "codex plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let mut lock = plugin.tracker.write().await;
        match lock.get_component_data_mut(&component_id) {
            Some(data) => {
                // Look up internal key by session_id (thread_id)
                let internal_key = match data.session_id_map.get(&session_id) {
                    Some(k) => k.clone(),
                    None => {
                        return Ok(Err(CodexError::NotFound(format!(
                            "session '{session_id}' not found"
                        ))));
                    }
                };

                // Remove from all state maps
                data.sessions.remove(&internal_key);
                data.session_id_map.remove(&session_id);
                data.session_metadata.remove(&internal_key);

                // Clear any current_sessions entries pointing to this session
                data.current_sessions.retain(|_, v| *v != internal_key);

                debug!(
                    component_id = %component_id,
                    session_id = %session_id,
                    "Codex session deleted"
                );

                Ok(Ok(()))
            }
            None => Ok(Err(CodexError::NotFound(
                "component not tracked".to_string(),
            ))),
        }
    }
```

- [ ] **Step 2: Commit**

```bash
git add crates/custom_plugin_codex/src/lib.rs
git commit -m "feat(codex): implement delete-session host method"
```

---

### Task 8: Implement list-sessions host method

**Files:**
- Modify: `crates/custom_plugin_codex/src/lib.rs`

- [ ] **Step 1: Add list-sessions to session::Host implementation**

Add after `delete_session`:

```rust
    #[instrument(skip_all)]
    async fn list_sessions(
        &mut self,
    ) -> wasmtime::Result<Result<Vec<SessionInfo>, CodexError>> {
        let Some(plugin) = self.get_plugin::<Codex>(PLUGIN_ID) else {
            return Ok(Err(CodexError::Internal(
                "codex plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let lock = plugin.tracker.read().await;
        match lock.get_component_data(&component_id) {
            Some(data) => {
                let list: Vec<SessionInfo> = data
                    .session_metadata
                    .iter()
                    .map(|(internal_key, meta)| {
                        let usage = data.sessions.get(internal_key).map(|s| TokenUsage {
                            input_tokens: s.usage.input_tokens,
                            cached_input_tokens: s.usage.cached_input_tokens,
                            output_tokens: s.usage.output_tokens,
                        });
                        SessionInfo {
                            session_id: meta.session_id.clone(),
                            thread_id: meta.thread_id.clone(),
                            created_at: meta.created_at.clone(),
                            token_usage: usage,
                        }
                    })
                    .collect();
                Ok(Ok(list))
            }
            None => Ok(Err(CodexError::NotFound(
                "component not tracked".to_string(),
            ))),
        }
    }
```

Note: `SessionInfo` is the generated WIT type from `bindings::custom::codex::types::SessionInfo`. Import it at the top of the file alongside the existing type imports.

- [ ] **Step 2: Add SessionInfo to imports**

Update the `use bindings::custom::codex::types::` line to include `SessionInfo`:

```rust
use bindings::custom::codex::types::{CodexError, CodexEvent, ExecStreamEvent, SessionInfo, TokenUsage};
```

- [ ] **Step 3: Commit**

```bash
git add crates/custom_plugin_codex/src/lib.rs
git commit -m "feat(codex): implement list-sessions host method"
```

---

### Task 9: Update on_workload_unbind cleanup

**Files:**
- Modify: `crates/custom_plugin_codex/src/lib.rs`

- [ ] **Step 1: Extend unbind cleanup**

The existing `on_workload_unbind` only cancels the token. The `ComponentData` fields (`sessions`, `current_sessions`, `session_metadata`, `session_id_map`) are all dropped naturally when `ComponentData` is dropped, so no additional explicit cleanup is needed. The `CancellationToken` cancel is sufficient to terminate background tasks.

No code change needed — this task is for verification only.

- [ ] **Step 2: Verify cleanup logic is correct**

The `remove_workload_with_cleanup` call will drop the `ComponentData` struct, which drops all HashMaps and triggers cancellation via the token. This is correct.

---

### Task 10: Build and fix compilation errors

**Files:**
- Modify: `crates/custom_plugin_codex/src/lib.rs` (fix any compile errors)

- [ ] **Step 1: Build the crate**

Run: `cargo build -p custom_plugin_codex 2>&1`

Expected: Possible compile errors from:
1. Generated bindgen code producing different type names than expected
2. Missing imports
3. Signature mismatches

Fix each error iteratively.

- [ ] **Step 2: Fix all compilation errors**

Common issues to expect:
- `SessionInfo` might be generated as a different name — check the actual generated type from `bindings::custom::codex::types`
- The `execute` signature change requires the bindgen to regenerate — ensure `cargo build` triggers regeneration
- The `chrono` import needs `use chrono` somewhere if using directly, or use `std::time` + formatting

If `chrono` is not in the workspace, replace with:

```rust
let now = {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{}", secs)
};
```

- [ ] **Step 3: Run clippy**

Run: `cargo clippy -p custom_plugin_codex 2>&1`
Fix any warnings or errors.

- [ ] **Step 4: Commit compilation fixes**

```bash
git add crates/custom_plugin_codex/
git commit -m "fix(codex): resolve compilation errors from session management changes"
```

---

### Task 11: Run tests

**Files:**
- Modify: `crates/custom_plugin_codex/src/lib.rs` (add tests if needed)

- [ ] **Step 1: Run existing tests**

Run: `cargo test -p custom_plugin_codex 2>&1`
Expected: All existing tests pass (test_plugin_id, test_world_exports, test_extract_config_*, test_convert_*, test_new_session_key_unique)

- [ ] **Step 2: Add test for session management state transitions**

Add a new test in the `tests` module:

```rust
    #[test]
    fn test_session_metadata_creation() {
        let meta = SessionMetadata {
            session_id: "thread-123".to_string(),
            thread_id: "thread-123".to_string(),
            context_key: "user-1".to_string(),
            created_at: "2026-04-13T00:00:00Z".to_string(),
        };
        assert_eq!(meta.session_id, "thread-123");
        assert_eq!(meta.context_key, "user-1");
    }

    #[test]
    fn test_current_sessions_tracking() {
        let mut current_sessions: HashMap<String, String> = HashMap::new();

        // No current session
        assert!(current_sessions.get("user-1").is_none());

        // Set current session
        current_sessions.insert("user-1".to_string(), "key-1".to_string());
        assert_eq!(current_sessions.get("user-1"), Some(&"key-1".to_string()));

        // Change session
        current_sessions.insert("user-1".to_string(), "key-2".to_string());
        assert_eq!(current_sessions.get("user-1"), Some(&"key-2".to_string()));

        // Delete clears references
        current_sessions.retain(|_, v| *v != "key-2");
        assert!(current_sessions.get("user-1").is_none());
    }
```

- [ ] **Step 3: Run all tests**

Run: `cargo test -p custom_plugin_codex 2>&1`
Expected: All tests pass.

- [ ] **Step 4: Commit tests**

```bash
git add crates/custom_plugin_codex/src/lib.rs
git commit -m "test(codex): add session management state transition tests"
```

---

### Task 12: Final build verification

- [ ] **Step 1: Full workspace build**

Run: `cargo build --workspace 2>&1`
Expected: Successful build with no errors.

- [ ] **Step 2: Full clippy check**

Run: `cargo clippy --workspace 2>&1`
Expected: No new warnings from the codex plugin changes.

- [ ] **Step 3: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix(codex): final build fixes for session management"
```
