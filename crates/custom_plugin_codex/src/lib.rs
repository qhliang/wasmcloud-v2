//! # Codex Host Plugin
//!
//! This plugin provides OpenAI Codex CLI integration for WASM components.
//! It spawns `codex exec` as a subprocess, forwards streaming JSONL output
//! back to the guest, and supports session management.
//!
//! ## Configuration (via interface config)
//!
//! ```ignore
//! custom:codex:
//!   config:
//!     api_token: "sk-..."               // Required. CODEX_API_KEY
//!     model: "gpt-5.4"                  // Required. --model flag
//!     base_url: "https://..."           // Optional. Custom LLM endpoint
//!     codex_binary_path: "/path/to/codex" // Optional. Default: ~/.cache/wash-codex/codex
//!     project_dir: "/path/to/project"   // Optional. Default: temp dir
//! ```

mod codex_process;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};
use wasmtime::component::Resource;

use etcetera::base_strategy::BaseStrategy;
use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::config::{resolve_field, resolve_optional_field};
use wash_runtime::plugin::{HostPlugin, WorkloadTracker, find_interface};
use wash_runtime::wit::{WitInterface, WitWorld};

use codex_process::{
    CodexJsonlEvent, CodexSpawnConfig, TokenUsageAccum, ensure_codex_binary, read_jsonl_output,
    spawn_codex_exec,
};

mod bindings {
    wasmtime::component::bindgen!({
        world: "codex",
        imports: {
            default: async | trappable | tracing,
        },
        with: {
            "custom:codex/executor.exec-stream": super::ExecStreamHandle,
        },
    });
}

use bindings::custom::codex::types::{
    ApprovalRequest, CodexConfig as WitCodexConfig, CodexError, CodexEvent, ExecStreamEvent,
    SessionInfo, TokenUsage,
};
const PLUGIN_ID: &str = "codex";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Host-side state for an active exec stream.
/// Stored in the wasmtime ResourceTable and polled by `next()`.
pub struct ExecStreamHandle {
    /// Key into the sessions map in ComponentData
    session_key: String,
    /// Whether the stream has ended
    ended: bool,
}

/// Metadata tracked for each session to support list/change/delete
#[allow(dead_code)]
struct SessionMetadata {
    session_id: String,
    thread_id: String,
    context_key: String,
    created_at: String,
}

/// Per-component data tracked by the plugin
struct ComponentData {
    /// Token that cancels ALL background tasks for this component
    cancel_token: tokio_util::sync::CancellationToken,
    /// Static interface config from wasmcloud config (fallback source)
    interface_config: HashMap<String, String>,
    /// Active codex sessions: internal key -> session state
    sessions: HashMap<String, CodexSessionState>,
    /// Reverse map: codex thread_id -> internal session key
    session_id_map: HashMap<String, String>,
    /// Current session per context-key: context-key -> internal session key
    current_sessions: HashMap<String, String>,
    /// Metadata for all sessions: internal session key -> metadata
    session_metadata: HashMap<String, SessionMetadata>,
}

/// State for a single codex exec session
struct CodexSessionState {
    /// Receiver for JSONL events from the background reader task
    event_rx: tokio::sync::mpsc::UnboundedReceiver<CodexJsonlEvent>,
    /// Buffered events pending pickup by next()
    pending_events: Vec<CodexJsonlEvent>,
    /// Whether the process has completed
    ended: bool,
    /// Token usage accumulated from turn.completed events
    usage: TokenUsageAccum,
    /// The session ID from thread.started event
    session_id: Option<String>,
    /// Stdin pipe to the codex process for writing approval responses
    stdin: Option<tokio::process::ChildStdin>,
    /// Whether to auto-approve all commands (default: true)
    auto_approve: bool,
    /// Item ID of the current pending approval request (if any)
    pending_approval_item_id: Option<String>,
}

/// Configuration for a workload's codex integration
#[derive(Clone, Debug)]
struct CodexConfig {
    pub binary_path: PathBuf,
    pub model: String,
    pub api_token: String,
    pub base_url: Option<String>,
    pub project_dir: PathBuf,
}

// ---------------------------------------------------------------------------
// The Codex Plugin
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Codex {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for Codex {
    fn default() -> Self {
        Self::new()
    }
}

impl Codex {
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
        }
    }
}

// ---------------------------------------------------------------------------
// Config parsing
// ---------------------------------------------------------------------------

fn extract_config(interface: &WitInterface) -> Result<CodexConfig, String> {
    let api_token = interface
        .config
        .get("api_token")
        .cloned()
        .unwrap_or_default();
    if api_token.is_empty() {
        return Err("missing required config: 'api_token'".to_string());
    }

    let model = interface.config.get("model").cloned().unwrap_or_default();
    if model.is_empty() {
        return Err("missing required config: 'model'".to_string());
    }

    let base_url = interface.config.get("base_url").cloned();

    let binary_path = interface
        .config
        .get("codex_binary_path")
        .cloned()
        .map(PathBuf::from)
        .unwrap_or_else(|| dirs_cache_path().join("codex"));

    let project_dir = interface
        .config
        .get("project_dir")
        .cloned()
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            tempfile::tempdir()
                .map(|d| d.keep())
                .unwrap_or_else(|_| std::env::temp_dir())
        });

    Ok(CodexConfig {
        binary_path,
        model,
        api_token,
        base_url,
        project_dir,
    })
}

/// Resolve a CodexConfig from the dynamic WIT config parameter, falling back
/// to the stored interface config for any missing fields.
fn resolve_codex_config(
    dynamic_config: Option<&WitCodexConfig>,
    interface_config: &HashMap<String, String>,
) -> Result<CodexConfig, String> {
    let api_token = resolve_field(
        dynamic_config.map(|c| c.api_token.clone()),
        interface_config,
        "api_token",
    )
    .map_err(|e| e.to_string())?;

    if api_token.is_empty() {
        return Err("missing required config: 'api_token'".to_string());
    }

    let model = resolve_field(
        dynamic_config.map(|c| c.model.clone()),
        interface_config,
        "model",
    )
    .map_err(|e| e.to_string())?;

    if model.is_empty() {
        return Err("missing required config: 'model'".to_string());
    }

    let base_url = resolve_optional_field(
        dynamic_config.and_then(|c| c.base_url.clone()),
        interface_config,
        "base_url",
    );

    let binary_path = resolve_optional_field(
        dynamic_config.and_then(|c| c.codex_binary_path.clone()),
        interface_config,
        "codex_binary_path",
    )
    .map(PathBuf::from)
    .unwrap_or_else(|| dirs_cache_path().join("codex"));

    let project_dir = resolve_optional_field(
        dynamic_config.and_then(|c| c.project_dir.clone()),
        interface_config,
        "project_dir",
    )
    .map(PathBuf::from)
    .unwrap_or_else(|| {
        tempfile::tempdir()
            .map(|d| d.keep())
            .unwrap_or_else(|_| std::env::temp_dir())
    });

    Ok(CodexConfig {
        binary_path,
        model,
        api_token,
        base_url,
        project_dir,
    })
}

/// Get the default cache directory for the codex binary.
fn dirs_cache_path() -> PathBuf {
    etcetera::base_strategy::choose_base_strategy()
        .map(|s| s.cache_dir().join("wash-codex"))
        .unwrap_or_else(|_| PathBuf::from(".wash-codex"))
}

// ---------------------------------------------------------------------------
// Helper: generate a unique session key
// ---------------------------------------------------------------------------

fn new_session_key() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}

// ---------------------------------------------------------------------------
// Helper: convert CodexJsonlEvent to WIT ExecStreamEvent
// ---------------------------------------------------------------------------

#[allow(clippy::type_complexity)]
fn extract_item_fields(
    item: &serde_json::Value,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let item_id = item
        .get("id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let item_type = item
        .get("type")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let text_content = item
        .get("text")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let command = item
        .get("command")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let status = item
        .get("status")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    (item_id, item_type, text_content, command, status)
}

fn convert_event(event: CodexJsonlEvent) -> ExecStreamEvent {
    match event {
        CodexJsonlEvent::ThreadStarted { thread_id } => ExecStreamEvent::Done(thread_id),
        CodexJsonlEvent::TurnStarted => ExecStreamEvent::Event(CodexEvent {
            event_type: "turn.started".to_string(),
            item_id: None,
            item_type: None,
            text_content: None,
            command: None,
            status: None,
            raw_json: "{}".to_string(),
        }),
        CodexJsonlEvent::ItemStarted { item } => {
            let raw = serde_json::to_string(&item).unwrap_or_default();
            let (item_id, item_type, text_content, command, status) = extract_item_fields(&item);
            ExecStreamEvent::Event(CodexEvent {
                event_type: "item.started".to_string(),
                item_id,
                item_type,
                text_content,
                command,
                status,
                raw_json: raw,
            })
        }
        CodexJsonlEvent::ItemCompleted { item } => {
            let raw = serde_json::to_string(&item).unwrap_or_default();
            let (item_id, item_type, text_content, command, status) = extract_item_fields(&item);
            ExecStreamEvent::Event(CodexEvent {
                event_type: "item.completed".to_string(),
                item_id,
                item_type,
                text_content,
                command,
                status,
                raw_json: raw,
            })
        }
        CodexJsonlEvent::TurnCompleted { usage } => ExecStreamEvent::Usage(TokenUsage {
            input_tokens: usage.input_tokens,
            cached_input_tokens: usage.cached_input_tokens,
            output_tokens: usage.output_tokens,
        }),
        CodexJsonlEvent::TurnFailed { error } => {
            let msg = error
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error")
                .to_string();
            ExecStreamEvent::Error(msg)
        }
        CodexJsonlEvent::Error { message } => ExecStreamEvent::Error(message),
        CodexJsonlEvent::ExecApprovalRequest { item } => {
            let item_id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let command = item
                .get("command")
                .and_then(|v| {
                    v.as_str()
                        .map(|s| s.to_string())
                        .or_else(|| serde_json::to_string(v).ok())
                })
                .unwrap_or_default();
            ExecStreamEvent::ApprovalNeeded(ApprovalRequest { item_id, command })
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: drain events from channel into pending buffer
// ---------------------------------------------------------------------------

fn drain_channel(session: &mut CodexSessionState) {
    while let Ok(event) = session.event_rx.try_recv() {
        match &event {
            CodexJsonlEvent::ThreadStarted { thread_id } => {
                session.session_id = Some(thread_id.clone());
            }
            CodexJsonlEvent::TurnCompleted { usage } => {
                session.usage.add(usage);
                session.ended = true;
            }
            CodexJsonlEvent::TurnFailed { .. } | CodexJsonlEvent::Error { .. } => {
                session.ended = true;
            }
            CodexJsonlEvent::ExecApprovalRequest { item } => {
                let item_id = item
                    .get("call_id")
                    .or_else(|| item.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                session.pending_approval_item_id = Some(item_id);
            }
            _ => {}
        }
        session.pending_events.push(event);
    }
}

// ---------------------------------------------------------------------------
// WIT types::Host (no methods needed for error/record variants)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::codex::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// Helper: create a new codex session
// ---------------------------------------------------------------------------

async fn create_new_session(
    ctx: &mut ActiveCtx<'_>,
    plugin: &Codex,
    component_id: &str,
    context_key: &str,
    prompt: &str,
    dynamic_config: Option<&WitCodexConfig>,
) -> wasmtime::Result<Result<(String, Resource<ExecStreamHandle>), CodexError>> {
    // Resolve config from dynamic param or fallback to interface config
    let config = {
        let lock = plugin.tracker.read().await;
        match lock.get_component_data(component_id) {
            Some(data) => match resolve_codex_config(dynamic_config, &data.interface_config) {
                Ok(c) => c,
                Err(e) => {
                    return Ok(Err(CodexError::ConfigError(e)));
                }
            },
            None => {
                return Ok(Err(CodexError::Internal(
                    "component not tracked".to_string(),
                )));
            }
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

    let (mut child, child_stdin) = match spawn_codex_exec(&spawn_config, prompt, None) {
        Ok(pair) => pair,
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
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string();

    let session = CodexSessionState {
        event_rx: rx,
        pending_events: Vec::new(),
        ended: false,
        usage: TokenUsageAccum::default(),
        session_id: None,
        stdin: Some(child_stdin),
        auto_approve: true,
        pending_approval_item_id: None,
    };

    {
        let mut lock = plugin.tracker.write().await;
        if let Some(data) = lock.get_component_data_mut(component_id) {
            data.sessions.insert(session_key.clone(), session);
            data.session_metadata.insert(
                session_key.clone(),
                SessionMetadata {
                    session_id: String::new(),
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

// ---------------------------------------------------------------------------
// WIT executor::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::codex::executor::Host for ActiveCtx<'a> {
    #[instrument(skip_all)]
    async fn execute(
        &mut self,
        context_key: String,
        prompt: String,
        config: Option<WitCodexConfig>,
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
            // Get session_id (codex thread_id) for resume
            let session_id = {
                let lock = plugin.tracker.read().await;
                lock.get_component_data(&component_id).and_then(|data| {
                    data.sessions
                        .get(internal_key)
                        .and_then(|s| s.session_id.clone())
                })
            };

            if let Some(sid) = session_id {
                // Resume existing current session
                let result =
                    bindings::custom::codex::session::Host::resume(self, sid, prompt, config).await;
                // Update current_sessions to point to the new internal key created by resume
                if let Ok(Ok(ref resource)) = result {
                    let new_key = self
                        .table
                        .get(resource)
                        .map(|h| h.session_key.clone())
                        .unwrap_or_default();
                    if !new_key.is_empty() {
                        let mut lock = plugin.tracker.write().await;
                        if let Some(data) = lock.get_component_data_mut(&component_id) {
                            data.current_sessions.insert(context_key, new_key);
                        }
                    }
                }
                return result;
            }
            // No thread_id yet — fall through to create new
        }

        // No current session — create new
        let (internal_key, resource) = match create_new_session(
            self,
            &plugin,
            &component_id,
            &context_key,
            &prompt,
            config.as_ref(),
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
}

// ---------------------------------------------------------------------------
// WIT executor::HostExecStream
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::codex::executor::HostExecStream for ActiveCtx<'a> {
    async fn next(
        &mut self,
        stream_resource: Resource<ExecStreamHandle>,
    ) -> wasmtime::Result<Result<(Vec<ExecStreamEvent>, bool), CodexError>> {
        // Read handle data first to avoid borrow conflicts
        let (session_key, was_ended) = {
            let handle = self.table.get(&stream_resource)?;
            (handle.session_key.clone(), handle.ended)
        };

        if was_ended {
            return Ok(Ok((vec![], true)));
        }

        let Some(plugin) = self.get_plugin::<Codex>(PLUGIN_ID) else {
            return Ok(Err(CodexError::Internal(
                "codex plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let mut lock = plugin.tracker.write().await;
        let (events, done) = match lock.get_component_data_mut(&component_id) {
            Some(data) => match data.sessions.get_mut(&session_key) {
                Some(session) => {
                    // Drain new events from channel
                    drain_channel(session);

                    // Handle auto-approval for ExecApprovalRequest events
                    if session.auto_approve && session.pending_approval_item_id.is_some() {
                        if let Some(ref mut stdin) = session.stdin {
                            use tokio::io::AsyncWriteExt;
                            let _ = stdin.write(b"y\n").await;
                        }
                        session.pending_approval_item_id = None;
                    }

                    // Convert pending events to WIT types
                    let wit_events: Vec<ExecStreamEvent> = session
                        .pending_events
                        .drain(..)
                        .map(convert_event)
                        .collect();

                    // If not auto_approve and there's a pending approval, filter out
                    // the approval event from auto-forwarding (it will be returned as
                    // the last event for the Wasm component to handle)
                    let done = session.ended;
                    (wit_events, done)
                }
                None => {
                    return Ok(Err(CodexError::NotFound("session not found".to_string())));
                }
            },
            None => {
                return Ok(Err(CodexError::NotFound(
                    "component not tracked".to_string(),
                )));
            }
        };

        // Update handle ended flag and session_id_map
        if done {
            let handle = self.table.get_mut(&stream_resource)?;
            handle.ended = true;
        }

        // Update session_id_map if session_id became available
        if let Some(data) = lock.get_component_data_mut(&component_id)
            && let Some(session) = data.sessions.get(&session_key)
            && let Some(ref sid) = session.session_id
            && !data.session_id_map.contains_key(sid)
        {
            data.session_id_map.insert(sid.clone(), session_key.clone());
        }

        // Update session_metadata with thread_id if it became available
        if let Some(data) = lock.get_component_data_mut(&component_id)
            && let Some(session) = data.sessions.get(&session_key)
            && let Some(ref sid) = session.session_id
            && let Some(meta) = data.session_metadata.get_mut(&session_key)
            && meta.session_id.is_empty()
        {
            meta.session_id = sid.clone();
            meta.thread_id = sid.clone();
        }

        Ok(Ok((events, done)))
    }

    async fn drop(&mut self, rep: Resource<ExecStreamHandle>) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WIT session::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::codex::session::Host for ActiveCtx<'a> {
    #[instrument(skip_all)]
    async fn get_usage(
        &mut self,
        session_id: String,
    ) -> wasmtime::Result<Result<TokenUsage, CodexError>> {
        let Some(plugin) = self.get_plugin::<Codex>(PLUGIN_ID) else {
            return Ok(Err(CodexError::Internal(
                "codex plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let lock = plugin.tracker.read().await;
        match lock.get_component_data(&component_id) {
            Some(data) => {
                // Look up internal key from session_id_map
                let session_key = match data.session_id_map.get(&session_id) {
                    Some(k) => k.clone(),
                    None => {
                        return Ok(Err(CodexError::NotFound(format!(
                            "session '{session_id}' not found"
                        ))));
                    }
                };

                match data.sessions.get(&session_key) {
                    Some(session) => Ok(Ok(TokenUsage {
                        input_tokens: session.usage.input_tokens,
                        cached_input_tokens: session.usage.cached_input_tokens,
                        output_tokens: session.usage.output_tokens,
                    })),
                    None => Ok(Err(CodexError::NotFound(format!(
                        "session '{session_id}' not found"
                    )))),
                }
            }
            None => Ok(Err(CodexError::NotFound(
                "component not tracked".to_string(),
            ))),
        }
    }

    #[instrument(skip_all)]
    async fn resume(
        &mut self,
        session_id: String,
        prompt: String,
        config: Option<WitCodexConfig>,
    ) -> wasmtime::Result<Result<Resource<ExecStreamHandle>, CodexError>> {
        let Some(plugin) = self.get_plugin::<Codex>(PLUGIN_ID) else {
            return Ok(Err(CodexError::Internal(
                "codex plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        // Resolve config from dynamic param or fallback to interface config
        let config = {
            let lock = plugin.tracker.read().await;
            match lock.get_component_data(&component_id) {
                Some(data) => match resolve_codex_config(config.as_ref(), &data.interface_config) {
                    Ok(c) => c,
                    Err(e) => {
                        return Ok(Err(CodexError::ConfigError(e)));
                    }
                },
                None => {
                    return Ok(Err(CodexError::Internal(
                        "component not tracked".to_string(),
                    )));
                }
            }
        };

        // Ensure binary exists (download if needed)
        if let Err(e) = ensure_codex_binary(&config.binary_path).await {
            return Ok(Err(CodexError::ProcessError(format!(
                "codex binary not available: {e}"
            ))));
        }

        // Spawn codex subprocess with resume
        let spawn_config = CodexSpawnConfig {
            binary_path: config.binary_path.clone(),
            model: config.model.clone(),
            api_token: config.api_token.clone(),
            base_url: config.base_url.clone(),
            project_dir: config.project_dir.clone(),
        };

        let (mut child, child_stdin) =
            match spawn_codex_exec(&spawn_config, &prompt, Some(&session_id)) {
                Ok(pair) => pair,
                Err(e) => {
                    return Ok(Err(CodexError::ProcessError(format!(
                        "failed to spawn codex resume: {e}"
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

        // Create channel for JSONL events
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Spawn background reader task
        let cancel_token = {
            let lock = plugin.tracker.read().await;
            match lock.get_component_data(&component_id) {
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
                    debug!("Codex resume JSONL reader cancelled");
                }
                _ = read_jsonl_output(stdout, tx) => {}
            }
        });

        // Store new session in tracker
        let new_session_key = new_session_key();
        let session = CodexSessionState {
            event_rx: rx,
            pending_events: Vec::new(),
            ended: false,
            usage: TokenUsageAccum::default(),
            session_id: Some(session_id.clone()),
            stdin: Some(child_stdin),
            auto_approve: true,
            pending_approval_item_id: None,
        };

        {
            let mut lock = plugin.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(&component_id) {
                data.sessions.insert(new_session_key.clone(), session);
                data.session_id_map
                    .insert(session_id, new_session_key.clone());
            }
        }

        let handle = ExecStreamHandle {
            session_key: new_session_key,
            ended: false,
        };

        let resource = self.table.push(handle)?;
        Ok(Ok(resource))
    }

    #[instrument(skip_all)]
    async fn new_session(
        &mut self,
        context_key: String,
        prompt: String,
        config: Option<WitCodexConfig>,
    ) -> wasmtime::Result<Result<Resource<ExecStreamHandle>, CodexError>> {
        let Some(plugin) = self.get_plugin::<Codex>(PLUGIN_ID) else {
            return Ok(Err(CodexError::Internal(
                "codex plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let (internal_key, resource) = match create_new_session(
            self,
            &plugin,
            &component_id,
            &context_key,
            &prompt,
            config.as_ref(),
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
                let internal_key = match data.session_id_map.get(&session_id) {
                    Some(k) => k.clone(),
                    None => {
                        return Ok(Err(CodexError::NotFound(format!(
                            "session '{session_id}' not found"
                        ))));
                    }
                };

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
                let internal_key = match data.session_id_map.get(&session_id) {
                    Some(k) => k.clone(),
                    None => {
                        return Ok(Err(CodexError::NotFound(format!(
                            "session '{session_id}' not found"
                        ))));
                    }
                };

                data.sessions.remove(&internal_key);
                data.session_id_map.remove(&session_id);
                data.session_metadata.remove(&internal_key);
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

    #[instrument(skip_all)]
    async fn list_sessions(&mut self) -> wasmtime::Result<Result<Vec<SessionInfo>, CodexError>> {
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

    #[instrument(skip_all)]
    async fn set_auto_approve(
        &mut self,
        session_id: String,
        auto_approve: bool,
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
                let internal_key = match data.session_id_map.get(&session_id) {
                    Some(k) => k.clone(),
                    None => {
                        return Ok(Err(CodexError::NotFound(format!(
                            "session '{session_id}' not found"
                        ))));
                    }
                };

                match data.sessions.get_mut(&internal_key) {
                    Some(session) => {
                        debug!(
                            session_id = %session_id,
                            auto_approve = auto_approve,
                            "Codex session auto-approve mode changed"
                        );
                        session.auto_approve = auto_approve;
                        Ok(Ok(()))
                    }
                    None => Ok(Err(CodexError::NotFound(format!(
                        "session '{session_id}' not found"
                    )))),
                }
            }
            None => Ok(Err(CodexError::NotFound(
                "component not tracked".to_string(),
            ))),
        }
    }

    #[instrument(skip_all)]
    async fn approve(
        &mut self,
        session_id: String,
        item_id: String,
        approved: bool,
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
                let internal_key = match data.session_id_map.get(&session_id) {
                    Some(k) => k.clone(),
                    None => {
                        return Ok(Err(CodexError::NotFound(format!(
                            "session '{session_id}' not found"
                        ))));
                    }
                };

                match data.sessions.get_mut(&internal_key) {
                    Some(session) => {
                        // Verify the item_id matches the pending approval
                        match &session.pending_approval_item_id {
                            Some(pending_id) if pending_id == &item_id => {}
                            Some(_) => {
                                return Ok(Err(CodexError::Internal(format!(
                                    "item '{item_id}' does not match pending approval"
                                ))));
                            }
                            None => {
                                return Ok(Err(CodexError::Internal(
                                    "no pending approval request".to_string(),
                                )));
                            }
                        }

                        // Write approval response to codex stdin
                        if let Some(ref mut stdin) = session.stdin {
                            let response = if approved { b"y\n" } else { b"n\n" };
                            use tokio::io::AsyncWriteExt;
                            if let Err(e) = stdin.write(response).await {
                                return Ok(Err(CodexError::ProcessError(format!(
                                    "failed to write approval to stdin: {e}"
                                ))));
                            }
                        }

                        debug!(
                            session_id = %session_id,
                            item_id = %item_id,
                            approved = approved,
                            "Codex approval response sent"
                        );

                        session.pending_approval_item_id = None;

                        Ok(Ok(()))
                    }
                    None => Ok(Err(CodexError::NotFound(format!(
                        "session '{session_id}' not found"
                    )))),
                }
            }
            None => Ok(Err(CodexError::NotFound(
                "component not tracked".to_string(),
            ))),
        }
    }
}

#[async_trait]
impl HostPlugin for Codex {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: HashSet::from([WitInterface::from("custom:codex/executor,session@0.1.0")]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Only handle codex interfaces
        let Some(interface) = find_interface(&interfaces, "custom", "codex")
        else {
            return Ok(());
        };

        // Extract and validate config (keep for validation + binary download)
        let interface_config = interface.config.clone();
        let config = match extract_config(interface) {
            Ok(c) => c,
            Err(e) => {
                warn!("Codex plugin config validation failed: {e}");
                return Ok(());
            }
        };

        // Add executor and session imports to linker (must always succeed
        // so the component can resolve its imports).
        bindings::custom::codex::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::codex::executor::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::codex::session::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        // Only track components (not services)
        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        // Ensure project dir exists
        if !config.project_dir.exists() {
            std::fs::create_dir_all(&config.project_dir)?;
        }

        // Ensure binary exists (download if needed) — non-fatal; will be
        // retried at execution time if it fails here.
        if let Err(e) = ensure_codex_binary(&config.binary_path).await {
            warn!(
                path = %config.binary_path.display(),
                "Codex binary not available yet (will retry at execution time): {e}"
            );
        }

        debug!(
            component_id = component_handle.id(),
            model = %config.model,
            binary = %config.binary_path.display(),
            project_dir = %config.project_dir.display(),
            "Codex plugin bound to component"
        );

        self.tracker.write().await.add_component(
            component_handle,
            ComponentData {
                cancel_token: tokio_util::sync::CancellationToken::new(),
                interface_config,
                sessions: HashMap::new(),
                session_id_map: HashMap::new(),
                current_sessions: HashMap::new(),
                session_metadata: HashMap::new(),
            },
        );

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let workload_cleanup = |_| async {};
        let component_cleanup = |component_data: ComponentData| async move {
            component_data.cancel_token.cancel();
        };

        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(workload_id, workload_cleanup, component_cleanup)
            .await;

        debug!(workload_id = %workload_id, "Codex plugin unbound");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() {
        let plugin = Codex::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_exports() {
        let plugin = Codex::new();
        let world = plugin.world();
        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "codex")
        );
    }

    #[test]
    fn test_extract_config_missing_api_token() {
        let mut config = HashMap::new();
        config.insert("model".to_string(), "gpt-5.4".to_string());

        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "codex".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config,
            name: None,
        };

        let result = extract_config(&interface);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("api_token"));
    }

    #[test]
    fn test_extract_config_missing_model() {
        let mut config = HashMap::new();
        config.insert("api_token".to_string(), "sk-test".to_string());

        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "codex".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config,
            name: None,
        };

        let result = extract_config(&interface);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("model"));
    }

    #[test]
    fn test_extract_config_success() {
        let mut config = HashMap::new();
        config.insert("api_token".to_string(), "sk-test".to_string());
        config.insert("model".to_string(), "gpt-5.4".to_string());

        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "codex".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config,
            name: None,
        };

        let cfg = extract_config(&interface).unwrap();
        assert_eq!(cfg.api_token, "sk-test");
        assert_eq!(cfg.model, "gpt-5.4");
        assert!(cfg.base_url.is_none());
    }

    #[test]
    fn test_extract_config_with_base_url() {
        let mut config = HashMap::new();
        config.insert("api_token".to_string(), "sk-test".to_string());
        config.insert("model".to_string(), "gpt-5.4".to_string());
        config.insert(
            "base_url".to_string(),
            "https://my-llm.example.com/v1".to_string(),
        );

        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "codex".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config,
            name: None,
        };

        let cfg = extract_config(&interface).unwrap();
        assert_eq!(cfg.base_url.unwrap(), "https://my-llm.example.com/v1");
    }

    #[test]
    fn test_convert_thread_started() {
        let event = CodexJsonlEvent::ThreadStarted {
            thread_id: "abc-123".to_string(),
        };
        let wit_event = convert_event(event);
        match wit_event {
            ExecStreamEvent::Done(id) => assert_eq!(id, "abc-123"),
            _ => panic!("expected Done variant"),
        }
    }

    #[test]
    fn test_convert_turn_completed() {
        let event = CodexJsonlEvent::TurnCompleted {
            usage: codex_process::CodexUsage {
                input_tokens: 100,
                cached_input_tokens: 50,
                output_tokens: 200,
            },
        };
        let wit_event = convert_event(event);
        match wit_event {
            ExecStreamEvent::Usage(usage) => {
                assert_eq!(usage.input_tokens, 100);
                assert_eq!(usage.cached_input_tokens, 50);
                assert_eq!(usage.output_tokens, 200);
            }
            _ => panic!("expected Usage variant"),
        }
    }

    #[test]
    fn test_new_session_key_unique() {
        let key1 = new_session_key();
        let key2 = new_session_key();
        assert_ne!(key1, key2);
    }
}
