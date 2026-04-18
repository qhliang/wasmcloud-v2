# LLM API Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `/v1/chat/completions` (OpenAI) and `/v1/responses` (Anthropic) HTTP endpoints to the existing `custom_plugin_llm_gateway` host plugin, with SSE streaming support, using byokey-translate for format conversion.

**Architecture:** The plugin receives HTTP requests via wasi:http/incoming-handler. Requests are routed to completions or responses handlers. byokey-translate handles API format translation. genai handles provider calls. Existing WASI chat/chat-streaming interfaces remain unchanged.

**Tech Stack:** Rust, genai, byokey-translate, byokey-types, serde/serde_json, tokio, wasmtime

---

## File Structure

| File | Responsibility |
|------|---------------|
| `crates/custom_plugin_llm_gateway/Cargo.toml` | Add byokey-translate, byokey-types dependencies |
| `crates/custom_plugin_llm_gateway/src/lib.rs` | Existing plugin code — add `mod http_handler;`, expose `LlmGatewayConfig` as pub, add `pub async fn handle_http_request` method |
| `crates/custom_plugin_llm_gateway/src/http_handler.rs` | New — HTTP request routing, completions/responses handlers, SSE streaming |
| `crates/custom_plugin_llm_gateway/src/openai_types.rs` | New — OpenAI Chat Completions serde request/response types |
| `crates/custom_plugin_llm_gateway/src/anthropic_types.rs` | New — Anthropic Messages API serde request/response types |

---

### Task 1: Add dependencies to Cargo.toml

**Files:**
- Modify: `crates/custom_plugin_llm_gateway/Cargo.toml`

- [ ] **Step 1: Add byokey crates to Cargo.toml**

Add to the `[dependencies]` section of `crates/custom_plugin_llm_gateway/Cargo.toml`:

```toml
# API format translation
byokey-translate = "0.6"
byokey-types = "0.6"
```

- [ ] **Step 2: Verify the crate compiles**

Run: `cargo build -p custom_plugin_llm_gateway`
Expected: Compiles successfully (may warn about unused dependencies)

- [ ] **Step 3: Commit**

```bash
git add crates/custom_plugin_llm_gateway/Cargo.toml
git commit -m "chore: add byokey-translate and byokey-types dependencies"
```

---

### Task 2: Create OpenAI Chat Completions types

**Files:**
- Create: `crates/custom_plugin_llm_gateway/src/openai_types.rs`

- [ ] **Step 1: Create openai_types.rs with serde types**

Create `crates/custom_plugin_llm_gateway/src/openai_types.rs`:

```rust
//! OpenAI Chat Completions API request/response types.
//! These types mirror the OpenAI Chat Completions API format.

use serde::{Deserialize, Serialize};

/// OpenAI Chat Completions request
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsRequest {
    pub model: String,
    pub messages: Vec<CompletionsMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<serde_json::Value>,
}

/// A message in the completions request
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsMessage {
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

/// OpenAI Chat Completions response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsResponse {
    pub id: String,
    pub object: &'static str,
    pub created: u64,
    pub model: String,
    pub choices: Vec<CompletionsChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<CompletionsUsage>,
}

/// A choice in the completions response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsChoice {
    pub index: u32,
    pub message: CompletionsResponseMessage,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

/// Response message
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsResponseMessage {
    pub role: String,
    pub content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<serde_json::Value>>,
}

/// Token usage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

/// SSE streaming chunk
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsChunk {
    pub id: String,
    pub object: &'static str,
    pub created: u64,
    pub model: String,
    pub choices: Vec<CompletionsChunkChoice>,
}

/// A choice in a streaming chunk
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsChunkChoice {
    pub index: u32,
    pub delta: CompletionsChunkDelta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

/// Delta content in a streaming chunk
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsChunkDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// OpenAI-compatible error response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsError {
    pub error: CompletionsErrorDetail,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionsErrorDetail {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: Option<String>,
}
```

- [ ] **Step 2: Add `mod openai_types;` to lib.rs**

Add at the top of `crates/custom_plugin_llm_gateway/src/lib.rs` (after the existing `use` block, before `mod bindings`):

```rust
mod openai_types;
mod anthropic_types;
```

- [ ] **Step 3: Commit**

```bash
git add crates/custom_plugin_llm_gateway/src/openai_types.rs crates/custom_plugin_llm_gateway/src/lib.rs
git commit -m "feat: add OpenAI Chat Completions serde types"
```

---

### Task 3: Create Anthropic Messages API types

**Files:**
- Create: `crates/custom_plugin_llm_gateway/src/anthropic_types.rs`

- [ ] **Step 1: Create anthropic_types.rs with serde types**

Create `crates/custom_plugin_llm_gateway/src/anthropic_types.rs`:

```rust
//! Anthropic Messages API request/response types.
//! These types mirror the Anthropic Messages API format.

use serde::{Deserialize, Serialize};

/// Anthropic Messages request
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesRequest {
    pub model: String,
    pub messages: Vec<ResponsesMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<serde_json::Value>,
}

/// A message in the responses request
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesMessage {
    pub role: String,
    pub content: serde_json::Value,
}

/// Anthropic Messages response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesResponse {
    pub id: String,
    #[serde(rename = "type")]
    pub response_type: String,
    pub role: String,
    pub content: Vec<ResponsesContentBlock>,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequence: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<ResponsesUsage>,
}

/// Content block in the response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesContentBlock {
    #[serde(rename = "type")]
    pub block_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
}

/// Token usage
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// Anthropic error response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesError {
    #[serde(rename = "type")]
    pub error_type: String,
    pub error: ResponsesErrorDetail,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesErrorDetail {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

// ---- SSE streaming event types ----

/// SSE event wrapper for Anthropic streaming
#[derive(Clone, Debug, Serialize)]
pub struct SseEvent {
    pub event: String,
    pub data: String,
}

/// Message start event data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageStartEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub message: ResponsesResponse,
}

/// Content block start event data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContentBlockStartEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub index: u32,
    pub content_block: ResponsesContentBlock,
}

/// Content block delta event data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContentBlockDeltaEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub index: u32,
    pub delta: TextDelta,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TextDelta {
    #[serde(rename = "type")]
    pub delta_type: String,
    pub text: String,
}

/// Content block stop event data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContentBlockStopEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub index: u32,
}

/// Message delta event data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageDeltaEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub delta: MessageDelta,
    pub usage: Option<ResponsesUsage>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageDelta {
    pub stop_reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequence: Option<String>,
}

/// Message stop event data
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageStopEvent {
    #[serde(rename = "type")]
    pub event_type: String,
}
```

- [ ] **Step 2: Commit**

```bash
git add crates/custom_plugin_llm_gateway/src/anthropic_types.rs
git commit -m "feat: add Anthropic Messages API serde types"
```

---

### Task 4: Create HTTP handler module

**Files:**
- Create: `crates/custom_plugin_llm_gateway/src/http_handler.rs`
- Modify: `crates/custom_plugin_llm_gateway/src/lib.rs`

- [ ] **Step 1: Create http_handler.rs with routing and completions handler**

Create `crates/custom_plugin_llm_gateway/src/http_handler.rs`:

```rust
//! HTTP handler for LLM API Gateway endpoints.
//!
//! Routes incoming HTTP requests to the appropriate handler:
//! - POST /v1/chat/completions  -> OpenAI Chat Completions format
//! - POST /v1/responses         -> Anthropic Messages format

use std::sync::Arc;

use genai::chat::{ChatMessage as GenaiChatMessage, ChatOptions, ChatRequest};
use genai::Client;
use tokio::sync::RwLock;
use tracing::{debug, instrument};

use crate::anthropic_types::*;
use crate::openai_types::*;
use crate::{to_genai_role, ComponentData, LlmGatewayConfig, PLUGIN_ID};
use wash_runtime::plugin::WorkloadTracker;

/// Result of handling an HTTP request
pub struct HttpResponse {
    pub status: u16,
    pub body: String,
    pub content_type: String,
    pub is_sse: bool,
}

impl HttpResponse {
    fn json(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
            content_type: "application/json".to_string(),
            is_sse: false,
        }
    }

    fn sse(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: body.into(),
            content_type: "text/event-stream".to_string(),
            is_sse: true,
        }
    }
}

/// Route and handle an incoming HTTP request.
///
/// This is the main entry point called when the plugin receives an HTTP request
/// via wasi:http/incoming-handler.
#[instrument(skip_all, fields(path = %path, method = %method))]
pub async fn handle_http_request(
    path: &str,
    method: &str,
    body: &[u8],
    config: &LlmGatewayConfig,
    client: Client,
) -> HttpResponse {
    // Only accept POST
    if method != "POST" {
        return HttpResponse::json(405, r#"{"error":{"message":"Method not allowed","type":"invalid_request_error","code":null}}"#);
    }

    match path {
        "/v1/chat/completions" | "/v1/chat/completions/" => {
            handle_completions(body, config, client).await
        }
        "/v1/responses" | "/v1/responses/" => {
            handle_responses(body, config, client).await
        }
        _ => HttpResponse::json(
            404,
            r#"{"error":{"message":"Not found","type":"invalid_request_error","code":null}}"#,
        ),
    }
}

/// Generate a chat completion ID
fn generate_completion_id() -> String {
    format!("chatcmpl-{}", uuid::Uuid::new_v4().simple())
}

/// Generate a message ID
fn generate_message_id() -> String {
    format!("msg-{}", uuid::Uuid::new_v4().simple())
}

// ============================================================================
// OpenAI Chat Completions handler
// ============================================================================

async fn handle_completions(
    body: &[u8],
    config: &LlmGatewayConfig,
    client: Client,
) -> HttpResponse {
    // Parse request
    let req = match serde_json::from_slice::<CompletionsRequest>(body) {
        Ok(r) => r,
        Err(e) => {
            let err = CompletionsError {
                error: CompletionsErrorDetail {
                    message: format!("invalid request body: {e}"),
                    error_type: "invalid_request_error".to_string(),
                    code: None,
                },
            };
            return HttpResponse::json(400, serde_json::to_string(&err).unwrap_or_default());
        }
    };

    let model = if req.model.is_empty() {
        &config.model_name
    } else {
        &req.model
    };

    let is_stream = req.stream.unwrap_or(false);

    debug!(
        model = %model,
        stream = is_stream,
        message_count = req.messages.len(),
        "Handling completions request"
    );

    // Build genai messages
    let mut all_messages = Vec::new();

    // Add preset system prompts
    for prompt in &config.system_prompts {
        match to_genai_role(&prompt.role) {
            genai::chat::ChatRole::System => {
                all_messages.push(GenaiChatMessage::system(&prompt.content));
            }
            genai::chat::ChatRole::Assistant => {
                all_messages.push(GenaiChatMessage::assistant(&prompt.content));
            }
            _ => {
                all_messages.push(GenaiChatMessage::user(&prompt.content));
            }
        }
    }

    // Convert request messages
    for msg in &req.messages {
        let content = match &msg.content {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|v| v.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join(""),
            Some(v) => v.to_string(),
            None => String::new(),
        };
        match msg.role.as_str() {
            "system" => all_messages.push(GenaiChatMessage::system(&content)),
            "assistant" => all_messages.push(GenaiChatMessage::assistant(&content)),
            _ => all_messages.push(GenaiChatMessage::user(&content)),
        }
    }

    // Build chat options (config defaults + request overrides)
    let mut opts = ChatOptions::default();
    if let Some(temp) = req.temperature.or(config.temperature) {
        opts = opts.with_temperature(temp);
    }
    if let Some(max_tokens) = req.max_tokens.or(config.max_tokens) {
        opts = opts.with_max_tokens(max_tokens);
    }
    if let Some(top_p) = req.top_p.or(config.top_p) {
        opts = opts.with_top_p(top_p);
    }

    if is_stream {
        handle_completions_streaming(client, model, all_messages, opts).await
    } else {
        handle_completions_sync(client, model, all_messages, opts, req).await
    }
}

async fn handle_completions_sync(
    client: Client,
    model: &str,
    messages: Vec<GenaiChatMessage>,
    opts: ChatOptions,
    req: CompletionsRequest,
) -> HttpResponse {
    let chat_req = ChatRequest::new(messages);

    let chat_res = match client
        .exec_chat(model, chat_req, Some(&opts))
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let err = CompletionsError {
                error: CompletionsErrorDetail {
                    message: e.to_string(),
                    error_type: "provider_error".to_string(),
                    code: None,
                },
            };
            return HttpResponse::json(500, serde_json::to_string(&err).unwrap_or_default());
        }
    };

    let content = chat_res.first_text().unwrap_or("").to_string();
    let response_model = chat_res.model_iden.model_name.to_string();
    let usage = CompletionsUsage {
        prompt_tokens: chat_res.usage.prompt_tokens.unwrap_or(0) as u64,
        completion_tokens: chat_res.usage.completion_tokens.unwrap_or(0) as u64,
        total_tokens: chat_res.usage.total_tokens.unwrap_or(0) as u64,
    };

    let response = CompletionsResponse {
        id: generate_completion_id(),
        object: "chat.completion",
        created: chrono::Utc::now().timestamp() as u64,
        model: response_model,
        choices: vec![CompletionsChoice {
            index: 0,
            message: CompletionsResponseMessage {
                role: "assistant".to_string(),
                content: Some(content),
                tool_calls: None,
            },
            finish_reason: Some("stop".to_string()),
        }],
        usage: Some(usage),
    };

    match serde_json::to_string(&response) {
        Ok(json) => HttpResponse::json(200, json),
        Err(e) => HttpResponse::json(500, format!(r#"{{"error":{{"message":"{e}","type":"server_error","code":null}}}}"#)),
    }
}

async fn handle_completions_streaming(
    client: Client,
    model: &str,
    messages: Vec<GenaiChatMessage>,
    opts: ChatOptions,
) -> HttpResponse {
    let chat_req = ChatRequest::new(messages);

    let chat_stream_res = match client
        .exec_chat_stream(model, chat_req, Some(&opts))
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let err = CompletionsError {
                error: CompletionsErrorDetail {
                    message: e.to_string(),
                    error_type: "provider_error".to_string(),
                    code: None,
                },
            };
            return HttpResponse::json(500, serde_json::to_string(&err).unwrap_or_default());
        }
    };

    let model_name = chat_stream_res.model_iden.model_name.to_string();
    let completion_id = generate_completion_id();
    let created = chrono::Utc::now().timestamp() as u64;

    // Collect stream events into SSE body
    use futures::StreamExt;
    let mut stream = chat_stream_res.stream;
    let mut sse_body = String::new();

    // First chunk with role
    let first_chunk = CompletionsChunk {
        id: completion_id.clone(),
        object: "chat.completion.chunk",
        created,
        model: model_name.clone(),
        choices: vec![CompletionsChunkChoice {
            index: 0,
            delta: CompletionsChunkDelta {
                role: Some("assistant".to_string()),
                content: None,
            },
            finish_reason: None,
        }],
    };
    let first_json = serde_json::to_string(&first_chunk).unwrap_or_default();
    sse_body.push_str(&format!("data: {first_json}\n\n"));

    while let Some(result) = stream.next().await {
        match result {
            Ok(genai::chat::ChatStreamEvent::Chunk(chunk)) => {
                if chunk.content.is_empty() {
                    continue;
                }
                let chunk_data = CompletionsChunk {
                    id: completion_id.clone(),
                    object: "chat.completion.chunk",
                    created,
                    model: model_name.clone(),
                    choices: vec![CompletionsChunkChoice {
                        index: 0,
                        delta: CompletionsChunkDelta {
                            role: None,
                            content: Some(chunk.content),
                        },
                        finish_reason: None,
                    }],
                };
                let json = serde_json::to_string(&chunk_data).unwrap_or_default();
                sse_body.push_str(&format!("data: {json}\n\n"));
            }
            Ok(genai::chat::ChatStreamEvent::End(stream_end)) => {
                let finish_chunk = CompletionsChunk {
                    id: completion_id.clone(),
                    object: "chat.completion.chunk",
                    created,
                    model: model_name.clone(),
                    choices: vec![CompletionsChunkChoice {
                        index: 0,
                        delta: CompletionsChunkDelta {
                            role: None,
                            content: None,
                        },
                        finish_reason: Some(
                            stream_end
                                .captured_stop_reason
                                .map(|r| format!("{r:?}"))
                                .unwrap_or_else(|| "stop".to_string()),
                        ),
                    }],
                };
                let json = serde_json::to_string(&finish_chunk).unwrap_or_default();
                sse_body.push_str(&format!("data: {json}\n\n"));
                sse_body.push_str("data: [DONE]\n\n");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                sse_body.push_str(&format!("data: {{\"error\":\"{e}\"}}\n\n"));
                break;
            }
        }
    }

    HttpResponse::sse(sse_body)
}

// ============================================================================
// Anthropic Responses handler
// ============================================================================

async fn handle_responses(
    body: &[u8],
    config: &LlmGatewayConfig,
    client: Client,
) -> HttpResponse {
    // Parse request
    let req = match serde_json::from_slice::<ResponsesRequest>(body) {
        Ok(r) => r,
        Err(e) => {
            let err = ResponsesError {
                error_type: "error".to_string(),
                error: ResponsesErrorDetail {
                    error_type: "invalid_request_error".to_string(),
                    message: format!("invalid request body: {e}"),
                },
            };
            return HttpResponse::json(400, serde_json::to_string(&err).unwrap_or_default());
        }
    };

    let model = if req.model.is_empty() {
        &config.model_name
    } else {
        &req.model
    };

    let is_stream = req.stream.unwrap_or(false);

    debug!(
        model = %model,
        stream = is_stream,
        message_count = req.messages.len(),
        "Handling responses request"
    );

    // Build genai messages
    let mut all_messages = Vec::new();

    // Add system prompt from request if present
    if let Some(ref sys) = req.system {
        let sys_text = match sys {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Array(arr) => arr
                .iter()
                .filter_map(|v| v.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("\n"),
            _ => sys.to_string(),
        };
        all_messages.push(GenaiChatMessage::system(&sys_text));
    }

    // Add preset system prompts
    for prompt in &config.system_prompts {
        match to_genai_role(&prompt.role) {
            genai::chat::ChatRole::System => {
                all_messages.push(GenaiChatMessage::system(&prompt.content));
            }
            genai::chat::ChatRole::Assistant => {
                all_messages.push(GenaiChatMessage::assistant(&prompt.content));
            }
            _ => {
                all_messages.push(GenaiChatMessage::user(&prompt.content));
            }
        }
    }

    // Convert request messages
    for msg in &req.messages {
        let content = match &msg.content {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Array(arr) => arr
                .iter()
                .filter_map(|v| v.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join(""),
            v => v.to_string(),
        };
        match msg.role.as_str() {
            "system" => all_messages.push(GenaiChatMessage::system(&content)),
            "assistant" => all_messages.push(GenaiChatMessage::assistant(&content)),
            _ => all_messages.push(GenaiChatMessage::user(&content)),
        }
    }

    // Build chat options
    let mut opts = ChatOptions::default();
    if let Some(temp) = req.temperature.or(config.temperature) {
        opts = opts.with_temperature(temp);
    }
    if let Some(max_tokens) = req.max_tokens.or(config.max_tokens) {
        opts = opts.with_max_tokens(max_tokens);
    }
    if let Some(top_p) = req.top_p.or(config.top_p) {
        opts = opts.with_top_p(top_p);
    }

    if is_stream {
        handle_responses_streaming(client, model, all_messages, opts).await
    } else {
        handle_responses_sync(client, model, all_messages, opts).await
    }
}

async fn handle_responses_sync(
    client: Client,
    model: &str,
    messages: Vec<GenaiChatMessage>,
    opts: ChatOptions,
) -> HttpResponse {
    let chat_req = ChatRequest::new(messages);

    let chat_res = match client
        .exec_chat(model, chat_req, Some(&opts))
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let err = ResponsesError {
                error_type: "error".to_string(),
                error: ResponsesErrorDetail {
                    error_type: "provider_error".to_string(),
                    message: e.to_string(),
                },
            };
            return HttpResponse::json(500, serde_json::to_string(&err).unwrap_or_default());
        }
    };

    let content = chat_res.first_text().unwrap_or("").to_string();
    let response_model = chat_res.model_iden.model_name.to_string();
    let usage = ResponsesUsage {
        input_tokens: chat_res.usage.prompt_tokens.unwrap_or(0) as u64,
        output_tokens: chat_res.usage.completion_tokens.unwrap_or(0) as u64,
    };

    let response = ResponsesResponse {
        id: generate_message_id(),
        response_type: "message".to_string(),
        role: "assistant".to_string(),
        content: vec![ResponsesContentBlock {
            block_type: "text".to_string(),
            text: Some(content),
            id: None,
            name: None,
            input: None,
        }],
        model: response_model,
        stop_reason: Some("end_turn".to_string()),
        stop_sequence: None,
        usage: Some(usage),
    };

    match serde_json::to_string(&response) {
        Ok(json) => HttpResponse::json(200, json),
        Err(e) => HttpResponse::json(500, format!(r#"{{"type":"error","error":{{"type":"server_error","message":"{e}"}}}}"#)),
    }
}

async fn handle_responses_streaming(
    client: Client,
    model: &str,
    messages: Vec<GenaiChatMessage>,
    opts: ChatOptions,
) -> HttpResponse {
    let chat_req = ChatRequest::new(messages);

    let chat_stream_res = match client
        .exec_chat_stream(model, chat_req, Some(&opts))
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let err = ResponsesError {
                error_type: "error".to_string(),
                error: ResponsesErrorDetail {
                    error_type: "provider_error".to_string(),
                    message: e.to_string(),
                },
            };
            return HttpResponse::json(500, serde_json::to_string(&err).unwrap_or_default());
        }
    };

    let model_name = chat_stream_res.model_iden.model_name.to_string();
    let msg_id = generate_message_id();

    use futures::StreamExt;
    let mut stream = chat_stream_res.stream;
    let mut sse_body = String::new();

    // message_start event
    let start_event = MessageStartEvent {
        event_type: "message_start".to_string(),
        message: ResponsesResponse {
            id: msg_id.clone(),
            response_type: "message".to_string(),
            role: "assistant".to_string(),
            content: vec![],
            model: model_name.clone(),
            stop_reason: None,
            stop_sequence: None,
            usage: Some(ResponsesUsage {
                input_tokens: 0,
                output_tokens: 0,
            }),
        },
    };
    let start_json = serde_json::to_string(&start_event).unwrap_or_default();
    sse_body.push_str(&format!("event: message_start\ndata: {start_json}\n\n"));

    // content_block_start event
    let block_start = ContentBlockStartEvent {
        event_type: "content_block_start".to_string(),
        index: 0,
        content_block: ResponsesContentBlock {
            block_type: "text".to_string(),
            text: Some(String::new()),
            id: None,
            name: None,
            input: None,
        },
    };
    let block_start_json = serde_json::to_string(&block_start).unwrap_or_default();
    sse_body.push_str(&format!("event: content_block_start\ndata: {block_start_json}\n\n"));

    while let Some(result) = stream.next().await {
        match result {
            Ok(genai::chat::ChatStreamEvent::Chunk(chunk)) => {
                if chunk.content.is_empty() {
                    continue;
                }
                let delta_event = ContentBlockDeltaEvent {
                    event_type: "content_block_delta".to_string(),
                    index: 0,
                    delta: TextDelta {
                        delta_type: "text_delta".to_string(),
                        text: chunk.content,
                    },
                };
                let json = serde_json::to_string(&delta_event).unwrap_or_default();
                sse_body.push_str(&format!("event: content_block_delta\ndata: {json}\n\n"));
            }
            Ok(genai::chat::ChatStreamEvent::End(stream_end)) => {
                // content_block_stop
                let block_stop = ContentBlockStopEvent {
                    event_type: "content_block_stop".to_string(),
                    index: 0,
                };
                let json = serde_json::to_string(&block_stop).unwrap_or_default();
                sse_body.push_str(&format!("event: content_block_stop\ndata: {json}\n\n"));

                // message_delta
                let msg_delta = MessageDeltaEvent {
                    event_type: "message_delta".to_string(),
                    delta: MessageDelta {
                        stop_reason: stream_end
                            .captured_stop_reason
                            .map(|r| format!("{r:?}"))
                            .unwrap_or_else(|| "end_turn".to_string()),
                        stop_sequence: None,
                    },
                    usage: stream_end.captured_usage.map(|u| ResponsesUsage {
                        input_tokens: u.prompt_tokens.unwrap_or(0) as u64,
                        output_tokens: u.completion_tokens.unwrap_or(0) as u64,
                    }),
                };
                let json = serde_json::to_string(&msg_delta).unwrap_or_default();
                sse_body.push_str(&format!("event: message_delta\ndata: {json}\n\n"));

                // message_stop
                let msg_stop = MessageStopEvent {
                    event_type: "message_stop".to_string(),
                };
                let json = serde_json::to_string(&msg_stop).unwrap_or_default();
                sse_body.push_str(&format!("event: message_stop\ndata: {json}\n\n"));
                break;
            }
            Ok(_) => {}
            Err(e) => {
                sse_body.push_str(&format!("event: error\ndata: {{\"type\":\"error\",\"error\":{{\"type\":\"provider_error\",\"message\":\"{e}\"}}}}\n\n"));
                break;
            }
        }
    }

    HttpResponse::sse(sse_body)
}
```

- [ ] **Step 2: Add `mod http_handler;` to lib.rs**

In `crates/custom_plugin_llm_gateway/src/lib.rs`, add alongside the other module declarations:

```rust
mod openai_types;
mod anthropic_types;
mod http_handler;
```

Also make `to_genai_role`, `ComponentData`, `LlmGatewayConfig`, and `PLUGIN_ID` accessible to the http_handler module by ensuring they are `pub` or `pub(crate)`:

- Change `fn to_genai_role` to `pub(crate) fn to_genai_role`
- Change `struct ComponentData` to `pub(crate) struct ComponentData`
- `LlmGatewayConfig` is already `pub`
- `PLUGIN_ID` is already `const` (accessible in the same crate)

- [ ] **Step 3: Add `uuid` and `chrono` dependencies to Cargo.toml**

Add to `crates/custom_plugin_llm_gateway/Cargo.toml`:

```toml
uuid = { workspace = true }
chrono = { workspace = true }
```

- [ ] **Step 4: Build and verify compilation**

Run: `cargo build -p custom_plugin_llm_gateway`
Expected: Compiles successfully

- [ ] **Step 5: Commit**

```bash
git add crates/custom_plugin_llm_gateway/src/http_handler.rs crates/custom_plugin_llm_gateway/src/lib.rs crates/custom_plugin_llm_gateway/Cargo.toml
git commit -m "feat: add HTTP handler for completions and responses endpoints"
```

---

### Task 5: Add unit tests for type parsing

**Files:**
- Modify: `crates/custom_plugin_llm_gateway/src/lib.rs` (test section)
- Modify: `crates/custom_plugin_llm_gateway/src/openai_types.rs`
- Modify: `crates/custom_plugin_llm_gateway/src/anthropic_types.rs`

- [ ] **Step 1: Add tests to openai_types.rs**

Append to `crates/custom_plugin_llm_gateway/src/openai_types.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_completions_request() {
        let json = r#"{
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "Hello"}
            ],
            "temperature": 0.7,
            "max_tokens": 4096,
            "stream": false
        }"#;
        let req: CompletionsRequest = serde_json::from_str(json).expect("parse failed");
        assert_eq!(req.model, "gpt-4o-mini");
        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.temperature, Some(0.7));
        assert_eq!(req.max_tokens, Some(4096));
        assert_eq!(req.stream, Some(false));
    }

    #[test]
    fn test_parse_completions_request_minimal() {
        let json = r#"{"model":"gpt-4","messages":[{"role":"user","content":"Hi"}]}"#;
        let req: CompletionsRequest = serde_json::from_str(json).expect("parse failed");
        assert_eq!(req.model, "gpt-4");
        assert_eq!(req.messages.len(), 1);
        assert!(req.temperature.is_none());
        assert!(req.max_tokens.is_none());
        assert!(req.stream.is_none());
    }

    #[test]
    fn test_serialize_completions_response() {
        let resp = CompletionsResponse {
            id: "chatcmpl-test".to_string(),
            object: "chat.completion",
            created: 1234567890,
            model: "gpt-4o-mini".to_string(),
            choices: vec![CompletionsChoice {
                index: 0,
                message: CompletionsResponseMessage {
                    role: "assistant".to_string(),
                    content: Some("Hello!".to_string()),
                    tool_calls: None,
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: Some(CompletionsUsage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
            }),
        };
        let json = serde_json::to_string(&resp).expect("serialize failed");
        assert!(json.contains("\"object\":\"chat.completion\""));
        assert!(json.contains("\"content\":\"Hello!\""));
    }

    #[test]
    fn test_serialize_completions_chunk() {
        let chunk = CompletionsChunk {
            id: "chatcmpl-test".to_string(),
            object: "chat.completion.chunk",
            created: 1234567890,
            model: "gpt-4o-mini".to_string(),
            choices: vec![CompletionsChunkChoice {
                index: 0,
                delta: CompletionsChunkDelta {
                    role: None,
                    content: Some("Hello".to_string()),
                },
                finish_reason: None,
            }],
        };
        let json = serde_json::to_string(&chunk).expect("serialize failed");
        assert!(json.contains("\"object\":\"chat.completion.chunk\""));
        assert!(json.contains("\"content\":\"Hello\""));
    }

    #[test]
    fn test_serialize_error_response() {
        let err = CompletionsError {
            error: CompletionsErrorDetail {
                message: "test error".to_string(),
                error_type: "invalid_request_error".to_string(),
                code: None,
            },
        };
        let json = serde_json::to_string(&err).expect("serialize failed");
        assert!(json.contains("\"message\":\"test error\""));
    }
}
```

- [ ] **Step 2: Add tests to anthropic_types.rs**

Append to `crates/custom_plugin_llm_gateway/src/anthropic_types.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_responses_request() {
        let json = r#"{
            "model": "claude-sonnet-4-5-20250514",
            "messages": [
                {"role": "user", "content": "Hello"}
            ],
            "system": "You are helpful",
            "temperature": 0.7,
            "max_tokens": 4096,
            "stream": false
        }"#;
        let req: ResponsesRequest = serde_json::from_str(json).expect("parse failed");
        assert_eq!(req.model, "claude-sonnet-4-5-20250514");
        assert_eq!(req.messages.len(), 1);
        assert_eq!(req.temperature, Some(0.7));
        assert_eq!(req.max_tokens, Some(4096));
        assert!(req.system.is_some());
    }

    #[test]
    fn test_parse_responses_request_minimal() {
        let json = r#"{"model":"claude-3-haiku","messages":[{"role":"user","content":"Hi"}]}"#;
        let req: ResponsesRequest = serde_json::from_str(json).expect("parse failed");
        assert_eq!(req.model, "claude-3-haiku");
        assert!(req.system.is_none());
        assert!(req.temperature.is_none());
    }

    #[test]
    fn test_serialize_responses_response() {
        let resp = ResponsesResponse {
            id: "msg-test".to_string(),
            response_type: "message".to_string(),
            role: "assistant".to_string(),
            content: vec![ResponsesContentBlock {
                block_type: "text".to_string(),
                text: Some("Hello!".to_string()),
                id: None,
                name: None,
                input: None,
            }],
            model: "claude-sonnet-4-5-20250514".to_string(),
            stop_reason: Some("end_turn".to_string()),
            stop_sequence: None,
            usage: Some(ResponsesUsage {
                input_tokens: 10,
                output_tokens: 5,
            }),
        };
        let json = serde_json::to_string(&resp).expect("serialize failed");
        assert!(json.contains("\"type\":\"message\""));
        assert!(json.contains("\"stop_reason\":\"end_turn\""));
        assert!(json.contains("\"text\":\"Hello!\""));
    }

    #[test]
    fn test_serialize_error_response() {
        let err = ResponsesError {
            error_type: "error".to_string(),
            error: ResponsesErrorDetail {
                error_type: "invalid_request_error".to_string(),
                message: "test error".to_string(),
            },
        };
        let json = serde_json::to_string(&err).expect("serialize failed");
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("\"message\":\"test error\""));
    }
}
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p custom_plugin_llm_gateway`
Expected: All tests pass

- [ ] **Step 4: Commit**

```bash
git add crates/custom_plugin_llm_gateway/src/
git commit -m "test: add unit tests for OpenAI and Anthropic type parsing"
```

---

### Task 6: Note on byokey-translate usage

> `byokey-translate` is added as a dependency but the current implementation uses direct serde types for format conversion. byokey-translate operates on `serde_json::Value` and translates between provider API formats (OpenAI <-> Claude), not between API formats and genai's internal types. If future needs require more complex cross-format translation at the JSON level, byokey-translate's functions can be integrated.

---

### Task 7: Run full check suite

**Files:** None (verification only)

- [ ] **Step 1: Run all tests**

Run: `cargo test -p custom_plugin_llm_gateway`
Expected: All tests pass

- [ ] **Step 2: Run clippy**

Run: `cargo clippy -p custom_plugin_llm_gateway -- -D warnings`
Expected: No errors. Fix any warnings that appear.

- [ ] **Step 3: Check formatting**

Run: `cargo +nightly fmt -p custom_plugin_llm_gateway -- --check`
Expected: No formatting issues. Fix if needed.

- [ ] **Step 4: Check unused dependencies**

Run: `cargo machete`
Expected: No unused dependencies in custom_plugin_llm_gateway. If flagged, remove the unused dep.

- [ ] **Step 5: Commit any fixes**

```bash
git add -A
git commit -m "chore: fix clippy and fmt issues"
```
