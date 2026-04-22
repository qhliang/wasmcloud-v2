# LLM Gateway Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the monolithic `custom_plugin_llm_gateway` into a pure provider host plugin, an HTTP wasm component, and a messaging wasm component.

**Architecture:** Provider host plugin keeps the genai integration (renamed from `custom_plugin_llm_gateway` to `custom_plugin_llm_gateway_provider`). Two new wasm components consume the provider via WIT bindings — one for OpenAI/Anthropic-compatible HTTP API, one for NATS JSON request/response.

**Tech Stack:** Rust, genai 0.6.0-beta.11, wstd 0.6.3, wit-bindgen 0.46.0, wasm32-wasip2 target

---

## File Structure

### Modified files
- `Cargo.toml` (workspace) — rename member, add exclude for wasm crates
- `crates/custom_plugin_llm_gateway/` → renamed to `crates/custom_plugin_llm_gateway_provider/`
- `crates/custom_plugin_llm_gateway_provider/src/lib.rs` — remove http_handler/anthropic_types/openai_types module refs, update PLUGIN_ID
- `crates/custom_plugin_llm_gateway_provider/Cargo.toml` — rename crate, remove uuid dep
- `crates/wash/src/host/mod.rs` — update plugin import path (if referencing old name)

### Deleted files
- `crates/custom_plugin_llm_gateway_provider/src/http_handler.rs`
- `crates/custom_plugin_llm_gateway_provider/src/anthropic_types.rs`
- `crates/custom_plugin_llm_gateway_provider/src/openai_types.rs`

### New files — llm-gateway-http component
- `crates/llm-gateway-http/Cargo.toml`
- `crates/llm-gateway-http/wit/world.wit`
- `crates/llm-gateway-http/wit/deps/custom-llm-gateway.wit` (copy from provider)
- `crates/llm-gateway-http/wit/deps/wasi-logging-0.1.0-draft/package.wit` (copy from examples)
- `crates/llm-gateway-http/wit/deps/wasi-http-0.2.2/package.wit` (copy from examples)
- `crates/llm-gateway-http/wit/deps/wasi-io-0.2.2/package.wit` (copy from examples)
- `crates/llm-gateway-http/wit/deps/wasi-cli-0.2.2/package.wit` (copy from examples)
- `crates/llm-gateway-http/wit/deps/wasi-clocks-0.2.2/package.wit` (copy from examples)
- `crates/llm-gateway-http/wit/deps/wasi-random-0.2.2/package.wit` (copy from examples)
- `crates/llm-gateway-http/wit/deps/wasi-config-0.2.0-rc.1/package.wit` (copy from examples)
- `crates/llm-gateway-http/src/lib.rs`
- `crates/llm-gateway-http/src/completions.rs`
- `crates/llm-gateway-http/src/responses.rs`
- `crates/llm-gateway-http/src/types.rs`
- `crates/llm-gateway-http/src/helpers.rs`

### New files — llm-gateway-messaging component
- `crates/llm-gateway-messaging/Cargo.toml`
- `crates/llm-gateway-messaging/wit/world.wit`
- `crates/llm-gateway-messaging/wit/deps/custom-llm-gateway.wit` (copy from provider)
- `crates/llm-gateway-messaging/wit/deps/wasi-logging-0.1.0-draft/package.wit` (copy from examples)
- `crates/llm-gateway-messaging/wit/deps/wasmcloud-messaging-0.2.0/package.wit` (copy from fixtures)
- `crates/llm-gateway-messaging/wit/deps/wasi-cli-0.2.2/package.wit` (copy)
- `crates/llm-gateway-messaging/wit/deps/wasi-clocks-0.2.2/package.wit` (copy)
- `crates/llm-gateway-messaging/wit/deps/wasi-io-0.2.2/package.wit` (copy)
- `crates/llm-gateway-messaging/wit/deps/wasi-random-0.2.2/package.wit` (copy)
- `crates/llm-gateway-messaging/src/lib.rs`

---

## Task 1: Rename provider plugin crate

**Files:**
- Rename: `crates/custom_plugin_llm_gateway/` → `crates/custom_plugin_llm_gateway_provider/`
- Modify: `Cargo.toml` (workspace root, line 11)
- Modify: `crates/custom_plugin_llm_gateway_provider/Cargo.toml`

- [ ] **Step 1: Rename the directory**

```bash
git mv crates/custom_plugin_llm_gateway crates/custom_plugin_llm_gateway_provider
```

- [ ] **Step 2: Update provider Cargo.toml — rename crate**

In `crates/custom_plugin_llm_gateway_provider/Cargo.toml`, change line 2 from:
```toml
name = "custom_plugin_llm_gateway"
```
to:
```toml
name = "custom_plugin_llm_gateway_provider"
```

Also update description to:
```toml
description = "Host plugin for LLM Gateway provider using genai multi-provider library"
```

Remove the `uuid` dependency line (it's only used by http_handler for generating response IDs):
```toml
uuid = { workspace = true }
```

- [ ] **Step 3: Update provider lib.rs — remove http modules and update PLUGIN_ID**

In `crates/custom_plugin_llm_gateway_provider/src/lib.rs`:

Remove lines 58-60 (the three module declarations):
```rust
mod anthropic_types;
mod http_handler;
mod openai_types;
```

Remove `pub(crate) fn to_genai_role` — it was only used by http_handler. It's still used in `lib.rs` by the chat implementations for preset prompt conversion, so **keep it**.

Actually, reviewing lib.rs more carefully: `to_genai_role` is used at line 423 in `lib.rs` itself (for preset prompts in chat/chat_streaming), and at line 598/606/764/772. So **keep** `to_genai_role`.

Change PLUGIN_ID from `"llm-gateway"` to `"llm-gateway-provider"`:
```rust
const PLUGIN_ID: &str = "llm-gateway-provider";
```

Update doc comment at line 11 from `custom_plugin_llm_gateway::LlmGateway` to `custom_plugin_llm_gateway_provider::LlmGateway`.

- [ ] **Step 4: Delete the three HTTP-related source files**

```bash
rm crates/custom_plugin_llm_gateway_provider/src/http_handler.rs
rm crates/custom_plugin_llm_gateway_provider/src/anthropic_types.rs
rm crates/custom_plugin_llm_gateway_provider/src/openai_types.rs
```

- [ ] **Step 5: Update workspace Cargo.toml**

In root `Cargo.toml`, change line 11 from:
```toml
"crates/custom_plugin_llm_gateway",
```
to:
```toml
"crates/custom_plugin_llm_gateway_provider",
```

- [ ] **Step 6: Find and update all references to the old crate name**

Search for `custom_plugin_llm_gateway` (without `_provider`) across the codebase and update any imports. Key locations to check:
- `crates/wash/src/host/mod.rs` or similar files that import the plugin
- `Cargo.toml` workspace dependency references
- Any `use custom_plugin_llm_gateway::` references

```bash
cargo build -p custom_plugin_llm_gateway_provider
```
Expected: Compiles successfully.

- [ ] **Step 7: Run existing tests**

```bash
cargo test -p custom_plugin_llm_gateway_provider
```
Expected: All tests pass (the tests in `lib.rs` only test config parsing, provider parsing, and type conversion — none depend on http_handler).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: rename custom_plugin_llm_gateway to llm_gateway_provider

Remove http_handler, anthropic_types, openai_types from the provider
plugin. These will be migrated to the new llm-gateway-http wasm
component."
```

---

## Task 2: Create llm-gateway-http wasm component scaffolding

**Files:**
- Create: `crates/llm-gateway-http/Cargo.toml`
- Create: `crates/llm-gateway-http/wit/world.wit`
- Create: `crates/llm-gateway-http/wit/deps/` (copy WIT deps)
- Create: `crates/llm-gateway-http/src/lib.rs` (minimal skeleton)

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p crates/llm-gateway-http/wit/deps
mkdir -p crates/llm-gateway-http/src
```

- [ ] **Step 2: Create Cargo.toml**

Write `crates/llm-gateway-http/Cargo.toml`:
```toml
[package]
name = "llm-gateway-http"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.46.0"
wstd = "0.6.3"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

[profile.release]
lto = true
opt-level = "s"
strip = true
```

- [ ] **Step 3: Create WIT world file**

Write `crates/llm-gateway-http/wit/world.wit`:
```wit
package wasmcloud:llm-gateway-http@0.1.0;

world llm-gateway-http {
  import wasi:logging/logging@0.1.0-draft;
  import custom:llm-gateway/chat@0.1.0;
}
```

- [ ] **Step 4: Copy WIT dependency files**

Copy `custom-llm-gateway.wit` from the provider:
```bash
cp crates/custom_plugin_llm_gateway_provider/wit/deps/custom-llm-gateway.wit crates/llm-gateway-http/wit/deps/
```

Copy wasi-logging and wasi-http deps from the examples (needed by wstd):
```bash
cp -r examples/http-api-distributed/wit/deps/wasi-logging-0.1.0-draft crates/llm-gateway-http/wit/deps/
```

Also copy the wasi-http, wasi-io, wasi-cli, wasi-clocks, wasi-random, wasi-config deps that wstd needs:
```bash
for dep in wasi-http-0.2.2 wasi-io-0.2.2 wasi-cli-0.2.2 wasi-clocks-0.2.2 wasi-random-0.2.2 wasi-config-0.2.0-rc.1; do
  cp -r examples/http-api-distributed/wit/deps/$dep crates/llm-gateway-http/wit/deps/
done
```

Note: Some of these dirs may have different version suffixes. Use `ls examples/http-api-distributed/wit/deps/` to verify actual directory names and copy accordingly.

- [ ] **Step 5: Create minimal lib.rs skeleton**

Write `crates/llm-gateway-http/src/lib.rs`:
```rust
mod bindings {
    wit_bindgen::generate!({
        path: "../wit",
        world: "llm-gateway-http",
        generate_all,
    });
}

use bindings::wasi::logging::logging::{Level, log};
use wstd::http::{Body, Request, Response, StatusCode};

const LOG_CTX: &str = "llm-gateway-http";

#[wstd::http_server]
async fn main(req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let path = req.uri().path();
    log(Level::Debug, LOG_CTX, &format!("Request: {} {}", req.method(), path));

    match path {
        "/v1/chat/completions" => completions::handle(req).await,
        "/v1/messages" => responses::handle(req).await,
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body("Not found\n".into())
            .map_err(Into::into),
    }
}

mod completions;
mod responses;
mod types;
mod helpers;
```

- [ ] **Step 6: Create placeholder module files**

Write `crates/llm-gateway-http/src/types.rs`:
```rust
use serde::{Deserialize, Serialize};

// ── OpenAI types ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsRequest {
    pub model: String,
    pub messages: Vec<CompletionsMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsResponseMessage {
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsChoice {
    pub index: u32,
    pub message: CompletionsResponseMessage,
    pub finish_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsResponse {
    pub id: String,
    pub object: &'static str,
    pub model: String,
    pub choices: Vec<CompletionsChoice>,
    pub usage: CompletionsUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsErrorDetail {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub param: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionsError {
    pub error: CompletionsErrorDetail,
}

// ── Anthropic types ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesMessage {
    pub role: String,
    pub content: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesRequest {
    pub model: String,
    pub messages: Vec<ResponsesMessage>,
    pub max_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesContentBlock {
    #[serde(rename = "type")]
    pub block_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesResponse {
    pub id: String,
    #[serde(rename = "type")]
    pub response_type: String,
    pub role: String,
    pub model: String,
    pub content: Vec<ResponsesContentBlock>,
    pub stop_reason: String,
    pub usage: ResponsesUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesErrorDetail {
    #[serde(rename = "type")]
    pub error_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesError {
    pub error: ResponsesErrorDetail,
}
```

Write `crates/llm-gateway-http/src/helpers.rs`:
```rust
use anyhow::Context as _;
use serde::de::DeserializeOwned;
use wstd::http::{Body, Request, Response, StatusCode};

use crate::bindings::wasi::logging::logging::{Level, log};
use crate::LOG_CTX;

pub fn json_response(status: StatusCode, body: &impl serde::Serialize) -> anyhow::Result<Response<Body>> {
    let json = serde_json::to_string(body).context("failed to serialize response")?;
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(json.into())
        .map_err(Into::into)
}

pub async fn parse_json_body<T: DeserializeOwned>(req: &mut Request<Body>) -> anyhow::Result<T> {
    req.body_mut().json().await.context("failed to parse request body")
}
```

Write `crates/llm-gateway-http/src/completions.rs`:
```rust
// Placeholder — will be implemented in Task 3
use wstd::http::{Body, Request, Response};

pub async fn handle(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    todo!("completions handler")
}
```

Write `crates/llm-gateway-http/src/responses.rs`:
```rust
// Placeholder — will be implemented in Task 3
use wstd::http::{Body, Request, Response};

pub async fn handle(_req: Request<Body>) -> anyhow::Result<Response<Body>> {
    todo!("responses handler")
}
```

- [ ] **Step 7: Verify it compiles**

```bash
cd crates/llm-gateway-http && cargo build --target wasm32-wasip2 --release
```
Expected: Compiles successfully (the `todo!()` will be fine since it's a runtime panic, not a compile error).

- [ ] **Step 8: Commit**

```bash
git add crates/llm-gateway-http/
git commit -m "feat: scaffold llm-gateway-http wasm component"
```

---

## Task 3: Implement completions handler

**Files:**
- Modify: `crates/llm-gateway-http/src/completions.rs`

- [ ] **Step 1: Implement the completions handler**

Write `crates/llm-gateway-http/src/completions.rs`:

The key difference from the old `http_handler.rs`: instead of calling `genai::Client::exec_chat()` directly, call the WIT binding `crate::bindings::custom::llm_gateway::chat::chat()`.

```rust
use crate::bindings::custom::llm_gateway::types::{ChatMessage, ChatOptions};
use crate::bindings::custom::llm_gateway::chat;
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::types::*;
use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;

pub async fn handle(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let chat_req: CompletionsRequest = match helpers::parse_json_body(&mut req).await {
        Ok(r) => r,
        Err(e) => {
            return helpers::json_response(StatusCode::BAD_REQUEST, &CompletionsError {
                error: CompletionsErrorDetail {
                    code: Some("invalid_request".to_string()),
                    message: Some(format!("failed to parse request: {e}")),
                    param: None,
                    r#type: Some("invalid_request_error".to_string()),
                },
            });
        }
    };

    if chat_req.messages.is_empty() {
        return helpers::json_response(StatusCode::BAD_REQUEST, &CompletionsError {
            error: CompletionsErrorDetail {
                code: Some("invalid_request".to_string()),
                message: Some("messages must not be empty".to_string()),
                param: None,
                r#type: Some("invalid_request_error".to_string()),
            },
        });
    }

    // Convert to WIT ChatMessage
    let messages: Vec<ChatMessage> = chat_req
        .messages
        .iter()
        .map(|m| ChatMessage {
            role: m.role.clone(),
            content: m.content.clone().unwrap_or_default(),
        })
        .collect();

    // Build WIT ChatOptions
    let options = ChatOptions {
        temperature: chat_req.temperature.map(|t| t as f32),
        max_tokens: chat_req.max_tokens.map(|t| t as u32),
        top_p: chat_req.top_p.map(|t| t as f32),
    };

    // Call provider via WIT binding (synchronous call)
    let result = chat::chat(&chat_req.model, &messages, Some(options), None);

    match result {
        Ok(response) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!("LLM chat ok: model={}, len={}", response.model, response.content.len()),
            );

            let finish_reason = response.finish_reason.unwrap_or_else(|| "stop".to_string());
            let usage = response.usage.map(|u| CompletionsUsage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
                total_tokens: u.total_tokens,
            }).unwrap_or(CompletionsUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
            });

            helpers::json_response(StatusCode::OK, &CompletionsResponse {
                id: format!("chatcmpl-{}", generate_id()),
                object: "chat.completion",
                model: response.model,
                choices: vec![CompletionsChoice {
                    index: 0,
                    message: CompletionsResponseMessage {
                        role: "assistant".to_string(),
                        content: Some(response.content),
                    },
                    finish_reason,
                }],
                usage,
            })
        }
        Err(e) => {
            log(Level::Error, LOG_CTX, &format!("LLM chat error: {:?}", e));
            let (status, code) = match &e {
                LlmError::AuthenticationError(_) => (StatusCode::UNAUTHORIZED, "authentication_error"),
                LlmError::RateLimitError(_) => (StatusCode::TOO_MANY_REQUESTS, "rate_limit_exceeded"),
                LlmError::ModelNotFound(_) => (StatusCode::NOT_FOUND, "model_not_found"),
                LlmError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "provider_error"),
            };
            let msg = format!("{:?}", e);
            helpers::json_response(status, &CompletionsError {
                error: CompletionsErrorDetail {
                    code: Some(code.to_string()),
                    message: Some(msg),
                    param: None,
                    r#type: Some("invalid_request_error".to_string()),
                },
            })
        }
    }
}

/// Generate a simple ID for responses (8 hex chars).
/// No uuid crate — keep it simple for wasm.
fn generate_id() -> String {
    // Use a timestamp-based simple ID
    // In wasm we don't have access to random, so use a counter-like approach
    static mut COUNTER: u64 = 0;
    // SAFETY: single-threaded wasm environment
    let id = unsafe {
        COUNTER += 1;
        COUNTER
    };
    format!("{:016x}", id)
}
```

Note: The `chat::chat()` call is synchronous (not async) because WIT `chat` function returns a `result` directly, not a future. The generated binding will be a plain function call.

Also need to import `LlmError` in the `use` statement for the match. Add to the top:
```rust
use crate::bindings::custom::llm_gateway::types::LlmError;
```

- [ ] **Step 2: Verify it compiles**

```bash
cd crates/llm-gateway-http && cargo build --target wasm32-wasip2 --release
```
Expected: Compiles. May need to adjust the `generate_id` function or the `LlmError` match arms based on actual generated binding names.

- [ ] **Step 3: Commit**

```bash
git add crates/llm-gateway-http/
git commit -m "feat: implement completions handler in llm-gateway-http"
```

---

## Task 4: Implement responses (Anthropic) handler

**Files:**
- Modify: `crates/llm-gateway-http/src/responses.rs`

- [ ] **Step 1: Implement the responses handler**

Write `crates/llm-gateway-http/src/responses.rs`:

```rust
use crate::bindings::custom::llm_gateway::chat;
use crate::bindings::custom::llm_gateway::types::{ChatMessage, ChatOptions, LlmError};
use crate::bindings::wasi::logging::logging::{Level, log};
use crate::helpers;
use crate::types::*;
use wstd::http::{Body, Request, Response, StatusCode};

use crate::LOG_CTX;

pub async fn handle(mut req: Request<Body>) -> anyhow::Result<Response<Body>> {
    let msg_req: ResponsesRequest = match helpers::parse_json_body(&mut req).await {
        Ok(r) => r,
        Err(e) => {
            return helpers::json_response(StatusCode::BAD_REQUEST, &ResponsesError {
                error: ResponsesErrorDetail {
                    error_type: "invalid_request_error".to_string(),
                    message: Some(format!("failed to parse request: {e}")),
                },
            });
        }
    };

    if msg_req.messages.is_empty() {
        return helpers::json_response(StatusCode::BAD_REQUEST, &ResponsesError {
            error: ResponsesErrorDetail {
                error_type: "invalid_request_error".to_string(),
                message: Some("messages must not be empty".to_string()),
            },
        });
    }

    // Convert to WIT ChatMessage, extracting text from Anthropic content format
    let messages: Vec<ChatMessage> = msg_req
        .messages
        .iter()
        .map(|m| ChatMessage {
            role: m.role.clone(),
            content: extract_text(&m.content),
        })
        .collect();

    let options = ChatOptions {
        temperature: msg_req.temperature.map(|t| t as f32),
        max_tokens: Some(msg_req.max_tokens as u32),
        top_p: msg_req.top_p.map(|t| t as f32),
    };

    let result = chat::chat(&msg_req.model, &messages, Some(options), None);

    match result {
        Ok(response) => {
            log(
                Level::Info,
                LOG_CTX,
                &format!("LLM responses ok: model={}, len={}", response.model, response.content.len()),
            );

            let stop_reason = response.finish_reason.as_deref().map(map_finish_reason).unwrap_or("end_turn");

            let usage = response.usage.map(|u| ResponsesUsage {
                input_tokens: u.prompt_tokens,
                output_tokens: u.completion_tokens,
            }).unwrap_or(ResponsesUsage {
                input_tokens: 0,
                output_tokens: 0,
            });

            helpers::json_response(StatusCode::OK, &ResponsesResponse {
                id: format!("msg_{:016x}", generate_id()),
                response_type: "message".to_string(),
                role: "assistant".to_string(),
                model: response.model,
                content: vec![ResponsesContentBlock {
                    block_type: "text".to_string(),
                    text: Some(response.content),
                }],
                stop_reason: stop_reason.to_string(),
                usage,
            })
        }
        Err(e) => {
            log(Level::Error, LOG_CTX, &format!("LLM responses error: {:?}", e));
            let (status, error_type) = match &e {
                LlmError::AuthenticationError(_) => (StatusCode::UNAUTHORIZED, "authentication_error"),
                LlmError::RateLimitError(_) => (StatusCode::TOO_MANY_REQUESTS, "rate_limit_error"),
                LlmError::ModelNotFound(_) => (StatusCode::NOT_FOUND, "not_found_error"),
                LlmError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "invalid_request_error"),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "provider_error"),
            };
            let msg = format!("{:?}", e);
            helpers::json_response(status, &ResponsesError {
                error: ResponsesErrorDetail {
                    error_type: error_type.to_string(),
                    message: Some(msg),
                },
            })
        }
    }
}

fn extract_text(content: &serde_json::Value) -> String {
    if let Some(s) = content.as_str() {
        return s.to_string();
    }
    if let Some(arr) = content.as_array() {
        let texts: Vec<String> = arr
            .iter()
            .filter_map(|block| block.get("text").and_then(|t| t.as_str()).map(String::from))
            .collect();
        return texts.join("");
    }
    String::new()
}

fn map_finish_reason(reason: &str) -> &'static str {
    match reason {
        "stop" => "end_turn",
        "length" => "max_tokens",
        "tool_calls" => "tool_use",
        _ => "end_turn",
    }
}

fn generate_id() -> u64 {
    static mut COUNTER: u64 = 0;
    unsafe {
        COUNTER += 1;
        COUNTER
    }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd crates/llm-gateway-http && cargo build --target wasm32-wasip2 --release
```
Expected: Compiles.

- [ ] **Step 3: Commit**

```bash
git add crates/llm-gateway-http/
git commit -m "feat: implement Anthropic responses handler in llm-gateway-http"
```

---

## Task 5: Create llm-gateway-messaging wasm component

**Files:**
- Create: `crates/llm-gateway-messaging/Cargo.toml`
- Create: `crates/llm-gateway-messaging/wit/world.wit`
- Create: `crates/llm-gateway-messaging/wit/deps/` (copy WIT deps)
- Create: `crates/llm-gateway-messaging/src/lib.rs`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p crates/llm-gateway-messaging/wit/deps
mkdir -p crates/llm-gateway-messaging/src
```

- [ ] **Step 2: Create Cargo.toml**

Write `crates/llm-gateway-messaging/Cargo.toml`:
```toml
[package]
name = "llm-gateway-messaging"
version = "0.1.0"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.46.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

[profile.release]
lto = true
opt-level = "s"
strip = true
```

- [ ] **Step 3: Create WIT world file**

Write `crates/llm-gateway-messaging/wit/world.wit`:
```wit
package wasmcloud:llm-gateway-messaging@0.1.0;

world llm-gateway-messaging {
  import wasi:logging/logging@0.1.0-draft;
  import wasmcloud:messaging/consumer@0.2.0;
  import custom:llm-gateway/chat@0.1.0;
  export wasmcloud:messaging/handler@0.2.0;
}
```

- [ ] **Step 4: Copy WIT dependency files**

```bash
# Copy llm-gateway types
cp crates/custom_plugin_llm_gateway_provider/wit/deps/custom-llm-gateway.wit crates/llm-gateway-messaging/wit/deps/

# Copy messaging deps from messaging-handler fixture
for dep in $(ls crates/wash-runtime/tests/fixtures/messaging-handler/wit/deps/); do
  cp -r crates/wash-runtime/tests/fixtures/messaging-handler/wit/deps/$dep crates/llm-gateway-messaging/wit/deps/
done
```

Note: The messaging-handler fixture has `wasi-logging-0.1.0-draft` and `wasmcloud-messaging-0.2.0` which we need. But it may be missing some wasi-* deps that the WIT bindings require. Check with `ls` and add any missing from the examples directory.

- [ ] **Step 5: Implement lib.rs**

Write `crates/llm-gateway-messaging/src/lib.rs`:

```rust
mod bindings {
    use crate::Component;

    wit_bindgen::generate!({
        path: "../wit",
        world: "llm-gateway-messaging",
        generate_all,
    });

    export!(Component);
}

use bindings::custom::llm_gateway::chat;
use bindings::custom::llm_gateway::types::{ChatMessage, ChatOptions, LlmError};
use bindings::wasi::logging::logging::{Level, log};
use bindings::wasmcloud::messaging::types::BrokerMessage;

const LOG_CTX: &str = "llm-gateway-msg";

struct Component;

// Request format (JSON)
#[derive(serde::Deserialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatRequestMessage>,
    options: Option<ChatRequestOptions>,
}

#[derive(serde::Deserialize)]
struct ChatRequestMessage {
    role: String,
    content: String,
}

#[derive(serde::Deserialize)]
struct ChatRequestOptions {
    temperature: Option<f32>,
    max_tokens: Option<u32>,
    top_p: Option<f32>,
}

// Response format (JSON)
#[derive(serde::Serialize)]
struct ChatResponse {
    content: String,
    model: String,
    usage: Option<TokenUsageResponse>,
    finish_reason: Option<String>,
}

#[derive(serde::Serialize)]
struct TokenUsageResponse {
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
}

#[derive(serde::Serialize)]
struct ErrorResponse {
    error: ErrorDetail,
}

#[derive(serde::Serialize)]
struct ErrorDetail {
    #[serde(rename = "type")]
    error_type: String,
    message: String,
}

impl bindings::exports::wasmcloud::messaging::handler::Guest for Component {
    fn handle_message(msg: BrokerMessage) -> Result<(), String> {
        log(Level::Debug, LOG_CTX, &format!("Received message on subject: {}", msg.subject));

        // Parse request
        let body_str = match core::str::from_utf8(&msg.body) {
            Ok(s) => s,
            Err(e) => {
                log(Level::Error, LOG_CTX, &format!("Invalid UTF-8 in message body: {e}"));
                return Err(format!("invalid utf-8 body: {e}"));
            }
        };

        let chat_req: ChatRequest = match serde_json::from_str(body_str) {
            Ok(r) => r,
            Err(e) => {
                log(Level::Error, LOG_CTX, &format!("Failed to parse request JSON: {e}"));
                publish_error(&msg.reply_to, "invalid_request", &format!("failed to parse: {e}"));
                return Ok(());
            }
        };

        // Convert to WIT messages
        let messages: Vec<ChatMessage> = chat_req
            .messages
            .iter()
            .map(|m| ChatMessage {
                role: m.role.clone(),
                content: m.content.clone(),
            })
            .collect();

        let options = chat_req.options.map(|o| ChatOptions {
            temperature: o.temperature,
            max_tokens: o.max_tokens,
            top_p: o.top_p,
        });

        // Call provider via WIT
        let result = chat::chat(&chat_req.model, &messages, options, None);

        let reply_body = match result {
            Ok(resp) => {
                log(Level::Info, LOG_CTX, &format!("Chat ok: model={}, len={}", resp.model, resp.content.len()));
                serde_json::to_string(&ChatResponse {
                    content: resp.content,
                    model: resp.model,
                    usage: resp.usage.map(|u| TokenUsageResponse {
                        prompt_tokens: u.prompt_tokens,
                        completion_tokens: u.completion_tokens,
                        total_tokens: u.total_tokens,
                    }),
                    finish_reason: resp.finish_reason,
                }).unwrap_or_else(|_| r#"{"error":{"type":"serialize_error","message":"failed to serialize response"}}"#.to_string())
            }
            Err(e) => {
                let (error_type, msg) = match &e {
                    LlmError::AuthenticationError(m) => ("authentication_error", m.clone()),
                    LlmError::RateLimitError(m) => ("rate_limit_error", m.clone()),
                    LlmError::ModelNotFound(m) => ("model_not_found", m.clone()),
                    LlmError::InvalidRequest(m) => ("invalid_request", m.clone()),
                    LlmError::ProviderError(m) => ("provider_error", m.clone()),
                    LlmError::Unexpected(m) => ("unexpected_error", m.clone()),
                };
                log(Level::Error, LOG_CTX, &format!("Chat error: {error_type}: {msg}"));
                serde_json::to_string(&ErrorResponse {
                    error: ErrorDetail {
                        error_type: error_type.to_string(),
                        message: msg,
                    },
                }).unwrap_or_else(|_| r#"{"error":{"type":"serialize_error","message":"failed to serialize error"}}"#.to_string())
            }
        };

        // Publish reply
        if let Some(reply_to) = &msg.reply_to {
            match bindings::wasmcloud::messaging::consumer::publish(reply_to, reply_body.as_bytes()) {
                Ok(()) => {
                    log(Level::Debug, LOG_CTX, &format!("Reply published to: {reply_to}"));
                }
                Err(e) => {
                    log(Level::Error, LOG_CTX, &format!("Failed to publish reply: {e:?}"));
                }
            }
        } else {
            log(Level::Warn, LOG_CTX, "No reply_to in message, skipping reply");
        }

        Ok(())
    }
}

fn publish_error(reply_to: &Option<String>, error_type: &str, message: &str) {
    if let Some(reply_to) = reply_to {
        let body = serde_json::to_string(&ErrorResponse {
            error: ErrorDetail {
                error_type: error_type.to_string(),
                message: message.to_string(),
            },
        }).unwrap_or_default();
        let _ = bindings::wasmcloud::messaging::consumer::publish(reply_to, body.as_bytes());
    }
}
```

- [ ] **Step 6: Verify it compiles**

```bash
cd crates/llm-gateway-messaging && cargo build --target wasm32-wasip2 --release
```
Expected: Compiles. May need adjustments to the WIT binding function names (e.g., `publish` signature, `BrokerMessage` field names) based on the actual generated bindings.

- [ ] **Step 7: Commit**

```bash
git add crates/llm-gateway-messaging/
git commit -m "feat: create llm-gateway-messaging wasm component"
```

---

## Task 6: Final verification and cleanup

**Files:**
- Verify: All three crates compile
- Verify: Workspace builds correctly

- [ ] **Step 1: Build provider plugin**

```bash
cargo build -p custom_plugin_llm_gateway_provider
```
Expected: Compiles.

- [ ] **Step 2: Build HTTP component**

```bash
cd crates/llm-gateway-http && cargo build --target wasm32-wasip2 --release
```
Expected: Compiles. Output at `target/wasm32-wasip2/release/llm_gateway_http.wasm`.

- [ ] **Step 3: Build messaging component**

```bash
cd crates/llm-gateway-messaging && cargo build --target wasm32-wasip2 --release
```
Expected: Compiles. Output at `target/wasm32-wasip2/release/llm_gateway_messaging.wasm`.

- [ ] **Step 4: Run provider tests**

```bash
cargo test -p custom_plugin_llm_gateway_provider
```
Expected: All tests pass.

- [ ] **Step 5: Run workspace clippy and fmt**

```bash
cargo clippy -p custom_plugin_llm_gateway_provider
cargo +nightly fmt -- --check
```
Expected: No warnings or errors.

- [ ] **Step 6: Commit any fixes**

```bash
git add -A
git commit -m "chore: final verification and fixes for LLM gateway refactor"
```
