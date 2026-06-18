//! # LLM Gateway Host Plugin
//!
//! This module implements a wasmCloud host plugin that provides
//! `custom:llm-gateway/chat@0.1.0` interfaces using the `genai` multi-provider
//! library. It supports OpenAI, Anthropic, Gemini, DeepSeek, Ollama, Groq, and
//! more through a unified interface.
//!
//! ## Usage
//!
//! ```ignore
//! use custom_plugin_llm_gateway_provider::LlmGateway;
//! use wash_runtime::host::HostBuilder;
//! use std::sync::Arc;
//!
//! let llm = LlmGateway::new();
//! let host = HostBuilder::new()
//!     .with_plugin(Arc::new(llm))?
//!     .build()?;
//! ```
//!
//! ## per-Workload Configuration
//!
//! Each workload must configure via interface config:
//!
//! ```ignore
//! // In the workload manifest or interface configuration:
//! // custom:llm-gateway:
//! //   config:
//! //     provider: "openai"             // Required. One of: openai, anthropic, gemini, deepseek, ollama, groq, openai-compat
//! //     model_name: "gpt-4o-mini"      // Required. The model name
//! //     api_key: "sk-your-api-key"     // Required. API key
//! //     base_url: "https://..."        // Required when provider is "openai-compat"
//! //     temperature: "0.7"             // Optional, default 0.7
//! //     top_p: "0.95"                  // Optional
//! //     max_tokens: "4096"             // Optional, default 4096
//! //     system_prompts: '[{"role":"system","content":"You are a helpful assistant"}]'  // Optional, JSON array
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
use genai::adapter::AdapterKind;
use genai::chat::{ChatMessage as GenaiChatMessage, ChatOptions, ChatRequest};
use genai::resolver::{AuthData, AuthResolver, Endpoint, ServiceTargetResolver};
use genai::{Client, ModelIden, ServiceTarget};
use opentelemetry::metrics::Counter;
use tokio::sync::RwLock;
use tracing::debug;
use wasmtime::component::Resource;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::{HostPlugin, WitInterfaces, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "llm-gateway",
        imports: {
            default: async | trappable | tracing,
        },
        with: {
            "custom:llm-gateway/chat-streaming.chat-stream": super::ChatStreamHandle,
        },
    });
}

use bindings::custom::llm_gateway::types::{
    BinaryPart, BinarySource, ChatChunk, ChatMessage, ChatOptions as WitChatOptions, ChatResponse,
    ChatRole, ChatStreamEvent, ContentPart, LlmConfig, LlmError,
    MessageContent as WitMessageContent, StopReason, StreamEnd as WitStreamEnd, TokenUsage,
    ToolCall, ToolResponse,
};

const PLUGIN_ID: &str = "llm-gateway-provider";

/// Host-side state for an active streaming chat response.
/// Stored in the wasmtime ResourceTable and polled by `next()`.
pub struct ChatStreamHandle {
    /// The genai ChatStream (implements futures::Stream)
    stream: futures::stream::BoxStream<'static, genai::Result<genai::chat::ChatStreamEvent>>,
    /// The model identifier returned by the provider
    model_iden: Option<ModelIden>,
    /// Whether the stream has ended
    ended: bool,
}

/// Default system prompt role
const DEFAULT_SYSTEM_PROMPT_ROLE: &str = "system";
/// Default system prompt content
const DEFAULT_SYSTEM_PROMPT_CONTENT: &str = "你是一个生活小助手，帮助解答用户遇到的各种问题";

/// A preset prompt entry with role and content
#[derive(Clone, Debug)]
pub struct PresetPrompt {
    pub role: String,
    pub content: String,
}

/// serde helper for parsing JSON system_prompts config
#[derive(serde::Deserialize)]
struct PresetPromptJson {
    role: String,
    content: String,
}

/// Configuration for LLM Gateway (per-workload)
#[derive(Clone, Debug)]
pub struct LlmGatewayConfig {
    /// Provider adapter kind (e.g., openai, anthropic, gemini, deepseek, ollama, groq, openai-compat)
    pub provider: AdapterKind,
    /// Custom base URL (required when provider is openai-compat)
    pub base_url: Option<String>,
    /// Model name to use for chat requests
    pub model_name: String,
    /// API key for the LLM provider
    pub api_key: String,
    /// Sampling temperature (default 0.7)
    pub temperature: Option<f64>,
    /// Top-p sampling (optional)
    pub top_p: Option<f64>,
    /// Maximum tokens to generate (default 4096)
    pub max_tokens: Option<u32>,
    /// Preset prompts prepended to every chat request
    pub system_prompts: Vec<PresetPrompt>,
}

/// Per-component data.
struct ComponentData {
    /// LLM Gateway config (parsed from interface config)
    config: Option<LlmGatewayConfig>,
    /// Cached genai client
    client: Option<Client>,
}

/// LLM Gateway plugin backed by the genai multi-provider library
#[derive(Clone)]
pub struct LlmGateway {
    /// Per-component state tracker
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
    /// Metrics
    metrics: Arc<LlmGatewayMetrics>,
}

struct LlmGatewayMetrics {
    chat_requests_total: Counter<u64>,
}

impl Default for LlmGatewayMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl LlmGatewayMetrics {
    fn new() -> Self {
        let meter = opentelemetry::global::meter("llm-gateway");
        let chat_requests_total = meter
            .u64_counter("llm_gateway_chat_requests_total")
            .with_description("Total number of chat completion requests")
            .build();
        Self {
            chat_requests_total,
        }
    }
}

impl Default for LlmGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl LlmGateway {
    /// Create a new LLM Gateway plugin.
    /// Configuration is provided per-workload via interface config.
    pub fn new() -> Self {
        let metrics = LlmGatewayMetrics::new();
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
            metrics: Arc::new(metrics),
        }
    }

    fn record_chat_request(&self, model: &str) {
        let attributes = [opentelemetry::KeyValue::new("model", model.to_string())];
        self.metrics.chat_requests_total.add(1, &attributes);
    }

    async fn get_or_create_client(&self, component_id: &str) -> anyhow::Result<Client> {
        // Check if client already exists
        {
            let lock = self.tracker.read().await;
            if let Some(data) = lock.get_component_data(component_id)
                && let Some(ref client) = data.client
            {
                return Ok(client.clone());
            }
        }

        // Get config for this component
        let config = {
            let lock = self.tracker.read().await;
            let data = lock.get_component_data(component_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "No LLM Gateway config found for component '{}'",
                    component_id
                )
            })?;
            data.config
                .as_ref()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "LLM Gateway config not set for component '{}'",
                        component_id
                    )
                })?
                .clone()
        };

        // Build genai client with custom auth resolver and service target resolver
        let api_key = config.api_key.clone();
        let base_url = config.base_url.clone();
        let provider = config.provider;

        let auth_resolver = AuthResolver::from_resolver_fn(
            move |_model_iden: ModelIden| -> Result<Option<AuthData>, genai::resolver::Error> {
                Ok(Some(AuthData::from_single(api_key.clone())))
            },
        );

        let target_resolver = ServiceTargetResolver::from_resolver_fn(
            move |mut service_target: ServiceTarget| -> Result<ServiceTarget, genai::resolver::Error> {
                // Override the adapter kind with the configured provider
                let model_name = service_target.model.model_name.clone();
                service_target.model = ModelIden::new(provider, model_name);

                // Set custom base URL if configured
                if let Some(ref url) = base_url {
                    service_target.endpoint = Endpoint::from_owned(url.clone());
                }

                Ok(service_target)
            },
        );

        let client = Client::builder()
            .with_auth_resolver(auth_resolver)
            .with_service_target_resolver(target_resolver)
            .build();

        // Cache the client
        {
            let mut lock = self.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(component_id) {
                data.client = Some(client.clone());
            }
        }
        Ok(client)
    }

    /// Create a one-off genai client using resolved config values.
    /// Used when a dynamic `llm-config` overrides the interface-level config.
    async fn create_client_with_config(
        &self,
        provider: AdapterKind,
        api_key: String,
        base_url: Option<String>,
    ) -> anyhow::Result<Client> {
        let ar = api_key.clone();
        let auth_resolver = AuthResolver::from_resolver_fn(
            move |_model_iden: ModelIden| -> Result<Option<AuthData>, genai::resolver::Error> {
                Ok(Some(AuthData::from_single(ar.clone())))
            },
        );

        let target_resolver = ServiceTargetResolver::from_resolver_fn(
            move |mut service_target: ServiceTarget| -> Result<ServiceTarget, genai::resolver::Error> {
                let model_name = service_target.model.model_name.clone();
                service_target.model = ModelIden::new(provider, model_name);

                if let Some(ref url) = base_url {
                    service_target.endpoint = Endpoint::from_owned(url.clone());
                }

                Ok(service_target)
            },
        );

        let client = Client::builder()
            .with_auth_resolver(auth_resolver)
            .with_service_target_resolver(target_resolver)
            .build();

        Ok(client)
    }
}

// ============================================================================
// Config parsing helpers
// ============================================================================

/// Parse provider string to AdapterKind.
/// Supports genai built-in providers plus "openai-compat" (maps to AdapterKind::OpenAI).
fn parse_provider(provider: &str) -> Result<AdapterKind, String> {
    match provider.to_lowercase().as_str() {
        "openai-compat" => Ok(AdapterKind::OpenAI),
        other => AdapterKind::from_lower_str(other).ok_or_else(|| {
            format!(
                "Unknown provider '{}'. Supported: openai, anthropic, gemini, deepseek, ollama, groq, openai-compat",
                other
            )
        }),
    }
}

/// Parse system_prompts from JSON string. Returns default prompt if empty or parse fails.
fn parse_system_prompts(json: &str) -> Vec<PresetPrompt> {
    if json.trim().is_empty() {
        return vec![PresetPrompt {
            role: DEFAULT_SYSTEM_PROMPT_ROLE.to_string(),
            content: DEFAULT_SYSTEM_PROMPT_CONTENT.to_string(),
        }];
    }
    match serde_json::from_str::<Vec<PresetPromptJson>>(json) {
        Ok(prompts) if !prompts.is_empty() => prompts
            .into_iter()
            .map(|p| PresetPrompt {
                role: p.role,
                content: p.content,
            })
            .collect(),
        _ => vec![PresetPrompt {
            role: DEFAULT_SYSTEM_PROMPT_ROLE.to_string(),
            content: DEFAULT_SYSTEM_PROMPT_CONTENT.to_string(),
        }],
    }
}

/// Extract and validate all config values from interface config.
fn extract_config(
    interface: &WitInterface,
    _workload_id: &str,
) -> Result<LlmGatewayConfig, String> {
    let provider_str = interface
        .config
        .get("provider")
        .cloned()
        .unwrap_or_default();
    if provider_str.is_empty() {
        return Err("Missing required config: 'provider'".to_string());
    }
    let provider = parse_provider(&provider_str)?;

    let model_name = interface
        .config
        .get("model_name")
        .cloned()
        .unwrap_or_default();
    if model_name.is_empty() {
        return Err("Missing required config: 'model_name'".to_string());
    }

    let api_key = interface.config.get("api_key").cloned().unwrap_or_default();
    if api_key.is_empty() {
        return Err("Missing required config: 'api_key'".to_string());
    }

    // base_url is required for openai-compat provider
    let base_url = interface.config.get("base_url").cloned();
    if provider == AdapterKind::OpenAI
        && base_url.is_none()
        && provider_str.to_lowercase().contains("compat")
    {
        return Err(
            "Missing required config: 'base_url' is required when provider is 'openai-compat'"
                .to_string(),
        );
    }

    let temperature = interface
        .config
        .get("temperature")
        .and_then(|v| v.parse::<f64>().ok());

    let top_p = interface
        .config
        .get("top_p")
        .and_then(|v| v.parse::<f64>().ok());

    let max_tokens = interface
        .config
        .get("max_tokens")
        .and_then(|v| v.parse::<u32>().ok());

    let system_prompts_json = interface
        .config
        .get("system_prompts")
        .cloned()
        .unwrap_or_default();
    let system_prompts = parse_system_prompts(&system_prompts_json);

    Ok(LlmGatewayConfig {
        provider,
        base_url,
        model_name,
        api_key,
        temperature,
        top_p,
        max_tokens,
        system_prompts,
    })
}

// ============================================================================
// Type conversion helpers
// ============================================================================

pub(crate) fn to_genai_role(role: ChatRole) -> genai::chat::ChatRole {
    match role {
        ChatRole::System => genai::chat::ChatRole::System,
        ChatRole::User => genai::chat::ChatRole::User,
        ChatRole::Assistant => genai::chat::ChatRole::Assistant,
        ChatRole::Tool => genai::chat::ChatRole::Tool,
    }
}

/// Convert a string role from preset prompts to genai ChatRole.
/// Preset prompts are configured as JSON with string roles.
fn to_genai_role_from_str(role: &str) -> genai::chat::ChatRole {
    match role {
        "system" => genai::chat::ChatRole::System,
        "assistant" => genai::chat::ChatRole::Assistant,
        "tool" => genai::chat::ChatRole::Tool,
        _ => genai::chat::ChatRole::User,
    }
}

fn to_genai_content_part(part: ContentPart) -> genai::chat::ContentPart {
    match part {
        ContentPart::Text(s) => genai::chat::ContentPart::Text(s),
        ContentPart::Binary(b) => genai::chat::ContentPart::Binary(genai::chat::Binary {
            content_type: b.content_type,
            source: match b.source {
                BinarySource::Url(u) => genai::chat::BinarySource::Url(u),
                BinarySource::Base64(b64) => {
                    genai::chat::BinarySource::Base64(std::sync::Arc::from(b64.as_str()))
                }
            },
            name: b.name,
        }),
        ContentPart::ToolCall(tc) => genai::chat::ContentPart::ToolCall(genai::chat::ToolCall {
            call_id: tc.call_id,
            fn_name: tc.fn_name,
            fn_arguments: serde_json::from_str(&tc.fn_arguments).unwrap_or(serde_json::Value::Null),
            thought_signatures: None,
        }),
        ContentPart::ToolResponse(tr) => {
            genai::chat::ContentPart::ToolResponse(genai::chat::ToolResponse {
                call_id: tr.call_id,
                fn_name: None,
                content: tr.content,
            })
        }
    }
}

fn to_genai_messages(messages: Vec<ChatMessage>) -> Vec<GenaiChatMessage> {
    messages
        .into_iter()
        .map(|m| {
            let role = to_genai_role(m.role);
            let parts: Vec<genai::chat::ContentPart> = m
                .content
                .parts
                .into_iter()
                .map(to_genai_content_part)
                .collect();
            GenaiChatMessage {
                role,
                content: genai::chat::MessageContent::from_parts(parts),
                options: None,
            }
        })
        .collect()
}

/// Merge config-level defaults with per-request options.
/// Per-request options take precedence.
fn build_chat_options(
    config: &LlmGatewayConfig,
    request_options: Option<WitChatOptions>,
) -> ChatOptions {
    let mut opts = ChatOptions::default();

    // Apply config defaults
    if let Some(temperature) = config.temperature {
        opts = opts.with_temperature(temperature);
    }
    if let Some(max_tokens) = config.max_tokens {
        opts = opts.with_max_tokens(max_tokens);
    }
    if let Some(top_p) = config.top_p {
        opts = opts.with_top_p(top_p);
    }

    // Override with per-request options if provided
    if let Some(req) = request_options {
        if let Some(temp) = req.temperature {
            opts = opts.with_temperature(f64::from(temp));
        }
        if let Some(max_tokens) = req.max_tokens {
            opts = opts.with_max_tokens(max_tokens);
        }
        if let Some(top_p) = req.top_p {
            opts = opts.with_top_p(f64::from(top_p));
        }
    }

    opts
}

fn to_wit_stop_reason(reason: genai::chat::StopReason) -> StopReason {
    match reason {
        genai::chat::StopReason::Completed(s) => StopReason::Completed(s),
        genai::chat::StopReason::MaxTokens(s) => StopReason::MaxTokens(s),
        genai::chat::StopReason::ToolCall(s) => StopReason::ToolCall(s),
        genai::chat::StopReason::ContentFilter(s) => StopReason::ContentFilter(s),
        genai::chat::StopReason::StopSequence(s) => StopReason::StopSequence(s),
        genai::chat::StopReason::Other(s) => StopReason::Other(s),
    }
}

fn to_wit_message_content(mc: genai::chat::MessageContent) -> WitMessageContent {
    WitMessageContent {
        parts: mc
            .into_parts()
            .into_iter()
            .map(to_wit_content_part)
            .collect(),
    }
}

fn to_wit_content_part(part: genai::chat::ContentPart) -> ContentPart {
    match part {
        genai::chat::ContentPart::Text(s) => ContentPart::Text(s),
        genai::chat::ContentPart::Binary(b) => ContentPart::Binary(BinaryPart {
            content_type: b.content_type,
            source: match b.source {
                genai::chat::BinarySource::Url(u) => BinarySource::Url(u),
                genai::chat::BinarySource::Base64(b64) => BinarySource::Base64(b64.to_string()),
            },
            name: b.name,
        }),
        genai::chat::ContentPart::ToolCall(tc) => ContentPart::ToolCall(ToolCall {
            call_id: tc.call_id,
            fn_name: tc.fn_name,
            fn_arguments: serde_json::to_string(&tc.fn_arguments)
                .unwrap_or_else(|_| "null".to_string()),
        }),
        genai::chat::ContentPart::ToolResponse(tr) => ContentPart::ToolResponse(ToolResponse {
            call_id: tr.call_id,
            content: tr.content,
        }),
        // Skip provider-internal variants (ThoughtSignature, ReasoningContent, Custom)
        _ => ContentPart::Text(String::new()),
    }
}

fn to_llm_error(err: genai::Error) -> LlmError {
    let msg = err.to_string();
    let lower = msg.to_lowercase();
    if lower.contains("auth") || lower.contains("api key") || lower.contains("unauthorized") {
        LlmError::AuthenticationError(msg)
    } else if lower.contains("rate limit") || lower.contains("too many requests") {
        LlmError::RateLimitError(msg)
    } else if lower.contains("not found") || lower.contains("does not exist") {
        LlmError::ModelNotFound(msg)
    } else {
        LlmError::ProviderError(msg)
    }
}

// ============================================================================
// Chat Interface Implementation
// ============================================================================

impl<'a> bindings::custom::llm_gateway::chat::Host for ActiveCtx<'a> {
    async fn chat(
        &mut self,
        model: String,
        messages: Vec<ChatMessage>,
        options: Option<WitChatOptions>,
        config: Option<LlmConfig>,
    ) -> wasmtime::Result<Result<ChatResponse, LlmError>> {
        let Ok(plugin) = self.try_get_plugin::<LlmGateway>(PLUGIN_ID) else {
            return Ok(Err(LlmError::Unexpected(
                "LLM Gateway plugin not available".to_string(),
            )));
        };

        // Validate input
        if messages.is_empty() {
            return Ok(Err(LlmError::InvalidRequest(
                "Messages list cannot be empty".to_string(),
            )));
        }

        let workload_id = self.workload_id.as_ref().to_string();
        let component_id: Arc<str> = self.component_id.clone();

        // Get config for this component
        let interface_config = {
            let lock = plugin.tracker.read().await;
            match lock.get_component_data(&component_id) {
                Some(data) => match data.config.clone() {
                    Some(c) => c,
                    None => {
                        return Ok(Err(LlmError::Unexpected(format!(
                            "LLM Gateway config not set for component '{}'",
                            component_id
                        ))));
                    }
                },
                None => {
                    return Ok(Err(LlmError::Unexpected(format!(
                        "No LLM Gateway config found for component '{}'",
                        component_id
                    ))));
                }
            }
        };

        // Use configured model name if the request model is empty, otherwise use request model
        let model = if model.is_empty() {
            interface_config.model_name.clone()
        } else {
            model
        };

        plugin.record_chat_request(&model);

        // Resolve api_key and base_url: dynamic config takes priority over interface config
        let api_key = config
            .as_ref()
            .map(|c| c.api_key.clone())
            .unwrap_or_else(|| interface_config.api_key.clone());
        let base_url = config
            .as_ref()
            .and_then(|c| c.base_url.clone())
            .or_else(|| interface_config.base_url.clone());
        let provider = interface_config.provider;

        // Get or create genai client: use dynamic client if config was provided,
        // otherwise use the cached workload client
        let client = if config.is_some() {
            match plugin
                .create_client_with_config(provider, api_key, base_url)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    return Ok(Err(LlmError::Unexpected(format!(
                        "Failed to create LLM client: {e}"
                    ))));
                }
            }
        } else {
            match plugin.get_or_create_client(&component_id).await {
                Ok(c) => c,
                Err(e) => {
                    return Ok(Err(LlmError::Unexpected(format!(
                        "Failed to create LLM client: {e}"
                    ))));
                }
            }
        };

        debug!(
            workload_id = %workload_id,
            model = %model,
            message_count = messages.len(),
            "Executing LLM chat request"
        );

        // Build chat request: prepend preset prompts, then user messages
        let mut all_messages = Vec::new();

        // Add preset system prompts
        for prompt in &interface_config.system_prompts {
            match to_genai_role_from_str(&prompt.role) {
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

        // Add user-provided messages
        all_messages.extend(to_genai_messages(messages));

        let chat_req = ChatRequest::new(all_messages);

        // Build merged chat options (config defaults + per-request overrides)
        let chat_options = build_chat_options(&interface_config, options);

        // Execute chat
        let chat_res = match client
            .exec_chat(&model, chat_req, Some(&chat_options))
            .await
        {
            Ok(res) => res,
            Err(e) => {
                debug!(error = %e, "LLM chat request failed");
                return Ok(Err(to_llm_error(e)));
            }
        };

        // Convert response
        let wit_content = to_wit_message_content(chat_res.content);
        let wit_stop_reason = chat_res.stop_reason.map(to_wit_stop_reason);
        let response_model = chat_res.model_iden.model_name.to_string();
        let usage = TokenUsage {
            prompt_tokens: chat_res.usage.prompt_tokens.unwrap_or(0) as u64,
            completion_tokens: chat_res.usage.completion_tokens.unwrap_or(0) as u64,
            total_tokens: chat_res.usage.total_tokens.unwrap_or(0) as u64,
        };

        debug!(
            workload_id = %workload_id,
            model = %response_model,
            content_parts = wit_content.parts.len(),
            prompt_tokens = usage.prompt_tokens,
            completion_tokens = usage.completion_tokens,
            "LLM chat request completed"
        );

        Ok(Ok(ChatResponse {
            content: wit_content,
            model: response_model,
            stop_reason: wit_stop_reason,
            usage: Some(usage),
        }))
    }
}

// ============================================================================
// Streaming Chat Interface Implementation
// ============================================================================

impl<'a> bindings::custom::llm_gateway::chat_streaming::Host for ActiveCtx<'a> {
    async fn chat_streaming(
        &mut self,
        model: String,
        messages: Vec<ChatMessage>,
        options: Option<WitChatOptions>,
        config: Option<LlmConfig>,
    ) -> wasmtime::Result<Result<Resource<ChatStreamHandle>, LlmError>> {
        let Ok(plugin) = self.try_get_plugin::<LlmGateway>(PLUGIN_ID) else {
            return Ok(Err(LlmError::Unexpected(
                "LLM Gateway plugin not available".to_string(),
            )));
        };

        // Validate input
        if messages.is_empty() {
            return Ok(Err(LlmError::InvalidRequest(
                "Messages list cannot be empty".to_string(),
            )));
        }

        let workload_id = self.workload_id.as_ref().to_string();
        let component_id: Arc<str> = self.component_id.clone();

        // Get config for this component
        let interface_config = {
            let lock = plugin.tracker.read().await;
            match lock.get_component_data(&component_id) {
                Some(data) => match data.config.clone() {
                    Some(c) => c,
                    None => {
                        return Ok(Err(LlmError::Unexpected(format!(
                            "LLM Gateway config not set for component '{}'",
                            component_id
                        ))));
                    }
                },
                None => {
                    return Ok(Err(LlmError::Unexpected(format!(
                        "No LLM Gateway config found for component '{}'",
                        component_id
                    ))));
                }
            }
        };

        // Use configured model name if the request model is empty
        let model = if model.is_empty() {
            interface_config.model_name.clone()
        } else {
            model
        };

        plugin.record_chat_request(&model);

        // Resolve api_key and base_url: dynamic config takes priority over interface config
        let api_key = config
            .as_ref()
            .map(|c| c.api_key.clone())
            .unwrap_or_else(|| interface_config.api_key.clone());
        let base_url = config
            .as_ref()
            .and_then(|c| c.base_url.clone())
            .or_else(|| interface_config.base_url.clone());
        let provider = interface_config.provider;

        // Get or create genai client: use dynamic client if config was provided,
        // otherwise use the cached workload client
        let client = if config.is_some() {
            match plugin
                .create_client_with_config(provider, api_key, base_url)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    return Ok(Err(LlmError::Unexpected(format!(
                        "Failed to create LLM client: {e}"
                    ))));
                }
            }
        } else {
            match plugin.get_or_create_client(&component_id).await {
                Ok(c) => c,
                Err(e) => {
                    return Ok(Err(LlmError::Unexpected(format!(
                        "Failed to create LLM client: {e}"
                    ))));
                }
            }
        };

        debug!(
            workload_id = %workload_id,
            model = %model,
            message_count = messages.len(),
            "Executing LLM streaming chat request"
        );

        // Build chat request: prepend preset prompts, then user messages
        let mut all_messages = Vec::new();
        for prompt in &interface_config.system_prompts {
            match to_genai_role_from_str(&prompt.role) {
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
        all_messages.extend(to_genai_messages(messages));
        let chat_req = ChatRequest::new(all_messages);

        let chat_options = build_chat_options(&interface_config, options);

        // Execute streaming chat
        let chat_stream_res = match client
            .exec_chat_stream(&model, chat_req, Some(&chat_options))
            .await
        {
            Ok(res) => res,
            Err(e) => {
                debug!(error = %e, "LLM streaming chat request failed");
                return Ok(Err(to_llm_error(e)));
            }
        };

        let model_iden = Some(chat_stream_res.model_iden);
        let boxed_stream = chat_stream_res.stream.boxed();

        let handle = ChatStreamHandle {
            stream: boxed_stream,
            model_iden,
            ended: false,
        };

        let resource = self.table.push(handle)?;
        Ok(Ok(resource))
    }
}

impl<'a> bindings::custom::llm_gateway::chat_streaming::HostChatStream for ActiveCtx<'a> {
    async fn next(
        &mut self,
        stream_resource: Resource<ChatStreamHandle>,
    ) -> wasmtime::Result<Result<(Vec<ChatStreamEvent>, bool), LlmError>> {
        let handle = self.table.get_mut(&stream_resource)?;

        if handle.ended {
            return Ok(Ok((vec![], true)));
        }

        let mut events: Vec<ChatStreamEvent> = Vec::new();
        let mut ended = false;

        // Drain currently available events from the stream
        while let Some(result) = handle.stream.next().now_or_never() {
            match result {
                Some(Ok(event)) => match event {
                    genai::chat::ChatStreamEvent::Start => { /* no-op */ }
                    genai::chat::ChatStreamEvent::Chunk(chunk) => {
                        events.push(ChatStreamEvent::Chunk(ChatChunk {
                            content_delta: chunk.content,
                        }));
                    }
                    genai::chat::ChatStreamEvent::End(stream_end) => {
                        let model = handle
                            .model_iden
                            .as_ref()
                            .map(|m| m.model_name.to_string())
                            .unwrap_or_default();

                        let usage = stream_end.captured_usage.map(|u| TokenUsage {
                            prompt_tokens: u.prompt_tokens.unwrap_or(0) as u64,
                            completion_tokens: u.completion_tokens.unwrap_or(0) as u64,
                            total_tokens: u.total_tokens.unwrap_or(0) as u64,
                        });

                        let wit_stop_reason =
                            stream_end.captured_stop_reason.map(to_wit_stop_reason);

                        events.push(ChatStreamEvent::End(WitStreamEnd {
                            model,
                            usage,
                            stop_reason: wit_stop_reason,
                        }));
                        ended = true;
                        handle.ended = true;
                        break;
                    }
                    // Skip reasoning/tool-call chunks for now
                    _ => {}
                },
                Some(Err(e)) => {
                    events.push(ChatStreamEvent::Error(e.to_string()));
                    ended = true;
                    handle.ended = true;
                    break;
                }
                None => {
                    // No more events available right now
                    break;
                }
            }
        }

        Ok(Ok((events, ended)))
    }

    async fn drop(&mut self, rep: Resource<ChatStreamHandle>) -> wasmtime::Result<()> {
        self.table.delete(rep)?;
        Ok(())
    }
}

// ============================================================================
// HostPlugin Implementation
// ============================================================================

#[async_trait]
impl HostPlugin for LlmGateway {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: HashSet::from([WitInterface::from(
                "custom:llm-gateway/chat,chat-streaming@0.1.0",
            )]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        // Find the llm-gateway interface
        let Some(interface) = interfaces.get("custom", "llm-gateway", &[]) else {
            tracing::warn!(
                "LlmGateway plugin requested for non-llm-gateway interface(s): {:?}",
                interfaces
            );
            return Ok(());
        };

        // Extract and validate all config
        let config = match extract_config(interface, "") {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("LLM Gateway config validation failed: {}", e);
                // Still return Ok to not block other interfaces, but log the error
                return Ok(());
            }
        };

        debug!(
            provider = ?config.provider,
            model = %config.model_name,
            temperature = config.temperature,
            max_tokens = config.max_tokens,
            preset_prompts = config.system_prompts.len(),
            "Configuring LLM Gateway for workload"
        );

        let linker = item.linker();
        bindings::custom::llm_gateway::chat::add_to_linker::<_, SharedCtx>(
            linker,
            extract_active_ctx,
        )?;
        bindings::custom::llm_gateway::chat_streaming::add_to_linker::<_, SharedCtx>(
            linker,
            extract_active_ctx,
        )?;

        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        debug!(
            component_id = component_handle.id(),
            "LlmGateway plugin bound to component"
        );

        self.tracker.write().await.add_component(
            component_handle,
            ComponentData {
                config: Some(config),
                client: None,
            },
        );

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(workload_id, |_| async {}, |_| async {})
            .await;
        debug!(workload_id = %workload_id, "LlmGateway plugin unbound");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_plugin_id() {
        let plugin = LlmGateway::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_exports() {
        let plugin = LlmGateway::new();
        let world = plugin.world();
        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "llm-gateway")
        );
    }

    #[test]
    fn test_default() {
        let plugin = LlmGateway::default();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_to_genai_role() {
        assert!(matches!(
            to_genai_role(ChatRole::System),
            genai::chat::ChatRole::System
        ));
        assert!(matches!(
            to_genai_role(ChatRole::Assistant),
            genai::chat::ChatRole::Assistant
        ));
        assert!(matches!(
            to_genai_role(ChatRole::User),
            genai::chat::ChatRole::User
        ));
        assert!(matches!(
            to_genai_role(ChatRole::Tool),
            genai::chat::ChatRole::Tool
        ));
    }

    #[test]
    fn test_to_genai_messages() {
        let messages = vec![
            ChatMessage {
                role: ChatRole::System,
                content: WitMessageContent {
                    parts: vec![ContentPart::Text("You are helpful".to_string())],
                },
            },
            ChatMessage {
                role: ChatRole::User,
                content: WitMessageContent {
                    parts: vec![ContentPart::Text("Hello".to_string())],
                },
            },
            ChatMessage {
                role: ChatRole::Assistant,
                content: WitMessageContent {
                    parts: vec![ContentPart::Text("Hi there".to_string())],
                },
            },
        ];
        let genai_msgs = to_genai_messages(messages);
        assert_eq!(genai_msgs.len(), 3);
    }

    #[test]
    fn test_parse_provider() {
        assert!(matches!(parse_provider("openai"), Ok(AdapterKind::OpenAI)));
        assert!(matches!(
            parse_provider("anthropic"),
            Ok(AdapterKind::Anthropic)
        ));
        assert!(matches!(parse_provider("gemini"), Ok(AdapterKind::Gemini)));
        assert!(matches!(
            parse_provider("deepseek"),
            Ok(AdapterKind::DeepSeek)
        ));
        assert!(matches!(parse_provider("ollama"), Ok(AdapterKind::Ollama)));
        assert!(matches!(parse_provider("groq"), Ok(AdapterKind::Groq)));
        assert!(matches!(
            parse_provider("openai-compat"),
            Ok(AdapterKind::OpenAI)
        ));
        assert!(parse_provider("unknown").is_err());
    }

    #[test]
    fn test_parse_system_prompts_default() {
        // Empty string returns default prompt
        let prompts = parse_system_prompts("");
        assert_eq!(prompts.len(), 1);
        assert_eq!(prompts[0].role, DEFAULT_SYSTEM_PROMPT_ROLE);
        assert_eq!(prompts[0].content, DEFAULT_SYSTEM_PROMPT_CONTENT);
    }

    #[test]
    fn test_parse_system_prompts_json() {
        let json = r#"[{"role":"system","content":"You are a coding assistant"},{"role":"user","content":"Be concise"}]"#;
        let prompts = parse_system_prompts(json);
        assert_eq!(prompts.len(), 2);
        assert_eq!(prompts[0].role, "system");
        assert_eq!(prompts[0].content, "You are a coding assistant");
        assert_eq!(prompts[1].role, "user");
        assert_eq!(prompts[1].content, "Be concise");
    }

    #[test]
    fn test_parse_system_prompts_invalid_json() {
        let prompts = parse_system_prompts("not json");
        assert_eq!(prompts.len(), 1);
        assert_eq!(prompts[0].role, DEFAULT_SYSTEM_PROMPT_ROLE);
    }

    #[test]
    fn test_build_chat_options_defaults() {
        let config = LlmGatewayConfig {
            provider: AdapterKind::OpenAI,
            base_url: None,
            model_name: "gpt-4o-mini".to_string(),
            api_key: "test".to_string(),
            temperature: Some(0.5),
            top_p: Some(0.9),
            max_tokens: Some(2048),
            system_prompts: vec![],
        };
        let opts = build_chat_options(&config, None);
        assert_eq!(opts.temperature, Some(0.5));
        assert_eq!(opts.top_p, Some(0.9));
        assert_eq!(opts.max_tokens, Some(2048));
    }

    #[test]
    fn test_build_chat_options_override() {
        let config = LlmGatewayConfig {
            provider: AdapterKind::OpenAI,
            base_url: None,
            model_name: "gpt-4o-mini".to_string(),
            api_key: "test".to_string(),
            temperature: Some(0.5),
            top_p: Some(0.9),
            max_tokens: Some(2048),
            system_prompts: vec![],
        };
        let req_opts = WitChatOptions {
            temperature: Some(1.0),
            max_tokens: Some(100),
            top_p: Some(0.5),
        };
        let opts = build_chat_options(&config, Some(req_opts));
        assert_eq!(opts.temperature, Some(1.0));
        assert_eq!(opts.top_p, Some(0.5));
        assert_eq!(opts.max_tokens, Some(100));
    }

    #[test]
    fn test_extract_config_missing_required() {
        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "llm-gateway".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config: HashMap::new(),
            name: None,
        };
        let result = extract_config(&interface, "test-workload");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("provider"));
    }

    #[test]
    fn test_extract_config_success() {
        let mut config = HashMap::new();
        config.insert("provider".to_string(), "openai".to_string());
        config.insert("model_name".to_string(), "gpt-4o-mini".to_string());
        config.insert("api_key".to_string(), "sk-test".to_string());

        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "llm-gateway".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config,
            name: None,
        };

        let cfg = extract_config(&interface, "test-workload").unwrap();
        assert!(matches!(cfg.provider, AdapterKind::OpenAI));
        assert_eq!(cfg.model_name, "gpt-4o-mini");
        assert_eq!(cfg.api_key, "sk-test");
        assert_eq!(cfg.temperature, None);
        assert_eq!(cfg.max_tokens, None);
        // Default system prompt
        assert_eq!(cfg.system_prompts.len(), 1);
        assert_eq!(cfg.system_prompts[0].role, DEFAULT_SYSTEM_PROMPT_ROLE);
    }

    #[test]
    fn test_extract_config_with_all_options() {
        let mut config = HashMap::new();
        config.insert("provider".to_string(), "openai-compat".to_string());
        config.insert("model_name".to_string(), "my-model".to_string());
        config.insert("api_key".to_string(), "sk-test".to_string());
        config.insert(
            "base_url".to_string(),
            "https://my-llm.example.com/v1".to_string(),
        );
        config.insert("temperature".to_string(), "0.3".to_string());
        config.insert("top_p".to_string(), "0.8".to_string());
        config.insert("max_tokens".to_string(), "8192".to_string());
        config.insert(
            "system_prompts".to_string(),
            r#"[{"role":"system","content":"You are a math tutor"}]"#.to_string(),
        );

        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "llm-gateway".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config,
            name: None,
        };

        let cfg = extract_config(&interface, "test-workload").unwrap();
        assert!(matches!(cfg.provider, AdapterKind::OpenAI));
        assert_eq!(cfg.base_url.unwrap(), "https://my-llm.example.com/v1");
        assert_eq!(cfg.temperature, Some(0.3));
        assert_eq!(cfg.top_p.unwrap(), 0.8);
        assert_eq!(cfg.max_tokens, Some(8192));
        assert_eq!(cfg.system_prompts.len(), 1);
        assert_eq!(cfg.system_prompts[0].content, "You are a math tutor");
    }
}
