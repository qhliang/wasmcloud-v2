use std::collections::BTreeMap;

// --- Request types (proxy serializes, messaging deserializes) ---

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ChatRequest<M> {
    pub model: String,
    pub messages: Vec<M>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<ChatOptionsJson>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ChatOptionsJson {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
}

// --- Response types (messaging serializes, proxy deserializes) ---

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ChatResponseJson {
    pub content: MessageContentJson,
    pub model: String,
    #[serde(default)]
    pub stop_reason: Option<StopReasonJson>,
    #[serde(default)]
    pub usage: Option<TokenUsageJson>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MessageContentJson {
    pub parts: Vec<ContentPartJson>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum ContentPartJson {
    Text(String),
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct StopReasonJson {
    #[serde(flatten)]
    pub inner: BTreeMap<String, String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TokenUsageJson {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

// --- Error types ---

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ErrorReply {
    pub error: ErrorDetail,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ErrorDetail {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}
