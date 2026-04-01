//! # DingTalk Stream Host Plugin
//!
//! This plugin provides DingTalk Stream Mode messaging to WASM components.
//! It uses the `dingtalk-stream` crate to maintain a WebSocket connection
//! to DingTalk servers and routes chatbot messages to guest components.
//!
//! ## Configuration
//!
//! ```yaml
//! custom:dingtalk-stream:
//!   config:
//!     client-id: "your_client_id"
//!     client-secret: "your_client_secret"
//! ```
//!
//! ## Guest Export
//!
//! The guest component must export `custom:dingtalk-stream/handler@0.1.0`:
//! ```wit
//! on-message: func(msg: chatbot-message) -> result<_, string>;
//! ```
//!
//! ## Guest Import
//!
//! The guest can call `custom:dingtalk-stream/sender@0.1.0`:
//! ```wit
//! send-text: func(conversation-id: string, sender-id: string, content: string) -> result<_, dingtalk-error>;
//! send-markdown: func(conversation-id: string, sender-id: string, title: string, content: string) -> result<_, dingtalk-error>;
//! send-oto-text: func(user-id: string, content: string) -> result<_, dingtalk-error>;
//! get-access-token: func() -> result<string, dingtalk-error>;
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use dingtalk_stream::{ChatbotMessage, ChatbotReplier, Credential, DingTalkStreamClient};
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};

use anyhow::Context as _;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::{ResolvedWorkload, WorkloadItem};
use wash_runtime::plugin::HostPlugin;
use wash_runtime::plugin::WorkloadTracker;
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "dingtalk-stream",
        imports: { default: async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

use bindings::custom::dingtalk_stream::types::DingtalkError;

const PLUGIN_ID: &str = "dingtalk-stream";

// ---------------------------------------------------------------------------
// Per-component data
// ---------------------------------------------------------------------------

/// Configuration extracted from interface config.
#[derive(Clone, Debug)]
struct PluginConfig {
    client_id: String,
    client_secret: String,
}

/// Per-component data tracked by the plugin.
struct ComponentData {
    /// Token that cancels the DingTalk stream background task.
    cancel_token: tokio_util::sync::CancellationToken,
    /// Resolved workload — set during on_workload_resolved.
    workload: Option<ResolvedWorkload>,
    /// ChatbotReplier for sending messages.
    replier: Option<ChatbotReplier>,
    /// Plugin config.
    config: PluginConfig,
}

// ---------------------------------------------------------------------------
// Plugin struct
// ---------------------------------------------------------------------------

/// The DingTalk Stream host plugin.
#[derive(Clone)]
pub struct DingTalk {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for DingTalk {
    fn default() -> Self {
        Self::new()
    }
}

impl DingTalk {
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
        }
    }
}

// ---------------------------------------------------------------------------
// Config parsing
// ---------------------------------------------------------------------------

fn extract_config(interface_config: &HashMap<String, String>) -> Option<PluginConfig> {
    let client_id = interface_config.get("client-id")?;
    let client_secret = interface_config.get("client-secret")?;
    Some(PluginConfig {
        client_id: client_id.clone(),
        client_secret: client_secret.clone(),
    })
}

// ---------------------------------------------------------------------------
// Bridge: CallbackHandler → guest on-message
// ---------------------------------------------------------------------------

/// A bridge that receives DingTalk CallbackHandler invocations and forwards
/// them into the guest WASM component's `on-message` export.
struct GuestCallbackBridge {
    workload: ResolvedWorkload,
    component_id: String,
    #[expect(dead_code)]
    replier: ChatbotReplier,
}

#[async_trait]
impl dingtalk_stream::CallbackHandler for GuestCallbackBridge {
    async fn process(&self, callback_message: &dingtalk_stream::MessageBody) -> (u16, String) {
        // Parse the raw callback data into a ChatbotMessage
        let data: serde_json::Value = match serde_json::from_str(&callback_message.data) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    component_id = %self.component_id,
                    error = %e,
                    "Failed to parse callback data as JSON"
                );
                return (200, "OK".to_owned());
            }
        };

        let incoming = match ChatbotMessage::from_value(&data) {
            Ok(msg) => msg,
            Err(e) => {
                warn!(
                    component_id = %self.component_id,
                    error = %e,
                    "Failed to parse ChatbotMessage"
                );
                return (200, "OK".to_owned());
            }
        };

        // Extract text content
        let text_content =
            ChatbotReplier::extract_text(&incoming).and_then(|v| v.into_iter().next());

        let is_at = incoming.is_in_at_list.unwrap_or(false);

        // Build WIT chatbot-message record
        let wit_msg = bindings::custom::dingtalk_stream::types::ChatbotMessage {
            conversation_type: incoming.conversation_type.clone().unwrap_or_default(),
            conversation_id: incoming.conversation_id.clone().unwrap_or_default(),
            sender_id: incoming.sender_id.clone().unwrap_or_default(),
            sender_nick: incoming.sender_nick.clone().unwrap_or_default(),
            message_id: incoming.message_id.clone().unwrap_or_default(),
            text_content,
            is_admin: incoming.is_admin.unwrap_or(false),
            is_at,
            raw_json: callback_message.data.clone(),
        };

        // Instantiate the guest component and call on-message
        let mut store = match self.workload.new_store(&self.component_id).await {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    component_id = %self.component_id,
                    error = %e,
                    "Failed to create store for callback"
                );
                return (200, "OK".to_owned());
            }
        };

        let instance_pre = match self.workload.instantiate_pre(&self.component_id).await {
            Ok(pre) => pre,
            Err(e) => {
                warn!(
                    component_id = %self.component_id,
                    error = %e,
                    "Failed to instantiate_pre for callback"
                );
                return (200, "OK".to_owned());
            }
        };

        let pre = match bindings::DingtalkStreamPre::new(instance_pre) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    component_id = %self.component_id,
                    error = %e,
                    "Failed to create DingtalkStreamPre"
                );
                return (200, "OK".to_owned());
            }
        };

        let proxy = match pre.instantiate_async(&mut store).await {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    component_id = %self.component_id,
                    error = %e,
                    "Failed to instantiate for callback"
                );
                return (200, "OK".to_owned());
            }
        };

        match proxy
            .custom_dingtalk_stream_handler()
            .call_on_message(&mut store, &wit_msg)
            .await
        {
            Ok(Ok(())) => {
                debug!(
                    component_id = %self.component_id,
                    "Guest on-message handled successfully"
                );
            }
            Ok(Err(e)) => {
                warn!(
                    component_id = %self.component_id,
                    error = %e,
                    "Guest on-message returned error"
                );
            }
            Err(e) => {
                warn!(
                    component_id = %self.component_id,
                    error = %e,
                    "Guest on-message call failed"
                );
            }
        }

        (200, "OK".to_owned())
    }
}

// ---------------------------------------------------------------------------
// Spawn the DingTalk stream client
// ---------------------------------------------------------------------------

fn spawn_dingtalk_stream(
    workload: ResolvedWorkload,
    component_id: String,
    config: PluginConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) -> ChatbotReplier {
    let credential = Credential::new(&config.client_id, &config.client_secret);

    // Build a temporary client just to get a ChatbotReplier
    let temp_client = DingTalkStreamClient::builder(credential.clone()).build();
    let replier = temp_client.chatbot_replier();

    let bridge = GuestCallbackBridge {
        workload,
        component_id: component_id.clone(),
        replier: replier.clone(),
    };

    let mut client = DingTalkStreamClient::builder(credential)
        .register_callback_handler(ChatbotMessage::TOPIC, bridge)
        .build();

    tokio::spawn(async move {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!(
                    component_id = %component_id,
                    "DingTalk stream task cancelled"
                );
            }
            result = client.start() => {
                match result {
                    Ok(()) => {
                        warn!(
                            component_id = %component_id,
                            "DingTalk stream client exited"
                        );
                    }
                    Err(e) => {
                        warn!(
                            component_id = %component_id,
                            error = %e,
                            "DingTalk stream client error"
                        );
                    }
                }
            }
        }
    });

    replier
}

// ---------------------------------------------------------------------------
// Build a synthetic ChatbotMessage for reply purposes
// ---------------------------------------------------------------------------

fn build_synthetic_message(conversation_id: &str, sender_id: &str) -> ChatbotMessage {
    let value = serde_json::json!({
        "conversationId": conversation_id,
        "conversationType": "1",
        "senderId": sender_id,
        "senderStaffId": sender_id,
        "senderNick": "",
        "msgId": "",
    });
    serde_json::from_value(value).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// WIT types::Host
// ---------------------------------------------------------------------------

impl bindings::custom::dingtalk_stream::types::Host for ActiveCtx<'_> {}

// ---------------------------------------------------------------------------
// WIT sender::Host — runtime message sending by the guest
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::dingtalk_stream::sender::Host for ActiveCtx<'a> {
    #[instrument(skip_all, fields(conversation_id = %conversation_id, sender_id = %sender_id))]
    async fn send_text(
        &mut self,
        conversation_id: String,
        sender_id: String,
        content: String,
    ) -> wasmtime::Result<Result<(), DingtalkError>> {
        let Some(plugin) = self.get_plugin::<DingTalk>(PLUGIN_ID) else {
            return Ok(Err(DingtalkError::Internal(
                "dingtalk-stream plugin not available".to_string(),
            )));
        };

        let replier = {
            let lock = plugin.tracker.read().await;
            let component_id = self.component_id.as_ref().to_string();
            match lock.get_component_data(&component_id) {
                Some(data) => match &data.replier {
                    Some(r) => r.clone(),
                    None => {
                        return Ok(Err(DingtalkError::Internal(
                            "replier not initialized".to_string(),
                        )));
                    }
                },
                None => {
                    return Ok(Err(DingtalkError::Internal(
                        "component not tracked".to_string(),
                    )));
                }
            }
        };

        let synthetic = build_synthetic_message(&conversation_id, &sender_id);
        match replier.reply_text(&content, &synthetic).await {
            Ok(_) => Ok(Ok(())),
            Err(dingtalk_stream::Error::Auth(e)) => {
                Ok(Err(DingtalkError::AuthFailed(e.to_string())))
            }
            Err(e) => Ok(Err(DingtalkError::SendFailed(e.to_string()))),
        }
    }

    #[instrument(skip_all, fields(conversation_id = %conversation_id, sender_id = %sender_id))]
    async fn send_markdown(
        &mut self,
        conversation_id: String,
        sender_id: String,
        title: String,
        content: String,
    ) -> wasmtime::Result<Result<(), DingtalkError>> {
        let Some(plugin) = self.get_plugin::<DingTalk>(PLUGIN_ID) else {
            return Ok(Err(DingtalkError::Internal(
                "dingtalk-stream plugin not available".to_string(),
            )));
        };

        let replier = {
            let lock = plugin.tracker.read().await;
            let component_id = self.component_id.as_ref().to_string();
            match lock.get_component_data(&component_id) {
                Some(data) => match &data.replier {
                    Some(r) => r.clone(),
                    None => {
                        return Ok(Err(DingtalkError::Internal(
                            "replier not initialized".to_string(),
                        )));
                    }
                },
                None => {
                    return Ok(Err(DingtalkError::Internal(
                        "component not tracked".to_string(),
                    )));
                }
            }
        };

        let synthetic = build_synthetic_message(&conversation_id, &sender_id);
        match replier.reply_markdown(&title, &content, &synthetic).await {
            Ok(_) => Ok(Ok(())),
            Err(dingtalk_stream::Error::Auth(e)) => {
                Ok(Err(DingtalkError::AuthFailed(e.to_string())))
            }
            Err(e) => Ok(Err(DingtalkError::SendFailed(e.to_string()))),
        }
    }

    #[instrument(skip_all, fields(user_id = %user_id))]
    async fn send_oto_text(
        &mut self,
        user_id: String,
        content: String,
    ) -> wasmtime::Result<Result<(), DingtalkError>> {
        let Some(plugin) = self.get_plugin::<DingTalk>(PLUGIN_ID) else {
            return Ok(Err(DingtalkError::Internal(
                "dingtalk-stream plugin not available".to_string(),
            )));
        };

        let replier = {
            let lock = plugin.tracker.read().await;
            let component_id = self.component_id.as_ref().to_string();
            match lock.get_component_data(&component_id) {
                Some(data) => match &data.replier {
                    Some(r) => r.clone(),
                    None => {
                        return Ok(Err(DingtalkError::Internal(
                            "replier not initialized".to_string(),
                        )));
                    }
                },
                None => {
                    return Ok(Err(DingtalkError::Internal(
                        "component not tracked".to_string(),
                    )));
                }
            }
        };

        let msg_param = serde_json::json!({"content": content}).to_string();
        match replier
            .send_oto_message(&user_id, "sampleText", &msg_param)
            .await
        {
            Ok(_) => Ok(Ok(())),
            Err(dingtalk_stream::Error::Auth(e)) => {
                Ok(Err(DingtalkError::AuthFailed(e.to_string())))
            }
            Err(e) => Ok(Err(DingtalkError::SendFailed(e.to_string()))),
        }
    }

    async fn get_access_token(&mut self) -> wasmtime::Result<Result<String, DingtalkError>> {
        let Some(plugin) = self.get_plugin::<DingTalk>(PLUGIN_ID) else {
            return Ok(Err(DingtalkError::Internal(
                "dingtalk-stream plugin not available".to_string(),
            )));
        };

        // We need to get the access token from a DingTalk client.
        // Build a temporary client from config to get the token.
        let config = {
            let lock = plugin.tracker.read().await;
            let component_id = self.component_id.as_ref().to_string();
            match lock.get_component_data(&component_id) {
                Some(data) => data.config.clone(),
                None => {
                    return Ok(Err(DingtalkError::Internal(
                        "component not tracked".to_string(),
                    )));
                }
            }
        };

        let credential = Credential::new(&config.client_id, &config.client_secret);
        let client = DingTalkStreamClient::builder(credential).build();
        match client.get_access_token().await {
            Ok(token) => Ok(Ok(token)),
            Err(dingtalk_stream::Error::Auth(e)) => {
                Ok(Err(DingtalkError::AuthFailed(e.to_string())))
            }
            Err(e) => Ok(Err(DingtalkError::Internal(e.to_string()))),
        }
    }
}

// ---------------------------------------------------------------------------
// HostPlugin implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl HostPlugin for DingTalk {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from(
                "custom:dingtalk-stream/sender,types@0.1.0",
            )]),
            exports: HashSet::from([WitInterface::from("custom:dingtalk-stream/handler@0.1.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Only handle dingtalk-stream interfaces
        let Some(interface) = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "dingtalk-stream")
        else {
            return Ok(());
        };

        // Parse config
        let config = match extract_config(&interface.config) {
            Some(c) => c,
            None => {
                warn!("Missing client-id or client-secret in dingtalk-stream interface config");
                return Ok(());
            }
        };

        // Add sender imports to linker
        bindings::custom::dingtalk_stream::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::dingtalk_stream::sender::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        // Only track components (not services)
        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        // Check if this component exports the handler interface
        let has_handler = component_handle
            .world()
            .exports
            .iter()
            .any(|i| i.namespace == "custom" && i.package == "dingtalk-stream");

        if has_handler {
            debug!(
                component_id = component_handle.id(),
                "Tracking component for DingTalk stream callbacks"
            );

            self.tracker.write().await.add_component(
                component_handle,
                ComponentData {
                    cancel_token: tokio_util::sync::CancellationToken::new(),
                    workload: None,
                    replier: None,
                    config,
                },
            );
        }

        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        workload: &ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        let (cancel_token, config) = {
            let mut lock = self.tracker.write().await;
            match lock.get_component_data_mut(component_id) {
                Some(data) => {
                    data.workload = Some(workload.clone());
                    (data.cancel_token.clone(), data.config.clone())
                }
                None => return Ok(()),
            }
        };

        // Validate that the component exports the handler interface
        let instance_pre = workload.instantiate_pre(component_id).await?;
        let _pre = bindings::DingtalkStreamPre::new(instance_pre)
            .map_err(anyhow::Error::from)
            .context("failed to instantiate dingtalk-stream pre")?;

        // Spawn the DingTalk stream client
        let replier = spawn_dingtalk_stream(
            workload.clone(),
            component_id.to_string(),
            config,
            cancel_token,
        );

        // Store the replier
        {
            let mut lock = self.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(component_id) {
                data.replier = Some(replier);
            }
        }

        debug!(
            component_id = %component_id,
            "DingTalk stream plugin resolved and client started"
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
        let plugin = DingTalk::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_imports() {
        let plugin = DingTalk::new();
        let world = plugin.world();
        assert!(
            world
                .imports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "dingtalk-stream")
        );
    }

    #[test]
    fn test_world_exports() {
        let plugin = DingTalk::new();
        let world = plugin.world();
        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "dingtalk-stream")
        );
    }

    #[test]
    fn test_extract_config() {
        let mut config = HashMap::new();
        config.insert("client-id".to_string(), "test_id".to_string());
        config.insert("client-secret".to_string(), "test_secret".to_string());

        let pc = extract_config(&config).unwrap();
        assert_eq!(pc.client_id, "test_id");
        assert_eq!(pc.client_secret, "test_secret");
    }

    #[test]
    fn test_extract_config_missing() {
        let config = HashMap::new();
        assert!(extract_config(&config).is_none());

        let mut config = HashMap::new();
        config.insert("client-id".to_string(), "test_id".to_string());
        assert!(extract_config(&config).is_none());
    }

    #[test]
    fn test_build_synthetic_message() {
        let msg = build_synthetic_message("conv_123", "user_456");
        assert_eq!(msg.conversation_id.as_deref(), Some("conv_123"));
        assert_eq!(msg.sender_id.as_deref(), Some("user_456"));
        assert_eq!(msg.conversation_type.as_deref(), Some("1"));
    }
}
