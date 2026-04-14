//! # DingTalk Stream Host Plugin (Resource-based)
//!
//! This plugin provides DingTalk Stream Mode messaging to WASM components.
//! It uses the `dingtalk-stream` crate to maintain a WebSocket connection
//! to DingTalk servers and routes chatbot messages to guest components.
//!
//! Two config sources with priority:
//! 1. Wasm dynamic config (passed via resource constructor)
//! 2. Static interface config (fallback from wasmcloud config)
//!
//! ## Guest Export
//!
//! The guest component must export `custom:dingtalk-stream/handler@0.1.0`:
//! ```wit
//! on-message: func(client: borrow<dingtalk-client>, msg: chatbot-message) -> result<_, string>;
//! ```
//!
//! ## Guest Import
//!
//! The guest can call `custom:dingtalk-stream/sender@0.1.0`:
//! ```wit
//! resource dingtalk-client {
//!     constructor(config: option<dingtalk-config>);
//!     send-text: func(...) -> result<_, dingtalk-error>;
//!     send-markdown: func(...) -> result<_, dingtalk-error>;
//!     send-oto-text: func(...) -> result<_, dingtalk-error>;
//!     get-access-token: func() -> result<string, dingtalk-error>;
//!     stop: func() -> result<_, dingtalk-error>;
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use dingtalk_stream::{ChatbotMessage, ChatbotReplier, Credential, DingTalkStreamClient};
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};
use wasmtime::component::Resource;


use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::{ResolvedWorkload, WorkloadItem};
use wash_runtime::plugin::config::resolve_field;
use wash_runtime::plugin::HostPlugin;
use wash_runtime::plugin::WorkloadTracker;
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "dingtalk-stream",
        imports: {
            default: async | trappable | tracing,
        },
        with: {
            "custom:dingtalk-stream/sender.dingtalk-client": super::DingtalkClientHandle,
        },
    });
}

use bindings::custom::dingtalk_stream::types::{DingtalkConfig, DingtalkError};

const PLUGIN_ID: &str = "dingtalk-stream";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Host-side state for a dingtalk-client resource instance.
pub struct DingtalkClientHandle {
    replier: ChatbotReplier,
    credential: Credential,
    cancel_token: tokio_util::sync::CancellationToken,
}

/// Per-component data.
struct ComponentData {
    /// Static interface config from wasmcloud config (fallback source)
    interface_config: HashMap<String, String>,
    /// Resolved workload
    workload: Option<ResolvedWorkload>,
}

// ---------------------------------------------------------------------------
// Plugin struct
// ---------------------------------------------------------------------------

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
// WIT types::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::dingtalk_stream::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::Host — empty (resource lives in HostDingtalkClient)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::dingtalk_stream::sender::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::HostDingtalkClient — resource constructor + methods
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::dingtalk_stream::sender::HostDingtalkClient for ActiveCtx<'a> {
    async fn new(
        &mut self,
        config: Option<DingtalkConfig>,
    ) -> wasmtime::Result<Resource<DingtalkClientHandle>> {
        let Some(plugin) = self.get_plugin::<DingTalk>(PLUGIN_ID) else {
            return Err(wasmtime::Error::msg("dingtalk-stream plugin not available"));
        };

        let component_id: Arc<str> = self.component_id.clone();
        let lock = plugin.tracker.read().await;
        let Some(data) = lock.get_component_data(&component_id) else {
            return Err(wasmtime::Error::msg("component not tracked"));
        };

        // Resolve client-id and client-secret with priority:
        // Wasm dynamic config > static interface config
        let client_id = match resolve_field(
            config.as_ref().map(|c| c.client_id.clone()),
            &data.interface_config,
            "client-id",
        ) {
            Ok(id) => id,
            Err(_) => {
                return Err(wasmtime::Error::msg(
                    "missing client-id: provide via constructor or interface config",
                ));
            }
        };

        let client_secret = match resolve_field(
            config.as_ref().map(|c| c.client_secret.clone()),
            &data.interface_config,
            "client-secret",
        ) {
            Ok(secret) => secret,
            Err(_) => {
                return Err(wasmtime::Error::msg(
                    "missing client-secret: provide via constructor or interface config",
                ));
            }
        };

        let Some(workload) = &data.workload else {
            return Err(wasmtime::Error::msg("workload not resolved yet"));
        };

        let workload = workload.clone();

        drop(lock);

        let cancel_token = tokio_util::sync::CancellationToken::new();
        let credential = Credential::new(&client_id, &client_secret);

        // Build a temporary client to get a ChatbotReplier
        let temp_client = DingTalkStreamClient::builder(credential.clone()).build();
        let replier = temp_client.chatbot_replier();

        // Spawn the background stream client
        spawn_dingtalk_stream(
            workload,
            component_id.to_string(),
            replier.clone(),
            credential.clone(),
            cancel_token.clone(),
        );

        let handle = DingtalkClientHandle {
            replier,
            credential,
            cancel_token,
        };

        let resource = self.table.push(handle)?;
        Ok(resource)
    }

    #[instrument(skip_all, fields(conversation_id = %conversation_id, sender_id = %sender_id))]
    async fn send_text(
        &mut self,
        client: Resource<DingtalkClientHandle>,
        conversation_id: String,
        sender_id: String,
        content: String,
    ) -> wasmtime::Result<Result<(), DingtalkError>> {
        let handle = self.table.get(&client)?;
        let synthetic = build_synthetic_message(&conversation_id, &sender_id);
        match handle.replier.reply_text(&content, &synthetic).await {
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
        client: Resource<DingtalkClientHandle>,
        conversation_id: String,
        sender_id: String,
        title: String,
        content: String,
    ) -> wasmtime::Result<Result<(), DingtalkError>> {
        let handle = self.table.get(&client)?;
        let synthetic = build_synthetic_message(&conversation_id, &sender_id);
        match handle.replier.reply_markdown(&title, &content, &synthetic).await {
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
        client: Resource<DingtalkClientHandle>,
        user_id: String,
        content: String,
    ) -> wasmtime::Result<Result<(), DingtalkError>> {
        let handle = self.table.get(&client)?;
        let msg_param = serde_json::json!({"content": content}).to_string();
        match handle
            .replier
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

    async fn get_access_token(
        &mut self,
        client: Resource<DingtalkClientHandle>,
    ) -> wasmtime::Result<Result<String, DingtalkError>> {
        let handle = self.table.get(&client)?;
        let client = DingTalkStreamClient::builder(handle.credential.clone()).build();
        match client.get_access_token().await {
            Ok(token) => Ok(Ok(token)),
            Err(dingtalk_stream::Error::Auth(e)) => {
                Ok(Err(DingtalkError::AuthFailed(e.to_string())))
            }
            Err(e) => Ok(Err(DingtalkError::Internal(e.to_string()))),
        }
    }

    async fn stop(
        &mut self,
        client: Resource<DingtalkClientHandle>,
    ) -> wasmtime::Result<Result<(), DingtalkError>> {
        let handle = self.table.get(&client)?;
        handle.cancel_token.cancel();
        debug!("DingTalk stream client stopped via stop()");
        Ok(Ok(()))
    }

    async fn drop(
        &mut self,
        rep: Resource<DingtalkClientHandle>,
    ) -> wasmtime::Result<()> {
        if let Ok(handle) = self.table.delete(rep) {
            handle.cancel_token.cancel();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Bridge: CallbackHandler -> guest on-message
// ---------------------------------------------------------------------------

/// A bridge that receives DingTalk CallbackHandler invocations and forwards
/// them into the guest WASM component's `on-message` export.
struct GuestCallbackBridge {
    workload: ResolvedWorkload,
    component_id: String,
    replier: ChatbotReplier,
    credential: Credential,
    cancel_token: tokio_util::sync::CancellationToken,
}

#[async_trait]
impl dingtalk_stream::CallbackHandler for GuestCallbackBridge {
    async fn process(&self, callback_message: &dingtalk_stream::MessageBody) -> (u16, String) {
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

        let text_content =
            ChatbotReplier::extract_text(&incoming).and_then(|v| v.into_iter().next());

        let is_at = incoming.is_in_at_list.unwrap_or(false);

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

        let workload = Arc::new(self.workload.clone());
        let cid = self.component_id.clone();
        let replier = self.replier.clone();
        let credential = self.credential.clone();
        let cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            let mut store = match workload.new_store(&cid).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        component_id = %cid,
                        error = %e,
                        "Failed to create store for callback"
                    );
                    return;
                }
            };

            // Create a temporary client handle for the callback
            let temp_handle = DingtalkClientHandle {
                replier,
                credential,
                cancel_token,
            };
            let client_resource = match store.data_mut().table.push(temp_handle) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        component_id = %cid,
                        error = %e,
                        "Failed to push client resource for callback"
                    );
                    return;
                }
            };

            let instance_pre = match workload.instantiate_pre(&cid).await {
                Ok(pre) => pre,
                Err(e) => {
                    warn!(
                        component_id = %cid,
                        error = %e,
                        "Failed to instantiate_pre for callback"
                    );
                    return;
                }
            };

            let pre = match bindings::DingtalkStreamPre::new(instance_pre) {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        component_id = %cid,
                        error = %e,
                        "Failed to create DingtalkStreamPre"
                    );
                    return;
                }
            };

            let proxy = match pre.instantiate_async(&mut store).await {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        component_id = %cid,
                        error = %e,
                        "Failed to instantiate for callback"
                    );
                    return;
                }
            };

            match proxy
                .custom_dingtalk_stream_handler()
                .call_on_message(&mut store, client_resource, &wit_msg)
            {
                Ok(Ok(())) => {
                    debug!(
                        component_id = %cid,
                        "Guest on-message handled successfully"
                    );
                }
                Ok(Err(e)) => {
                    warn!(
                        component_id = %cid,
                        error = %e,
                        "Guest on-message returned error"
                    );
                }
                Err(e) => {
                    warn!(
                        component_id = %cid,
                        error = %e,
                        "Guest on-message call failed"
                    );
                }
            }
        });

        (200, "OK".to_owned())
    }
}

// ---------------------------------------------------------------------------
// Spawn the DingTalk stream client
// ---------------------------------------------------------------------------

fn spawn_dingtalk_stream(
    workload: ResolvedWorkload,
    component_id: String,
    replier: ChatbotReplier,
    credential: Credential,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    let bridge = GuestCallbackBridge {
        workload: workload.clone(),
        component_id: component_id.clone(),
        replier: replier.clone(),
        credential: credential.clone(),
        cancel_token: cancel_token.clone(),
    };

    let mut client = DingTalkStreamClient::builder(credential)
        .register_callback_handler(ChatbotMessage::TOPIC, bridge)
        .build();

    let cid_for_log = component_id.clone();

    tokio::spawn(async move {
        debug!(component_id = %cid_for_log, "Starting DingTalk stream client");
        tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!(
                    component_id = %cid_for_log,
                    "DingTalk stream task cancelled"
                );
            }
            result = client.start() => {
                match result {
                    Ok(()) => {
                        warn!(
                            component_id = %cid_for_log,
                            "DingTalk stream client exited"
                        );
                    }
                    Err(e) => {
                        warn!(
                            component_id = %cid_for_log,
                            error = %e,
                            "DingTalk stream client error"
                        );
                    }
                }
            }
        }
    });
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
        let Some(interface) = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "dingtalk-stream")
        else {
            return Ok(());
        };

        let interface_config = interface.config.clone();

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

        debug!(
            component_id = component_handle.id(),
            "DingTalk stream plugin bound to component"
        );

        self.tracker.write().await.add_component(
            component_handle,
            ComponentData {
                interface_config,
                workload: None,
            },
        );

        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        workload: &ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        let mut lock = self.tracker.write().await;
        if let Some(data) = lock.get_component_data_mut(component_id) {
            data.workload = Some(workload.clone());
        }
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(workload_id, |_| async {}, |_| async {})
            .await;
        debug!(workload_id = %workload_id, "DingTalk stream plugin unbound");
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
    fn test_build_synthetic_message() {
        let msg = build_synthetic_message("conv_123", "user_456");
        assert_eq!(msg.conversation_id.as_deref(), Some("conv_123"));
        assert_eq!(msg.sender_id.as_deref(), Some("user_456"));
        assert_eq!(msg.conversation_type.as_deref(), Some("1"));
    }
}
