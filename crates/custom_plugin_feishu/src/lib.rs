//! # Feishu (Lark) Host Plugin
//!
//! This plugin provides Feishu IM messaging and full platform API access to WASM
//! components via the WebSocket long-connection (长连接) mode using the `open-lark` SDK.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{debug, warn};

use anyhow::Context as _;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::{ResolvedWorkload, WorkloadItem};
use wash_runtime::plugin::HostPlugin;
use wash_runtime::plugin::WorkloadTracker;
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "feishu",
        imports: { default: async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

// Feature modules — each implements the corresponding WIT Host trait.
mod r#ai;
mod r#bot;
mod calendar;
mod cardkit;
mod contact;
mod docs;
mod group;
mod http;
mod im;
mod mail;
mod task;

use bindings::custom::feishu::types::FeishuError;

pub(crate) const PLUGIN_ID: &str = "feishu";

// ---------------------------------------------------------------------------
// Per-component data
// ---------------------------------------------------------------------------

/// Configuration extracted from interface config.
#[derive(Clone, Debug)]
pub(crate) struct PluginConfig {
    pub app_id: String,
    pub app_secret: String,
}

/// Per-component data tracked by the plugin.
pub(crate) struct ComponentData {
    /// Token that cancels the Feishu WebSocket background task.
    pub cancel_token: tokio_util::sync::CancellationToken,
    /// Resolved workload — set during on_workload_resolved.
    pub workload: Option<ResolvedWorkload>,
    /// The open-lark LarkClient for sending messages.
    pub client: Option<Arc<open_lark::prelude::LarkClient>>,
    /// Plugin config.
    pub config: PluginConfig,
}

// ---------------------------------------------------------------------------
// Plugin struct
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Feishu {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for Feishu {
    fn default() -> Self {
        Self::new()
    }
}

impl Feishu {
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
    let app_id = interface_config.get("app-id")?;
    let app_secret = interface_config.get("app-secret")?;
    Some(PluginConfig {
        app_id: app_id.clone(),
        app_secret: app_secret.clone(),
    })
}

// ---------------------------------------------------------------------------
// Spawn the Feishu WebSocket client
// ---------------------------------------------------------------------------

fn spawn_feishu_ws(
    workload: ResolvedWorkload,
    component_id: String,
    config: PluginConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Arc<open_lark::prelude::LarkClient> {
    use open_lark::prelude::*;

    let client = Arc::new(
        LarkClient::builder(&config.app_id, &config.app_secret)
            .with_app_type(AppType::SelfBuild)
            .with_enable_token_cache(true)
            .build(),
    );

    let shared_config = Arc::new(client.config.clone());

    let workload = Arc::new(workload);
    let cid = component_id.clone();

    // EventDispatcherHandler contains `Box<dyn EventHandler>` which is not
    // Send+Sync, so we must construct it **inside** the dedicated thread to
    // avoid crossing the Send boundary.
    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                warn!(
                    component_id = %component_id,
                    error = %e,
                    "Failed to build tokio runtime for Feishu WS"
                );
                return;
            }
        };

        let handler = {
            let workload = workload.clone();
            let cid = cid.clone();
            move |event: open_lark::service::im::v1::p2_im_message_receive_v1::P2ImMessageReceiveV1| {
                let msg = &event.event.message;
                let sender = &event.event.sender;

                let text_content = if msg.message_type == "text" {
                    serde_json::from_str::<serde_json::Value>(&msg.content)
                        .ok()
                        .and_then(|v| {
                            v.get("text")
                                .and_then(|t| t.as_str())
                                .map(String::from)
                        })
                } else {
                    None
                };

                let im_msg = bindings::custom::feishu::types::ImMessage {
                    message_id: msg.message_id.clone(),
                    chat_id: msg.chat_id.clone(),
                    sender_id: sender.sender_id.open_id.clone(),
                    sender_type: sender.sender_type.clone(),
                    message_type: msg.message_type.clone(),
                    text_content,
                    create_time: msg.create_time.clone(),
                    raw_json: serde_json::json!({
                        "message_id": msg.message_id,
                        "chat_id": msg.chat_id,
                        "message_type": msg.message_type,
                        "content": msg.content,
                        "sender_open_id": sender.sender_id.open_id,
                    })
                    .to_string(),
                };

                let workload = workload.clone();
                let cid = cid.clone();

                tokio::spawn(async move {
                    let mut store = match workload.new_store(&cid).await {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Failed to create store for Feishu callback");
                            return;
                        }
                    };

                    let instance_pre = match workload.instantiate_pre(&cid).await {
                        Ok(pre) => pre,
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Failed to instantiate_pre for Feishu callback");
                            return;
                        }
                    };

                    let pre = match bindings::FeishuPre::new(instance_pre) {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Failed to create FeishuPre");
                            return;
                        }
                    };

                    let proxy = match pre.instantiate_async(&mut store).await {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Failed to instantiate for Feishu callback");
                            return;
                        }
                    };

                    match proxy.custom_feishu_handler().call_on_message(&mut store, &im_msg).await {
                        Ok(Ok(())) => {
                            debug!(component_id = %cid, "Guest on-message handled successfully");
                        }
                        Ok(Err(e)) => {
                            warn!(component_id = %cid, error = %e, "Guest on-message returned error");
                        }
                        Err(e) => {
                            warn!(component_id = %cid, error = %e, "Guest on-message call failed");
                        }
                    }
                });
            }
        };

        let event_handler = match EventDispatcherHandler::builder()
            .register_p2_im_message_receive_v1(handler)
        {
            Ok(b) => b.build(),
            Err(e) => {
                warn!(component_id = %component_id, error = %e, "Failed to register IM message handler");
                return;
            }
        };

        rt.block_on(async {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(component_id = %component_id, "Feishu WebSocket task cancelled");
                }
                result = open_lark::client::ws_client::LarkWsClient::open(
                    shared_config,
                    event_handler,
                ) => {
                    match result {
                        Ok(()) => {
                            warn!(component_id = %component_id, "Feishu WebSocket client exited");
                        }
                        Err(e) => {
                            warn!(component_id = %component_id, error = %e, "Feishu WebSocket client error");
                        }
                    }
                }
            }
        });
    });

    client
}

// ---------------------------------------------------------------------------
// WIT types::Host
// ---------------------------------------------------------------------------

impl bindings::custom::feishu::types::Host for ActiveCtx<'_> {}

// ---------------------------------------------------------------------------
// Helper: get tenant access token
// ---------------------------------------------------------------------------

pub(crate) async fn get_tenant_token(
    client: &open_lark::prelude::LarkClient,
) -> Result<String, FeishuError> {
    let token_manager = client.config.token_manager.lock().await;
    token_manager
        .get_tenant_access_token(&client.config, "", "", &client.config.app_ticket_manager)
        .await
        .map_err(|e| FeishuError::AuthFailed(e.to_string()))
}

// ---------------------------------------------------------------------------
// Helper: get client from plugin tracker
// ---------------------------------------------------------------------------

pub(crate) async fn get_client(
    plugin: &Feishu,
    component_id: &str,
) -> Result<Arc<open_lark::prelude::LarkClient>, FeishuError> {
    let lock = plugin.tracker.read().await;
    match lock.get_component_data(component_id) {
        Some(data) => match &data.client {
            Some(c) => Ok(c.clone()),
            None => Err(FeishuError::Internal(
                "Feishu client not initialized".to_string(),
            )),
        },
        None => Err(FeishuError::Internal("component not tracked".to_string())),
    }
}

// ---------------------------------------------------------------------------
// Macro: add all feishu interfaces to linker
// ---------------------------------------------------------------------------

macro_rules! add_all_feishu_linkers {
    ($linker:expr, $ctx_fn:expr) => {{
        bindings::custom::feishu::types::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::contact_sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::group_sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::ai_sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::calendar_sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::cardkit_sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::mail_sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::task_sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::bot_sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
        bindings::custom::feishu::docs_sender::add_to_linker::<_, SharedCtx>($linker, $ctx_fn)?;
    }};
}

// ---------------------------------------------------------------------------
// HostPlugin implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl HostPlugin for Feishu {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([
                WitInterface::from("custom:feishu/sender,types@0.1.0"),
                WitInterface::from("custom:feishu/contact-sender@0.1.0"),
                WitInterface::from("custom:feishu/group-sender@0.1.0"),
                WitInterface::from("custom:feishu/ai-sender@0.1.0"),
                WitInterface::from("custom:feishu/calendar-sender@0.1.0"),
                WitInterface::from("custom:feishu/cardkit-sender@0.1.0"),
                WitInterface::from("custom:feishu/mail-sender@0.1.0"),
                WitInterface::from("custom:feishu/task-sender@0.1.0"),
                WitInterface::from("custom:feishu/bot-sender@0.1.0"),
                WitInterface::from("custom:feishu/docs-sender@0.1.0"),
            ]),
            exports: HashSet::from([WitInterface::from("custom:feishu/handler@0.1.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let Some(interface) = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "feishu")
        else {
            return Ok(());
        };

        let config = match extract_config(&interface.config) {
            Some(c) => c,
            None => {
                warn!("Missing app-id or app-secret in feishu interface config");
                return Ok(());
            }
        };

        add_all_feishu_linkers!(item.linker(), extract_active_ctx);

        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        let has_handler = component_handle
            .world()
            .exports
            .iter()
            .any(|i| i.namespace == "custom" && i.package == "feishu");

        if has_handler {
            debug!(
                component_id = component_handle.id(),
                "Tracking component for Feishu IM callbacks"
            );

            self.tracker.write().await.add_component(
                component_handle,
                ComponentData {
                    cancel_token: tokio_util::sync::CancellationToken::new(),
                    workload: None,
                    client: None,
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

        let instance_pre = workload.instantiate_pre(component_id).await?;
        let _pre = bindings::FeishuPre::new(instance_pre)
            .map_err(anyhow::Error::from)
            .context("failed to instantiate feishu pre")?;

        let client = spawn_feishu_ws(
            workload.clone(),
            component_id.to_string(),
            config,
            cancel_token,
        );

        {
            let mut lock = self.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(component_id) {
                data.client = Some(client);
            }
        }

        debug!(
            component_id = %component_id,
            "Feishu plugin resolved and WebSocket client started"
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
        let plugin = Feishu::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_imports() {
        let plugin = Feishu::new();
        let world = plugin.world();
        assert!(
            world
                .imports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "feishu")
        );
    }

    #[test]
    fn test_world_exports() {
        let plugin = Feishu::new();
        let world = plugin.world();
        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "feishu")
        );
    }

    #[test]
    fn test_extract_config() {
        let mut config = HashMap::new();
        config.insert("app-id".to_string(), "cli_test".to_string());
        config.insert("app-secret".to_string(), "secret_test".to_string());

        let pc = extract_config(&config).unwrap();
        assert_eq!(pc.app_id, "cli_test");
        assert_eq!(pc.app_secret, "secret_test");
    }

    #[test]
    fn test_extract_config_missing() {
        let config = HashMap::new();
        assert!(extract_config(&config).is_none());

        let mut config = HashMap::new();
        config.insert("app-id".to_string(), "test_id".to_string());
        assert!(extract_config(&config).is_none());
    }
}
