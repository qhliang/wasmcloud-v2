//! # WeChat iLink Bot Host Plugin
//!
//! This plugin provides WeChat iLink Bot messaging integration for WASM components
//! via the `weixin-agent` SDK. It uses long-poll message monitoring and supports
//! sending text/media messages to arbitrary contacts.
//!
//! ## Configuration (via interface config)
//!
//! ```ignore
//! custom:wechat:
//!   config:
//!     token: "your-bot-token"    // Required. iLink Bot token
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::{ResolvedWorkload, WorkloadItem};
use wash_runtime::plugin::{HostPlugin, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "wechat",
        imports: { default: async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

use bindings::custom::wechat::types::WechatError;

const PLUGIN_ID: &str = "wechat";

// ---------------------------------------------------------------------------
// Per-component data
// ---------------------------------------------------------------------------

/// Configuration extracted from interface config.
#[derive(Clone, Debug)]
pub(crate) struct PluginConfig {
    pub token: String,
}

/// Per-component data tracked by the plugin.
pub(crate) struct ComponentData {
    /// Token that cancels the WeixinClient background task.
    pub cancel_token: tokio_util::sync::CancellationToken,
    /// Resolved workload — set during on_workload_resolved.
    pub workload: Option<ResolvedWorkload>,
    /// The weixin-agent client for sending messages.
    pub client: Option<Arc<weixin_agent::WeixinClient>>,
    /// Plugin config.
    pub config: PluginConfig,
}

// ---------------------------------------------------------------------------
// Plugin struct
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Wechat {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for Wechat {
    fn default() -> Self {
        Self::new()
    }
}

impl Wechat {
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
    let token = interface_config.get("token")?;
    Some(PluginConfig {
        token: token.clone(),
    })
}

// ---------------------------------------------------------------------------
// Spawn the WeixinClient long-poll in a background thread
// ---------------------------------------------------------------------------

fn spawn_weixin_client(
    workload: ResolvedWorkload,
    component_id: String,
    config: PluginConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Option<Arc<weixin_agent::WeixinClient>> {
    use weixin_agent::{MessageContext, MessageHandler, WeixinClient, WeixinConfig};

    struct WasmMessageHandler {
        workload: Arc<ResolvedWorkload>,
        component_id: String,
    }

    #[async_trait::async_trait]
    impl MessageHandler for WasmMessageHandler {
        async fn on_message(&self, ctx: &MessageContext) -> weixin_agent::Result<()> {
            let media_type = ctx.media.as_ref().map(|_| "media").unwrap_or("text");

            let wx_msg = bindings::custom::wechat::types::WechatMessage {
                message_id: ctx.message_id.clone(),
                sender: ctx.from.clone(),
                receiver: ctx.to.clone(),
                message_type: media_type.to_string(),
                text_content: ctx.body.clone(),
                timestamp: ctx.timestamp,
                raw_json: serde_json::json!({
                    "message_id": ctx.message_id,
                    "from": ctx.from,
                    "to": ctx.to,
                    "timestamp": ctx.timestamp,
                    "body": ctx.body,
                    "session_id": ctx.session_id,
                    "context_token": ctx.context_token,
                })
                .to_string(),
            };

            let workload = self.workload.clone();
            let cid = self.component_id.clone();

            tokio::spawn(async move {
                let mut store = match workload.new_store(&cid).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(component_id = %cid, error = %e, "Failed to create store for WeChat callback");
                        return;
                    }
                };

                let instance_pre = match workload.instantiate_pre(&cid).await {
                    Ok(pre) => pre,
                    Err(e) => {
                        warn!(component_id = %cid, error = %e, "Failed to instantiate_pre for WeChat callback");
                        return;
                    }
                };

                let pre = match bindings::WechatPre::new(instance_pre) {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(component_id = %cid, error = %e, "Failed to create WechatPre");
                        return;
                    }
                };

                let proxy = match pre.instantiate_async(&mut store).await {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(component_id = %cid, error = %e, "Failed to instantiate for WeChat callback");
                        return;
                    }
                };

                match proxy
                    .custom_wechat_handler()
                    .call_on_message(&mut store, &wx_msg)
                    .await
                {
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

            Ok(())
        }
    }

    let wx_config = match WeixinConfig::builder().token(&config.token).build() {
        Ok(c) => c,
        Err(e) => {
            warn!(
                component_id = %component_id,
                error = %e,
                "Failed to build WeixinConfig"
            );
            return None;
        }
    };

    let handler = WasmMessageHandler {
        workload: Arc::new(workload),
        component_id: component_id.clone(),
    };

    let client = match WeixinClient::builder(wx_config).on_message(handler).build() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            warn!(
                component_id = %component_id,
                error = %e,
                "Failed to build WeixinClient"
            );
            return None;
        }
    };

    let shared_client = client.clone();
    let cancel_token_for_thread = cancel_token.clone();
    let cid_for_log = component_id.clone();

    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                warn!(
                    component_id = %cid_for_log,
                    error = %e,
                    "Failed to build tokio runtime for WeChat client"
                );
                return;
            }
        };

        rt.block_on(async {
            tokio::select! {
                _ = cancel_token_for_thread.cancelled() => {
                    debug!(component_id = %cid_for_log, "WeChat client task cancelled");
                }
                result = client.start(None) => {
                    match result {
                        Ok(()) => {
                            warn!(component_id = %cid_for_log, "WeChat client exited");
                        }
                        Err(e) => {
                            warn!(component_id = %cid_for_log, error = %e, "WeChat client error");
                        }
                    }
                }
            }
        });
    });

    Some(shared_client)
}

// ---------------------------------------------------------------------------
// WIT types::Host
// ---------------------------------------------------------------------------

impl bindings::custom::wechat::types::Host for ActiveCtx<'_> {}

// ---------------------------------------------------------------------------
// WIT sender::Host
// ---------------------------------------------------------------------------

impl bindings::custom::wechat::sender::Host for ActiveCtx<'_> {
    async fn send_text(
        &mut self,
        to: String,
        text: String,
    ) -> wasmtime::Result<Result<(), WechatError>> {
        let Some(plugin) = self.get_plugin::<Wechat>(PLUGIN_ID) else {
            return Ok(Err(WechatError::Internal(
                "wechat plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let client = {
            let lock = plugin.tracker.read().await;
            match lock.get_component_data(&component_id) {
                Some(data) => match &data.client {
                    Some(c) => c.clone(),
                    None => {
                        return Ok(Err(WechatError::NotReady(
                            "wechat client not initialized".to_string(),
                        )));
                    }
                },
                None => {
                    return Ok(Err(WechatError::Internal(
                        "component not tracked".to_string(),
                    )));
                }
            }
        };

        match client.send_text(&to, &text, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(WechatError::SendFailed(e.to_string()))),
        }
    }

    async fn send_media(
        &mut self,
        to: String,
        file_path: String,
    ) -> wasmtime::Result<Result<(), WechatError>> {
        let Some(plugin) = self.get_plugin::<Wechat>(PLUGIN_ID) else {
            return Ok(Err(WechatError::Internal(
                "wechat plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let client = {
            let lock = plugin.tracker.read().await;
            match lock.get_component_data(&component_id) {
                Some(data) => match &data.client {
                    Some(c) => c.clone(),
                    None => {
                        return Ok(Err(WechatError::NotReady(
                            "wechat client not initialized".to_string(),
                        )));
                    }
                },
                None => {
                    return Ok(Err(WechatError::Internal(
                        "component not tracked".to_string(),
                    )));
                }
            }
        };

        let path = std::path::Path::new(&file_path);
        match client.send_media(&to, path, None).await {
            Ok(_) => Ok(Ok(())),
            Err(e) => Ok(Err(WechatError::SendFailed(e.to_string()))),
        }
    }
}

// ---------------------------------------------------------------------------
// WIT login::Host
// ---------------------------------------------------------------------------

impl bindings::custom::wechat::login::Host for ActiveCtx<'_> {
    async fn qr_start(&mut self) -> wasmtime::Result<Result<String, WechatError>> {
        let Some(plugin) = self.get_plugin::<Wechat>(PLUGIN_ID) else {
            return Ok(Err(WechatError::Internal(
                "wechat plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let client = {
            let lock = plugin.tracker.read().await;
            match lock.get_component_data(&component_id) {
                Some(data) => match &data.client {
                    Some(c) => c.clone(),
                    None => {
                        return Ok(Err(WechatError::NotReady(
                            "wechat client not initialized".to_string(),
                        )));
                    }
                },
                None => {
                    return Ok(Err(WechatError::Internal(
                        "component not tracked".to_string(),
                    )));
                }
            }
        };

        let qr_api = client.qr_login();
        match qr_api.start(None).await {
            Ok(session) => {
                let json = serde_json::json!({
                    "qrcode": session.qrcode,
                    "qrcode_img_content": session.qrcode_img_content,
                });
                Ok(Ok(json.to_string()))
            }
            Err(e) => Ok(Err(WechatError::Internal(e.to_string()))),
        }
    }

    async fn qr_poll_status(
        &mut self,
        session_json: String,
    ) -> wasmtime::Result<Result<String, WechatError>> {
        let Some(plugin) = self.get_plugin::<Wechat>(PLUGIN_ID) else {
            return Ok(Err(WechatError::Internal(
                "wechat plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let client = {
            let lock = plugin.tracker.read().await;
            match lock.get_component_data(&component_id) {
                Some(data) => match &data.client {
                    Some(c) => c.clone(),
                    None => {
                        return Ok(Err(WechatError::NotReady(
                            "wechat client not initialized".to_string(),
                        )));
                    }
                },
                None => {
                    return Ok(Err(WechatError::Internal(
                        "component not tracked".to_string(),
                    )));
                }
            }
        };

        // Parse session from JSON
        let parsed: serde_json::Value = match serde_json::from_str(&session_json) {
            Ok(v) => v,
            Err(e) => {
                return Ok(Err(WechatError::Internal(e.to_string())));
            }
        };
        let session = weixin_agent::QrLoginSession {
            qrcode: parsed
                .get("qrcode")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            qrcode_img_content: parsed
                .get("qrcode_img_content")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
        };

        let qr_api = client.qr_login();
        match qr_api.poll_status(&session).await {
            Ok(status) => {
                let json = match status {
                    weixin_agent::LoginStatus::Wait => {
                        serde_json::json!({"status": "wait"})
                    }
                    weixin_agent::LoginStatus::Scanned => {
                        serde_json::json!({"status": "scanned"})
                    }
                    weixin_agent::LoginStatus::ScannedButRedirect { redirect_host } => {
                        serde_json::json!({"status": "scanned_redirect", "redirect_host": redirect_host})
                    }
                    weixin_agent::LoginStatus::Confirmed {
                        bot_token,
                        ilink_bot_id,
                        base_url,
                        ilink_user_id,
                    } => {
                        serde_json::json!({
                            "status": "confirmed",
                            "bot_token": bot_token,
                            "ilink_bot_id": ilink_bot_id,
                            "base_url": base_url,
                            "ilink_user_id": ilink_user_id,
                        })
                    }
                    weixin_agent::LoginStatus::Expired => {
                        serde_json::json!({"status": "expired"})
                    }
                };
                Ok(Ok(json.to_string()))
            }
            Err(e) => Ok(Err(WechatError::Internal(e.to_string()))),
        }
    }
}

// ---------------------------------------------------------------------------
// HostPlugin implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl HostPlugin for Wechat {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from("custom:wechat/handler@0.1.0")]),
            exports: HashSet::from([WitInterface::from("custom:wechat/sender,login@0.1.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let Some(interface) = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "wechat")
        else {
            return Ok(());
        };

        let config = match extract_config(&interface.config) {
            Some(c) => c,
            None => {
                warn!("Missing token in wechat interface config");
                return Ok(());
            }
        };

        bindings::custom::wechat::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::wechat::sender::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::wechat::login::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        debug!(
            component_id = component_handle.id(),
            "Tracking component for WeChat callbacks"
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
        let _pre = bindings::WechatPre::new(instance_pre)?;

        let client = spawn_weixin_client(
            workload.clone(),
            component_id.to_string(),
            config,
            cancel_token,
        );

        if let Some(client) = client {
            let mut lock = self.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(component_id) {
                data.client = Some(client);
            }
        }

        debug!(
            component_id = %component_id,
            "WeChat plugin resolved and client started"
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
        let plugin = Wechat::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_imports() {
        let plugin = Wechat::new();
        let world = plugin.world();
        assert!(
            world
                .imports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "wechat")
        );
    }

    #[test]
    fn test_world_exports() {
        let plugin = Wechat::new();
        let world = plugin.world();
        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "wechat")
        );
    }

    #[test]
    fn test_extract_config() {
        let mut config = HashMap::new();
        config.insert("token".to_string(), "test-bot-token".to_string());

        let pc = extract_config(&config).unwrap();
        assert_eq!(pc.token, "test-bot-token");
    }

    #[test]
    fn test_extract_config_missing() {
        let config = HashMap::new();
        assert!(extract_config(&config).is_none());
    }
}
