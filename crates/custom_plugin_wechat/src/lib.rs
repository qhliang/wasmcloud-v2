//! # WeChat iLink Bot Host Plugin (Resource-based)
//!
//! Two config sources with priority:
//! 1. Wasm dynamic config (passed via resource constructor)
//! 2. Static interface config (fallback from wasmcloud config)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};
use wasmtime::component::Resource;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::{ResolvedWorkload, WorkloadItem};
use wash_runtime::plugin::config::resolve_field;
use wash_runtime::plugin::{HostPlugin, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "wechat",
        imports: {
            default: async | trappable | tracing,
        },
        exports: { default: async | tracing },
        with: {
            "custom:wechat/sender.wechat-client": super::WechatClientHandle,
        },
    });
}

use bindings::custom::wechat::types::{WechatConfig, WechatError, WechatMessage};

const PLUGIN_ID: &str = "wechat";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Host-side state for a wechat-client resource instance.
/// The cancel_token here is per-resource only; the long-poll loop has its own
/// token stored in ComponentData.
pub struct WechatClientHandle {
    client: Arc<weixin_agent::WeixinClient>,
}

/// Per-component data.
struct ComponentData {
    /// Static interface config from wasmcloud config (fallback source)
    interface_config: HashMap<String, String>,
    /// Resolved workload
    workload: Option<ResolvedWorkload>,
    /// Shared WeixinClient (started in on_workload_resolved, independent of resource lifetime)
    client: Option<Arc<weixin_agent::WeixinClient>>,
    /// Cancellation token for the long-poll loop
    poll_cancel_token: Option<tokio_util::sync::CancellationToken>,
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
// WIT types::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::wechat::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::Host — empty (resource lives in HostWechatClient)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::wechat::sender::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::HostWechatClient — resource constructor + methods
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::wechat::sender::HostWechatClient for ActiveCtx<'a> {
    async fn new(
        &mut self,
        _config: Option<WechatConfig>,
    ) -> wasmtime::Result<Resource<WechatClientHandle>> {
        let Some(plugin) = self.get_plugin::<Wechat>(PLUGIN_ID) else {
            return Err(wasmtime::Error::msg("wechat plugin not available"));
        };

        let component_id: Arc<str> = self.component_id.clone();
        let lock = plugin.tracker.read().await;
        let Some(data) = lock.get_component_data(&component_id) else {
            return Err(wasmtime::Error::msg("component not tracked"));
        };

        let Some(client) = data.client.clone() else {
            return Err(wasmtime::Error::msg(
                "wechat client not started — long-poll loop should be running from on_workload_resolved",
            ));
        };

        drop(lock);

        let handle = WechatClientHandle { client };

        let resource = self.table.push(handle)?;
        Ok(resource)
    }

    #[instrument(skip_all)]
    async fn send_text(
        &mut self,
        client: Resource<WechatClientHandle>,
        to: String,
        text: String,
    ) -> wasmtime::Result<Result<(), WechatError>> {
        let handle = self.table.get(&client)?;

        match handle.client.send_text(&to, &text, None).await {
            Ok(_) => {
                debug!(to = %to, "WeChat send_text OK");
                Ok(Ok(()))
            }
            Err(e) => {
                warn!(to = %to, error = %e, "WeChat send_text failed");
                Ok(Err(WechatError::SendFailed(e.to_string())))
            }
        }
    }

    #[instrument(skip_all)]
    async fn send_media(
        &mut self,
        client: Resource<WechatClientHandle>,
        to: String,
        file_path: String,
    ) -> wasmtime::Result<Result<(), WechatError>> {
        let handle = self.table.get(&client)?;
        let path = std::path::Path::new(&file_path);

        match handle.client.send_media(&to, path, None).await {
            Ok(_) => {
                debug!(to = %to, "WeChat send_media OK");
                Ok(Ok(()))
            }
            Err(e) => {
                warn!(to = %to, error = %e, "WeChat send_media failed");
                Ok(Err(WechatError::SendFailed(e.to_string())))
            }
        }
    }

    async fn qr_start(
        &mut self,
        client: Resource<WechatClientHandle>,
    ) -> wasmtime::Result<Result<String, WechatError>> {
        let handle = self.table.get(&client)?;

        let qr_api = handle.client.qr_login();
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
        client: Resource<WechatClientHandle>,
        session_json: String,
    ) -> wasmtime::Result<Result<String, WechatError>> {
        let handle = self.table.get(&client)?;

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

        let qr_api = handle.client.qr_login();
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

    async fn stop(
        &mut self,
        _client: Resource<WechatClientHandle>,
    ) -> wasmtime::Result<Result<(), WechatError>> {
        debug!("WeChat client resource stop() — long-poll loop unaffected");
        Ok(Ok(()))
    }

    async fn drop(&mut self, rep: Resource<WechatClientHandle>) -> wasmtime::Result<()> {
        let _ = self.table.delete(rep);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Spawn the WeixinClient long-poll in a background thread
// ---------------------------------------------------------------------------

fn spawn_weixin_client(
    workload: ResolvedWorkload,
    component_id: String,
    token: String,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Option<Arc<weixin_agent::WeixinClient>> {
    use std::sync::Mutex;
    use weixin_agent::{MessageContext, MessageHandler, WeixinClient, WeixinConfig};

    struct WasmMessageHandler {
        workload: Arc<ResolvedWorkload>,
        component_id: String,
        /// Shared reference to the client, set after construction.
        client_ref: Arc<Mutex<Option<Arc<WeixinClient>>>>,
    }

    #[async_trait::async_trait]
    impl MessageHandler for WasmMessageHandler {
        async fn on_message(&self, ctx: &MessageContext) -> weixin_agent::Result<()> {
            // Record metrics via global meter
            {
                let meter = opentelemetry::global::meter("wechat");
                let counter = meter
                    .u64_counter("wechat_messages_total")
                    .with_description("Total number of WeChat messages processed")
                    .build();
                counter.add(1, &[opentelemetry::KeyValue::new("direction", "inbound")]);
            }

            let media_type = ctx.media.as_ref().map(|_| "media").unwrap_or("text");

            let wx_msg = WechatMessage {
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

            let client = {
                let guard = self
                    .client_ref
                    .lock()
                    .map_err(|_| weixin_agent::Error::Config("client lock poisoned".to_string()))?;
                match guard.clone() {
                    Some(c) => c,
                    None => {
                        warn!(component_id = %self.component_id, "WeChat client not set in handler yet");
                        return Err(weixin_agent::Error::Config(
                            "client not initialized".to_string(),
                        ));
                    }
                }
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

                // Temporary client handle for the callback — guest can call methods on it
                let temp_handle = WechatClientHandle { client };
                let client_resource = match store.data_mut().table.push(temp_handle) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(component_id = %cid, error = %e, "Failed to push client resource for callback");
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
                    .call_on_message(&mut store, client_resource, &wx_msg)
                    .await
                {
                    Ok(Ok(())) => {
                        debug!(component_id = %cid, "Guest WeChat on-message handled successfully");
                    }
                    Ok(Err(e)) => {
                        warn!(component_id = %cid, error = %e, "Guest WeChat on-message returned error");
                    }
                    Err(e) => {
                        warn!(component_id = %cid, error = %e, "Guest WeChat on-message call failed");
                    }
                }
            });

            Ok(())
        }
    }

    let client_ref: Arc<Mutex<Option<Arc<WeixinClient>>>> = Arc::new(Mutex::new(None));

    let wx_config = match WeixinConfig::builder().token(&token).build() {
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
        client_ref: client_ref.clone(),
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

    // Set the client reference so the handler can access it in callbacks
    if let Ok(mut guard) = client_ref.lock() {
        *guard = Some(client.clone());
    }

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
            exports: HashSet::from([WitInterface::from("custom:wechat/sender@0.1.0")]),
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

        let interface_config = interface.config.clone();

        bindings::custom::wechat::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::wechat::sender::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        debug!(
            component_id = component_handle.id(),
            "WeChat plugin bound to component"
        );

        self.tracker.write().await.add_component(
            component_handle,
            ComponentData {
                interface_config,
                workload: None,
                client: None,
                poll_cancel_token: None,
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

            // Start the long-poll loop once the workload is resolved
            if data.client.is_none() {
                let token = match resolve_field(None, &data.interface_config, "token") {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(
                            component_id,
                            error = %e,
                            "wechat: no token configured, skipping long-poll start"
                        );
                        return Ok(());
                    }
                };

                let cancel_token = tokio_util::sync::CancellationToken::new();

                if let Some(client) = spawn_weixin_client(
                    workload.clone(),
                    component_id.to_string(),
                    token,
                    cancel_token.clone(),
                ) {
                    debug!(
                        component_id,
                        "WeChat long-poll loop started from on_workload_resolved"
                    );
                    data.client = Some(client);
                    data.poll_cancel_token = Some(cancel_token);
                }
            }
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
            .remove_workload_with_cleanup(
                workload_id,
                |_| async {},
                |data| async move {
                    if let Some(token) = data.poll_cancel_token.as_ref() {
                        token.cancel();
                        debug!(workload_id, "WeChat long-poll cancelled on unbind");
                    }
                },
            )
            .await;
        debug!(workload_id = %workload_id, "WeChat plugin unbound");
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

        let val = resolve_field(None, &config, "token").unwrap();
        assert_eq!(val, "test-bot-token");
    }

    #[test]
    fn test_extract_config_missing() {
        let config = HashMap::new();
        assert!(resolve_field(None, &config, "token").is_err());
    }
}
