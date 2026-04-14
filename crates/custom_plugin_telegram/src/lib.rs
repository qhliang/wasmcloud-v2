//! # Telegram Host Plugin
//!
//! This plugin provides Telegram Bot integration for WASM components.
//! It uses teloxide for long-polling message reception and sending.
//!
//! ## Configuration (via interface config)
//!
//! ```ignore
//! custom:telegram:
//!   config:
//!     bot_token: "123456:ABC-DEF..."
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use teloxide::prelude::*;
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};
use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::ResolvedWorkload;
use wash_runtime::plugin::{HostPlugin, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "telegram",
        imports: { default: async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

use bindings::custom::telegram::types::{TelegramError, TelegramMessage};

const PLUGIN_ID: &str = "telegram";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Per-component data tracked by the plugin
struct ComponentData {
    /// Token that cancels ALL background tasks for this component
    cancel_token: tokio_util::sync::CancellationToken,
    /// Resolved workload — set during on_workload_resolved
    workload: Option<ResolvedWorkload>,
    /// The teloxide Bot instance
    bot: Option<Arc<Bot>>,
    /// Plugin config
    config: PluginConfig,
}

/// Configuration for a workload's telegram integration
#[derive(Clone, Debug)]
struct PluginConfig {
    bot_token: String,
}

// ---------------------------------------------------------------------------
// The Telegram Plugin
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Telegram {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for Telegram {
    fn default() -> Self {
        Self::new()
    }
}

impl Telegram {
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
    let bot_token = interface_config.get("bot_token")?;
    Some(PluginConfig {
        bot_token: bot_token.clone(),
    })
}

// ---------------------------------------------------------------------------
// WIT types::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::telegram::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::telegram::sender::Host for ActiveCtx<'a> {
    #[instrument(skip_all)]
    async fn send_text(
        &mut self,
        chat_id: String,
        text: String,
    ) -> wasmtime::Result<Result<(), TelegramError>> {
        let Some(plugin) = self.get_plugin::<Telegram>(PLUGIN_ID) else {
            return Ok(Err(TelegramError::Internal(
                "telegram plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();
        let lock = plugin.tracker.read().await;

        match lock.get_component_data(&component_id) {
            Some(data) => match &data.bot {
                Some(bot) => {
                    let chat_id_val =
                        teloxide::types::ChatId(chat_id.parse::<i64>().map_err(|_| {
                            TelegramError::SendFailed(format!("invalid chat_id: {chat_id}"))
                        })?);

                    match bot.send_message(chat_id_val, &text).await {
                        Ok(_) => {
                            debug!(chat_id = %chat_id, "Telegram send_text OK");
                            Ok(Ok(()))
                        }
                        Err(e) => {
                            warn!(chat_id = %chat_id, error = %e, "Telegram send_text failed");
                            Ok(Err(TelegramError::SendFailed(e.to_string())))
                        }
                    }
                }
                None => Ok(Err(TelegramError::NotReady(
                    "telegram bot not initialized".to_string(),
                ))),
            },
            None => Ok(Err(TelegramError::Internal(
                "component not tracked".to_string(),
            ))),
        }
    }

    #[instrument(skip_all)]
    async fn send_media(
        &mut self,
        chat_id: String,
        file_path: String,
        caption: Option<String>,
    ) -> wasmtime::Result<Result<(), TelegramError>> {
        let Some(plugin) = self.get_plugin::<Telegram>(PLUGIN_ID) else {
            return Ok(Err(TelegramError::Internal(
                "telegram plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();
        let lock = plugin.tracker.read().await;

        match lock.get_component_data(&component_id) {
            Some(data) => match &data.bot {
                Some(bot) => {
                    let chat_id_val =
                        teloxide::types::ChatId(chat_id.parse::<i64>().map_err(|_| {
                            TelegramError::SendFailed(format!("invalid chat_id: {chat_id}"))
                        })?);

                    let path = std::path::Path::new(&file_path);
                    if !path.exists() {
                        return Ok(Err(TelegramError::SendFailed(format!(
                            "file not found: {file_path}"
                        ))));
                    }

                    let document = teloxide::types::InputFile::file(path);
                    let mut builder = bot.send_document(chat_id_val, document);
                    if let Some(ref cap) = caption {
                        builder = builder.caption(cap);
                    }

                    match builder.await {
                        Ok(_) => {
                            debug!(chat_id = %chat_id, "Telegram send_media OK");
                            Ok(Ok(()))
                        }
                        Err(e) => {
                            warn!(chat_id = %chat_id, error = %e, "Telegram send_media failed");
                            Ok(Err(TelegramError::SendFailed(e.to_string())))
                        }
                    }
                }
                None => Ok(Err(TelegramError::NotReady(
                    "telegram bot not initialized".to_string(),
                ))),
            },
            None => Ok(Err(TelegramError::Internal(
                "component not tracked".to_string(),
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Background bot spawning
// ---------------------------------------------------------------------------

fn spawn_telegram_bot(
    workload: ResolvedWorkload,
    component_id: String,
    config: PluginConfig,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Option<Arc<Bot>> {
    let bot = Arc::new(Bot::new(&config.bot_token));

    let bot_clone = bot.clone();
    let cancel_token_clone = cancel_token.clone();
    let cid_for_log = component_id.clone();

    tokio::spawn(async move {
        debug!(component_id = %cid_for_log, "Starting Telegram bot long polling");

        let mut listener = teloxide::update_listeners::polling_default(bot_clone).await;
        use teloxide::update_listeners::AsUpdateStream;
        let stream = listener.as_stream();

        use futures::StreamExt;
        tokio::pin!(stream);

        loop {
            tokio::select! {
                _ = cancel_token_clone.cancelled() => {
                    debug!(component_id = %cid_for_log, "Telegram bot polling cancelled");
                    break;
                }
                update = stream.next() => {
                    match update {
                        Some(Ok(update)) => {
                            if let teloxide::types::UpdateKind::Message(message) = update.kind {
                                handle_telegram_message(
                                    &workload,
                                    &component_id,
                                    &message,
                                ).await;
                            }
                        }
                        Some(Err(e)) => {
                            warn!(
                                component_id = %cid_for_log,
                                error = %e,
                                "Telegram polling error"
                            );
                        }
                        None => {
                            debug!(component_id = %cid_for_log, "Telegram polling stream ended");
                            break;
                        }
                    }
                }
            }
        }
    });

    Some(bot)
}

async fn handle_telegram_message(
    workload: &ResolvedWorkload,
    component_id: &str,
    message: &teloxide::types::Message,
) {
    let text_content = message.text().map(|s| s.to_string());
    let sender_username = message.from.as_ref().and_then(|u| u.username.clone());
    let chat_id = message.chat.id.0.to_string();
    let sender_id = message
        .from
        .as_ref()
        .map(|u| u.id.0.to_string())
        .unwrap_or_default();
    let message_id = message.id.0.to_string();
    let timestamp = message.date;

    let tg_msg = TelegramMessage {
        message_id,
        chat_id,
        sender_id,
        sender_username,
        text_content,
        timestamp: timestamp.timestamp(),
    };

    let workload = Arc::new(workload.clone());
    let cid = component_id.to_string();

    tokio::spawn(async move {
        let mut store = match workload.new_store(&cid).await {
            Ok(s) => s,
            Err(e) => {
                warn!(component_id = %cid, error = %e, "Failed to create store for Telegram callback");
                return;
            }
        };

        let instance_pre = match workload.instantiate_pre(&cid).await {
            Ok(pre) => pre,
            Err(e) => {
                warn!(component_id = %cid, error = %e, "Failed to instantiate_pre for Telegram callback");
                return;
            }
        };

        let pre = match bindings::TelegramPre::new(instance_pre) {
            Ok(p) => p,
            Err(e) => {
                warn!(component_id = %cid, error = %e, "Failed to create TelegramPre");
                return;
            }
        };

        let proxy = match pre.instantiate_async(&mut store).await {
            Ok(p) => p,
            Err(e) => {
                warn!(component_id = %cid, error = %e, "Failed to instantiate for Telegram callback");
                return;
            }
        };

        match proxy
            .custom_telegram_handler()
            .call_on_message(&mut store, &tg_msg)
            .await
        {
            Ok(Ok(())) => {
                debug!(component_id = %cid, "Guest Telegram on-message handled successfully");
            }
            Ok(Err(e)) => {
                warn!(component_id = %cid, error = %e, "Guest Telegram on-message returned error");
            }
            Err(e) => {
                warn!(component_id = %cid, error = %e, "Guest Telegram on-message call failed");
            }
        }
    });
}

// ---------------------------------------------------------------------------
// HostPlugin implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl HostPlugin for Telegram {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from("custom:telegram/sender@0.1.0")]),
            exports: HashSet::from([WitInterface::from("custom:telegram/handler@0.1.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let Some(interface) = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "telegram")
        else {
            return Ok(());
        };

        let config = match extract_config(&interface.config) {
            Some(c) => c,
            None => {
                warn!("Telegram plugin config validation failed: missing bot_token");
                return Ok(());
            }
        };

        // Add bindings to linker
        bindings::custom::telegram::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::telegram::sender::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        // Only track components (not services)
        let wash_runtime::engine::workload::WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        debug!(
            component_id = component_handle.id(),
            "Telegram plugin bound to component"
        );

        self.tracker.write().await.add_component(
            component_handle,
            ComponentData {
                cancel_token: tokio_util::sync::CancellationToken::new(),
                workload: None,
                bot: None,
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

        let bot = spawn_telegram_bot(
            workload.clone(),
            component_id.to_string(),
            config,
            cancel_token,
        );

        if let Some(bot) = bot {
            let mut lock = self.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(component_id) {
                data.bot = Some(bot);
            }
        }

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

        debug!(workload_id = %workload_id, "Telegram plugin unbound");
        Ok(())
    }
}
