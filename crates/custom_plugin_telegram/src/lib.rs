//! # Telegram Host Plugin (Resource-based)
//!
//! Two config sources with priority:
//! 1. Wasm dynamic config (passed via resource constructor)
//! 2. Static interface config (fallback from wasmcloud config)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use teloxide::prelude::*;
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};
use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::ResolvedWorkload;
use wash_runtime::plugin::config::resolve_field;
use wash_runtime::plugin::{HostPlugin, WorkloadTracker, find_interface};
use wash_runtime::wit::{WitInterface, WitWorld};
use wasmtime::component::Resource;

mod bindings {
    wasmtime::component::bindgen!({
        world: "telegram",
        imports: {
            default: async | trappable | tracing,
        },
        exports: { default: async | tracing },
        with: {
            "custom:telegram/sender.telegram-bot": super::TelegramBotHandle,
        },
    });
}

use bindings::custom::telegram::types::{TelegramConfig, TelegramError, TelegramMessage};

const PLUGIN_ID: &str = "telegram";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Host-side state for a telegram-bot resource instance.
/// The polling loop has its own cancel token stored in ComponentData.
pub struct TelegramBotHandle {
    bot: Arc<Bot>,
}

/// Per-component data.
struct ComponentData {
    /// Static interface config from wasmcloud config (fallback source)
    interface_config: HashMap<String, String>,
    /// Resolved workload
    workload: Option<ResolvedWorkload>,
    /// Shared Bot instance (created in on_workload_resolved)
    bot: Option<Arc<Bot>>,
    /// Cancellation token for the polling loop
    poll_cancel_token: Option<tokio_util::sync::CancellationToken>,
}

// ---------------------------------------------------------------------------
// Plugin struct
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
// WIT types::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::telegram::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::Host — empty (resource lives in HostTelegramBot)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::telegram::sender::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::HostTelegramBot — resource constructor + methods
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::telegram::sender::HostTelegramBot for ActiveCtx<'a> {
    async fn new(
        &mut self,
        _config: Option<TelegramConfig>,
    ) -> wasmtime::Result<Resource<TelegramBotHandle>> {
        let Some(plugin) = self.get_plugin::<Telegram>(PLUGIN_ID) else {
            return Err(wasmtime::Error::msg("telegram plugin not available"));
        };

        let component_id: Arc<str> = self.component_id.clone();
        let lock = plugin.tracker.read().await;
        let Some(data) = lock.get_component_data(&component_id) else {
            return Err(wasmtime::Error::msg("component not tracked"));
        };

        let Some(bot) = data.bot.clone() else {
            return Err(wasmtime::Error::msg(
                "telegram bot not started — polling should be running from on_workload_resolved",
            ));
        };

        drop(lock);

        let handle = TelegramBotHandle { bot };

        let resource = self.table.push(handle)?;
        Ok(resource)
    }

    #[instrument(skip_all)]
    async fn send_text(
        &mut self,
        bot: Resource<TelegramBotHandle>,
        chat_id: String,
        text: String,
    ) -> wasmtime::Result<Result<(), TelegramError>> {
        let handle = self.table.get(&bot)?;
        let chat_id_val = chat_id
            .parse::<i64>()
            .map_err(|_| wasmtime::Error::msg(format!("invalid chat_id: {chat_id}")))?;
        let chat_id_val = teloxide::types::ChatId(chat_id_val);

        match handle.bot.send_message(chat_id_val, &text).await {
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

    #[instrument(skip_all)]
    async fn send_media(
        &mut self,
        bot: Resource<TelegramBotHandle>,
        chat_id: String,
        file_path: String,
        caption: Option<String>,
    ) -> wasmtime::Result<Result<(), TelegramError>> {
        let handle = self.table.get(&bot)?;
        let chat_id_val = chat_id
            .parse::<i64>()
            .map_err(|_| wasmtime::Error::msg(format!("invalid chat_id: {chat_id}")))?;
        let chat_id_val = teloxide::types::ChatId(chat_id_val);

        let path = std::path::Path::new(&file_path);
        if !path.exists() {
            return Ok(Err(TelegramError::SendFailed(format!(
                "file not found: {file_path}"
            ))));
        }

        let document = teloxide::types::InputFile::file(path);
        let mut builder = handle.bot.send_document(chat_id_val, document);
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

    async fn stop(
        &mut self,
        _bot: Resource<TelegramBotHandle>,
    ) -> wasmtime::Result<Result<(), TelegramError>> {
        debug!("Telegram bot resource stop() — polling loop unaffected");
        Ok(Ok(()))
    }

    async fn drop(&mut self, rep: Resource<TelegramBotHandle>) -> wasmtime::Result<()> {
        let _ = self.table.delete(rep);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Background bot spawning
// ---------------------------------------------------------------------------

fn spawn_telegram_bot(
    workload: ResolvedWorkload,
    component_id: String,
    bot: Arc<Bot>,
    cancel_token: tokio_util::sync::CancellationToken,
) {
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
                                    &bot,
                                    &message,
                                ).await;
                            }
                        }
                        Some(Err(e)) => {
                            warn!(component_id = %cid_for_log, error = %e, "Telegram polling error");
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
}

async fn handle_telegram_message(
    workload: &ResolvedWorkload,
    component_id: &str,
    bot: &Arc<Bot>,
    message: &teloxide::types::Message,
) {
    // Record metrics via global meter
    {
        let meter = opentelemetry::global::meter("telegram");
        let counter = meter
            .u64_counter("telegram_messages_total")
            .with_description("Total number of Telegram messages processed")
            .build();
        counter.add(1, &[opentelemetry::KeyValue::new("direction", "inbound")]);
    }

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
    let bot_clone = (*bot).clone();

    tokio::spawn(async move {
        let mut store = match workload.new_store(&cid).await {
            Ok(s) => s,
            Err(e) => {
                warn!(component_id = %cid, error = %e, "Failed to create store for Telegram callback");
                return;
            }
        };

        // Temporary bot handle for the callback — guest can call methods on it
        let temp_handle = TelegramBotHandle { bot: bot_clone };
        let bot_resource = match store.data_mut().table.push(temp_handle) {
            Ok(r) => r,
            Err(e) => {
                warn!(component_id = %cid, error = %e, "Failed to push bot resource for callback");
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
            .call_on_message(&mut store, bot_resource, &tg_msg)
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
            imports: HashSet::from([WitInterface::from("custom:telegram/handler@0.1.0")]),
            exports: HashSet::from([WitInterface::from("custom:telegram/sender@0.1.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut wash_runtime::engine::workload::WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let Some(interface) = find_interface(&interfaces, "custom", "telegram") else {
            return Ok(());
        };

        let interface_config = interface.config.clone();

        bindings::custom::telegram::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::telegram::sender::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

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
                interface_config,
                workload: None,
                bot: None,
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

            if data.bot.is_none() {
                let bot_token = match resolve_field(None, &data.interface_config, "bot_token") {
                    Ok(token) => token,
                    Err(e) => {
                        warn!(
                            component_id,
                            error = %e,
                            "telegram: no bot_token configured, skipping polling start"
                        );
                        return Ok(());
                    }
                };

                let cancel_token = tokio_util::sync::CancellationToken::new();
                let bot = Arc::new(Bot::new(&bot_token));

                spawn_telegram_bot(
                    workload.clone(),
                    component_id.to_string(),
                    bot.clone(),
                    cancel_token.clone(),
                );

                debug!(
                    component_id,
                    "Telegram polling started from on_workload_resolved"
                );
                data.bot = Some(bot);
                data.poll_cancel_token = Some(cancel_token);
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
                        debug!(workload_id, "Telegram polling cancelled on unbind");
                    }
                },
            )
            .await;
        debug!(workload_id = %workload_id, "Telegram plugin unbound");
        Ok(())
    }
}
