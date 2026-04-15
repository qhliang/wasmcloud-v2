# Plugin Config Optimization — Full Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Convert all custom host plugins to WIT resource pattern with dynamic config support. Config resolved at runtime with priority: Wasm dynamic config > static `interface.config` fallback.

**Architecture:** Each plugin's client/connection becomes a WIT resource instance. The resource constructor resolves config, creates clients, starts background tasks. `on_workload_item_bind`/`on_workload_resolved` simplified to registration only. Resource methods replace free functions. `stop()` method + unbind cleanup for lifecycle.

**Tech Stack:** Rust, Wasmtime Component Model, WIT, teloxide, weixin-agent, open-lark, etc.

**Reference Implementation:** Task 2 (Telegram) is the canonical example. All subsequent plugin tasks follow the same pattern — read Task 2 first, then apply the differences described for each plugin.

---

## Plugin Overview

| # | Plugin | Config Fields | Has Handler? | Has Existing Resource? | Complexity |
|---|--------|--------------|-------------|----------------------|------------|
| 2 | telegram | `bot-token` | yes | no | Medium |
| 3 | wechat | `token` | yes | no | Medium |
| 4 | dingtalk | `client-id, client-secret` | yes | no | Medium |
| 5 | feishu | `app-id, app-secret` | yes | no | High (many interfaces) |
| 6 | cf-d1 | `account-id, api-token, database-id` | no | no | Low |
| 7 | mail | `smtp-host, smtp-port?, username, password, default-from, imap-host?` | no | no | Low |
| 8 | llm-gateway | `provider, model-name, api-key, base-url?, temperature?, ...` | no | yes (`chat-stream`) | Medium (add config to existing) |
| 9 | codex | `api-token, model, base-url?, codex-binary-path?, project-dir?` | no | yes (`exec-stream`) | Medium (add config to existing) |
| — | crontab | N/A | — | — | No change |

---

## File Structure

| Action | File |
|--------|------|
| Create | `crates/wash-runtime/src/plugin/config.rs` |
| Modify | `crates/wash-runtime/src/plugin/mod.rs` |
| Modify | `crates/custom_plugin_telegram/wit/deps/custom-telegram.wit` |
| Modify | `crates/custom_plugin_telegram/src/lib.rs` |
| Modify | `crates/custom_plugin_wechat/wit/deps/wechat.wit` |
| Modify | `crates/custom_plugin_wechat/src/lib.rs` |
| Modify | `crates/custom_plugin_dingtalk_stream/wit/deps/dingtalk-stream.wit` |
| Modify | `crates/custom_plugin_dingtalk_stream/src/lib.rs` |
| Modify | `crates/custom_plugin_feishu/wit/deps/feishu.wit` |
| Modify | `crates/custom_plugin_feishu/src/lib.rs` |
| Modify | `crates/custom_plugin_cf_d1/wit/deps/custom-cf-d1.wit` |
| Modify | `crates/custom_plugin_cf_d1/src/lib.rs` |
| Modify | `crates/custom_plugin_mail/wit/deps/custom-mail.wit` |
| Modify | `crates/custom_plugin_mail/src/lib.rs` |
| Modify | `crates/custom_plugin_llm_gateway/wit/deps/custom-llm-gateway.wit` |
| Modify | `crates/custom_plugin_llm_gateway/src/lib.rs` |
| Modify | `crates/custom_plugin_codex/wit/deps/custom-codex.wit` |
| Modify | `crates/custom_plugin_codex/src/lib.rs` |
| Modify | `examples/http-api-distributed/http-api/src/telegram.rs` |

---

### Task 1: Create Shared Config Resolution Helper

**Files:**
- Create: `crates/wash-runtime/src/plugin/config.rs`
- Modify: `crates/wash-runtime/src/plugin/mod.rs`

- [ ] **Step 1: Create `config.rs`**

```rust
// crates/wash-runtime/src/plugin/config.rs
//! Shared config resolution helpers for host plugins.
//!
//! Provides a standard pattern for resolving config fields with priority:
//! Wasm dynamic value > static interface config fallback.

use std::collections::HashMap;

/// Resolve a required config field.
///
/// Wasm dynamic value takes priority, then falls back to interface static config.
/// Returns an error if neither source has the key.
pub fn resolve_field(
    wasm_value: Option<String>,
    interface_config: &HashMap<String, String>,
    key: &str,
) -> anyhow::Result<String> {
    if let Some(val) = wasm_value {
        return Ok(val);
    }
    interface_config
        .get(key)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing required config field: {key}"))
}

/// Resolve an optional config field.
///
/// Wasm dynamic value takes priority, then falls back to interface static config.
/// Returns `None` if neither source has the key.
pub fn resolve_optional_field(
    wasm_value: Option<String>,
    interface_config: &HashMap<String, String>,
    key: &str,
) -> Option<String> {
    if wasm_value.is_some() {
        return wasm_value;
    }
    interface_config.get(key).cloned()
}
```

- [ ] **Step 2: Export config module from `mod.rs`**

Add to `crates/wash-runtime/src/plugin/mod.rs` (after existing `pub mod` lines):

```rust
pub mod config;
```

- [ ] **Step 3: Build to verify**

Run: `cargo build -p wash-runtime`
Expected: Build succeeds with no errors

- [ ] **Step 4: Commit**

```bash
git add crates/wash-runtime/src/plugin/config.rs crates/wash-runtime/src/plugin/mod.rs
git commit -m "feat: add shared config resolution helper for plugins"
```

---

### Task 2: Telegram Plugin (Reference Implementation)

**Read this task first** — all subsequent tasks follow the same pattern.

**Files:**
- Modify: `crates/custom_plugin_telegram/wit/deps/custom-telegram.wit`
- Modify: `crates/custom_plugin_telegram/src/lib.rs`
- Modify: `examples/http-api-distributed/http-api/src/telegram.rs`

- [ ] **Step 1: Update WIT — add config record, convert to resource, update handler**

Replace entire content of `crates/custom_plugin_telegram/wit/deps/custom-telegram.wit`:

```wit
package custom:telegram@0.1.0;

interface types {
    variant telegram-error {
        internal(string),
        send-failed(string),
        not-ready(string),
    }

    record telegram-config {
        bot-token: string,
    }

    record telegram-message {
        message-id: string,
        chat-id: string,
        sender-id: string,
        sender-username: option<string>,
        text-content: option<string>,
        timestamp: s64,
    }
}

interface sender {
    use types.{telegram-error, telegram-config};

    resource telegram-bot {
        constructor(config: option<telegram-config>);
        send-text: func(chat-id: string, text: string) -> result<_, telegram-error>;
        send-media: func(chat-id: string, file-path: string, caption: option<string>) -> result<_, telegram-error>;
        stop: func() -> result<_, telegram-error>;
    }
}

interface handler {
    use types.{telegram-message};
    use sender.{telegram-bot};

    /// Called by the host when a Telegram message arrives.
    /// Passes the bot instance that received the message.
    on-message: func(bot: &telegram-bot, msg: telegram-message) -> result<_, string>;
}
```

- [ ] **Step 2: Rewrite `lib.rs` — resource-based implementation**

Replace entire content of `crates/custom_plugin_telegram/src/lib.rs`. Key structural changes from the original:

1. **`TelegramBotHandle`** replaces direct `Arc<Bot>` — holds bot + cancel_token, stored in ResourceTable
2. **`ComponentData`** simplified — only `interface_config: HashMap<String, String>` + `workload: Option<ResolvedWorkload>`
3. **`sender::Host`** — only contains resource constructor (`telegram_bot_constructor`)
4. **`sender::HostTelegramBot`** — contains `send_text`, `send_media`, `stop`, `drop`
5. **`on_workload_item_bind`** — stores `interface.config` as fallback, adds linker bindings, no config validation
6. **`on_workload_resolved`** — only stores workload reference, no bot creation
7. **Constructor** — resolves config via `resolve_field(wasm_value, &interface_config, "bot_token")`, creates bot, calls `spawn_telegram_bot`, pushes handle to `self.table`
8. **`handle_telegram_message`** — creates temporary `TelegramBotHandle` in callback store's resource table, passes `Resource<_>` to `call_on_message`

```rust
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
use wash_runtime::plugin::{HostPlugin, WorkloadTracker};
use wash_runtime::wit::{WitInterface, WitWorld};
use wasmtime::component::Resource;

mod bindings {
    wasmtime::component::bindgen!({
        world: "telegram",
        imports: { default: async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

use bindings::custom::telegram::types::{TelegramConfig, TelegramError, TelegramMessage};

const PLUGIN_ID: &str = "telegram";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Host-side state for a telegram-bot resource instance.
pub struct TelegramBotHandle {
    bot: Arc<Bot>,
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
// WIT sender::Host — resource constructor
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::telegram::sender::Host for ActiveCtx<'a> {
    async fn telegram_bot_constructor(
        &mut self,
        config: Option<TelegramConfig>,
    ) -> wasmtime::Result<Result<Resource<TelegramBotHandle>, TelegramError>> {
        let Some(plugin) = self.get_plugin::<Telegram>(PLUGIN_ID) else {
            return Ok(Err(TelegramError::Internal(
                "telegram plugin not available".to_string(),
            )));
        };

        let component_id: Arc<str> = self.component_id.clone();
        let lock = plugin.tracker.read().await;
        let Some(data) = lock.get_component_data(&component_id) else {
            return Ok(Err(TelegramError::Internal(
                "component not tracked".to_string(),
            )));
        };

        let bot_token = match resolve_field(
            config.as_ref().map(|c| c.bot_token.clone()),
            &data.interface_config,
            "bot_token",
        ) {
            Ok(token) => token,
            Err(_) => {
                return Ok(Err(TelegramError::NotReady(
                    "missing bot_token: provide via constructor or interface config".to_string(),
                )));
            }
        };

        let Some(workload) = &data.workload else {
            return Ok(Err(TelegramError::NotReady(
                "workload not resolved yet".to_string(),
            )));
        };

        drop(lock);

        let cancel_token = tokio_util::sync::CancellationToken::new();
        let bot = Arc::new(Bot::new(&bot_token));

        spawn_telegram_bot(
            workload.clone(),
            component_id.to_string(),
            bot.clone(),
            cancel_token.clone(),
        );

        let handle = TelegramBotHandle {
            bot,
            cancel_token,
        };

        let resource = self.table.push(handle)?;
        Ok(Ok(resource))
    }
}

// ---------------------------------------------------------------------------
// WIT sender::HostTelegramBot — resource methods
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::telegram::sender::HostTelegramBot for ActiveCtx<'a> {
    #[instrument(skip_all)]
    async fn send_text(
        &mut self,
        bot: Resource<TelegramBotHandle>,
        chat_id: String,
        text: String,
    ) -> wasmtime::Result<Result<(), TelegramError>> {
        let handle = self.table.get(&bot)?;
        let chat_id_val = chat_id.parse::<i64>().map_err(|_| {
            wasmtime::Error::msg(format!("invalid chat_id: {chat_id}"))
        })?;
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
        let chat_id_val = chat_id.parse::<i64>().map_err(|_| {
            wasmtime::Error::msg(format!("invalid chat_id: {chat_id}"))
        })?;
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
        bot: Resource<TelegramBotHandle>,
    ) -> wasmtime::Result<Result<(), TelegramError>> {
        let handle = self.table.get(&bot)?;
        handle.cancel_token.cancel();
        debug!("Telegram bot stopped via stop()");
        Ok(Ok(()))
    }

    async fn drop(
        &mut self,
        rep: Resource<TelegramBotHandle>,
    ) -> wasmtime::Result<()> {
        if let Ok(handle) = self.table.delete(rep) {
            handle.cancel_token.cancel();
        }
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
    let text_content = message.text().map(|s| s.to_string());
    let sender_username = message.from.as_ref().and_then(|u| u.username.clone());
    let chat_id = message.chat.id.0.to_string();
    let sender_id = message.from.as_ref().map(|u| u.id.0.to_string()).unwrap_or_default();
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
    let bot_clone = (**bot).clone();

    tokio::spawn(async move {
        let mut store = match workload.new_store(&cid).await {
            Ok(s) => s,
            Err(e) => {
                warn!(component_id = %cid, error = %e, "Failed to create store for Telegram callback");
                return;
            }
        };

        // Temporary bot handle for the callback — guest can call methods on it
        let temp_handle = TelegramBotHandle {
            bot: bot_clone,
            cancel_token: tokio_util::sync::CancellationToken::new(),
        };
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
        let Some(interface) = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "telegram")
        else {
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

        debug!(component_id = component_handle.id(), "Telegram plugin bound to component");

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
        debug!(workload_id = %workload_id, "Telegram plugin unbound");
        Ok(())
    }
}
```

**Note:** Exact trait method names (`telegram_bot_constructor`, `HostTelegramBot`) depend on the generated bindings. Run `cargo build -p custom_plugin_telegram 2>&1 | head -80` to see the actual trait signatures and adjust accordingly.

- [ ] **Step 3: Build to verify**

Run: `cargo build -p custom_plugin_telegram`
Expected: Build succeeds. Fix any trait method name mismatches from compiler errors.

- [ ] **Step 4: Update example guest component**

In `examples/http-api-distributed/http-api/src/telegram.rs`, change free functions to instance methods:

```rust
// Before:
use crate::bindings::custom::telegram::sender;
match sender::send_text(&body.chat_id, &body.text) { ... }

// After:
use crate::bindings::custom::telegram::sender::TelegramBot;
let bot = TelegramBot::new(None)?;
match bot.send_text(&body.chat_id, &body.text) { ... }
```

Same pattern for `send_media`. Guest bindings must be regenerated first via `wash build --skip-fetch`.

- [ ] **Step 5: Commit**

```bash
git add crates/custom_plugin_telegram/ examples/http-api-distributed/http-api/src/telegram.rs
git commit -m "feat(telegram): convert to resource-based plugin with dynamic config"
```

---

### Task 3: WeChat Plugin

**Pattern:** Same as Telegram. Long-connection plugin with handler.

**Files:**
- Modify: `crates/custom_plugin_wechat/wit/deps/wechat.wit`
- Modify: `crates/custom_plugin_wechat/src/lib.rs`

**Current config:** `{token: String}`
**Current ComponentData:** `{cancel_token, workload, client: Option<Arc<WeixinClient>>, config: PluginConfig}`
**Current interfaces:** sender (send-text, send-media), login (qr-start, qr-poll-status), handler (on-message)

- [ ] **Step 1: Update WIT**

```wit
package custom:wechat@0.1.0;

interface types {
    variant wechat-error {
        internal(string),
        send-failed(string),
        not-ready(string),
    }

    record wechat-config {
        token: string,
    }

    record wechat-message {
        message-id: string,
        sender: string,
        receiver: string,
        message-type: string,
        text-content: option<string>,
        timestamp: s64,
        raw-json: string,
    }
}

interface sender {
    use types.{wechat-error, wechat-config};

    resource wechat-client {
        constructor(config: option<wechat-config>);
        send-text: func(to: string, text: string) -> result<_, wechat-error>;
        send-media: func(to: string, file-path: string) -> result<_, wechat-error>;
        qr-start: func() -> result<string, wechat-error>;
        qr-poll-status: func(session-json: string) -> result<string, wechat-error>;
        stop: func() -> result<_, wechat-error>;
    }
}

interface handler {
    use types.{wechat-message};
    use sender.{wechat-client};

    on-message: func(client: &wechat-client, msg: wechat-message) -> result<_, string>;
}

world wechat {
    import sender;
    export handler;
}
```

Changes: `sender` + `login` interfaces merged into `wechat-client` resource. Added `wechat-config` record. Handler takes `&wechat-client`.

- [ ] **Step 2: Rewrite `lib.rs`**

Follow the Telegram pattern with these differences:
- **Handle struct:** `WechatClientHandle { client: Arc<WeixinClient>, cancel_token: CancellationToken }`
- **Config fields:** `resolve_field(..., "token")` — single field
- **Resource methods:** `send_text`, `send_media`, `qr_start`, `qr_poll_status`, `stop`, `drop`
- **Background task:** `spawn_weixin_client` called from constructor instead of `on_workload_resolved`
- **Handler callback:** pass `&wechat-client` resource reference via temporary handle

- [ ] **Step 3: Build and commit**

```bash
cargo build -p custom_plugin_wechat
git add crates/custom_plugin_wechat/
git commit -m "feat(wechat): convert to resource-based plugin with dynamic config"
```

---

### Task 4: DingTalk Stream Plugin

**Pattern:** Same as Telegram. Long-connection plugin with handler.

**Files:**
- Modify: `crates/custom_plugin_dingtalk_stream/wit/deps/dingtalk-stream.wit`
- Modify: `crates/custom_plugin_dingtalk_stream/src/lib.rs`

**Current config:** `{client-id, client-secret}`
**Current ComponentData:** `{cancel_token, workload, replier: Option<ChatbotReplier>, config: PluginConfig}`

- [ ] **Step 1: Update WIT**

```wit
package custom:dingtalk-stream@0.1.0;

interface types {
    variant dingtalk-error {
        internal(string),
        auth-failed(string),
        send-failed(string),
    }

    record dingtalk-config {
        client-id: string,
        client-secret: string,
    }

    record chatbot-message {
        conversation-type: string,
        conversation-id: string,
        sender-id: string,
        sender-nick: string,
        message-id: string,
        text-content: option<string>,
        is-admin: bool,
        is-at: bool,
        raw-json: string,
    }
}

interface sender {
    use types.{dingtalk-error, dingtalk-config};

    resource dingtalk-client {
        constructor(config: option<dingtalk-config>);
        send-text: func(conversation-id: string, sender-id: string, content: string) -> result<_, dingtalk-error>;
        send-markdown: func(conversation-id: string, sender-id: string, title: string, content: string) -> result<_, dingtalk-error>;
        send-oto-text: func(user-id: string, content: string) -> result<_, dingtalk-error>;
        get-access-token: func() -> result<string, dingtalk-error>;
        stop: func() -> result<_, dingtalk-error>;
    }
}

interface handler {
    use types.{chatbot-message};
    use sender.{dingtalk-client};

    on-message: func(client: &dingtalk-client, msg: chatbot-message) -> result<_, string>;
}

world dingtalk-stream {
    import sender;
    export handler;
}
```

- [ ] **Step 2: Rewrite `lib.rs`**

Follow the Telegram pattern with these differences:
- **Handle struct:** `DingtalkClientHandle { client: Arc<DingTalkClient>, cancel_token: CancellationToken }` (adapt based on actual client types used for sending + stream connection)
- **Config fields:** `resolve_field(..., "client-id")` and `resolve_field(..., "client-secret")` — two required fields
- **Resource methods:** `send_text`, `send_markdown`, `send_oto_text`, `get_access_token`, `stop`, `drop`
- **Background task:** DingTalk stream connection started from constructor

- [ ] **Step 3: Build and commit**

```bash
cargo build -p custom_plugin_dingtalk_stream
git add crates/custom_plugin_dingtalk_stream/
git commit -m "feat(dingtalk): convert to resource-based plugin with dynamic config"
```

---

### Task 5: Feishu Plugin

**Pattern:** Same as Telegram but **high complexity** — 10+ interfaces all sharing one resource.

**Files:**
- Modify: `crates/custom_plugin_feishu/wit/deps/feishu.wit`
- Modify: `crates/custom_plugin_feishu/src/lib.rs`

**Current config:** `{app-id, app-secret}`
**Current ComponentData:** `{cancel_token, workload, client: Option<Arc<LarkClient>>, config: PluginConfig}`

**Key design decision:** All sender interfaces (sender, contact-sender, group-sender, ai-sender, calendar-sender, cardkit-sender, mail-sender, task-sender, bot-sender, docs-sender) become methods on a single `feishu-client` resource. This is a large resource but keeps the config resolution simple — one constructor, one config, one client.

- [ ] **Step 1: Update WIT**

```wit
package custom:feishu@0.1.0;

interface types {
    variant feishu-error {
        internal(string),
        auth-failed(string),
        send-failed(string),
    }

    record feishu-config {
        app-id: string,
        app-secret: string,
    }

    record im-message {
        message-id: string,
        chat-id: string,
        sender-id: string,
        sender-type: string,
        message-type: string,
        text-content: option<string>,
        create-time: string,
        raw-json: string,
    }
}

interface sender {
    use types.{feishu-error, feishu-config};

    resource feishu-client {
        constructor(config: option<feishu-config>);

        // IM messaging
        send-text: func(chat-id: string, content: string) -> result<_, feishu-error>;
        send-text-to-user: func(receive-id: string, receive-id-type: string, content: string) -> result<_, feishu-error>;
        reply-message: func(message-id: string, content: string) -> result<_, feishu-error>;
        get-access-token: func() -> result<string, feishu-error>;

        // Contact
        get-user: func(user-id: string, user-id-type: string) -> result<string, feishu-error>;
        batch-get-users: func(request-json: string) -> result<string, feishu-error>;
        search-users: func(request-json: string) -> result<string, feishu-error>;
        list-department-users: func(request-json: string) -> result<string, feishu-error>;
        get-department: func(department-id: string, department-id-type: string) -> result<string, feishu-error>;
        list-sub-departments: func(request-json: string) -> result<string, feishu-error>;

        // Group
        create-group: func(request-json: string) -> result<string, feishu-error>;
        get-group: func(chat-id: string) -> result<string, feishu-error>;
        update-group: func(chat-id: string, request-json: string) -> result<_, feishu-error>;
        add-group-members: func(chat-id: string, request-json: string) -> result<string, feishu-error>;
        remove-group-members: func(chat-id: string, request-json: string) -> result<_, feishu-error>;
        list-group-members: func(chat-id: string, request-json: string) -> result<string, feishu-error>;

        // AI
        recognize-text: func(request-json: string) -> result<string, feishu-error>;
        translate: func(request-json: string) -> result<string, feishu-error>;

        // Calendar
        list-calendars: func(request-json: string) -> result<string, feishu-error>;
        create-calendar-event: func(calendar-id: string, request-json: string) -> result<string, feishu-error>;
        list-calendar-events: func(calendar-id: string, request-json: string) -> result<string, feishu-error>;
        get-calendar-event: func(calendar-id: string, event-id: string) -> result<string, feishu-error>;
        delete-calendar-event: func(calendar-id: string, event-id: string) -> result<_, feishu-error>;

        // CardKit
        create-card: func(request-json: string) -> result<string, feishu-error>;
        update-card: func(card-id: string, request-json: string) -> result<_, feishu-error>;

        // Mail
        send-mail: func(user-mailbox-id: string, request-json: string) -> result<string, feishu-error>;
        list-mails: func(user-mailbox-id: string, request-json: string) -> result<string, feishu-error>;
        get-mail: func(user-mailbox-id: string, message-id: string) -> result<string, feishu-error>;

        // Task
        create-task: func(request-json: string) -> result<string, feishu-error>;
        get-task: func(task-guid: string) -> result<string, feishu-error>;
        update-task: func(task-guid: string, request-json: string) -> result<_, feishu-error>;
        delete-task: func(task-guid: string) -> result<_, feishu-error>;
        list-tasks: func(request-json: string) -> result<string, feishu-error>;
        create-tasklist: func(request-json: string) -> result<string, feishu-error>;
        list-tasklists: func(request-json: string) -> result<string, feishu-error>;

        // Bot
        get-bot-info: func() -> result<string, feishu-error>;

        // Docs
        create-document: func(request-json: string) -> result<string, feishu-error>;
        get-document: func(document-id: string) -> result<string, feishu-error>;
        create-spreadsheet: func(request-json: string) -> result<string, feishu-error>;
        get-spreadsheet: func(spreadsheet-token: string) -> result<string, feishu-error>;
        create-bitable: func(request-json: string) -> result<string, feishu-error>;
        list-bitable-tables: func(app-token: string) -> result<string, feishu-error>;
        list-bitable-records: func(app-token: string, table-id: string, request-json: string) -> result<string, feishu-error>;

        // Lifecycle
        stop: func() -> result<_, feishu-error>;
    }
}

interface handler {
    use types.{im-message};
    use sender.{feishu-client};

    on-message: func(client: &feishu-client, msg: im-message) -> result<_, string>;
}

world feishu {
    import sender;
    export handler;
}
```

All 10 sender interfaces collapsed into one `feishu-client` resource. This is a large resource (~35 methods) but keeps the config/auth simple.

- [ ] **Step 2: Rewrite `lib.rs`**

Follow the Telegram pattern with these differences:
- **Handle struct:** `FeishuClientHandle { client: Arc<LarkClient>, cancel_token: CancellationToken }`
- **Config fields:** `resolve_field(..., "app-id")` and `resolve_field(..., "app-secret")`
- **Resource methods:** All ~35 methods from the 10 original interfaces. Each method delegates to the LarkClient.
- **Host trait:** Only `sender::Host` (constructor) and `sender::HostFeishuClient` (all methods + drop)
- **Linker:** Only `types::add_to_linker` and `sender::add_to_linker` (single interface now)
- **World:** imports = `sender`, exports = `handler`

**Implementation tip:** Since all methods just delegate to `LarkClient` with JSON in/out, the implementations are mechanical. Copy the method bodies from the current separate `Host` impls, changing `self` access from plugin tracker lookup to `self.table.get(&resource)?`.

- [ ] **Step 3: Build and commit**

```bash
cargo build -p custom_plugin_feishu
git add crates/custom_plugin_feishu/
git commit -m "feat(feishu): convert to resource-based plugin with dynamic config"
```

---

### Task 6: Cloudflare D1 Plugin

**Pattern:** Simple API plugin. No handler, no background tasks. Client created lazily.

**Files:**
- Modify: `crates/custom_plugin_cf_d1/wit/deps/custom-cf-d1.wit`
- Modify: `crates/custom_plugin_cf_d1/src/lib.rs`

**Current config:** `{account-id, api-token, database-id}` (extracted inline in `on_workload_item_bind`)

- [ ] **Step 1: Update WIT**

```wit
package custom:cf-d1@0.1.0;

interface types {
    variant query-error {
        invalid-query(string),
        invalid-params(string),
        database-error(string),
        connection-error(string),
        unexpected(string),
    }

    record d1-config {
        account-id: string,
        api-token: string,
        database-id: string,
    }

    variant column-value {
        null,
        integer(s64),
        real(f64),
        text(string),
        blob(list<u8>),
    }

    record result-row {
        values: list<column-value>,
    }

    record column-meta {
        name: string,
        column-type: option<string>,
    }

    record query-result {
        columns: list<column-meta>,
        rows: list<result-row>,
        rows-affected: u64,
        last-insert-rowid: option<s64>,
    }
}

interface query {
    use types.{query-error, query-result, column-value, result-row, d1-config};

    resource d1-client {
        constructor(config: option<d1-config>);
        query: func(sql: string, params: list<column-value>) -> result<query-result, query-error>;
        query-batch: func(sql: string) -> result<list<query-result>, query-error>;
        query-one: func(sql: string, params: list<column-value>) -> result<option<result-row>, query-error>;
        stop: func() -> result<_, query-error>;
    }
}

world d1 {
    import query;
}
```

- [ ] **Step 2: Rewrite `lib.rs`**

Simpler than Telegram — no handler, no background tasks:
- **Handle struct:** `D1ClientHandle { config: CloudflareD1Config, http_client: reqwest::Client }`
- **Config fields:** `resolve_field(..., "account_id")`, `resolve_field(..., "api_token")`, `resolve_field(..., "database_id")`
- **Constructor:** Resolve config, create HTTP client, push handle
- **Resource methods:** `query`, `query_batch`, `query_one` — copy existing implementation logic, use config from handle instead of tracker lookup
- **No handler callback:** No background task, no `on_workload_resolved` changes needed
- **`ComponentData`:** Only `interface_config` + `workload` (workload may not even be needed)

- [ ] **Step 3: Build and commit**

```bash
cargo build -p custom_plugin_cf_d1
git add crates/custom_plugin_cf_d1/
git commit -m "feat(cf-d1): convert to resource-based plugin with dynamic config"
```

---

### Task 7: Mail Plugin

**Pattern:** Simple API plugin. No handler, no background tasks.

**Files:**
- Modify: `crates/custom_plugin_mail/wit/deps/custom-mail.wit`
- Modify: `crates/custom_plugin_mail/src/lib.rs`

**Current config:** `{smtp-host, smtp-port?, username, password, default-from, imap-host?}`

- [ ] **Step 1: Update WIT**

```wit
package custom:mail@0.1.0;

interface types {
    variant mail-error {
        config-error(string),
        send-failed(string),
        invalid-address(string),
        internal(string),
    }

    record mail-config {
        smtp-host: string,
        smtp-port: option<u16>,
        username: string,
        password: string,
        default-from: string,
        imap-host: option<string>,
    }
}

interface sender {
    use types.{mail-error, mail-config};

    resource mail-client {
        constructor(config: option<mail-config>);
        send-mail: func(to: string, subject: string, body-text: option<string>, body-html: option<string>, cc: option<string>, bcc: option<string>) -> result<_, mail-error>;
        list-mails: func(mailbox: option<string>, search-criteria: option<string>, limit: option<u32>) -> result<string, mail-error>;
        get-mail: func(message-id: string, mailbox: option<string>) -> result<string, mail-error>;
        stop: func() -> result<_, mail-error>;
    }
}

world mail {
    import sender;
}
```

- [ ] **Step 2: Rewrite `lib.rs`**

- **Handle struct:** `MailClientHandle { config: PluginConfig }` (SMTP/IMAP connections created per-operation)
- **Config fields:** `resolve_field(..., "smtp-host")`, `resolve_field(..., "username")`, `resolve_field(..., "password")`, `resolve_field(..., "default-from")`, `resolve_optional_field(..., "smtp-port")` (parse to u16), `resolve_optional_field(..., "imap-host")`
- **Constructor:** Resolve all fields, build `PluginConfig`, push handle
- **Resource methods:** `send_mail`, `list_mails`, `get_mail` — use config from handle

- [ ] **Step 3: Build and commit**

```bash
cargo build -p custom_plugin_mail
git add crates/custom_plugin_mail/
git commit -m "feat(mail): convert to resource-based plugin with dynamic config"
```

---

### Task 8: LLM Gateway Plugin

**Pattern:** Existing resource (`chat-stream`). Add config param to `chat`/`chat-streaming` functions.

**Files:**
- Modify: `crates/custom_plugin_llm_gateway/wit/deps/custom-llm-gateway.wit`
- Modify: `crates/custom_plugin_llm_gateway/src/lib.rs`

**Current config:** `{provider, model-name, api-key, base-url?, temperature?, top-p?, max-tokens?, system-prompts?}` — complex, extracted with validation in `extract_config`

**Current pattern:** Config stored per-workload in `configs: Arc<RwLock<HashMap<String, LlmGatewayConfig>>>`. `get_or_create_client` creates genai client lazily.

**Design:** Add `llm-config` record and make `chat`/`chat-streaming` accept `option<llm-config>`. When provided, use dynamic config; when `none`, fall back to stored interface config. Keep existing `chat-stream` resource as-is.

- [ ] **Step 1: Update WIT**

Add to `types` interface:

```wit
    record llm-config {
        api-key: string,
        base-url: option<string>,
    }
```

Update `chat` interface:

```wit
interface chat {
    use types.{llm-error, chat-message, chat-response, chat-options, llm-config};

    /// Execute a chat completion with optional dynamic config.
    chat: func(model: string, messages: list<chat-message>, options: option<chat-options>, config: option<llm-config>) -> result<chat-response, llm-error>;
}
```

Update `chat-streaming` interface similarly — add `config: option<llm-config>` parameter to `chat-streaming` function.

**Note:** Only `api-key` and `base-url` are in the WIT config record. Provider, model, temperature etc. are already passed as function parameters (`model`, `options`). This keeps the config minimal — only the auth/endpoint info that must come from config.

- [ ] **Step 2: Update `lib.rs`**

Changes are more surgical than other plugins:
- Import `resolve_field`, `resolve_optional_field`
- Update `chat` implementation: resolve `api_key` and `base_url` from dynamic config or fallback to interface config, then proceed as before
- Update `chat-streaming` implementation: same config resolution pattern
- **No new resource** — `chat-stream` resource stays as-is
- **No changes to `on_workload_item_bind`** — still stores interface config as before (needed as fallback)

- [ ] **Step 3: Build and commit**

```bash
cargo build -p custom_plugin_llm_gateway
git add crates/custom_plugin_llm_gateway/
git commit -m "feat(llm-gateway): add dynamic config support to chat functions"
```

---

### Task 9: Codex Plugin

**Pattern:** Existing resource (`exec-stream`). Add config param to `execute`/`resume`/`new-session`.

**Files:**
- Modify: `crates/custom_plugin_codex/wit/deps/custom-codex.wit`
- Modify: `crates/custom_plugin_codex/src/lib.rs`

**Current config:** `{api-token, model, base-url?, codex-binary-path?, project-dir?}` — extracted with validation

**Design:** Similar to LLM Gateway. Add `codex-config` record. Functions that need config get an `option<codex-config>` parameter.

- [ ] **Step 1: Update WIT**

Add to `types` interface:

```wit
    record codex-config {
        api-token: string,
        model: string,
        base-url: option<string>,
        codex-binary-path: option<string>,
        project-dir: option<string>,
    }
```

Update `executor` interface:

```wit
interface executor {
    use types.{codex-error, exec-stream-event, codex-config};

    resource exec-stream {
        next: func() -> result<tuple<list<exec-stream-event>, bool>, codex-error>;
    }

    execute: func(context-key: string, prompt: string, config: option<codex-config>) -> result<exec-stream, codex-error>;
}
```

Update `session` interface — add `config: option<codex-config>` to `new-session`, `resume`.

- [ ] **Step 2: Update `lib.rs`**

- Update `execute`, `resume`, `new-session` implementations to resolve config from dynamic param or fallback
- Config resolution: `resolve_field(..., "api_token")`, `resolve_field(..., "model")`, `resolve_optional_field(..., "base_url")`, etc.
- **No new resource** — `exec-stream` stays as-is
- **ComponentData** still stores sessions/state, but config comes from function parameter or fallback

- [ ] **Step 3: Build and commit**

```bash
cargo build -p custom_plugin_codex
git add crates/custom_plugin_codex/
git commit -m "feat(codex): add dynamic config support to executor functions"
```

---

### Task 10: Full Workspace Build and Verification

- [ ] **Step 1: Build the workspace**

Run: `cargo build --workspace`
Expected: Build succeeds with no errors

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace`
Expected: No new warnings or errors

- [ ] **Step 3: Run existing tests**

Run: `cargo test --workspace`
Expected: All tests pass

- [ ] **Step 4: Format check**

Run: `cargo +nightly fmt -- --check`
Expected: No formatting issues. If issues, run `cargo +nightly fmt` and commit.

- [ ] **Step 5: Final commit if any fixes needed**

```bash
git add -A
git commit -m "style: apply cargo fmt and clippy fixes across all plugins"
```

---

## Implementation Notes

### Generated Binding Names

The `wasmtime::component::bindgen!` macro generates trait names based on WIT:
- Interface `sender` → `bindings::custom::telegram::sender::Host` (constructor)
- Resource `telegram-bot` → `bindings::custom::telegram::sender::HostTelegramBot` (methods + drop)

**Always** run `cargo build -p <plugin>` after changing WIT to see actual trait signatures. The method names in this plan are best-effort — compiler errors will give the exact names.

### Resource Passing in Callbacks (handler plugins)

For plugins with handler interfaces (Telegram, WeChat, DingTalk, Feishu):
1. Background task receives message
2. Creates new store via `workload.new_store()`
3. Pushes temporary handle into `store.data_mut().table`
4. Calls guest's `on_message(resource, msg)`
5. Guest can call methods on the passed resource

The temporary handle's `cancel_token` is separate from the original — `stop()` on it is a no-op, which is correct for a callback context.

### Config Key Mapping

Plugin WIT config record fields map to `interface.config` keys:
- WIT `bot-token` → config key `bot_token` (kebab-case in WIT, snake_case in config)
- WIT `app-id` → config key `app-id` (keep as-is for existing configs)

The `resolve_field` function uses the config key as stored in `interface.config`.

### No-Change Plugins

- **Crontab**: Schedule config is declarative. No resource needed.
- **Blobstore**: Uses standard `wasi:blobstore`. No custom config.
- **CF-KV**: Uses standard `wasi:keyvalue`. No custom config.
