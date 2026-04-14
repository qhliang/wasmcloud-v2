# Custom Plugin Configuration Optimization Design

## Problem

All custom host plugins currently read configuration exclusively from `interface.config` (a `HashMap<String, String>` set in wasmcloud config). This means configuration is static — set at deploy time and not changeable by the Wasm component at runtime.

## Goal

Support two configuration sources with priority ordering:

1. **Wasm dynamic config** — Wasm component reads config from `wasi:config/store`, then passes it via WIT resource constructor
2. **Plugin static config** — Existing `interface.config` from wasmcloud config

**Priority**: Wasm config > Plugin static config (automatic fallback)

## Design

### WIT Resource Pattern

Each plugin that needs configuration introduces a WIT `resource` type with:
- A `config` record containing the plugin's configuration fields
- A resource type whose constructor accepts `option<config>`
- All operations become methods on the resource instance

Example (Telegram):

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
    }
}

interface handler {
    use types.{telegram-message};
    on-message: func(msg: telegram-message) -> result<_, string>;
}
```

### Host-side Fallback Logic

In the resource constructor implementation:

1. If Wasm passes `some(config)` — use those values directly
2. If Wasm passes `none` — fall back to `interface.config` (stored in `ComponentData` during `on_workload_item_bind`)
3. If neither has the required keys — return error

The interface config is still loaded during `on_workload_item_bind` and stored in `ComponentData` as the fallback source.

### Wasm-side Usage

```rust
// Option A: Pass config explicitly (e.g., from wasi:config)
let token = wasi::config::store::get("telegram_bot_token")?;
let bot = TelegramBot::new(Some(TelegramConfig { bot_token: token }))?;

// Option B: Let plugin use its static config
let bot = TelegramBot::new(None)?;

// Use the bot instance
bot.send_text("123456789", "Hello!")?;
```

### Per-plugin Changes

| Plugin | Config Record | Resource | Notes |
|--------|--------------|----------|-------|
| telegram | `{bot-token}` | `telegram-bot` | New resource |
| wechat | `{token}` | `wechat-client` | sender + login become methods |
| feishu | `{app-id, app-secret}` | `feishu-client` | Multiple interfaces share one resource |
| dingtalk | `{client-id, client-secret}` | `dingtalk-client` | Same pattern |
| cf-d1 | `{account-id, api-token, database-id}` | `d1-client` | New resource |
| llm-gateway | `{api-key, base-url?}` | Keep existing resource | Add config param to constructor |
| codex | `{api-token, model, base-url?, codex-binary-path?, project-dir?}` | Keep existing resource | Add config param |
| mail | `{smtp-host, smtp-port?, username, password, imap-host?}` | `mail-client` | New resource |
| crontab | N/A | No change | Schedule config is declarative |
| blobstore | N/A | No change | Uses standard wasi:blobstore |
| cf-kv | N/A | No change | Uses standard wasi:keyvalue |

### Implementation Strategy

Since this affects 9 plugins, implement incrementally:

1. **Phase 1**: Create a shared config resolution helper in `wash-runtime`
2. **Phase 2**: Convert one plugin (telegram) as the reference implementation
3. **Phase 3**: Roll out to remaining plugins

The shared helper provides a standard `resolve_config` function:

```rust
/// Resolve a config field: Wasm value takes priority, then fallback to interface config
fn resolve_field(
    wasm_value: Option<String>,
    interface_config: &HashMap<String, String>,
    key: &str,
) -> Result<String, ConfigError> {
    if let Some(val) = wasm_value {
        return Ok(val);
    }
    interface_config
        .get(key)
        .cloned()
        .ok_or(ConfigError::Missing(key))
}
```

### Backward Compatibility

- Wasm components that pass `none` to the constructor get the same behavior as today
- Existing interface config continues to work unchanged
- No breaking changes to wasmcloud config files

## Scope

This design covers only the configuration resolution mechanism. The per-plugin WIT redesign is mechanical but must be done for each plugin individually, following the same pattern.
