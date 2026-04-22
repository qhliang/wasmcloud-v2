# LLM Gateway 重构设计

## 背景

当前 `custom_plugin_llm_gateway` 是一个单一的 host plugin，同时包含：
- LLM provider 调用逻辑（genai 多后端）
- OpenAI/Anthropic HTTP 兼容层（`http_handler.rs`、`anthropic_types.rs`、`openai_types.rs`）

HTTP 兼容层编译进宿主二进制但无法独立暴露 HTTP 端点，实际只能通过 wasm component 间接使用。

## 目标

将 LLM Gateway 拆分为三层，职责清晰：

1. **Provider host plugin** — 纯粹的 LLM 执行层，通过 WIT 接口导出
2. **HTTP component** — 对外暴露 OpenAI/Anthropic 兼容的 HTTP API
3. **Messaging component** — 通过 NATS 消息总线暴露 LLM 调用能力

## 架构

```
外部 HTTP 客户端              外部 NATS 客户端
     │                            │
     ▼                            ▼
┌──────────────────┐    ┌────────────────────────┐
│ llm-gateway-http │    │ llm-gateway-messaging   │
│ wasm component   │    │ wasm component          │
│                  │    │                         │
│ wasi:http/       │    │ export                  │
│   incoming-      │    │   wasmcloud:messaging/  │
│   handler        │    │   handler               │
│                  │    │                         │
│ /v1/chat/        │    │ 订阅 NATS subject       │
│   completions    │    │ JSON over NATS          │
│ /v1/messages     │    │ 请求→响应               │
│                  │    │                         │
│ 调用 chat()      │    │ 调用 chat()             │
└────────┬─────────┘    └──────────┬──────────────┘
         │ WIT import               │ WIT import
         ▼                          ▼
┌──────────────────────────────────────────────┐
│ llm-gateway-provider (host plugin)           │
│                                              │
│ 导出:                                        │
│   custom:llm-gateway/chat@0.1.0              │
│   custom:llm-gateway/chat-streaming@0.1.0    │
│                                              │
│ genai 多后端:                                │
│   OpenAI / Anthropic / Gemini / DeepSeek /   │
│   Ollama / Groq / openai-compat             │
│                                              │
│ per-workload 配置 + client 缓存              │
└──────────────────────────────────────────────┘
```

## 三个制品

### 1. llm-gateway-provider（host plugin）

**来源**: 从 `custom_plugin_llm_gateway` 重命名

**位置**: `crates/custom_plugin_llm_gateway_provider/`

**变化**:
- 删除 `http_handler.rs`、`anthropic_types.rs`、`openai_types.rs`
- crate 名改为 `custom_plugin_llm_gateway_provider`
- 插件 ID 改为 `"llm-gateway-provider"`
- WIT 定义**不变** — 仍然导出 `chat` + `chat-streaming`
- `lib.rs` 中 `HostPlugin` 实现、`LlmGateway` 结构、genai client 管理、配置解析、metrics 全部保留
- `ChatStreamHandle` 及流式接口实现保留

**不变项**:
- `wit/deps/custom-llm-gateway.wit` — 完整保留
- `wit/world.wit` — 完整保留
- `bindings` 生成代码 — 不变
- 所有测试 — 保留

### 2. llm-gateway-http（wasm component，新建）

**位置**: `crates/llm-gateway-http/`

**目标平台**: `wasm32-wasip2`

**类型**: wasm component，通过 `#[wstd::http_server]` 导出 `wasi:http/incoming-handler`

**WIT world**:
```wit
package wasmcloud:llm-gateway-http@0.1.0;

world llm-gateway-http {
  import wasi:logging/logging@0.1.0-draft;
  import custom:llm-gateway/chat@0.1.0;
  // wasi:http/incoming-handler 由 wstd 宏隐式导出
}
```

**HTTP 端点**:
- `POST /v1/chat/completions` — OpenAI Chat Completions API 格式
- `POST /v1/messages` — Anthropic Messages API 格式

两者都只支持同步响应（`stream` 参数忽略或返回错误），调用 provider `chat()` 接口。

**请求格式**（OpenAI）:
```json
{
  "model": "gpt-4o-mini",
  "messages": [{"role": "user", "content": "hello"}],
  "temperature": 0.7,
  "max_tokens": 1024
}
```

**响应格式**（OpenAI）:
```json
{
  "id": "chatcmpl-<uuid>",
  "object": "chat.completion",
  "model": "gpt-4o-mini",
  "choices": [{"index": 0, "message": {"role": "assistant", "content": "..."}, "finish_reason": "stop"}],
  "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
}
```

**请求格式**（Anthropic）:
```json
{
  "model": "claude-sonnet-4-20250514",
  "messages": [{"role": "user", "content": "hello"}],
  "max_tokens": 1024
}
```

**响应格式**（Anthropic）:
```json
{
  "id": "msg_<uuid>",
  "type": "message",
  "role": "assistant",
  "model": "claude-sonnet-4-20250514",
  "content": [{"type": "text", "text": "..."}],
  "stop_reason": "end_turn",
  "usage": {"input_tokens": 10, "output_tokens": 20}
}
```

**代码结构**:
```
crates/llm-gateway-http/
├── Cargo.toml
├── wit/
│   ├── world.wit
│   └── deps/
│       ├── wasi-logging-0.1.0-draft/
│       └── custom-llm-gateway.wit
└── src/
    ├── lib.rs          # #[wstd::http_server] main + 路由
    ├── completions.rs  # /v1/chat/completions 处理
    ├── responses.rs    # /v1/messages 处理
    └── types.rs        # OpenAI/Anthropic serde 类型定义
```

**依赖**: `wit-bindgen`, `wstd`, `serde`, `serde_json`, `uuid`

**关键实现**: `http_handler.rs` + `anthropic_types.rs` + `openai_types.rs` 中的逻辑迁移至此，从直接调用 genai client 改为通过 WIT binding 调用 `custom:llm-gateway/chat`。

### 3. llm-gateway-messaging（wasm component，新建）

**位置**: `crates/llm-gateway-messaging/`

**目标平台**: `wasm32-wasip2`

**类型**: wasm component，导出 `wasmcloud:messaging/handler`

**WIT world**:
```wit
package wasmcloud:llm-gateway-messaging@0.1.0;

world llm-gateway-messaging {
  import wasi:logging/logging@0.1.0-draft;
  import wasmcloud:messaging/consumer@0.2.0;
  import custom:llm-gateway/chat@0.1.0;
  export wasmcloud:messaging/handler@0.2.0;
}
```

**NATS 交互**:
- 订阅 subject（如 `llm-gateway.chat`）— 由 host 侧 messaging plugin 配置
- 收到消息后解析 JSON 请求，调用 provider `chat()`，将结果作为回复发布

**请求格式**（JSON over NATS）:
```json
{
  "model": "gpt-4o-mini",
  "messages": [{"role": "user", "content": "hello"}],
  "options": {
    "temperature": 0.7,
    "max_tokens": 1024
  }
}
```

**响应格式**（JSON over NATS）:
```json
{
  "content": "Hello! How can I help you?",
  "model": "gpt-4o-mini",
  "usage": {
    "prompt_tokens": 10,
    "completion_tokens": 8,
    "total_tokens": 18
  },
  "finish_reason": null
}
```

**错误响应**:
```json
{
  "error": {
    "type": "provider_error",
    "message": "LLM provider error: ..."
  }
}
```

**代码结构**:
```
crates/llm-gateway-messaging/
├── Cargo.toml
├── wit/
│   ├── world.wit
│   └── deps/
│       ├── wasi-logging-0.1.0-draft/
│       ├── wasmcloud-messaging-0.2.0/
│       └── custom-llm-gateway.wit
└── src/
    └── lib.rs          # messaging handler + JSON 解析 + chat 调用
```

**依赖**: `wit-bindgen`, `serde`, `serde_json`

## 配置方式

Provider config 通过 `.wash/config.yaml` 的 `host_interfaces` 注入，与现有模式相同：

```yaml
host_interfaces:
  - namespace: custom
    package: llm-gateway-provider
    interfaces:
      - chat
      - chat-streaming
    config:
      provider: openai-compat
      base_url: https://openrouter.ai/api/v1/
      model_name: z-ai/glm-4.5-air:free
      api_key: sk-or-v1-xxx
      temperature: "0.7"
      max_tokens: "4096"
```

## 现有代码迁移

### 从 provider plugin 迁移到 HTTP component

| 源文件（provider） | 目标（HTTP component） | 说明 |
|---------------------|----------------------|------|
| `http_handler.rs` | `completions.rs` + `responses.rs` | 拆分为独立模块 |
| `openai_types.rs` | `types.rs`（部分） | OpenAI serde 类型 |
| `anthropic_types.rs` | `types.rs`（部分） | Anthropic serde 类型 |

关键改动：genai client 直接调用改为 WIT binding 调用 `custom:llm-gateway/chat`。

### 不变项

- Provider plugin 的 WIT 定义（`chat` + `chat-streaming`）
- Provider plugin 的 genai 集成、client 缓存、配置解析
- Provider plugin 的所有单元测试
- 现有 `http-api-distributed` 示例中的 LLM 调用方式不变

## Workspace Cargo.toml 变更

- `custom_plugin_llm_gateway` 重命名为 `custom_plugin_llm_gateway_provider`
- 新增 `llm-gateway-http`（仅 wasm 编译目标，不参与 `cargo build --workspace`）
- 新增 `llm-gateway-messaging`（仅 wasm 编译目标）

## 非目标

- 不提供 HTTP SSE 流式输出（后续可扩展）
- 不提供 messaging 流式输出
- 不改变现有 WIT 接口定义
- 不影响 `http-api-distributed` 示例
