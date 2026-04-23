# llm-gateway-messaging-proxy 设计

## 背景

当前架构中 `llm-gateway-provider` 是 host plugin，只能运行在有 genai 配置的 wasmCloud host 上。其他 host 上的 wasm component 无法直接调用 LLM。

`llm-gateway-messaging` 已实现通过 NATS 消息总线暴露 LLM 调用能力。现在需要一个反向代理组件：导出 `custom:llm-gateway/chat` 接口，内部通过 NATS 请求-响应转发到远端 messaging 组件。

## 目标

创建 `llm-gateway-messaging-proxy` wasm component，让没有本地 provider plugin 的 wasmCloud host 上的 component 也能通过 `custom:llm-gateway/chat` WIT 接口调用远端 LLM。

## 架构

```
Host B (无 provider)
┌─────────────────────────────────────────┐
│  业务 wasm component                     │
│  导入 custom:llm-gateway/chat            │
└───────────────┬─────────────────────────┘
                │ WIT call
                ▼
┌─────────────────────────────────────────┐
│  llm-gateway-messaging-proxy (wasm)      │
│  导出 custom:llm-gateway/chat            │
│  导入 wasmcloud:messaging/consumer       │
│                                          │
│  chat() → JSON serialize →              │
│  consumer::request("llm-gateway.chat")  │
│  → JSON deserialize → ChatResponse      │
└───────────────┬─────────────────────────┘
                │ NATS request-reply
                ▼
┌─────────────────────────────────────────┐
│  Host A (有 provider)                    │
│  llm-gateway-messaging (wasm)           │
│    → llm-gateway-provider (host plugin)  │
│    → genai → LLM API                    │
└─────────────────────────────────────────┘
```

## 制品

### llm-gateway-messaging-proxy（wasm component，新建）

**位置**: `crates/llm-gateway-messaging-proxy/`

**目标平台**: `wasm32-wasip2`

**WIT world**:
```wit
package wasmcloud:llm-gateway-messaging-proxy@0.1.0;

world llm-gateway-messaging-proxy {
  import wasi:logging/logging@0.1.0-draft;
  import wasmcloud:messaging/consumer@0.2.0;
  export custom:llm-gateway/chat@0.1.0;
}
```

**接口实现**:
- 导出 `custom:llm-gateway/chat` 的 `chat` 函数
- 不导出 `chat-streaming`（仅支持同步）

**NATS 交互**:
- 使用 `consumer::request(subject, body, timeout_ms)` 发送请求
- `subject` 通过 interface config 配置（默认 `"llm-gateway.chat"`）
- `timeout_ms` 通过 interface config 配置（默认 `30000`）
- 请求/响应 JSON 格式与 `llm-gateway-messaging` 完全一致

**请求 JSON**（发送到 NATS）:
```json
{
  "model": "gpt-4o-mini",
  "messages": [
    {"role": "User", "content": [{"Text": "hello"}]}
  ],
  "options": {
    "temperature": 0.7,
    "max_tokens": 1024
  }
}
```

**响应 JSON**（从 NATS 收到）:
```json
{
  "content": {"parts": [{"Text": "Hello!"}]},
  "model": "gpt-4o-mini",
  "stop_reason": {"Completed": "stop"},
  "usage": {"prompt_tokens": 10, "completion_tokens": 8, "total_tokens": 18}
}
```

**错误处理**:
- NATS request 超时 → `LlmError::ProviderError("request timed out")`
- NATS request 失败 → `LlmError::ProviderError(...)`
- 响应包含 `"error"` 字段 → 解析为对应 `LlmError` 变体
- 响应 JSON 解析失败 → `LlmError::Unexpected(...)`

**代码结构**:
```
crates/llm-gateway-messaging-proxy/
├── Cargo.toml
├── wit/
│   ├── world.wit
│   └── deps/
│       ├── custom-llm-gateway.wit
│       ├── wasi-logging-0.1.0-draft/
│       ├── wasmcloud-messaging-0.2.0/
│       └── ... (其他 wasi deps)
└── src/
    └── lib.rs
```

**依赖**: `wit-bindgen`, `serde`, `serde_json`

## 配置

通过 `.wash/config.yaml` 的 `host_interfaces` 注入（作为消费者端配置）:

```yaml
host_interfaces:
  - namespace: wasmcloud
    package: messaging
    interfaces:
      - consumer
    config:
      # messaging 组件订阅的 NATS subject
      llm_request_subject: "llm-gateway.chat"
      # 请求超时（毫秒）
      llm_request_timeout_ms: "30000"
```

注意：subject 和 timeout 配置读取方式需要与 wash-runtime 的 interface config 机制兼容。如果 interface config 无法传递给 wasm component，则需要通过 wasi:config 或其他机制。

## 非目标

- 不支持 streaming（chat-streaming 接口不导出）
- 不改变现有 llm-gateway-messaging 的 JSON 格式
- 不改变 WIT 接口定义
