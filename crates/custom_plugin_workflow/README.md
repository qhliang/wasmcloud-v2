# Workflow Host Plugin

基于 [acts](https://docs.rs/acts/latest/acts/) workflow engine 的工作流执行插件，为 WASM 组件提供工作流定义、启动、查询能力，并通过回调推送生命周期事件。

## 核心流程

```
┌──────────────┐   manager::start()     ┌──────────────────┐
│  Guest (WASM) │ ────────────────────▶  │   Host Plugin     │
│              │                        │                   │
│ import manager│                        │  acts::Engine     │
│   .start()   │                        │  ├─ executor      │──▶ 部署/启动/查询
│   .list_     │                        │  │  model().deploy│
│   processes()│                        │  │  proc().start  │
│   .process_  │                        │  │  proc().list   │
│   status()   │                        │  │  proc().get    │
│   .complete_ │                        │  └─ act().complete│
│   task()     │                        │                   │
│              │                        │  Channel callbacks│
│ export handler│  ◀── on_start() ────  │  ├─ on_start      │
│   .on_start()│     ◀── on_message()   │  ├─ on_message    │
│   .on_message│     ◀── on_complete()  │  ├─ on_complete   │
│   .on_complet│     ◀── on_error()     │  └─ on_error      │
│   .on_error()│                        │                   │
└──────────────┘                        │  ┌─────────────┐  │
                                        │  │ mpsc channel│  │
                                        │  │ (consumer)  │  │
                                        │  └─────────────┘  │
                                        └──────────────────┘
```

1. **Engine 初始化** — 组件 bind 时创建 `acts::Engine` 实例并 `start()`
2. **Channel 注册** — 在 Engine 上注册四个 Channel 回调（`on_start`/`on_message`/`on_complete`/`on_error`），回调通过 mpsc channel 汇入 consumer task
3. **Deploy & Start** — Guest 调用 `manager::start(workflow-def, vars)`，Host 解析 YAML → deploy → 启动 process
4. **事件分发** — Consumer task 串行消费 mpsc 中的事件，逐条调用 WASM handler
5. **查询** — `list-processes()` 和 `process-status()` 直接查询 Engine 内部 store

## WIT 接口

```wit
package custom:workflow@0.1.0;

interface types {
    record var-pair {
        key: string,
        value: string,
    }

    record proc-info {
        pid: string,
        mid: string,
        state: string,
        start-time: s64,
        end-time: s64,
    }

    record workflow-event {
        pid: string,
        mid: string,
        event-type: string,
        state: string,
        name: string,
        inputs: list<var-pair>,
        outputs: list<var-pair>,
    }
}

interface manager {
    use types.{var-pair, proc-info};

    start: func(workflow-def: string, vars: list<var-pair>) -> result<string, string>;
    list-processes: func() -> result<list<proc-info>, string>;
    process-status: func(pid: string) -> result<proc-info, string>;
    complete-task: func(pid: string, nid: string, outputs: list<var-pair>) -> result<_, string>;
}

interface handler {
    use types.{workflow-event};

    on-start: func(event: workflow-event) -> result<_, string>;
    on-message: func(event: workflow-event) -> result<_, string>;
    on-complete: func(event: workflow-event) -> result<_, string>;
    on-error: func(pid: string, error: string) -> result<_, string>;
}
```

## Workflow YAML 格式

```yaml
id: my-workflow
name: demo
ver: "0.1.0"
steps:
  - id: step1
    name: First Step
    acts:
      - id: act1
        name: Print Message
        uses: acts.core.msg
        with:
          message: Hello from workflow!
```

- `id`、`ver` 为必填字段
- `acts.core.msg` 是 acts 内置的简单消息输出 action
- 通过 `with` 传递参数给 action

## 使用方法

### 1. 在 WASM 组件中声明依赖

```wit
// world.wit
world my-component {
    import custom:workflow/manager@0.1.0;
    export custom:workflow/handler@0.1.0;
}
```

### 2. 实现 Guest handler

```rust
mod bindings {
    wit_bindgen::generate!({ path: "../wit", world: "my-component", generate_all });
    use super::CustomHandler;
    export!(CustomHandler);
}

struct CustomHandler;

impl bindings::exports::custom::workflow::handler::Guest for CustomHandler {
    fn on_start(event: bindings::custom::workflow::types::WorkflowEvent) -> Result<(), String> {
        log::info!("WF START: pid={}, type={}, name={}", event.pid, event.event_type, event.name);
        Ok(())
    }

    fn on_message(event: bindings::custom::workflow::types::WorkflowEvent) -> Result<(), String> {
        log::info!("WF MSG: pid={}, state={}, name={}", event.pid, event.state, event.name);
        Ok(())
    }

    fn on_complete(event: bindings::custom::workflow::types::WorkflowEvent) -> Result<(), String> {
        log::info!("WF DONE: pid={}, type={}, name={}", event.pid, event.event_type, event.name);
        Ok(())
    }

    fn on_error(pid: String, error: String) -> Result<(), String> {
        log::error!("WF ERROR: pid={}, error={}", pid, error);
        Ok(())
    }
}
```

### 3. 启动和查询 workflow

```rust
use bindings::custom::workflow::manager;
use bindings::custom::workflow::types::VarPair;

let vars = vec![
    VarPair { key: "input".into(), value: "hello".into() },
];

// 启动
let pid = manager::start(yaml_str, &vars)?;

// 查询列表
let procs = manager::list_processes()?;
for p in &procs {
    log::info!("pid={}, mid={}, state={}", p.pid, p.mid, p.state);
}

// 查询单个状态
let info = manager::process_status(&pid)?;
log::info!("state={}", info.state);
```

## 架构设计

| 特性 | 实现 |
|------|------|
| Engine 生命周期 | 在 `on_workload_item_bind` 创建，每个 component 独立 Engine 实例 |
| 事件串行 | `mpsc::unbounded_channel` 收集四个 Channel 回调事件，consumer task 在 `on_workload_resolved` 时启动 |
| 并发安全 | 单一 consumer task 逐条分发，避免多 Channel 回调并发访问 store/instance |
| 回调持有 | `Arc<Channel>` 引用存储在 `ComponentData` 中，保证 Channel 回调不被 drop |
| 存储 | 使用 acts 默认 `MemoryStore`，进程重启后状态丢失 |
| 错误处理 | acts 错误包装为 `Ok(Err(...))` 返回给 guest，不触发 wasmtime trap |
| 清理 | `on_workload_unbind` 时取消 `CancellationToken`，关闭 event channel |

## 依赖

| Crate | 用途 |
|------|------|
| `acts` v0.20 | 工作流引擎核心（含 `rquickjs` 嵌入式 JS 运行时） |
| `serde` + `serde_json` | 变量序列化 |
| `tokio` + `tokio-util` | 异步运行时和 `CancellationToken` |
