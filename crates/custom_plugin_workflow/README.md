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
3. **Deploy & Start** — Guest 调用 `manager::start(exec-id, workflow-def, vars)`，Host 解析 YAML → deploy → 启动 process，并记录 `pid → exec-id` 映射
4. **事件分发** — Consumer task 串行消费 mpsc 中的事件，从映射回填调用方分配的 `exec-id`，逐条调用 WASM handler
5. **查询** — `list-processes()` 和 `process-status()` 直接查询 Engine 内部 store

> **exec-id 说明** — `start` 的第一个参数由调用方（Guest）分配，用于标识一次工作流执行。Host 在 `start` 时保存 `pid → exec-id` 映射，之后所有生命周期回调（`on_start`/`on_message`/`on_complete`/`on_error`）都会把该 exec-id 原样回传给 Guest，无需 Guest 在 workflow vars 中内嵌。

## WIT 接口

```wit
package custom:workflow@0.2.0;

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
}

/// Host-provided API for workflow lifecycle management
interface manager {
    use types.{var-pair, proc-info};

    /// Deploy and start a workflow.
    /// exec-id: caller-assigned execution id, echoed back via lifecycle callbacks.
    /// workflow-def: YAML/JSON workflow definition string.
    /// vars: initial variables passed to the process.
    /// Returns the process id (pid).
    start: func(exec-id: string, workflow-def: string, vars: list<var-pair>) -> result<string, string>;

    /// List all running process instances.
    list-processes: func() -> result<list<proc-info>, string>;

    /// Query a process status by pid.
    process-status: func(pid: string) -> result<proc-info, string>;

    /// Complete a pending task (e.g. human interaction node).
    complete-task: func(pid: string, nid: string, outputs: list<var-pair>) -> result<_, string>;
}

/// Interface that the guest component must export.
interface handler {
    use types.{var-pair};

    on-start:    func(exec-id: string, pid: string) -> result<_, string>;
    on-message:  func(exec-id: string, pid: string, message: list<var-pair>) -> result<_, string>;
    on-complete: func(exec-id: string, pid: string, outputs: list<var-pair>) -> result<_, string>;
    on-error:    func(exec-id: string, pid: string, error: string) -> result<_, string>;
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
    import custom:workflow/manager@0.2.0;
    export custom:workflow/handler@0.2.0;
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
    fn on_start(exec_id: String, pid: String) -> Result<(), String> {
        log::info!("WF START: exec_id={}, pid={}", exec_id, pid);
        Ok(())
    }

    fn on_message(
        exec_id: String,
        pid: String,
        message: Vec<bindings::custom::workflow::types::VarPair>,
    ) -> Result<(), String> {
        log::info!("WF MSG: exec_id={}, pid={}, vars={}", exec_id, pid, message.len());
        Ok(())
    }

    fn on_complete(
        exec_id: String,
        pid: String,
        outputs: Vec<bindings::custom::workflow::types::VarPair>,
    ) -> Result<(), String> {
        log::info!("WF DONE: exec_id={}, pid={}, outputs={}", exec_id, pid, outputs.len());
        Ok(())
    }

    fn on_error(exec_id: String, pid: String, error: String) -> Result<(), String> {
        log::error!("WF ERROR: exec_id={}, pid={}, error={}", exec_id, pid, error);
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
let exec_id = "demo-1".to_string();

// 启动（exec-id 由调用方分配，会通过生命周期回调原样回传）
let pid = manager::start(&exec_id, yaml_str, &vars)?;

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
| exec-id 透传 | `exec_id_by_pid: Arc<RwLock<HashMap<String, String>>>` 在 `start` 时记录 `pid → exec-id`，回调据此回填调用方分配的 exec-id，不依赖 guest 内嵌 |
| 存储 | 使用 acts 默认 `MemoryStore`，进程重启后状态丢失 |
| 错误处理 | acts 错误包装为 `Ok(Err(...))` 返回给 guest，不触发 wasmtime trap |
| 清理 | `on_workload_unbind` 时取消 `CancellationToken`，关闭 event channel |

## 依赖

| Crate | 用途 |
|------|------|
| `acts` v0.20 | 工作流引擎核心（含 `rquickjs` 嵌入式 JS 运行时） |
| `serde` + `serde_json` | 变量序列化 |
| `tokio` + `tokio-util` | 异步运行时和 `CancellationToken` |
