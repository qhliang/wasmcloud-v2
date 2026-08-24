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
| exec-id 透传 | `exec_id_by_pid: Arc<RwLock<HashMap<String, String>>>` 在 `start` 时记录 `pid → exec-id`；分发时经 `resolve_exec_id()` 查映射（未命中时通过 `exec_id_notify: Arc<Notify>` 有界等待，5s 超时兜底空串），回调据此回填调用方分配的 exec-id，不依赖 guest 内嵌 |
| 存储 | 使用 acts 默认 `MemoryStore`，进程重启后状态丢失 |
| 错误处理 | acts 错误包装为 `Ok(Err(...))` 返回给 guest，不触发 wasmtime trap |
| 清理 | `on_workload_unbind` 时取消 `CancellationToken`，关闭 event channel |

## 依赖

| Crate | 用途 |
|------|------|
| `acts` v0.20 | 工作流引擎核心（含 `rquickjs` 嵌入式 JS 运行时） |
| `serde` + `serde_json` | 变量序列化 |
| `tokio` + `tokio-util` | 异步运行时和 `CancellationToken` |

## 已知限制

以下为当前实现已识别的遗留问题，均不阻塞常规使用，按影响程度排序：

1. **P2 — 多进程极端并发下 exec-id 可能超时兜底为空串（低概率）**
   事件分发依赖 `exec_id_notify: Arc<Notify>` 唤醒等待 `pid → exec-id` 映射的 consumer。`notify_one()` 每次只唤醒**一个**等待者：若两个进程几乎同时 `start()`（p1/p2 并行），insert(p1) 的 notify 唤醒的是等待 p2 的 resolver、而 insert(p2) 的 notify 又唤醒其他等待者，则等待 p2 的 resolver 可能直到 5s 超时才兜底（拿到空 exec-id 并输出 warn）。该行为与修复前一致，不会挂死 consumer，仅在需要精确 exec-id 时会有一次空串回调。
   - **触发条件**：两个及以上进程在 consumer 恰好停在等待点的时间窗口内并行 `start`
   - **根治方案**：`start` 内先 `notify_waiters()` 再 `notify_one()`（唤醒全部已等待者 + 为未来等待者存储 permit），或改用 watch/generation 机制

2. **P3 — `exec_id_by_pid` 映射只增不减**
   `start` 记录的 `pid → exec-id` 映射在进程结束后不会清理。内存占用可忽略；若未来 acts 复用 pid 且映射未更新，理论上可能出现串扰（当前 pid 生成策略不复用，实际无影响）。

3. **P3 — `EXEC_ID_WAIT_TIMEOUT` 是每轮等待时长而非总超时**
   常量注释表述为"总超时 5s"，但实现中该值作用于等待循环的**每一轮** `notified()`。若 resolver 被多次唤醒仍未命中映射，累计等待时间会超过 5s（不会死循环，最终仍会兜底）。
