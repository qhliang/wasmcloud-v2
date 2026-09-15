# Crontab Host Plugin

基于 cron 表达式的定时/延时回调插件，为 WASM 组件提供周期任务和一次性延时任务的触发能力。支持静态配置（interface config）与运行时动态调度（`scheduler` 接口）两种方式。

## 核心流程

```
┌──────────────┐   scheduler::schedule()  ┌──────────────────┐
│  Guest (WASM) │ ───────────────────────▶  │   Host Plugin     │
│              │                           │                   │
│ import       │                           │  tokio::task     │
│   scheduler  │                           │  ├─ cron task    │──▶ 周期触发
│   .schedule()│                           │  │  (schedule)   │
│   .schedule_ │                           │  │  ├─ spawn     │
│   delay()    │                           │  │  └─ compute    │
│   .remove()  │                           │  │     delay     │
│   .list_     │                           │  └─ delay task   │──▶ 一次性触发
│   schedules()│                           │      (one-shot)  │
│              │                           │                   │
│ export handler│  ◀── handle_tick() ────  │  ┌─────────────┐  │
│   .handle_   │     (async)               │  │ mpsc channel│  │
│   tick()     │                           │  └─────────────┘  │
└──────────────┘                           └──────────────────┘
```

1. **调度注册** — 静态：interface config 中 `schedule.<key>` 格式（`name=<name>;cron=<expr>` 或 `name=<name>;delay-ms=<ms>`）；动态：Guest 调用 `scheduler::schedule()` / `schedule-delay()`
2. **任务派发** — 每个 schedule 派生独立 tokio task，cron 表达式解析（`cron` crate）或延时计时
3. **Tick 回调** — 触发时通过 mpsc channel 串行调用 guest 导出的 `handle-tick(name)`（async export，host 侧经 `run_concurrent` 调用）

## 配置

Interface config 键格式：`schedule.<任意后缀>`，值格式：
- 周期：`name=<name>;cron=<cron-expr>`
- 一次性：`name=<name>;delay-ms=<ms>`

```yaml
custom:crontab:
  config:
    schedule.tick: "name=tick;cron=*/30 * * * *"
    schedule.cleanup: "name=cleanup;cron=0 0 * * *"
    schedule.init: "name=init;delay-ms=5000"
```

## WIT 接口

```wit
package custom:crontab@0.2.0;

interface types {
    variant schedule-error {
        invalid-expression(string),
        not-found(string),
        already-exists(string),
        internal(string),
    }
}

/// Host-provided scheduling API that guest can call to manage schedules at runtime.
interface scheduler {
    use types.{schedule-error};

    /// Add a periodic cron schedule.
    schedule: func(name: string, cron-expression: string) -> result<_, schedule-error>;

    /// Schedule a one-shot callback after the given delay.
    schedule-delay: func(name: string, delay-ms: u64) -> result<_, schedule-error>;

    /// Remove a schedule by name.
    remove: func(name: string) -> result<_, schedule-error>;

    /// List active schedule names.
    list-schedules: func() -> result<list<string>, schedule-error>;
}

/// Interface that the guest component must export.
interface handler {
    /// Called by the host when a scheduled tick fires.
    handle-tick: async func(name: string) -> result<_, string>;
}
```

## 使用方法

### 1. 在 WASM 组件中声明依赖

```wit
// world.wit
world my-component {
    import custom:crontab/scheduler@0.2.0;
    export custom:crontab/handler@0.2.0;
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

impl bindings::exports::custom::crontab::handler::Guest for CustomHandler {
    async fn handle_tick(name: String) -> Result<(), String> {
        log::info!("CRONTAB TICK: schedule '{}' fired", name);
        Ok(())
    }
}
```

### 3. 运行时动态调度

```rust
use bindings::custom::crontab::scheduler;

// 注册周期调度（cron 表达式）
scheduler::schedule("tick", "*/30 * * * *")?;

// 注册一次性延时调度
scheduler::schedule_delay("init", 5_000)?;

// 列出所有调度
let names = scheduler::list_schedules()?;

// 移除调度
scheduler::remove("tick")?;
```

## 架构设计

| 特性 | 实现 |
|------|------|
| 调度存储 | `HashMap<String, Schedule>`（schedule 注册于组件 bind / config 解析时） |
| cron 解析 | `cron` crate 计算下一次触发延迟，`tokio::time::sleep` 循环 |
| 一次性调度 | `schedule_delay` 创建延时 task，触发后自动移除 |
| 事件串行 | mpsc channel 汇入单一 consumer task 串行分发，避免并发访问 store/instance |
| 回调方式 | async export，`store.run_concurrent` + accessor 调用 `handle-tick` |
| 配置解析 | `parse_schedule_config()` 解析 `name=...;cron=...` / `name=...;delay-ms=...` |
| 清理 | `on_workload_unbind` 取消所有 task 与 channel |
| 重绑防护 | 组件**未经 `on_workload_unbind`** 就被再次 bind 时，先取消上一代的 `cancel_token` 再覆盖跟踪数据（`cancel_stale_generation()`），并打 `warn!` 日志；否则旧代任务永久存活且其 token 已不可达 |

### 生命周期与「重绑」语义

每个组件在 bind 时创建一代 `CancellationToken`（`ComponentData.cancel_token`），该代派生的所有
cron / delay 任务持有它的 child token；`on_workload_unbind` 会取消它并删除跟踪数据。

问题出在 **bind 没有配对的 unbind** 时：`WorkloadTracker::add_component` 只是覆盖跟踪项，被覆盖
的 `ComponentData` 仅被 drop —— 而 `CancellationToken` 的 `Drop` 只递减句柄引用计数、**不会
cancel** —— 于是上一代任务（持有仍然有效的 child token）**永久存活**，其父 token 又已不可达，
任何代码都无法再停掉它。表现为同一次 tick 被多触发一条执行，并随重绑次数累积。

因此 `on_workload_item_bind` 在覆盖前先调用 `cancel_stale_generation()`：同一组件被再次 bind 时
先取消上一代、再登记新一代，并打一条
`component bound again without unbind — cancelled the previous generation of schedules`
的 `warn!`，把这条原先完全静默的路径变为可观测。健康的 unbind → bind 路径不受影响
（无可取消对象时为 no-op）。

## 依赖

| Crate | 用途 |
|------|------|
| `cron` | cron 表达式解析与下次触发时间计算 |
| `tokio` + `tokio-util` | 异步任务调度和 `CancellationToken` |
| `serde` | 配置序列化 |
