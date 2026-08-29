# Task Queue Plugin

`custom:task-queue` 是基于宿主共享 NATS 数据面连接实现的持久化任务队列插件。它面向 P3 组件绑定，使用 JetStream 保存任务、元数据和终态结果，使用 core NATS 转发回调。

## 核心流程

1. **绑定阶段**：组件声明导入 `custom:task-queue/producer` 或 `custom:task-queue/task-control`。插件读取 host-interface 配置，解析并校验队列配置。
2. **解析阶段**：组件 workload 解析成功后，插件为队列懒创建 JetStream 资源，并启动 durable pull consumer 的消费循环。
3. **生产阶段**：生产者调用 `submit()`，插件生成 UUIDv7 task id，写入 META KV，再发布 TASKS 消息。调用方还可以使用 `query-status()` 和 `cancel-task()`。
4. **执行阶段**：消费者调用 `handle-task()` 处理任务。宿主每 10 秒发送一次 JetStream lease 续期；失败时 `Err` 会触发重试，最终次数耗尽后产生终态。
5. **回调阶段**：任务过程中的 heartbeat、attempt failure 和 complete 事件通过 host-local channel 分发给生产者导出的 `observer`。
6. **归档阶段**：终态结果默认写入 `<queue>-results`，便于即使生产者暂时不可用也能审计或恢复结果。

## WIT 接口

### 生产者导入

```wit
interface producer {
  submit: func(task: task) -> result<string, string>;
  query-status: func(task-id: string) -> result<option<task-info>, string>;
  cancel-task: func(task-id: string) -> result<_, string>;
}
```

### 消费者控制导入

```wit
interface task-control {
  send-heartbeat: func(task-id: string, info: string) -> result<_, string>;
  is-cancelled: func(task-id: string) -> bool;
}
```

### 组件导出

```wit
interface observer {
  on-heartbeat: async func(task-id: string, info: string) -> result<_, string>;
  on-attempt-failed: async func(event: attempt-failure) -> result<_, string>;
  on-complete: async func(outcome: task-result) -> result<_, string>;
}

interface worker {
  handle-task: async func(task: task) -> result<option<list<u8>>, string>;
}
```

组件角色分为三类，彼此独立、互不强制：

- **生产者（producer-only）**：导入 `producer`，导出 `observer`；
- **消费者（worker-only）**：导入 `task-control`，导出 `worker`；
- **两者兼具**：同时导入 `producer` 与 `task-control`，并导出 `observer` 与 `worker`。

插件按各组件实际导出的接口分别绑定 `observer` / `worker`，因此上述三类组件均可正常接收回调或执行任务。

## 配置

配置声明在 host-interface 的 `config` 中。同一个队列的多个绑定必须使用一致配置。

| 键 | 默认值 | 说明 |
|---|---|---|
| `queue` | 必填 | 队列名，`^[A-Za-z0-9_-]{1,64}$` 且首字符为字母或数字 |
| `ack-wait-ms` | `30000` | JetStream ack wait |
| `lease-renew-interval-ms` | `10000` | JetStream lease 续期间隔 |
| `default-dispatch-timeout-ms` | `600000` | 从提交到首次执行的默认超时 |
| `default-execution-timeout-ms` | `3600000` | 协作式执行超时 |
| `max-deliver` | `3` | JetStream 最大投递次数 |
| `retry-backoff-ms` | `1000,5000,15000,60000` | 逗号分隔的多级重试退避 |
| `results-archive` | `true` | 是否创建并写入结果归档流 |

Heartbeat 的 `info` 字符串最大为 8 KiB，同一 task 的两次 heartbeat 至少间隔 1000 ms。任务 payload 最大为 1 MiB；更大的数据应由业务方先写入对象存储，再在 payload 中传递引用。

## JetStream 资源

| 资源 | 名称 | 保留策略 |
|---|---|---|
| 任务流 | `<queue>` | WorkQueue |
| 元数据 KV | `<queue>-meta` | KV，history 为 1 |
| 结果归档流 | `<queue>-results` | Limits |

任务主题是 `<queue>.tasks.<task-id>`，结果主题是 `<queue>.results.<task-id>`。Heartbeat 不使用 JetStream。

## 使用示例

```yaml
hostInterfaces:
  - name: custom:task-queue/producer
    config:
      queue: agent-task
  - name: custom:task-queue/task-control
    config:
      queue: agent-task
      retry-backoff-ms: "1000,5000,15000,60000"
```

Wasm 组件需要同时绑定对应的 WIT import 和 export：

- 生产者：import `custom:task-queue/producer`，export `custom:task-queue/observer`
- 消费者：import `custom:task-queue/task-control`，export `custom:task-queue/worker`

## 宿主集成

插件在 host 和 dev 模式都使用内置共享数据面 NATS 客户端，不额外配置 `nats-url`、JetStream domain 或证书。dev 模式只有在 `data-nats-url` 已配置时才会注册本插件，因为本插件不支持内存回退。

## 共享实现分层

插件与 Rust native 服务共享 `crates/task_queue_core`。协议类型、`QueueConfig`、JetStream 资源命名、META KV 读写、任务提交/取消、结果 schema 和重试退避都在核心库中实现，避免 wasm 和 native consumer 协议漂移。

Rust worker 可引用 `crates/task_queue_worker`，实现 `task_queue_core::worker::Worker` 后使用 `WorkerRunner` 拉取同一个 `<queue>-worker` durable consumer。详细接入方式见 `crates/task_queue_worker/README.md`。

## Native Rust Worker 接入

native worker 不需要实现 WIT 接口，也不由 `custom:task-queue` 启动。它直接连接宿主数据面 NATS 的 JetStream，实现 `Worker` 并使用 `WorkerRunner` 消费任务：

```rust
use async_nats::{connect, jetstream};
use std::time::Duration;
use task_queue_core::config::QueueConfig;
use task_queue_core::nats::now_ms;
use task_queue_core::types::{TaskError, TaskOutput};
use task_queue_core::worker::{TaskContext, Worker};
use task_queue_worker::WorkerRunner;
use tokio_util::sync::CancellationToken;

struct LongRunningWorker {
    total_work_ms: u64,
}

const TICK_MS: u64 = 10_000;

#[async_trait::async_trait]
impl Worker for LongRunningWorker {
    async fn handle_task(&self, task: TaskContext) -> Result<TaskOutput, TaskError> {
        // Business progress uses 10-second checkpoints. WorkerRunner also
        // renews the JetStream lease every 10 seconds independently.
        let mut ticker = tokio::time::interval(Duration::from_millis(TICK_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut completed_ms = 0_u64;
        // Notify observers before the first business side effect.
        task.send_heartbeat("started".to_string()).await?;

        while completed_ms < self.total_work_ms {
            // Never start the next side effect after cancellation.
            if task.is_cancelled().await? {
                task.send_heartbeat("cancelled".to_string()).await?;
                return Err(TaskError::guest("task cancelled"));
            }

            ticker.tick().await;
            completed_ms = completed_ms.saturating_add(TICK_MS);

            // Honor the envelope execution deadline before continuing.
            if now_ms() >= task.execution_deadline_ms {
                return Err(TaskError::system("execution deadline exceeded"));
            }

            // Publish progress at each completed checkpoint.
            task.send_heartbeat(format!(
                "{{\"task_id\":\"{}\",\"progress_ms\":{completed_ms},\"total_ms\":{}}}",
                task.task_id, self.total_work_ms
            ))
            .await?;
        }

        if task.is_cancelled().await? {
            task.send_heartbeat("cancelled after work".to_string()).await?;
            return Err(TaskError::guest("task cancelled"));
        }

        Ok(Some(format!(
            "{{\"task_id\":\"{}\",\"completed_ms\":{completed_ms}}}",
            task.task_id
        )
        .into_bytes()))
    }
}

let shutdown = CancellationToken::new();
let client = connect("nats.example:4222").await?;
let runner = WorkerRunner::connect(
    jetstream::new(client),
    QueueConfig::new("agent-task"),
    LongRunningWorker {
        total_work_ms: 5 * 60 * 1000,
    },
).await?;

// WorkerRunner handles JetStream Progress lease renewal every 10 seconds
// while `handle_task` is running. It is separate from send_heartbeat.
let runner_task = tokio::spawn({
    let runner = runner.clone();
    let shutdown = shutdown.clone();
    async move { runner.run(shutdown).await }
});

// Keep the process running until the operator asks it to stop.
tokio::signal::ctrl_c().await?;
shutdown.cancel();
runner_task.await??;
```

业务示例每 10 秒触发一次检查点：先调用 `is_cancelled()`，已取消时停止新副作用并返回 guest error；随后发送一次进度 heartbeat。`WorkerRunner` 会在任务执行期间每 10 秒向 JetStream 发送 `Progress` 续期，业务代码不需要也不应该直接续期。成功时返回 `Some(output)`，可恢复失败返回 guest `TaskError`，deadline 或基础设施失败返回 system `TaskError`。

执行顺序如下：

1. 任务启动时发送一次 `started` heartbeat。
2. 每个检查点先检查 `is_cancelled()`，取消后不再开始新副作用。
3. `ticker.tick()` 等待并模拟一个 10 秒工作分片；实际业务可替换为 DB 批处理、模型推理或文件同步。
4. 分片完成后检查执行 deadline，再发送带进度的 heartbeat。
5. 全部分片完成后做最终取消检查，再返回 output bytes；`WorkerRunner` 负责把它作为成功结果 ack。

### Heartbeat 与续期

| 机制 | 发送者 | 作用 |
|---|---|---|
| `send_heartbeat()` | 业务 worker | 在启动、进度、取消等关键节点向 observer 发布业务状态 |
| JetStream `Progress` | `WorkerRunner` | 任务执行期间每 10 秒续租，防止 `AckWait` 到期重投 |

`WorkerRunner` 中的续期逻辑会克隆当前消息的 JetStream acker，并启动独立 ticker：

```rust
let renewer = tokio::spawn(async move {
    let mut ticker = tokio::time::interval(Duration::from_millis(
        task_queue_core::config::LEASE_RENEW_INTERVAL_MS,
    ));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = renew_cancel.cancelled() => break,
            _ = ticker.tick() => {
                if AckAction::Progress.apply(&renew_acker).await.is_err() {
                    break;
                }
            }
        }
    }
});
```

业务 `handle_task` 返回后，runner 会先取消这个续期任务，再根据结果发送 `Ack`、`Nak` 或 `Term`。

`TaskContext::payload` 是解码后的原始字节。真实业务可以把 JSON heartbeat 替换为普通字符串或压缩数据；超过 8 KiB 会失败。业务副作用必须放在取消检查之后并按 `task_id` 保证幂等。

`WorkerRunner` 使用同一个 `<queue>-worker` durable consumer，处理成功后 `Ack`；guest 错误按 `retry-backoff-ms` `Nak`，达到 `max-deliver` 或遇到 envelope/系统错误时 `Term`。

如果 worker 运行在 Kubernetes 外，需要授予 NATS 地址访问权限；开启数据面 TLS 时还必须提供可被 NATS `verify_and_map` 接受的客户端证书。不要把 Linux 容器镜像当作 wasmCloud native workload 部署；可使用独立 Kubernetes Deployment 或 wasmCloud 原生 service。

## 当前限制

- 执行超时和取消是协作式的，不会强制中断未返回的 Wasm 调用。
- dispatch timeout scanner、metadata CAS 和终态回调去重仍在 P0 范围内推进。
- 任务执行语义是 at-least-once，业务副作用需要根据 task id 保证幂等。
