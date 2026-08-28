# Task Queue Worker Runner

`task_queue_worker` 提供把 Rust native 服务接入 `custom:task-queue` 的最小运行器。它复用 `task_queue_core` 的 JetStream 资源和协议，负责拉取任务、解码 envelope、租约续期和 ack/nak/term 语义。

## 接入

```rust
use async_nats::jetstream;
use std::time::Duration;
use task_queue_core::config::QueueConfig;
use task_queue_core::nats::now_ms;
use task_queue_core::types::{TaskError, TaskOutput};
use task_queue_core::worker::{TaskContext, Worker};
use task_queue_worker::WorkerRunner;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct LongRunningWorker {
    total_work_ms: u64,
}

#[async_trait::async_trait]
impl Worker for LongRunningWorker {
    async fn handle_task(&self, task: TaskContext) -> Result<TaskOutput, TaskError> {
        const TICK_MS: u64 = 10_000;
        // The business task uses a 10-second checkpoint. WorkerRunner also
        // renews the JetStream lease every 10 seconds independently of this.
        let mut ticker = tokio::time::interval(Duration::from_millis(TICK_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut completed_ms = 0_u64;

        // Publish an explicit lifecycle event so observers know work started.
        task.send_heartbeat("started".to_string()).await?;
        while completed_ms < self.total_work_ms {
            // Always check cancellation before starting the next side effect.
            if task.is_cancelled().await? {
                task.send_heartbeat("cancelled".to_string()).await?;
                return Err(TaskError::guest("task cancelled"));
            }

            // Replace this tick with one resumable slice of business work.
            ticker.tick().await;
            completed_ms = completed_ms.saturating_add(TICK_MS);

            // Stop before doing more work after the envelope deadline.
            if now_ms() >= task.execution_deadline_ms {
                return Err(TaskError::system("execution deadline exceeded"));
            }

            // Key checkpoint: publish progress after each completed slice.
            task.send_heartbeat(format!(
                "{{\"progress_ms\":{completed_ms},\"total_ms\":{}}}",
                self.total_work_ms
            ))
            .await?;
        }

        if task.is_cancelled().await? {
            task.send_heartbeat("cancelled after work".to_string()).await?;
            return Err(TaskError::guest("task cancelled"));
        }

        Ok(Some(
            format!("{{\"completed_ms\":{completed_ms}}}").into_bytes(),
        ))
    }
}

let shutdown = CancellationToken::new();
let client = async_nats::connect("nats.example:4222").await?;
let runner = WorkerRunner::connect(
    jetstream::new(client),
    QueueConfig::new("agent-task"),
    LongRunningWorker {
        total_work_ms: 5 * 60 * 1000,
    },
).await?;
// WorkerRunner handles JetStream Progress lease renewal every 10 seconds
// while `handle_task` is running. It is separate from send_heartbeat.
runner.run(shutdown).await?;
```

业务代码实现 `Worker::handle_task`，并在长任务检查点先调用 `is_cancelled()`，再发送 heartbeat 和执行下一分片。返回 `Some(output)` 表示成功；`TaskError::guest` 表示可恢复业务失败，会按 backoff 重试；`TaskError::system` 表示系统/deadline 失败。heartbeat 与 JetStream `Progress` 租约续期互不替代。

示例中的 `LongRunningWorker` 展示了长任务骨架：

| 阶段 | 动作 |
|---|---|
| 开始 | 发送 `started` heartbeat |
| 每个周期 | 先 `is_cancelled()`，取消则返回 guest error |
| 工作分片 | `ticker.tick()` 模拟 10 秒工作，实际可替换为批处理、推理或同步 |
| deadline | 分片完成后用 `execution_deadline_ms` 检查是否超时 |
| 进度 | 每个关键节点发送 `started`、`progress_ms`、`cancelled` 或完成前 heartbeat |
| 续期 | `WorkerRunner` 每 10 秒自动发送 JetStream `Progress`，业务代码无需续期 |
| 完成 | 返回 `Some(output)`，由 runner ack 任务 |

真实业务不要假设 heartbeat 成功代表结果已持久化。所有可恢复状态应在下一个检查点前写入业务存储，并在重试时按 `task_id` 幂等处理。

## 运行语义

- worker 使用 `<queue>-worker` durable pull consumer，与 wasm worker 共享同一队列。
- 拉取任务后每 10 秒发送一次 JetStream `Progress`，默认 `AckWait` 为 30 s。
- 成功处理任务后发送 `Ack`。
- guest 错误未达到 `MaxDeliver` 时按重试退避发送 `Nak`；达到上限后发送 `Term`。
- envelope 无效、schema 不支持或系统错误发送 `Term`。
- 进程在 ack 前退出时，租约到期后 JetStream 会重新投递。

## 当前限制

- runner 顺序处理消息；更高并发应由调用方运行多个 runner 或在 `Worker` 内安全并发。
- `TaskContext::is_cancelled` 只检查本地 token；跨进程取消仍需扩展读取 META KV。
- terminal result、attempt failure 和 metadata 状态更新由上层业务或宿主插件负责，runner 目前专注于执行和 ack。
- 生产环境必须保证业务副作用幂等，因为 JetStream 语义是 at-least-once。
