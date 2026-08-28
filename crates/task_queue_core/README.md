# Task Queue Core

`task_queue_core` 是 `custom:task-queue` 的共享协议与 JetStream 运行时层。它供 wasm 宿主插件和 Rust native worker 共同使用，保证任务 envelope、元数据、资源命名、ack 语义和重试策略一致。

## 分层

| 模块 | 职责 |
|---|---|
| `config` | `QueueConfig`、默认值、host-interface 配置解析、重试退避 |
| `types` | 任务、元数据、envelope、状态、失败记录和结果类型 |
| `nats` | 创建 JetStream 资源、读写 META KV、发布结果、统一 `AckAction` |
| `queue` | `TaskProducer` 的提交、查询和 CAS 取消 |
| `worker` | `Worker`、`Observer`、`TaskContext`、heartbeat 与 cancellation trait |
| `events` | heartbeat、attempt failure、terminal result 事件 schema |

## 快速接入

生产者：

```rust
use task_queue_core::config::QueueConfig;
use task_queue_core::queue::TaskProducer;
use task_queue_core::types::Task;

let config = QueueConfig::new("agent-task");
let producer = TaskProducer::connect(jetstream, config).await?;
let task_id = producer.submit(Task { payload: b"hello".to_vec() }).await?;
```

消费者实现 `Worker`：

```rust
use async_trait::async_trait;
use task_queue_core::types::{TaskError, TaskOutput};
use task_queue_core::worker::{TaskContext, Worker};

struct MyWorker;

#[async_trait]
impl Worker for MyWorker {
    async fn handle_task(&self, task: TaskContext) -> Result<TaskOutput, TaskError> {
        task.send_heartbeat("started".to_string()).await?;
        if task.is_cancelled().await? {
            return Err(TaskError::guest("cancelled"));
        }
        Ok(Some(task.payload.clone()))
    }
}
```

`TaskContext::payload` 是解码后的原始业务字节。`send_heartbeat` 通过 core NATS 发布，不影响 JetStream 重投递；`is_cancelled` 当前只检查本地 cancellation token。`TaskProducer::cancel_task` 会使用 KV revision 做 CAS 更新。

## JetStream 对象

| 对象 | 名称 | 主题 |
|---|---|---|
| 任务流 | `<queue>` | `<queue>.tasks.<task-id>` |
| 元数据 KV | `<queue>-meta` | key 为 `<queue>.<task-id>` |
| 结果归档流 | `<queue>-results` | `<queue>.results.<task-id>` |
| durable worker | `<queue>-worker` | filter `<queue>.tasks.>` |

`QueueHandles::create` 会按需创建资源。结果归档可通过 `results-archive: false` 关闭。

## 消息格式

任务消息是 JSON `TaskEnvelope`。`raw` encoding 使用 base64 保留原始字节；`binary` encoding 使用 JSON byte array。当前生产者默认生成 `raw`，native consumer 解码时会兼容 schema 版本 1。

元数据包含 `schema_version`、状态、attempt、时间戳、取消标记和失败记录。JetStream KV revision 用于取消等状态更新时的 CAS。

## 默认约束

| 参数 | 默认值 |
|---|---|
| ack wait | 30 s |
| lease 续期间隔 | 10 s |
| dispatch timeout | 10 min |
| execution timeout | 1 h |
| max deliver | 3 |
| retry backoff | 1 s, 5 s, 15 s, 60 s |
| payload 最大值 | 1 MiB |
| heartbeat info 最大值 | 8 KiB |
| heartbeat 最小间隔 | 1 s |
