# 容器服务接入 Task Queue 方案

## 结论

`custom:task-queue` 继续把 P3 WIT worker 作为宿主侧的权威执行模型。容器服务直接消费 JetStream；它不导出 WIT 接口，也不是由插件启动的工作负载。这是原生 JetStream worker 接入方式，不是新增一种“容器 workload 类型”。

协议目标是持久化、可演进，并同时服务 wasm 和原生消费者。插件当前 wasm 执行路径后续应迁移到同一份任务 envelope 和控制事件主题。

## 数据面

插件创建以下持久化对象：

| 对象 | 名称 |
|---|---|
| 任务流 | `<queue>` |
| 元数据 KV | `<queue>-meta` |
| 结果流 | `<queue>-results`（可显式关闭） |
| 共享 durable consumer | `<queue>-worker` |

当前任务主题是 `<queue>.tasks.<task-id>`。当前 wasm 实现内部使用 `TaskEnvelope { id, payload }`；建议引入如下稳定 envelope，把 schema 显式化：

```json
{
  "schema_version": 1,
  "task_id": "01990000-0000-7000-8000-000000000000",
  "payload_encoding": "raw",
  "payload_base64": "aGVsbG8="
}
```

`payload_encoding: "raw"` 表示 `payload_base64` 解码后就是 `producer.submit(task)` 传入的原始字节。消费者必须忽略未知字段，遇到不支持的 schema 版本时使用 `Term` 拒绝任务。本地示例可以直接设置 NATS 消息头 `Nats-Msg-Id: binary`，跳过 base64，直接发布二进制 payload。

`task_id` 是 JetStream subject 的最后一段。消费者可以从 subject 推导 ID，但执行前仍必须解码 envelope 并完成 schema 检查。

## Worker Consumer

复用现有共享 durable consumer，让插件创建的任务和容器创建的任务共享同一队列、投递预算、重试状态和 WorkQueue 保留语义：

```text
Consumer: <queue>-worker
Filter: <queue>.tasks.>
AckWait: 30s
MaxDeliver: 3
```

容器 worker 使用显式批量拉取，并自行控制并发。`nats.rs` 支持批量拉取和 `AckKind::Progress`：

```rust
let batch = consumer.batch()
    .expires(Duration::from_secs(5))
    .max_messages(16)
    .messages()
    .await?;
while let Some(message) = batch.next().await {
    let (message, acker) = message?.clone().split();
    // handle(acker, message).await?;
}
```

建议每个 worker 进程同时只处理一个任务；除非任务明显是 I/O 密集且业务明确支持并行，否则不要提高并发。如果不同进程需要不同的 retry/backoff 要求，应使用不同 durable pull subscription；同质 worker 集群优先共享 `<queue>-worker`。

## 租约续期

JetStream `AckWait` 就是租约。worker 只有在业务副作用和完成事件都已持久化后才应 `Ack`。

任务拉取后立即启动续期循环：

```rust
let renew_acker = acker.clone();
let renewer = tokio::spawn(async move {
    let mut ticker = tokio::time::interval(Duration::from_secs(10));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if renew_acker.ack_with(AckKind::Progress).await.is_err() {
            break;
        }
    }
});
```

退出前先停止续期任务。收到 `SIGTERM` 时应停止接受新批次，明确完成或放弃当前任务；只有在 worker 明确希望重启后继续处理时，才再发送一次 `Progress`。`Progress` 只延长租约，不会保留 worker 本地状态，因此必须有独立的执行超时。

## 元数据与取消

KV bucket 是 `<queue>-meta`。任务的元数据 key 是：

```text
agent-task.<task-id>
```

这是插件当前 `metadata_key` 的格式，不是 KV 的内部 subject `$KV.agent-task.<task-id>`。

建议的元数据 JSON 在插件内部 `TaskMeta` 基础上补充 `schema_version`：

```json
{
  "schema_version": 1,
  "id": "01990000-0000-7000-8000-000000000000",
  "queue": "agent-task",
  "state": "running",
  "attempt": 1,
  "created_at_ms": 1770000000000,
  "dispatched_at_ms": 1770000000001,
  "completed_at_ms": null,
  "deadline_ms": 1770003600000,
  "cancel_requested": false,
  "attempts": []
}
```

状态值包括 `queued`、`running`、`dispatch-timeout`、`execution-timeout`、`cancelled`、`max-retries-exceeded`、`succeeded` 和 `failed`。元数据不存在，或状态不是 `queued`/`running` 时，容器应拒绝执行。

插件补齐 CAS 前，更稳妥的更新方式是：

```text
1. 读取当前 revision。
2. 使用 update(key, new_value, revision) 写回。
3. 更新失败时重新读取，并遵循更新的状态。
```

这样可以避免重启后的旧 worker 覆盖取消标记或终态结果。同样的 CAS 要求也适用于插件自己的元数据更新。

生产者或运维请求取消时写入：

```json
{"cancel_requested": true}
```

容器 worker 应在每个重要副作用前读取元数据，并在长任务中周期性检查。发现取消标记后清理资源并结束本次 attempt。插件任务也可以继续通过现有 `custom:task-queue/task-control` 获得取消状态。

## 执行超时

JetStream 续期只证明 worker 存活，不能当作执行超时。否则一个健康但卡死的 worker 可以无限续租。

建议把绝对执行截止时间写入 envelope：

```json
{"execution_deadline_ms": 1770003600000}
```

截止时间是绝对时间，只在任务被明确重新投递时刷新。队列级默认仍是一小时。容器必须在执行主要副作用前检查截止时间，超时后返回错误或终止 attempt。插件 wasm 路径也应使用同一字段。

## 心跳与事件

不要把进度事件写入任务流；这会干扰 WorkQueue 保留策略和任务 subject 状态。心跳与控制事件使用 core NATS。

现有队列命名已经隐含以下控制主题：

| 事件 | 主题 |
|---|---|
| worker 心跳 / 进度 | `<queue>.heartbeat` |
| attempt 失败 | `<queue>.events.attempt-failed` |
| 任务完成 | `<queue>.events.complete` |

元数据 KV 仍是权威状态存储，因此这些事件允许在重试后丢失，不等于任务状态丢失。容器生产者可以直接订阅事件做通知或审计；调试时也可以用临时本地订阅。

该主题集合与已批准的插件设计兼容。插件后续迁移后，回调 payload 还会包含确定性 `target-hash`，宿主会消费 `<queue>.cb.<target-hash>` 下的 queue-grouped 主题。只需要队列级可观测性的容器可以先使用上表主题；需要和 wasm observer 完全兼容的客户端应消费最终回调主题。

建议事件 schema：

```json
{
  "schema_version": 1,
  "type": "heartbeat",
  "queue": "agent-task",
  "task_id": "01990000-0000-7000-8000-000000000000",
  "attempt": 1,
  "timestamp_ms": 1770000000123,
  "producer": {
    "namespace": "default",
    "workload": "http-api",
    "component": "http-api"
  },
  "data": "5%"
}
```

`attempt-failed` 事件的 `data` 是失败 attempt 对象；`complete` 事件的 `data` 是 `task-result` 对象。事件 payload 是 UTF-8 JSON，并沿用心跳 8 KiB 上限；更大的进度数据应写入对象存储/blobstore 后传递引用。`producer` 字段只用于描述，不能作为授权依据。

## 终态结果

成功完成时发布结果事件，并在结果归档未关闭时写入结果流。worker 应在 `Ack` 前完成：

```text
1. CAS 元数据到终态。
2. 发布 `<queue>.events.complete`。
3. 归档开启时发布 `<queue>.results.<task-id>`。
4. Ack JetStream 任务。
```

发布失败时不 `Ack`。元数据写入成功后进程重启可能造成事件重复；订阅者必须按 `(task_id, attempt, status)` 去重。第 3 步在 `Ack` 前重复执行通常是安全的，因为结果按任务 subject 覆盖，但实现必须显式处理重复发布失败。

结果 schema：

```json
{
  "schema_version": 1,
  "id": "01990000-0000-7000-8000-000000000000",
  "queue": "agent-task",
  "status": "succeeded",
  "attempt": 1,
  "output_base64": "aGVsbG8=",
  "error": null,
  "completed_at_ms": 1770000000456
}
```

当前归档结果中的 `output` 是 JSON 字节数组。新增 `output_base64` 可以让归档表示更小、更跨语言。迁移期间读取端应同时兼容两个字段；写入端按 envelope schema version 选择其中一个。

## 失败与重试

| 场景 | 动作 |
|---|---|
| envelope / 元数据无效或不支持 | `Term`；发布失败结果；`Ack` |
| 元数据已是终态或 dispatch timeout | `Ack`；不执行 |
| 可恢复业务错误 | 按配置退避 `Nak` |
| 投递次数达到 `MaxDeliver` | `Term`；发布 `max-retries-exceeded` |
| 致命 / 系统错误 | `Term`；发布 `failed` |
| 进程在 `Ack` 前退出 | 租约到期；JetStream 重新投递 |

每个 `Nak`/`Term` 都应先完成元数据 CAS，保证 attempt 和状态在队列动作前持久化。容器必须保留任务 payload 或幂等键，并按 `task_id` 去重，因为 JetStream 仍是 at-least-once 语义。

## 部署形态

P3 插件 workload 在 dev 模式下需要 `dev.data_nats_url` 指向已开启 JetStream 的 NATS。wasm 组件导入 `task-control` 并导出 `worker`，由插件走 wasm 执行路径。

容器 worker 对应 runtime 的 native workload `service`，它必须导出 `wasi:cli/run`。这里的 image 必须是 wasmCloud component image，不是 Linux 容器镜像。出站 DNS 和主机访问需要显式授权：

```yaml
apiVersion: runtime.wasmcloud.dev/v1alpha1
kind: WorkloadDeployment
metadata:
  name: task-worker
spec:
  replicas: 1
  template:
    spec:
      components:
        - name: worker
          image: registry.example.com/acme/task-worker:1.0.0
          localResources:
            allowedIpNameLookups: ["nats", "nats.wasmcloud-system.svc"]
            allowedHosts: ["nats.wasmcloud-system.svc:4222"]
      service:
        image: registry.example.com/acme/task-worker-service:1.0.0
```

示例同时列出 component 和 service 两种入口。通常二选一：长驻进程使用 service；如果原生任务 API 由其他 component 调用，则使用 component。

如果 worker 镜像只是包了一层 Linux 容器，它目前无法作为 wasmCloud native service 部署。应改写为组件化 NATS 客户端，或使用独立 Kubernetes Deployment 连接数据面 NATS service。

## NATS 访问

当前 chart 会创建 `nats.<release-namespace>.svc:4222`，并在 `charts/runtime-operator/templates/nats/config.yaml` 中开启 JetStream。宿主数据面使用该服务。DNS 名称依赖 release 命名空间，`nats.wasmcloud-system.svc` 只是示例。

`global.tls.enabled` 为 true 时，NATS 服务启用 TLS 和 `verify_and_map`。非宿主 worker 需要提供能映射到授权用户的客户端证书，或单独配置账号/凭据。不要假设可以使用明文 `nats://`。

## 与当前实现的差距

wasm producer 的事件当前由插件在宿主本地查找第一个 producer component 并分发。已批准设计要求确定性 core-NATS 回调主题和 queue group。上述事件协议与该迁移兼容；worker 侧 WIT 不需要变化。

当前已新增 `crates/task_queue_core` 和 `crates/task_queue_worker`。协议类型、JetStream 资源创建、META KV、任务提交/取消和 worker runner 已抽成共享库；wasm 插件也已迁移到核心库。`WorkerRunner` 可直接消费 `<queue>-worker`，但跨进程取消、dispatch timeout scanner 和确定性 NATS callback routing 仍在 `custom_plugin_task_queue` 的后续工作中。

在上述剩余能力完成前，该接入方式适合验证和单队列场景，但不适合多宿主生产环境。
