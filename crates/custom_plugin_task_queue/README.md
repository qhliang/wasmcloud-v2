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

导入 `producer` 的组件必须导出 `observer`。导入 `task-control` 的组件必须导出 `worker`。同一个组件可以同时扮演生产者和消费者。

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

## 当前限制

- 执行超时和取消是协作式的，不会强制中断未返回的 Wasm 调用。
- dispatch timeout scanner、metadata CAS 和终态回调去重仍在 P0 范围内推进。
- 任务执行语义是 at-least-once，业务副作用需要根据 task id 保证幂等。
