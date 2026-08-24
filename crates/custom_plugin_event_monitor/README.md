# Event Monitor Host Plugin

Kubernetes 事件监听和分发插件，为 WASM 组件提供集群资源变更的实时通知能力。

## 核心流程

```
┌──────────────┐   watcher::create()    ┌──────────────────┐
│  Guest (WASM) │ ────────────────────▶  │   Host Plugin     │
│              │                        │                   │
│ import watcher│                        │  kube::Client     │──▶ K8s API
│   .create()  │                        │  Discovery::run() │
│   .list_all_ │                        │  Api::all_with()  │
│   resources()│                        │  watcher()        │
│   .watch_    │                        │                   │
│   resources()│                        │  ┌─────────────┐  │
│   .unwatch_  │                        │  │ Watcher-1   │  │
│   resources()│                        │  │ Watcher-2   │  │
│              │                        │  │ Watcher-N   │  │
│ export handler│  ◀── handle_event() ─ │  └──┬──┬──┬───┘  │
│   .handle_   │     (串行)              │     │  │  │      │
│   event()    │                        │  ┌──▼──▼──▼───┐  │
└──────────────┘                        │  │ mpsc channel│  │
                                        │  │ (consumer)  │  │
                                        │  └─────────────┘  │
                                        └──────────────────┘
```

1. **Create** — Guest 调用 `watcher::create(url, token)` 连接集群
2. **Discovery** — `list-all-resources()` 通过 kube Discovery 扫描所有 API Group（含 CRD），返回支持 watch + list 的资源列表
3. **Watch** — `watch-resources(resources)` 按 GVK 批量启动 watcher，重复调用覆盖旧 watcher
4. **Dispatch** — 所有 watcher 的事件通过 `mpsc` channel 汇入单一 consumer task 串行分发，避免并发 store/instance 冲突
5. **Unwatch** — `unwatch-resources()` 取消所有 watcher

## WIT 接口

```wit
package custom:event-monitor@0.2.0;

interface types {
    variant event-action { applied, deleted }

    record k8s-event {
        group: string,
        version: string,
        kind: string,
        name: string,
        namespace: option<string>,
        action: event-action,
    }

    record watchable-resource {
        group: string,
        version: string,
        kind: string,
    }

    record watch-rule {
        id: string,
        res: watchable-resource,
        namespace: option<string>,
        condition: string,
    }
}

interface watcher {
    create: func(api-server-url: string, token: string) -> result<_, string>;
    list-all-resources: func() -> result<list<watchable-resource>, string>;
    watch-resources: func(rules: list<watch-rule>) -> result<_, string>;
    unwatch-resources: func() -> result<_, string>;
}

interface handler {
    use types.{k8s-event};

    handle-event: async func(id: string, event: k8s-event) -> result<_, string>;
}
```

## 使用方法

### 1. 在 WASM 组件中声明依赖

```wit
// world.wit
world my-component {
    import custom:event-monitor/watcher@0.2.0;
    export custom:event-monitor/handler@0.2.0;
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

impl bindings::exports::custom::event_monitor::handler::Guest for CustomHandler {
    async fn handle_event(
        id: String,
        event: bindings::exports::custom::event_monitor::handler::K8sEvent,
    ) -> Result<(), String> {
        log::info!(
            "EVENT: rule={}, action={:?}, gvk={}/{}/{}, name={}, ns={:?}",
            id, event.action, event.group, event.version, event.kind, event.name, event.namespace,
        );
        Ok(())
    }
}
```

### 3. 调用 watcher API

```rust
use bindings::custom::event_monitor::watcher;
use bindings::custom::event_monitor::types::WatchRule;

// 连接集群
watcher::create("https://kubernetes.default.svc", "bearer-token")?;

// 列出所有可监听资源
let resources = watcher::list_all_resources()?;

// 按规则监听（rule 的 id 会原样回传 handle-event 回调，condition 为 jsonlogic 表达式，空串匹配所有）
let rules = vec![
    WatchRule {
        id: "rule-1".into(),
        res: WatchableResource { group: String::new(), version: "v1".into(), kind: "Pod".into() },
        namespace: None,
        condition: String::new(),
    },
    WatchRule {
        id: "rule-2".into(),
        res: WatchableResource { group: "apps".into(), version: "v1".into(), kind: "Deployment".into() },
        namespace: Some("default".into()),
        condition: r#"{"==": [{"var": "type"}, "Normal"]}"#.into(),
    },
];
watcher::watch_resources(&rules)?;

// 取消监听
watcher::unwatch_resources()?;
```

## 架构设计

| 特性 | 实现 |
|------|------|
| 资源发现 | `kube::Discovery` 全量扫描，`resources_by_stability()` 跨版本聚合 |
| 过滤 | 只返回同时支持 `watch` + `list` 的资源 |
| 多资源 watch | `Discovery::resolve_gvk()` 查找 `ApiResource` → `Api::<DynamicObject>::all_with()` |
| 覆盖机制 | `watch_resources()` 内部先取消所有旧 watcher 再建立新的 |
| 事件串行 | `tokio::sync::mpsc::unbounded_channel` 收集所有 watcher 事件，单一 consumer 逐条分发 |
| 生命周期 | `CancellationToken` 树：parent 取消则所有 watcher + consumer 一起停止 |
| TLS | `rustls-tls`，跳过证书验证（`accept_invalid_certs = true`） |

## 依赖

| Crate | 用途 |
|------|------|
| `kube` + `kube-runtime` | K8s API 客户端和 watcher |
| `k8s-openapi` | K8s 资源类型定义（供 `kube` 使用，需启用版本 feature） |
| `tokio` + `tokio-util` | 异步运行时和 `CancellationToken` |
| `futures` | `StreamExt` 用于 watcher 流 |
