//! # Event Monitor Host Plugin
//!
//! Watches Kubernetes API server events and dispatches them to guest components.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use futures::StreamExt;
use kube::{
    Api, Client as KubeClient, Config as KubeConfig,
    api::DynamicObject,
    core::GroupVersionKind,
    discovery::{Discovery, verbs},
    runtime::watcher::{self, watcher as watch, Event as WatchEvent},
};
use tokio::sync::{RwLock, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::{ResolvedWorkload, WorkloadItem};
use wash_runtime::plugin::{HostPlugin, WitInterfaces, WorkloadTracker};
use wash_runtime::wit::WitInterface;

mod bindings {
    wasmtime::component::bindgen!({
        world: "event-monitor",
        imports: { default: async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

use bindings::custom::event_monitor::types::{EventAction, K8sEvent, WatchableResource};

const PLUGIN_ID: &str = "event-monitor";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

struct WatcherState {
    cancel_token: CancellationToken,
}

/// A queued event to be dispatched serially.
struct QueuedEvent {
    gvk: GroupVersionKind,
    event: WatchEvent<DynamicObject>,
}

struct ComponentData {
    cancel_token: CancellationToken,
    workload: Option<ResolvedWorkload>,
    client: Option<KubeClient>,
    watchers: Vec<WatcherState>,
    /// Serialized event dispatch channel: watchers push here, a single
    /// consumer task creates stores and calls handle_event one at a time.
    event_tx: Option<mpsc::UnboundedSender<QueuedEvent>>,
}

#[derive(Clone)]
pub struct EventMonitor {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for EventMonitor {
    fn default() -> Self { Self::new() }
}

impl EventMonitor {
    pub fn new() -> Self {
        Self { tracker: Arc::new(RwLock::new(WorkloadTracker::default())) }
    }

    /// Start a background serial consumer that reads from `event_rx` and
    /// dispatches to the guest one event at a time, avoiding concurrent
    /// store / instance access to the same component.
    fn start_event_consumer(
        workload: ResolvedWorkload,
        component_id: String,
        cancel_token: CancellationToken,
        mut event_rx: mpsc::UnboundedReceiver<QueuedEvent>,
    ) {
        tokio::spawn(async move {
            info!(component_id = %component_id, "Event consumer started");
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(component_id = %component_id, "Event consumer cancelled");
                        break;
                    }
                    msg = event_rx.recv() => match msg {
                        Some(q) => dispatch_event(&workload, &component_id, &q.gvk, q.event).await,
                        None => { info!(component_id = %component_id, "Event channel closed"); break; }
                    }
                }
            }
        });
    }

    fn spawn_watcher(
        component_id: String,
        api: Api<DynamicObject>,
        gvk: GroupVersionKind,
        cancel_token: CancellationToken,
        event_tx: mpsc::UnboundedSender<QueuedEvent>,
    ) {
        tokio::spawn(async move {
            let mut stream = Box::pin(watch(api, watcher::Config::default()));
            let gvk_str = format!("{}/{}/{}", gvk.group, gvk.version, gvk.kind);
            info!(component_id = %component_id, gvk = %gvk_str, "Watcher started");
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(component_id = %component_id, gvk = %gvk_str, "Watcher cancelled");
                        break;
                    }
                    ev = stream.next() => match ev {
                        Some(Ok(e)) => {
                            let _ = event_tx.send(QueuedEvent { gvk: gvk.clone(), event: e });
                        }
                        Some(Err(e)) => warn!(component_id = %component_id, error = %e, "Stream error"),
                        None => { info!(component_id = %component_id, gvk = %gvk_str, "Stream ended"); break; }
                    }
                }
            }
        });
    }
}

fn build_kube_config(url: &str, token: &str) -> anyhow::Result<KubeConfig> {
    let url: http::Uri = url.parse().context("invalid api_server_url")?;
    let mut cfg = KubeConfig::new(url);
    cfg.auth_info.token = Some(token.to_string().into());
    cfg.accept_invalid_certs = true;
    Ok(cfg)
}

async fn dispatch_event(
    workload: &ResolvedWorkload,
    component_id: &str,
    gvk: &GroupVersionKind,
    event: WatchEvent<DynamicObject>,
) {
    let (action, obj) = match event {
        WatchEvent::Apply(p) | WatchEvent::InitApply(p) => (EventAction::Applied, p),
        WatchEvent::Delete(p) => (EventAction::Deleted, p),
        WatchEvent::Init | WatchEvent::InitDone => { return; }
    };

    let k8s = K8sEvent {
        group: gvk.group.clone(),
        version: gvk.version.clone(),
        kind: gvk.kind.clone(),
        name: obj.metadata.name.clone().unwrap_or_default(),
        namespace: obj.metadata.namespace.clone(),
        action,
    };

    debug!(component_id = %component_id, gvk = %format!("{}/{}", k8s.group, k8s.kind), name = %k8s.name, "Dispatching");

    let Ok(mut store) = workload.new_store(component_id).await else { warn!(component_id = %component_id, "new_store failed"); return; };
    let Ok(ip) = workload.instantiate_pre(component_id).await else { warn!(component_id = %component_id, "instantiate_pre failed"); return; };
    let Ok(pre) = bindings::EventMonitorPre::new(ip) else { warn!(component_id = %component_id, "EventMonitorPre failed"); return; };
    let Ok(proxy) = pre.instantiate_async(&mut store).await else { warn!(component_id = %component_id, "instantiate_async failed"); return; };

    match proxy.custom_event_monitor_handler().call_handle_event(&mut store, &k8s).await {
        Ok(Ok(())) => debug!(component_id = %component_id, "Event handled"),
        Ok(Err(e)) => warn!(component_id = %component_id, error = %e, "Handler error"),
        Err(e) => warn!(component_id = %component_id, error = %e, "Call failed"),
    }
}

fn cancel_all_watchers(data: &mut ComponentData) {
    for w in data.watchers.drain(..) { w.cancel_token.cancel(); }
}

// ---------------------------------------------------------------------------
// WIT watcher::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::event_monitor::watcher::Host for ActiveCtx<'a> {
    async fn create(&mut self, api_server_url: String, token: String) -> wasmtime::Result<Result<(), String>> {
        let Ok(plugin) = self.try_get_plugin::<EventMonitor>(PLUGIN_ID) else { return Ok(Err("plugin not available".into())); };
        let cid = self.component_id.as_ref().to_string();

        let kcfg = build_kube_config(&api_server_url, &token).map_err(|e| wasmtime::Error::msg(format!("config: {e}")))?;
        let client = KubeClient::try_from(kcfg).map_err(|e| wasmtime::Error::msg(format!("connect: {e}")))?;
        client.apiserver_version().await.map_err(|e| wasmtime::Error::msg(format!("unreachable: {e}")))?;

        let mut lock = plugin.tracker.write().await;
        let Some(data) = lock.get_component_data_mut(&cid) else { return Ok(Err("not tracked".into())); };
        cancel_all_watchers(data);
        data.client = Some(client);

        info!(component_id = %cid, "K8s client connected");
        Ok(Ok(()))
    }

    async fn list_all_resources(&mut self) -> wasmtime::Result<Result<Vec<WatchableResource>, String>> {
        let Ok(plugin) = self.try_get_plugin::<EventMonitor>(PLUGIN_ID) else { return Ok(Err("plugin not available".into())); };
        let cid = self.component_id.as_ref().to_string();

        let client = {
            let lock = plugin.tracker.read().await;
            let Some(data) = lock.get_component_data(&cid) else { return Ok(Err("not tracked".into())); };
            let Some(c) = data.client.clone() else { return Ok(Err("not connected".into())); };
            c
        };

        let discovery = Discovery::new(client).run().await.map_err(|e| wasmtime::Error::msg(format!("discovery: {e}")))?;

        let mut resources = Vec::new();
        let mut group_count = 0u32;
        for group in discovery.groups() {
            group_count += 1;
            let group_name = group.name().to_string();
            let by_stability = group.resources_by_stability();
            info!(component_id = %cid, group = %group_name, count = by_stability.len(), "Discovered group");
            for (ar, caps) in by_stability {
                if caps.supports_operation(verbs::WATCH) && caps.supports_operation(verbs::LIST) {
                    resources.push(WatchableResource {
                        group: ar.group.clone(), version: ar.version.clone(), kind: ar.kind.clone(),
                    });
                }
            }
        }

        info!(component_id = %cid, groups = group_count, resources = resources.len(), "Listed all watchable resources");
        Ok(Ok(resources))
    }

    async fn watch_resources(&mut self, resources: Vec<WatchableResource>) -> wasmtime::Result<Result<(), String>> {
        let Ok(plugin) = self.try_get_plugin::<EventMonitor>(PLUGIN_ID) else { return Ok(Err("plugin not available".into())); };
        let cid = self.component_id.as_ref().to_string();

        let (_workload, client, event_tx) = {
            let mut lock = plugin.tracker.write().await;
            let Some(data) = lock.get_component_data_mut(&cid) else { return Ok(Err("not tracked".into())); };
            cancel_all_watchers(data);
            let Some(wl) = data.workload.clone() else { return Ok(Err("not resolved".into())); };
            let Some(c) = data.client.clone() else { return Ok(Err("not connected".into())); };

            // Re-create the event channel to drain any stale events from
            // previous watchers (old consumer already cancelled).
            let (tx, rx) = mpsc::unbounded_channel();
            data.event_tx = Some(tx.clone());

            // Start a fresh serial consumer
            let child = data.cancel_token.child_token();
            EventMonitor::start_event_consumer(wl.clone(), cid.clone(), child, rx);

            (wl, c, tx)
        };

        let discovery = Discovery::new(client.clone()).run().await.map_err(|e| wasmtime::Error::msg(format!("discovery: {e}")))?;

        for res in &resources {
            let gvk = GroupVersionKind { group: res.group.clone(), version: res.version.clone(), kind: res.kind.clone() };
            let gvk_str = format!("{}/{}/{}", gvk.group, gvk.version, gvk.kind);

            let Some((ar, caps)) = discovery.resolve_gvk(&gvk) else {
                warn!(component_id = %cid, gvk = %gvk_str, "Not found, skipping");
                continue;
            };
            if !caps.supports_operation(verbs::WATCH) {
                warn!(component_id = %cid, gvk = %gvk_str, "No watch support, skipping");
                continue;
            }

            let api = Api::<DynamicObject>::all_with(client.clone(), &ar);
            let child = {
                let lock = plugin.tracker.read().await;
                let Some(data) = lock.get_component_data(&cid) else { break; };
                data.cancel_token.child_token()
            };

            EventMonitor::spawn_watcher(cid.clone(), api, gvk, child.clone(), event_tx.clone());

            let mut lock = plugin.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(&cid) {
                data.watchers.push(WatcherState { cancel_token: child });
            }

            info!(component_id = %cid, gvk = %gvk_str, "Watcher created");
        }

        Ok(Ok(()))
    }

    async fn unwatch_resources(&mut self) -> wasmtime::Result<Result<(), String>> {
        let Ok(plugin) = self.try_get_plugin::<EventMonitor>(PLUGIN_ID) else { return Ok(Err("plugin not available".into())); };
        let cid = self.component_id.as_ref().to_string();
        let mut lock = plugin.tracker.write().await;
        let Some(data) = lock.get_component_data_mut(&cid) else { return Ok(Err("not tracked".into())); };
        let n = data.watchers.len();
        cancel_all_watchers(data);
        info!(component_id = %cid, count = n, "Watchers cancelled");
        Ok(Ok(()))
    }
}

impl<'a> bindings::custom::event_monitor::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// HostPlugin trait
// ---------------------------------------------------------------------------

#[async_trait]
impl HostPlugin for EventMonitor {
    fn id(&self) -> &'static str { PLUGIN_ID }

    fn world(&self) -> wash_runtime::wit::WitWorld {
        wash_runtime::wit::WitWorld {
            imports: HashSet::from([WitInterface::from("custom:event-monitor/watcher,types@0.1.0")]),
            exports: HashSet::from([WitInterface::from("custom:event-monitor/handler@0.1.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(&self, item: &mut WorkloadItem<'a>, interfaces: WitInterfaces<'_>) -> anyhow::Result<()> {
        if interfaces.get("custom", "event-monitor", &[]).is_none() { return Ok(()); }

        bindings::custom::event_monitor::types::add_to_linker::<_, SharedCtx>(item.linker(), extract_active_ctx)?;
        bindings::custom::event_monitor::watcher::add_to_linker::<_, SharedCtx>(item.linker(), extract_active_ctx)?;

        let WorkloadItem::Component(ch) = item else { return Ok(()); };

        debug!(component_id = ch.id(), "EventMonitor bound");
        self.tracker.write().await.add_component(ch, ComponentData {
            cancel_token: CancellationToken::new(),
            workload: None,
            client: None,
            watchers: Vec::new(),
            event_tx: None,
        });
        Ok(())
    }

    async fn on_workload_resolved(&self, workload: &ResolvedWorkload, component_id: &str) -> anyhow::Result<()> {
        if let Some(data) = self.tracker.write().await.get_component_data_mut(component_id) {
            data.workload = Some(workload.clone());
        }
        Ok(())
    }

    async fn on_workload_unbind(&self, workload_id: &str, _interfaces: WitInterfaces<'_>) -> anyhow::Result<()> {
        self.tracker.write().await.remove_workload_with_cleanup(
            workload_id,
            |_| async {},
            |mut data: ComponentData| async move { cancel_all_watchers(&mut data); },
        ).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() { assert_eq!(EventMonitor::new().id(), PLUGIN_ID); }

    #[test]
    fn test_world_imports() {
        let w = EventMonitor::new().world();
        assert!(w.imports.iter().any(|i| i.namespace == "custom" && i.package == "event-monitor"));
    }

    #[test]
    fn test_world_exports() {
        let w = EventMonitor::new().world();
        assert!(w.exports.iter().any(|i| i.namespace == "custom" && i.package == "event-monitor"));
    }
}
