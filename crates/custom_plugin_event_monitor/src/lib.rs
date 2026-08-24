//! # Event Monitor Host Plugin
//!
//! Watches Kubernetes API server events and dispatches them to guest components.
//! Supports jsonlogic-based conditional filtering on the host side to avoid
//! unnecessary guest calls.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use futures::StreamExt;
use jsonlogic::apply;
use kube::{
    Api, Client as KubeClient, Config as KubeConfig,
    api::DynamicObject,
    core::{GroupVersion, GroupVersionKind},
    discovery::{self, ApiGroup, Discovery, Scope, verbs},
    runtime::watcher::{self, Event as WatchEvent, watcher as watch},
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
use bindings::custom::event_monitor::watcher::WatchRule;

const PLUGIN_ID: &str = "event-monitor";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

struct WatcherState {
    cancel_token: CancellationToken,
}

/// A queued event to be dispatched serially.
struct QueuedEvent {
    id: String,
    gvk: GroupVersionKind,
    event: WatchEvent<DynamicObject>,
}

struct ComponentData {
    cancel_token: CancellationToken,
    workload: Option<ResolvedWorkload>,
    client: Option<KubeClient>,
    watchers: Vec<WatcherState>,
    /// Serialized event dispatch channel.
    event_tx: Option<mpsc::UnboundedSender<QueuedEvent>>,
}

#[derive(Clone)]
pub struct EventMonitor {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for EventMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl EventMonitor {
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
        }
    }

    /// Start a background serial consumer that reads from `event_rx` and
    /// dispatches to the guest one event at a time.
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
                        Some(q) => dispatch_event(&workload, &component_id, &q.id, &q.gvk, q.event).await,
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
        id: String,
        condition: Option<serde_json::Value>,
        cancel_token: CancellationToken,
        event_tx: mpsc::UnboundedSender<QueuedEvent>,
    ) {
        tokio::spawn(async move {
            let mut stream = Box::pin(watch(api, watcher::Config::default()));
            let gvk_str = format!("{}/{}/{}", gvk.group, gvk.version, gvk.kind);
            info!(component_id = %component_id, gvk = %gvk_str, id = %id, "Watcher started");
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(component_id = %component_id, gvk = %gvk_str, "Watcher cancelled");
                        break;
                    }
                    ev = stream.next() => match ev {
                        Some(Ok(e)) => {
                            // Evaluate jsonlogic condition on the host side
                            if let Some(ref cond) = condition
                                && !evaluate_condition_for_event(cond, &e)
                            {
                                debug!(component_id = %component_id, gvk = %gvk_str, id = %id, "Event filtered by condition");
                                continue;
                            }
                            let _ = event_tx.send(QueuedEvent {
                                id: id.clone(),
                                gvk: gvk.clone(),
                                event: e,
                            });
                        }
                        Some(Err(e)) => warn!(component_id = %component_id, error = %e, "Stream error"),
                        None => { info!(component_id = %component_id, gvk = %gvk_str, "Stream ended"); break; }
                    }
                }
            }
        });
    }
}

/// Parse a jsonlogic condition string into a serde_json::Value.
/// Returns None for empty/null conditions (match all).
fn parse_condition(raw: &str) -> Option<serde_json::Value> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "null" || trimmed == "true" {
        return None;
    }
    serde_json::from_str::<serde_json::Value>(trimmed).ok()
}

/// Evaluate a jsonlogic condition against a K8s watch event.
fn evaluate_condition_for_event(
    condition: &serde_json::Value,
    event: &WatchEvent<DynamicObject>,
) -> bool {
    let obj = match event {
        WatchEvent::Apply(o) | WatchEvent::InitApply(o) | WatchEvent::Delete(o) => o,
        _ => return false,
    };

    let data = match serde_json::to_value(obj) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "Failed to serialize DynamicObject for jsonlogic");
            return false;
        }
    };

    match apply(condition, &data) {
        Ok(v) => v.as_bool().unwrap_or(false),
        Err(e) => {
            warn!(error = %e, "jsonlogic evaluation failed");
            false
        }
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
    id: &str,
    gvk: &GroupVersionKind,
    event: WatchEvent<DynamicObject>,
) {
    let (action, obj) = match event {
        WatchEvent::Apply(p) | WatchEvent::InitApply(p) => (EventAction::Applied, p),
        WatchEvent::Delete(p) => (EventAction::Deleted, p),
        WatchEvent::Init | WatchEvent::InitDone => {
            return;
        }
    };

    let k8s = K8sEvent {
        group: gvk.group.clone(),
        version: gvk.version.clone(),
        kind: gvk.kind.clone(),
        name: obj.metadata.name.clone().unwrap_or_default(),
        namespace: obj.metadata.namespace.clone(),
        action,
    };

    debug!(component_id = %component_id, id = %id, gvk = %format!("{}/{}", k8s.group, k8s.kind), name = %k8s.name, "Dispatching");

    let Ok(mut store) = workload.new_store(component_id).await else {
        warn!(component_id = %component_id, "new_store failed");
        return;
    };
    let Ok(ip) = workload.instantiate_pre(component_id).await else {
        warn!(component_id = %component_id, "instantiate_pre failed");
        return;
    };
    let Ok(pre) = bindings::EventMonitorPre::new(ip) else {
        warn!(component_id = %component_id, "EventMonitorPre failed");
        return;
    };
    let Ok(proxy) = pre.instantiate_async(&mut store).await else {
        warn!(component_id = %component_id, "instantiate_async failed");
        return;
    };

    let handler = proxy.custom_event_monitor_handler();
    match store
        .run_concurrent(async move |accessor| {
            handler
                .call_handle_event(accessor, id.to_string(), k8s.clone())
                .await
        })
        .await
    {
        Ok(Ok(Ok(()))) => debug!(component_id = %component_id, id = %id, "Event handled"),
        Ok(Ok(Err(e))) => {
            warn!(component_id = %component_id, id = %id, error = %e, "Handler error")
        }
        Ok(Err(e)) => warn!(component_id = %component_id, id = %id, error = %e, "Call failed"),
        Err(e) => {
            warn!(component_id = %component_id, id = %id, error = %e, "run_concurrent failed")
        }
    }
}

fn cancel_all_watchers(data: &mut ComponentData) {
    for w in data.watchers.drain(..) {
        w.cancel_token.cancel();
    }
}

// ---------------------------------------------------------------------------
// WIT implementation: watcher interface
// ---------------------------------------------------------------------------

impl bindings::custom::event_monitor::watcher::Host for ActiveCtx<'_> {
    async fn create(
        &mut self,
        api_server_url: String,
        token: String,
    ) -> wasmtime::Result<Result<(), String>> {
        let Ok(plugin) = self.try_get_plugin::<EventMonitor>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let cid = self.component_id.as_ref().to_string();

        let kcfg = build_kube_config(&api_server_url, &token)
            .map_err(|e| wasmtime::Error::msg(format!("config: {e}")))?;
        let client = KubeClient::try_from(kcfg)
            .map_err(|e| wasmtime::Error::msg(format!("connect: {e}")))?;
        client
            .apiserver_version()
            .await
            .map_err(|e| wasmtime::Error::msg(format!("unreachable: {e}")))?;

        let mut lock = plugin.tracker.write().await;
        let Some(data) = lock.get_component_data_mut(&cid) else {
            return Ok(Err("not tracked".into()));
        };
        cancel_all_watchers(data);
        data.client = Some(client);

        info!(component_id = %cid, "K8s client connected");
        Ok(Ok(()))
    }

    async fn list_all_resources(
        &mut self,
    ) -> wasmtime::Result<Result<Vec<WatchableResource>, String>> {
        let Ok(plugin) = self.try_get_plugin::<EventMonitor>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let cid = self.component_id.as_ref().to_string();

        let client = {
            let lock = plugin.tracker.read().await;
            let Some(data) = lock.get_component_data(&cid) else {
                return Ok(Err("not tracked".into()));
            };
            let Some(c) = data.client.clone() else {
                return Ok(Err("not connected".into()));
            };
            c
        };

        let discovery = Discovery::new(client)
            .run()
            .await
            .map_err(|e| wasmtime::Error::msg(format!("discovery: {e}")))?;

        let mut resources = Vec::new();
        for group in discovery.groups() {
            let by_stability = group.resources_by_stability();
            for (ar, caps) in by_stability {
                if caps.supports_operation(verbs::WATCH) && caps.supports_operation(verbs::LIST) {
                    resources.push(WatchableResource {
                        group: ar.group.clone(),
                        version: ar.version.clone(),
                        kind: ar.kind.clone(),
                    });
                }
            }
        }

        info!(component_id = %cid, count = resources.len(), "Resources listed");
        Ok(Ok(resources))
    }

    async fn watch_resources(
        &mut self,
        rules: Vec<WatchRule>,
    ) -> wasmtime::Result<Result<(), String>> {
        if rules.is_empty() {
            return Ok(Err("no rules provided".into()));
        }

        let Ok(plugin) = self.try_get_plugin::<EventMonitor>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let cid = self.component_id.as_ref().to_string();

        // Cancel any existing watchers
        {
            let mut lock = plugin.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(&cid) {
                cancel_all_watchers(data);
            }
        }

        let (client, wl) = {
            let lock = plugin.tracker.read().await;
            let Some(data) = lock.get_component_data(&cid) else {
                return Ok(Err("not tracked".into()));
            };
            let Some(c) = data.client.clone() else {
                return Ok(Err("not connected".into()));
            };
            let Some(w) = data.workload.clone() else {
                return Ok(Err("no workload".into()));
            };
            (c, w)
        };

        // Create new serial event channel
        let (tx, rx) = mpsc::unbounded_channel();

        {
            let mut lock = plugin.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(&cid) {
                data.event_tx = Some(tx.clone());
            }
        }

        // Start a fresh serial consumer
        let child = {
            let lock = plugin.tracker.read().await;
            let Some(data) = lock.get_component_data(&cid) else {
                return Ok(Err("not tracked".into()));
            };
            data.cancel_token.child_token()
        };
        EventMonitor::start_event_consumer(wl, cid.clone(), child, rx);

        // Resolve only the group/version pairs referenced by the rules, instead of
        // running a full API discovery. Each unique pair costs a single request.
        let mut resolved: HashMap<(String, String), ApiGroup> = HashMap::new();
        for rule in &rules {
            let res = &rule.res;
            let key = (res.group.clone(), res.version.clone());
            if resolved.contains_key(&key) {
                continue;
            }
            let gv = GroupVersion::gv(&res.group, &res.version);
            match discovery::pinned_group(&client, &gv).await {
                Ok(group) => {
                    resolved.insert(key, group);
                }
                Err(e) => {
                    warn!(component_id = %cid, group = %res.group, version = %res.version, error = %e, "Discovery failed, skipping group version");
                }
            }
        }

        for rule in &rules {
            let res = &rule.res;
            let gvk = GroupVersionKind {
                group: res.group.clone(),
                version: res.version.clone(),
                kind: res.kind.clone(),
            };
            let gvk_str = format!("{}/{}/{}", gvk.group, gvk.version, gvk.kind);

            let Some(group) = resolved.get(&(res.group.clone(), res.version.clone())) else {
                warn!(component_id = %cid, gvk = %gvk_str, id = %rule.id, "Not found, skipping");
                continue;
            };
            let Some((ar, caps)) = group
                .versioned_resources(&res.version)
                .into_iter()
                .find(|(ar, _)| ar.kind == res.kind)
            else {
                warn!(component_id = %cid, gvk = %gvk_str, id = %rule.id, "Not found, skipping");
                continue;
            };
            if !caps.supports_operation(verbs::WATCH) {
                warn!(component_id = %cid, gvk = %gvk_str, id = %rule.id, "No watch support, skipping");
                continue;
            }

            let condition = parse_condition(&rule.condition);
            let api = if let Some(ns) = rule.namespace.as_deref().filter(|ns| !ns.is_empty())
                && caps.scope == Scope::Namespaced
            {
                Api::<DynamicObject>::namespaced_with(client.clone(), ns, &ar)
            } else {
                Api::<DynamicObject>::all_with(client.clone(), &ar)
            };
            let child = {
                let lock = plugin.tracker.read().await;
                let Some(data) = lock.get_component_data(&cid) else {
                    break;
                };
                data.cancel_token.child_token()
            };

            EventMonitor::spawn_watcher(
                cid.clone(),
                api,
                gvk,
                rule.id.clone(),
                condition,
                child.clone(),
                tx.clone(),
            );

            let mut lock = plugin.tracker.write().await;
            if let Some(data) = lock.get_component_data_mut(&cid) {
                data.watchers.push(WatcherState {
                    cancel_token: child,
                });
            }

            info!(component_id = %cid, gvk = %gvk_str, id = %rule.id, "Watcher created");
        }

        Ok(Ok(()))
    }

    async fn unwatch_resources(&mut self) -> wasmtime::Result<Result<(), String>> {
        let Ok(plugin) = self.try_get_plugin::<EventMonitor>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let cid = self.component_id.as_ref().to_string();
        let mut lock = plugin.tracker.write().await;
        let Some(data) = lock.get_component_data_mut(&cid) else {
            return Ok(Err("not tracked".into()));
        };
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
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> wash_runtime::wit::WitWorld {
        wash_runtime::wit::WitWorld {
            imports: HashSet::from([WitInterface::from(
                "custom:event-monitor/watcher,types@0.2.0",
            )]),
            exports: HashSet::from([WitInterface::from("custom:event-monitor/handler@0.2.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        if interfaces.get("custom", "event-monitor", &[]).is_none() {
            return Ok(());
        }

        bindings::custom::event_monitor::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::event_monitor::watcher::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        let WorkloadItem::Component(ch) = item else {
            return Ok(());
        };

        debug!(component_id = ch.id(), "EventMonitor bound");
        self.tracker.write().await.add_component(
            ch,
            ComponentData {
                cancel_token: CancellationToken::new(),
                workload: None,
                client: None,
                watchers: Vec::new(),
                event_tx: None,
            },
        );
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        workload: &ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        if let Some(data) = self
            .tracker
            .write()
            .await
            .get_component_data_mut(component_id)
        {
            data.workload = Some(workload.clone());
        }
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(
                workload_id,
                |_| async {},
                |mut data: ComponentData| async move {
                    cancel_all_watchers(&mut data);
                },
            )
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() {
        assert_eq!(EventMonitor::new().id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_imports() {
        let w = EventMonitor::new().world();
        assert!(
            w.imports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "event-monitor")
        );
    }

    #[test]
    fn test_world_exports() {
        let w = EventMonitor::new().world();
        assert!(
            w.exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "event-monitor")
        );
    }

    #[test]
    fn test_parse_condition_empty() {
        assert!(parse_condition("").is_none());
        assert!(parse_condition("  ").is_none());
        assert!(parse_condition("null").is_none());
        assert!(parse_condition("true").is_none());
    }

    #[test]
    fn test_parse_condition_valid() {
        let c = parse_condition(r#"{"==": [{"var": "type"}, "Normal"]}"#);
        assert!(c.is_some());
    }
}
