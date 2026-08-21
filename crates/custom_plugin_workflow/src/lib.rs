//! # Workflow Host Plugin
//!
//! Integrates the `acts` workflow engine into wasmCloud.
//! Host manages workflow lifecycle, guest receives lifecycle events.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::time::Duration;

use acts::Channel;
use acts::{Engine, Vars, Workflow};
use acts::{Event, Message, MessageState};
use async_trait::async_trait;
use tokio::sync::{Notify, RwLock, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::{ResolvedWorkload, WorkloadItem};
use wash_runtime::plugin::{HostPlugin, WitInterfaces, WorkloadTracker};
use wash_runtime::wit::WitInterface;

mod bindings {
    wasmtime::component::bindgen!({
        world: "workflow",
        imports: { default: async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

use bindings::custom::workflow::manager::ProcInfo;
use bindings::custom::workflow::types::VarPair;

const PLUGIN_ID: &str = "workflow";

/// Upper bound for waiting on a pid -> exec-id mapping. `start` records the
/// mapping before returning, so this timeout only fires on an unexpected
/// invariant violation; it exists so the consumer never hangs.
const EXEC_ID_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Queued callback types
// ---------------------------------------------------------------------------

enum CallbackKind {
    Start,
    Message,
    Complete,
    Error(String),
}

struct QueuedEvent {
    kind: CallbackKind,
    pid: String,
    inputs: Vec<VarPair>,
    outputs: Vec<VarPair>,
}

type EventTx = mpsc::UnboundedSender<QueuedEvent>;
type EventRx = mpsc::UnboundedReceiver<QueuedEvent>;

struct ComponentData {
    cancel_token: CancellationToken,
    workload: Option<ResolvedWorkload>,
    engine: Option<Engine>,
    event_tx: Option<EventTx>,
    event_rx: Option<EventRx>,
    /// Hold Arc<Channel> references so callbacks stay alive.
    _chan_start: Option<Arc<Channel>>,
    _chan_message: Option<Arc<Channel>>,
    _chan_complete: Option<Arc<Channel>>,
    _chan_error: Option<Arc<Channel>>,
    /// Maps a process id (pid) to the caller-assigned exec-id, populated in
    /// `start` so lifecycle callbacks can report the exec-id without relying on
    /// the guest embedding it in workflow vars.
    exec_id_by_pid: Arc<StdRwLock<HashMap<String, String>>>,
    /// Wakes the event consumer after `start` records a pid -> exec-id mapping.
    /// `start` always records the mapping before returning, so the consumer can
    /// wait on this instead of racing the write from the channel callbacks.
    exec_id_notify: Arc<Notify>,
}

#[derive(Clone)]
pub struct WorkflowPlugin {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for WorkflowPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowPlugin {
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
        }
    }

    fn start_event_consumer(
        workload: ResolvedWorkload,
        component_id: String,
        cancel_token: CancellationToken,
        event_rx: EventRx,
        exec_id_map: Arc<StdRwLock<HashMap<String, String>>>,
        exec_id_notify: Arc<Notify>,
    ) {
        tokio::spawn(async move {
            info!(component_id = %component_id, "Workflow event consumer started");
            let mut rx = event_rx;
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        info!(component_id = %component_id, "Workflow event consumer cancelled");
                        break;
                    }
                    msg = rx.recv() => match msg {
                        Some(q) => dispatch_event(
                            &workload,
                            &component_id,
                            q,
                            exec_id_map.clone(),
                            exec_id_notify.clone(),
                        )
                        .await,
                        None => { info!(component_id = %component_id, "Event channel closed"); break; }
                    }
                }
            }
        });
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn vars_from_pairs(pairs: &[VarPair]) -> Vars {
    let mut vars = Vars::new();
    for p in pairs {
        vars.insert(p.key.clone(), serde_json::Value::String(p.value.clone()));
    }
    vars
}

fn state_str(state: MessageState) -> &'static str {
    match state {
        MessageState::None => "none",
        MessageState::Created => "created",
        MessageState::Completed => "completed",
        MessageState::Submitted => "submitted",
        MessageState::Backed => "backed",
        MessageState::Cancelled => "cancelled",
        MessageState::Aborted => "aborted",
        MessageState::Skipped => "skipped",
        MessageState::Error => "error",
        MessageState::Removed => "removed",
    }
}

fn msg_to_pairs(inputs: &Vars) -> Vec<VarPair> {
    inputs
        .iter()
        .map(|(k, v)| VarPair {
            key: k.clone(),
            value: match &v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            },
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Resolves the caller-assigned exec-id for `pid`, waiting (bounded) until
/// `start` records it. `start` always records the mapping before returning, so
/// the wait completes promptly; the timeout guards against an invariant
/// violation that would otherwise hang the consumer.
async fn resolve_exec_id(
    exec_id_map: &Arc<StdRwLock<HashMap<String, String>>>,
    exec_id_notify: &Arc<Notify>,
    pid: &str,
) -> String {
    loop {
        if let Some(exec_id) = exec_id_map.read().ok().and_then(|m| m.get(pid).cloned()) {
            return exec_id;
        }
        if tokio::time::timeout(EXEC_ID_WAIT_TIMEOUT, exec_id_notify.notified())
            .await
            .is_err()
        {
            warn!(pid = %pid, "timed out waiting for exec-id mapping, using empty exec-id");
            return String::new();
        }
    }
}

async fn dispatch_event(
    workload: &ResolvedWorkload,
    component_id: &str,
    q: QueuedEvent,
    exec_id_map: Arc<StdRwLock<HashMap<String, String>>>,
    exec_id_notify: Arc<Notify>,
) {
    let Ok(mut store) = workload.new_store(component_id).await else {
        warn!(component_id = %component_id, "new_store failed");
        return;
    };
    let Ok(ip) = workload.instantiate_pre(component_id).await else {
        warn!(component_id = %component_id, "instantiate_pre failed");
        return;
    };
    let Ok(pre) = bindings::WorkflowPre::new(ip) else {
        warn!(component_id = %component_id, "WorkflowPre failed");
        return;
    };
    let Ok(proxy) = pre.instantiate_async(&mut store).await else {
        warn!(component_id = %component_id, "instantiate_async failed");
        return;
    };

    let handler = proxy.custom_workflow_handler();

    let pid_for_log = q.pid.clone();
    let exec_id = resolve_exec_id(&exec_id_map, &exec_id_notify, &q.pid).await;
    let result = match q.kind {
        CallbackKind::Start => handler.call_on_start(&mut store, &exec_id, &q.pid).await,
        CallbackKind::Message => {
            handler
                .call_on_message(&mut store, &exec_id, &q.pid, &q.inputs)
                .await
        }
        CallbackKind::Complete => {
            handler
                .call_on_complete(&mut store, &exec_id, &q.pid, &q.outputs)
                .await
        }
        CallbackKind::Error(err) => {
            handler
                .call_on_error(&mut store, &exec_id, &q.pid, &err)
                .await
        }
    };

    match result {
        Ok(Ok(())) => debug!(component_id = %component_id, pid = %pid_for_log, "Event handled"),
        Ok(Err(e)) => {
            warn!(component_id = %component_id, pid = %pid_for_log, error = %e, "Handler error")
        }
        Err(e) => {
            warn!(component_id = %component_id, pid = %pid_for_log, error = %e, "Call failed")
        }
    }
}

// ---------------------------------------------------------------------------
// WIT manager implementation
// ---------------------------------------------------------------------------

impl bindings::custom::workflow::manager::Host for ActiveCtx<'_> {
    async fn start(
        &mut self,
        exec_id: String,
        workflow_def: String,
        vars: Vec<VarPair>,
    ) -> wasmtime::Result<Result<String, String>> {
        if workflow_def.trim().is_empty() {
            return Ok(Err("workflow definition is empty".into()));
        }

        let Ok(plugin) = self.try_get_plugin::<WorkflowPlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let cid = self.component_id.as_ref().to_string();

        let engine = {
            let lock = plugin.tracker.read().await;
            let Some(data) = lock.get_component_data(&cid) else {
                return Ok(Err("not tracked".into()));
            };
            let Some(eng) = data.engine.clone() else {
                return Ok(Err("engine not initialized".into()));
            };
            eng
        };

        let wf = match Workflow::from_yml(&workflow_def) {
            Ok(w) => w,
            Err(e) => return Ok(Err(format!("invalid workflow yml: {e}"))),
        };

        if wf.id.trim().is_empty() {
            return Ok(Err("workflow yml must include an 'id' field".into()));
        }
        let mid = wf.id.clone();

        info!(component_id = %cid, mid = %mid, "Deploying workflow");

        if let Err(e) = engine.executor().model().deploy(&wf, None) {
            return Ok(Err(format!("deploy failed: {e}")));
        }

        let proc_vars = vars_from_pairs(&vars);
        let pid = match engine.executor().proc().start(&wf.id, proc_vars) {
            Ok(p) => p,
            Err(e) => return Ok(Err(format!("start failed: {e}"))),
        };

        // Record exec-id -> pid so lifecycle callbacks can echo it back without
        // relying on the guest embedding it in the workflow vars. The notify
        // wakes the consumer, which resolves the mapping at dispatch time.
        {
            let lock = plugin.tracker.read().await;
            if let Some(data) = lock.get_component_data(&cid)
                && let Ok(mut m) = data.exec_id_by_pid.write()
            {
                m.insert(pid.clone(), exec_id.clone());
                data.exec_id_notify.notify_one();
            }
        }

        info!(component_id = %cid, pid = %pid, mid = %mid, exec_id = %exec_id, "Workflow started");

        Ok(Ok(pid))
    }

    async fn list_processes(&mut self) -> wasmtime::Result<Result<Vec<ProcInfo>, String>> {
        let Ok(plugin) = self.try_get_plugin::<WorkflowPlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let cid = self.component_id.as_ref().to_string();

        let engine = {
            let lock = plugin.tracker.read().await;
            let Some(data) = lock.get_component_data(&cid) else {
                return Ok(Err("not tracked".into()));
            };
            let Some(eng) = data.engine.clone() else {
                return Ok(Err("engine not initialized".into()));
            };
            eng
        };

        match engine.executor().proc().list(&acts::query::Query::new()) {
            Ok(page) => {
                let infos: Vec<ProcInfo> = page
                    .rows
                    .iter()
                    .map(|p| ProcInfo {
                        pid: p.id.clone(),
                        mid: p.mid.clone(),
                        state: p.state.clone(),
                        start_time: p.start_time,
                        end_time: p.end_time,
                    })
                    .collect();
                Ok(Ok(infos))
            }
            Err(e) => Ok(Err(format!("list failed: {e}"))),
        }
    }

    async fn process_status(&mut self, pid: String) -> wasmtime::Result<Result<ProcInfo, String>> {
        let Ok(plugin) = self.try_get_plugin::<WorkflowPlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let cid = self.component_id.as_ref().to_string();

        let engine = {
            let lock = plugin.tracker.read().await;
            let Some(data) = lock.get_component_data(&cid) else {
                return Ok(Err("not tracked".into()));
            };
            let Some(eng) = data.engine.clone() else {
                return Ok(Err("engine not initialized".into()));
            };
            eng
        };

        match engine.executor().proc().get(&pid) {
            Ok(info) => Ok(Ok(ProcInfo {
                pid: info.id,
                mid: info.mid,
                state: info.state,
                start_time: info.start_time,
                end_time: info.end_time,
            })),
            Err(e) => Ok(Err(format!("status query failed: {e}"))),
        }
    }

    async fn complete_task(
        &mut self,
        pid: String,
        nid: String,
        outputs: Vec<VarPair>,
    ) -> wasmtime::Result<Result<(), String>> {
        let Ok(plugin) = self.try_get_plugin::<WorkflowPlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let cid = self.component_id.as_ref().to_string();

        let engine = {
            let lock = plugin.tracker.read().await;
            let Some(data) = lock.get_component_data(&cid) else {
                return Ok(Err("not tracked".into()));
            };
            data.engine
                .clone()
                .ok_or_else(|| wasmtime::Error::msg("not initialized"))?
        };

        let vars = vars_from_pairs(&outputs);
        engine
            .executor()
            .act()
            .complete(&pid, &nid, vars)
            .map_err(|e| wasmtime::Error::msg(format!("complete_task: {e}")))?;

        info!(component_id = %cid, pid = %pid, nid = %nid, "Task completed");
        Ok(Ok(()))
    }
}

impl<'a> bindings::custom::workflow::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// HostPlugin trait
// ---------------------------------------------------------------------------

#[async_trait]
impl HostPlugin for WorkflowPlugin {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> wash_runtime::wit::WitWorld {
        wash_runtime::wit::WitWorld {
            imports: HashSet::from([WitInterface::from("custom:workflow/manager,types@0.2.0")]),
            exports: HashSet::from([WitInterface::from("custom:workflow/handler@0.2.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        if interfaces.get("custom", "workflow", &[]).is_none() {
            return Ok(());
        }

        bindings::custom::workflow::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::workflow::manager::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        let WorkloadItem::Component(ch) = item else {
            return Ok(());
        };

        // Create the acts engine
        let engine = Engine::new()
            .start()
            .map_err(|e| anyhow::anyhow!("Engine start failed: {e}"))?;

        // Create the event channel
        let (tx, rx) = mpsc::unbounded_channel::<QueuedEvent>();

        // Maps pid -> exec-id, populated by `start`. Resolved by the event
        // consumer at dispatch time so the channel callbacks never race the
        // write. `exec_id_notify` wakes the consumer once the mapping lands.
        let exec_id_map = Arc::new(StdRwLock::new(HashMap::new()));
        let exec_id_notify = Arc::new(Notify::new());

        // Register channel callbacks
        let chan_start = engine.channel();
        let chan_message = engine.channel();
        let chan_complete = engine.channel();
        let chan_error = engine.channel();

        {
            let tx = tx.clone();
            chan_start.on_start(move |e: &Event<Message>| {
                let msg = e.inner();
                let _ = tx.send(QueuedEvent {
                    kind: CallbackKind::Start,
                    pid: msg.pid.clone(),
                    inputs: msg_to_pairs(&msg.inputs),
                    outputs: msg_to_pairs(&msg.outputs),
                });
            });
        }
        {
            let tx = tx.clone();
            chan_message.on_message(move |e: &Event<Message>| {
                let msg = e.inner();
                let _ = tx.send(QueuedEvent {
                    kind: CallbackKind::Message,
                    pid: msg.pid.clone(),
                    inputs: msg_to_pairs(&msg.inputs),
                    outputs: msg_to_pairs(&msg.outputs),
                });
            });
        }
        {
            let tx = tx.clone();
            chan_complete.on_complete(move |e: &Event<Message>| {
                let msg = e.inner();
                let _ = tx.send(QueuedEvent {
                    kind: CallbackKind::Complete,
                    pid: msg.pid.clone(),
                    inputs: msg_to_pairs(&msg.inputs),
                    outputs: msg_to_pairs(&msg.outputs),
                });
            });
        }
        {
            let tx = tx.clone();
            chan_error.on_error(move |e: &Event<Message>| {
                let msg = e.inner();
                let _ = tx.send(QueuedEvent {
                    kind: CallbackKind::Error(format!("state={}", state_str(msg.state))),
                    pid: msg.pid.clone(),
                    inputs: msg_to_pairs(&msg.inputs),
                    outputs: msg_to_pairs(&msg.outputs),
                });
            });
        }

        debug!(component_id = ch.id(), "Workflow plugin bound");

        self.tracker.write().await.add_component(
            ch,
            ComponentData {
                cancel_token: CancellationToken::new(),
                workload: None,
                engine: Some(engine),
                event_tx: Some(tx),
                event_rx: Some(rx),
                _chan_start: Some(chan_start),
                _chan_message: Some(chan_message),
                _chan_complete: Some(chan_complete),
                _chan_error: Some(chan_error),
                exec_id_by_pid: exec_id_map,
                exec_id_notify,
            },
        );

        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        workload: &ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        let mut lock = self.tracker.write().await;
        if let Some(data) = lock.get_component_data_mut(component_id) {
            let first_time = data.workload.is_none();
            data.workload = Some(workload.clone());
            // Start event consumer on first resolution
            if first_time && let Some(rx) = data.event_rx.take() {
                let child = data.cancel_token.child_token();
                WorkflowPlugin::start_event_consumer(
                    workload.clone(),
                    component_id.into(),
                    child,
                    rx,
                    data.exec_id_by_pid.clone(),
                    data.exec_id_notify.clone(),
                );
            }
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
                    data.event_tx.take();
                    data.cancel_token.cancel();
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
        assert_eq!(WorkflowPlugin::new().id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_imports() {
        let w = WorkflowPlugin::new().world();
        assert!(
            w.imports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "workflow")
        );
    }

    #[test]
    fn test_world_exports() {
        let w = WorkflowPlugin::new().world();
        assert!(
            w.exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "workflow")
        );
    }

    #[test]
    fn test_vars_from_pairs() {
        let pairs = vec![VarPair {
            key: "a".into(),
            value: "1".into(),
        }];
        let vars = vars_from_pairs(&pairs);
        assert_eq!(vars.get::<String>("a"), Some("1".into()));
    }

    #[tokio::test]
    async fn test_resolve_exec_id_immediate() {
        let map = Arc::new(StdRwLock::new(HashMap::new()));
        let notify = Arc::new(Notify::new());
        map.write().unwrap().insert("p1".into(), "exec-1".into());
        assert_eq!(resolve_exec_id(&map, &notify, "p1").await, "exec-1");
    }

    #[tokio::test]
    async fn test_resolve_exec_id_waits_for_mapping() {
        let map = Arc::new(StdRwLock::new(HashMap::new()));
        let notify = Arc::new(Notify::new());

        let map_task = map.clone();
        let notify_task = notify.clone();
        let resolver =
            tokio::spawn(async move { resolve_exec_id(&map_task, &notify_task, "p1").await });

        // Let the resolver start waiting, then record the mapping and notify.
        // The test is deterministic either way: if the resolver has not started
        // awaiting yet, notify_one stores the notification.
        tokio::time::sleep(Duration::from_millis(50)).await;
        map.write().unwrap().insert("p1".into(), "exec-1".into());
        notify.notify_one();

        assert_eq!(resolver.await.unwrap(), "exec-1");
    }
}
