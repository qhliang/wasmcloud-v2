//! # Crontab Host Plugin
//!
//! This plugin provides cron-based periodic and one-shot callbacks to WASM
//! components. It reads schedule configuration from interface config or allows
//! the guest to dynamically manage schedules at runtime via the `scheduler`
//! interface.
//!
//! ## Configuration
//!
//! Interface config entries with format: `name=<name>;cron=<expr>` or
//! `name=<name>;delay-ms=<ms>`. Use config keys prefixed with `schedule.`.
//!
//! Example (YAML):
//! ```yaml
//! custom:crontab:
//!   config:
//!     schedule.tick: "name=tick;cron=*/30 * * * *"
//!     schedule.cleanup: "name=cleanup;cron=0 0 * * *"
//!     schedule.init: "name=init;delay-ms=5000"
//! ```
//!
//! ## Guest Export
//!
//! The guest component must export `custom:crontab/handler@0.1.0` with:
//! ```wit
//! handle-tick: func(name: string) -> result<_, string>;
//! ```

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use chrono::Utc;
use cron::Schedule;
use tokio::sync::RwLock;
use tracing::{debug, instrument, warn};

use anyhow::Context as _;

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::{ResolvedWorkload, WorkloadItem};
use wash_runtime::plugin::HostPlugin;
use wash_runtime::plugin::WorkloadTracker;
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "crontab",
        imports: { default: async | trappable | tracing },
        exports: { default: async | tracing },
    });
}

use bindings::custom::crontab::types::ScheduleError;

const PLUGIN_ID: &str = "crontab";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A parsed schedule entry
#[derive(Clone, Debug)]
enum ScheduleKind {
    /// Periodic cron: 5-field expression like `*/30 * * * *`
    Cron(Box<Schedule>),
    /// One-shot delay in milliseconds
    Delay(u64),
}

/// Per-component data tracked by the plugin
struct ComponentData {
    /// Token that cancels ALL tasks for this component
    cancel_token: tokio_util::sync::CancellationToken,
    /// Active schedule names (for listing / dedup)
    names: HashSet<String>,
    /// Parsed schedules (stored during bind, used during resolve)
    schedules: Vec<(String, ScheduleKind)>,
    /// Resolved workload — set during on_workload_resolved, used to spawn runtime tasks
    workload: Option<ResolvedWorkload>,
    /// Per-schedule cancel tokens (child of cancel_token) for individual removal
    task_tokens: HashMap<String, tokio_util::sync::CancellationToken>,
}

/// The crontab host plugin
#[derive(Clone)]
pub struct Crontab {
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
}

impl Default for Crontab {
    fn default() -> Self {
        Self::new()
    }
}

impl Crontab {
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
        }
    }
}

// ---------------------------------------------------------------------------
// WIT types::Host (schedule-error is a variant, no host methods needed)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::crontab::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT scheduler::Host — runtime schedule management by the guest
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::crontab::scheduler::Host for ActiveCtx<'a> {
    #[instrument(skip_all, fields(name = %name, cron = %cron_expression))]
    async fn schedule(
        &mut self,
        name: String,
        cron_expression: String,
    ) -> wasmtime::Result<Result<(), ScheduleError>> {
        let schedule = match Schedule::from_str(&format!("0 {cron_expression} *")) {
            Ok(s) => s,
            Err(e) => {
                return Ok(Err(ScheduleError::InvalidExpression(format!(
                    "invalid cron expression '{cron_expression}': {e}"
                ))));
            }
        };

        let Some(plugin) = self.get_plugin::<Crontab>(PLUGIN_ID) else {
            return Ok(Err(ScheduleError::Internal(
                "crontab plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        // Check for duplicate name, then record it + spawn the task
        {
            let mut lock = plugin.tracker.write().await;
            let data = match lock.get_component_data_mut(&component_id) {
                Some(d) => d,
                None => {
                    return Ok(Err(ScheduleError::Internal(
                        "component not tracked — ensure handler is exported".to_string(),
                    )));
                }
            };

            if data.names.contains(&name) {
                return Ok(Err(ScheduleError::AlreadyExists(format!(
                    "schedule '{name}' already exists"
                ))));
            }

            let workload = match &data.workload {
                Some(w) => w.clone(),
                None => {
                    return Ok(Err(ScheduleError::Internal(
                        "workload not resolved yet".to_string(),
                    )));
                }
            };

            let task_token = data.cancel_token.child_token();
            data.names.insert(name.clone());
            data.task_tokens.insert(name.clone(), task_token.clone());

            debug!(
                component_id = %component_id,
                name = %name,
                "Runtime cron schedule added, spawning task"
            );

            spawn_cron_task(workload, component_id.clone(), name, schedule, task_token);
        }

        Ok(Ok(()))
    }

    #[instrument(skip_all, fields(name = %name))]
    async fn schedule_delay(
        &mut self,
        name: String,
        delay_ms: u64,
    ) -> wasmtime::Result<Result<(), ScheduleError>> {
        let Some(plugin) = self.get_plugin::<Crontab>(PLUGIN_ID) else {
            return Ok(Err(ScheduleError::Internal(
                "crontab plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        // Check for duplicate name, then record it + spawn the task
        {
            let mut lock = plugin.tracker.write().await;
            let data = match lock.get_component_data_mut(&component_id) {
                Some(d) => d,
                None => {
                    return Ok(Err(ScheduleError::Internal(
                        "component not tracked — ensure handler is exported".to_string(),
                    )));
                }
            };

            if data.names.contains(&name) {
                return Ok(Err(ScheduleError::AlreadyExists(format!(
                    "schedule '{name}' already exists"
                ))));
            }

            let workload = match &data.workload {
                Some(w) => w.clone(),
                None => {
                    return Ok(Err(ScheduleError::Internal(
                        "workload not resolved yet".to_string(),
                    )));
                }
            };

            let task_token = data.cancel_token.child_token();
            data.names.insert(name.clone());
            data.task_tokens.insert(name.clone(), task_token.clone());

            debug!(
                component_id = %component_id,
                name = %name,
                delay_ms,
                "Runtime delay schedule added, spawning task"
            );

            spawn_delay_task(workload, component_id.clone(), name, delay_ms, task_token);
        }

        Ok(Ok(()))
    }

    #[instrument(skip_all, fields(name = %name))]
    async fn remove(&mut self, name: String) -> wasmtime::Result<Result<(), ScheduleError>> {
        let Some(plugin) = self.get_plugin::<Crontab>(PLUGIN_ID) else {
            return Ok(Err(ScheduleError::Internal(
                "crontab plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let mut lock = plugin.tracker.write().await;
        if let Some(data) = lock.get_component_data_mut(&component_id) {
            if data.names.remove(&name) {
                // Cancel the per-schedule task token if present
                if let Some(task_token) = data.task_tokens.remove(&name) {
                    task_token.cancel();
                }
                debug!(component_id = %component_id, name = %name, "Schedule removed and task cancelled");
                Ok(Ok(()))
            } else {
                Ok(Err(ScheduleError::NotFound(format!(
                    "schedule '{name}' not found"
                ))))
            }
        } else {
            Ok(Err(ScheduleError::Internal(
                "component not tracked".to_string(),
            )))
        }
    }

    async fn list_schedules(&mut self) -> wasmtime::Result<Result<Vec<String>, ScheduleError>> {
        let Some(plugin) = self.get_plugin::<Crontab>(PLUGIN_ID) else {
            return Ok(Err(ScheduleError::Internal(
                "crontab plugin not available".to_string(),
            )));
        };

        let component_id = self.component_id.as_ref().to_string();

        let lock = plugin.tracker.read().await;
        if let Some(data) = lock.get_component_data(&component_id) {
            let names: Vec<String> = data.names.iter().cloned().collect();
            Ok(Ok(names))
        } else {
            Ok(Ok(vec![]))
        }
    }
}

// ---------------------------------------------------------------------------
// Config parsing
// ---------------------------------------------------------------------------

/// Parse a config string like `name=tick;cron=*/30 * * * *` or `name=init;delay-ms=5000`
fn parse_schedule_config(value: &str) -> Option<(String, ScheduleKind)> {
    let mut name = None;
    let mut cron_expr = None;
    let mut delay_ms = None;

    for pair in value.split(';') {
        let pair = pair.trim();
        if let Some((k, v)) = pair.split_once('=') {
            match k.trim() {
                "name" => name = Some(v.trim().to_string()),
                "cron" => cron_expr = Some(v.trim().to_string()),
                "delay-ms" => delay_ms = Some(v.trim().to_string()),
                _ => {}
            }
        }
    }

    let name = name?;

    if let Some(expr) = cron_expr {
        // Normalize 5-field cron (min hour dom month dow) to 7-field (sec min hour dom month dow year)
        let normalized = match expr.split_whitespace().count() {
            5 => format!("0 {expr} *"),
            _ => expr,
        };
        let schedule = Schedule::from_str(&normalized).ok()?;
        Some((name, ScheduleKind::Cron(Box::new(schedule))))
    } else if let Some(ms) = delay_ms {
        let ms: u64 = ms.parse().ok()?;
        Some((name, ScheduleKind::Delay(ms)))
    } else {
        None
    }
}

/// Extract all schedules from interface config values.
fn extract_schedules(config: &HashMap<String, String>) -> Vec<(String, ScheduleKind)> {
    let mut schedules = Vec::new();

    for (key, value) in config {
        if !key.starts_with("schedule") && !value.contains("cron=") && !value.contains("delay-ms=")
        {
            continue;
        }

        if let Some((name, kind)) = parse_schedule_config(value) {
            debug!(key = %key, name = %name, "Parsed schedule from config");
            schedules.push((name, kind));
        } else {
            debug!(key = %key, value = %value, "Failed to parse schedule config");
        }
    }

    schedules
}

// ---------------------------------------------------------------------------
// Cron scheduling helpers
// ---------------------------------------------------------------------------

/// Compute the duration until the next cron fire time from now.
fn next_cron_delay(schedule: &Schedule) -> Option<std::time::Duration> {
    let now = chrono::DateTime::<Utc>::from(std::time::SystemTime::now());
    let next = schedule.after(&now).next()?;
    let diff = next - now;
    diff.to_std().ok()
}

/// Spawn a cron scheduling loop for a single schedule entry.
fn spawn_cron_task(
    workload: ResolvedWorkload,
    component_id: String,
    name: String,
    schedule: Schedule,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            let delay = match next_cron_delay(&schedule) {
                Some(d) => d,
                None => {
                    warn!(
                        component_id = %component_id,
                        name = %name,
                        "No next cron fire time, stopping"
                    );
                    break;
                }
            };

            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(
                        component_id = %component_id,
                        name = %name,
                        "Cron task cancelled"
                    );
                    break;
                }
                _ = tokio::time::sleep(delay) => {
                    debug!(
                        component_id = %component_id,
                        name = %name,
                        "Cron tick firing"
                    );

                    let mut store = match workload.new_store(&component_id).await {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(component_id = %component_id, "Failed to create store: {e}");
                            continue;
                        }
                    };

                    let instance_pre = match workload.instantiate_pre(&component_id).await {
                        Ok(pre) => pre,
                        Err(e) => {
                            warn!(component_id = %component_id, "Failed to instantiate_pre: {e}");
                            continue;
                        }
                    };

                    let pre = match bindings::CrontabPre::new(instance_pre) {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(component_id = %component_id, "Failed to create CrontabPre: {e}");
                            continue;
                        }
                    };

                    let proxy = match pre.instantiate_async(&mut store).await {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(component_id = %component_id, "Failed to instantiate: {e}");
                            continue;
                        }
                    };

                    match proxy
                        .custom_crontab_handler()
                        .call_handle_tick(&mut store, &name)
                        .await
                    {
                        Ok(Ok(())) => {
                            debug!(
                                component_id = %component_id,
                                name = %name,
                                "Tick handled successfully"
                            );
                        }
                        Ok(Err(e)) => {
                            warn!(
                                component_id = %component_id,
                                name = %name,
                                error = %e,
                                "Tick handler returned error"
                            );
                        }
                        Err(e) => {
                            warn!(
                                component_id = %component_id,
                                name = %name,
                                error = %e,
                                "Tick handler call failed"
                            );
                        }
                    }
                }
            }
        }
    });
}

/// Spawn a one-shot delay task.
fn spawn_delay_task(
    workload: ResolvedWorkload,
    component_id: String,
    name: String,
    delay_ms: u64,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!(
                    component_id = %component_id,
                    name = %name,
                    "Delay task cancelled before firing"
                );
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(delay_ms)) => {
                debug!(
                    component_id = %component_id,
                    name = %name,
                    delay_ms,
                    "Delay tick firing"
                );

                let mut store = match workload.new_store(&component_id).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(component_id = %component_id, "Failed to create store: {e}");
                        return;
                    }
                };

                let instance_pre = match workload.instantiate_pre(&component_id).await {
                    Ok(pre) => pre,
                    Err(e) => {
                        warn!(component_id = %component_id, "Failed to instantiate_pre: {e}");
                        return;
                    }
                };

                let pre = match bindings::CrontabPre::new(instance_pre) {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(component_id = %component_id, "Failed to create CrontabPre: {e}");
                        return;
                    }
                };

                let proxy = match pre.instantiate_async(&mut store).await {
                    Ok(p) => p,
                    Err(e) => {
                        warn!(component_id = %component_id, "Failed to instantiate: {e}");
                        return;
                    }
                };

                match proxy
                    .custom_crontab_handler()
                    .call_handle_tick(&mut store, &name)
                    .await
                {
                    Ok(Ok(())) => {
                        debug!(
                            component_id = %component_id,
                            name = %name,
                            "Delay tick handled successfully"
                        );
                    }
                    Ok(Err(e)) => {
                        warn!(
                            component_id = %component_id,
                            name = %name,
                            error = %e,
                            "Delay tick handler returned error"
                        );
                    }
                    Err(e) => {
                        warn!(
                            component_id = %component_id,
                            name = %name,
                            error = %e,
                            "Delay tick handler call failed"
                        );
                    }
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// HostPlugin implementation
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl HostPlugin for Crontab {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from("custom:crontab/scheduler,types@0.1.0")]),
            exports: HashSet::from([WitInterface::from("custom:crontab/handler@0.1.0")]),
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        // Only handle crontab interfaces
        let Some(interface) = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "crontab")
        else {
            return Ok(());
        };

        // Add scheduler imports to linker
        bindings::custom::crontab::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::crontab::scheduler::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;

        // Only track components (not services)
        let WorkloadItem::Component(component_handle) = item else {
            return Ok(());
        };

        // Check if this component exports the handler interface
        let has_handler = component_handle
            .world()
            .exports
            .iter()
            .any(|i| i.namespace == "custom" && i.package == "crontab");

        if has_handler {
            debug!(
                component_id = component_handle.id(),
                "Tracking component for crontab callbacks"
            );

            // Parse schedules from interface config
            let schedules = extract_schedules(&interface.config);
            let schedule_names: HashSet<String> =
                schedules.iter().map(|(n, _)| n.clone()).collect();

            self.tracker.write().await.add_component(
                component_handle,
                ComponentData {
                    cancel_token: tokio_util::sync::CancellationToken::new(),
                    names: schedule_names,
                    schedules,
                    workload: None,
                    task_tokens: HashMap::new(),
                },
            );
        }

        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        workload: &ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        // Get cancel token and stored schedules from tracker
        let (cancel_token, schedules) = {
            let mut lock = self.tracker.write().await;
            match lock.get_component_data_mut(component_id) {
                Some(data) => {
                    data.workload = Some(workload.clone());
                    (data.cancel_token.clone(), data.schedules.clone())
                }
                None => return Ok(()),
            }
        };

        if schedules.is_empty() {
            return Ok(());
        }

        // Validate that the component exports the handler interface
        let instance_pre = workload.instantiate_pre(component_id).await?;
        let _pre = bindings::CrontabPre::new(instance_pre)
            .map_err(anyhow::Error::from)
            .context("failed to instantiate crontab pre")?;

        let workload = workload.clone();
        let component_id_owned = component_id.to_string();

        for (name, kind) in schedules {
            // Create a child cancel token per schedule so it can be removed individually
            let task_token = cancel_token.child_token();
            {
                let mut lock = self.tracker.write().await;
                if let Some(data) = lock.get_component_data_mut(&component_id_owned) {
                    data.task_tokens.insert(name.clone(), task_token.clone());
                }
            }

            match kind {
                ScheduleKind::Cron(schedule) => {
                    debug!(
                        component_id = %component_id_owned,
                        name = %name,
                        "Spawning cron task"
                    );
                    spawn_cron_task(
                        workload.clone(),
                        component_id_owned.clone(),
                        name,
                        *schedule,
                        task_token,
                    );
                }
                ScheduleKind::Delay(delay_ms) => {
                    debug!(
                        component_id = %component_id_owned,
                        name = %name,
                        delay_ms,
                        "Spawning delay task"
                    );
                    spawn_delay_task(
                        workload.clone(),
                        component_id_owned.clone(),
                        name,
                        delay_ms,
                        task_token,
                    );
                }
            }
        }

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let workload_cleanup = |_| async {};
        let component_cleanup = |component_data: ComponentData| async move {
            component_data.cancel_token.cancel();
        };

        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(workload_id, workload_cleanup, component_cleanup)
            .await;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() {
        let plugin = Crontab::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_world_imports() {
        let plugin = Crontab::new();
        let world = plugin.world();
        assert!(
            world
                .imports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "crontab")
        );
    }

    #[test]
    fn test_world_exports() {
        let plugin = Crontab::new();
        let world = plugin.world();
        assert!(
            world
                .exports
                .iter()
                .any(|i| i.namespace == "custom" && i.package == "crontab")
        );
    }

    #[test]
    fn test_parse_schedule_config_cron() {
        let (name, kind) = parse_schedule_config("name=tick;cron=*/30 * * * *").unwrap();
        assert_eq!(name, "tick");
        assert!(matches!(kind, ScheduleKind::Cron(_)));
    }

    #[test]
    fn test_parse_schedule_config_delay() {
        let (name, kind) = parse_schedule_config("name=init;delay-ms=5000").unwrap();
        assert_eq!(name, "init");
        assert!(matches!(kind, ScheduleKind::Delay(5000)));
    }

    #[test]
    fn test_parse_schedule_config_with_spaces() {
        let (name, kind) = parse_schedule_config("name = tick ; cron = */30 * * * *").unwrap();
        assert_eq!(name, "tick");
        assert!(matches!(kind, ScheduleKind::Cron(_)));
    }

    #[test]
    fn test_parse_schedule_config_invalid() {
        assert!(parse_schedule_config("invalid").is_none());
        assert!(parse_schedule_config("name=test").is_none());
    }

    #[test]
    fn test_parse_schedule_config_invalid_cron_expr() {
        assert!(parse_schedule_config("name=test;cron=invalid").is_none());
    }

    #[test]
    fn test_extract_schedules() {
        let mut config = HashMap::new();
        config.insert(
            "schedule.tick".to_string(),
            "name=tick;cron=*/30 * * * *".to_string(),
        );
        config.insert(
            "schedule.cleanup".to_string(),
            "name=cleanup;cron=0 0 * * *".to_string(),
        );
        config.insert(
            "schedule.init".to_string(),
            "name=init;delay-ms=5000".to_string(),
        );
        config.insert("other".to_string(), "ignored".to_string());

        let schedules = extract_schedules(&config);
        assert_eq!(schedules.len(), 3);

        let names: Vec<&str> = schedules.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"tick"));
        assert!(names.contains(&"cleanup"));
        assert!(names.contains(&"init"));
    }

    #[test]
    fn test_cron_next_delay() {
        let schedule = Schedule::from_str("0 */30 * * * * *").unwrap();
        let delay = next_cron_delay(&schedule);
        assert!(delay.is_some());
        assert!(delay.unwrap().as_secs() <= 1800);
    }

    #[test]
    fn test_parse_hourly_cron() {
        let (name, kind) = parse_schedule_config("name=hourly;cron=0 * * * *").unwrap();
        assert_eq!(name, "hourly");
        assert!(matches!(kind, ScheduleKind::Cron(_)));
    }
}
