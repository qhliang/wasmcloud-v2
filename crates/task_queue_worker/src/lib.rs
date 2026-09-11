//! Native Rust worker runner.
//!
//! The runner decodes the shared task envelope, renews the JetStream lease,
//! invokes the application worker, and maps success/failure to ack semantics.
//! When the queue stream or its durable consumer disappears (a NATS rebuild
//! backed by an ephemeral JetStream volume, for example), the runner rebuilds
//! the consumer and re-subscribes instead of exiting — see
//! [`WorkerRunner::run`].

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;

use anyhow::Result;
use async_nats::jetstream::Context as JetStreamContext;
use futures::StreamExt as _;
use task_queue_core::config::QueueConfig;
use task_queue_core::events::ControlEvent;
use task_queue_core::nats::{AckAction, QueueHandles, now_ms};
use task_queue_core::types::{TaskEnvelope, TaskError, TaskErrorSource, TaskStatus};
use task_queue_core::worker::{SharedWorker, TaskContext, Worker};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Upper bound on how long `run` waits for in-flight deliveries to finish after
/// shutdown. Aligned with the queue `ack_wait` window: a delivery that has not
/// acked by then will lose its lease and be redelivered anyway, so waiting
/// longer buys nothing.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// How often a parked acquisition re-reads the limit. The limit can be raised
/// while runners are parked; without a periodic re-check they would stay parked
/// until some unrelated release happened to wake them.
const LIMIT_RECHECK: Duration = Duration::from_secs(1);

/// How long to wait before rebuilding the task consumer after a subscription
/// failure.
///
/// The stream and its durable consumer are externally deletable state: a NATS
/// rebuild backed by an ephemeral JetStream volume wipes them both, after which
/// the pull subscription only ever reports "no responders". A runner that
/// fetched its consumer once at startup therefore stayed stuck forever with
/// every task parked in the queue. Instead of exiting, `run` rebuilds the
/// consumer — and the stream, when the producer has not recreated it yet —
/// after this delay.
const RECONNECT_DELAY: Duration = Duration::from_secs(60);

/// Waits for `delay`, returning `false` as soon as `shutdown` is cancelled.
async fn sleep_or_cancel(shutdown: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

/// Supplies the number of deliveries that may be processed concurrently.
///
/// It is polled before every acquisition, so an embedder whose limit changes at
/// runtime (for example one pushed through config sync) is honoured without
/// restarting the runner. Snapshotting the value at startup instead would
/// silently apply a stale limit for the whole process lifetime.
pub type ConcurrencyLimit = Arc<dyn Fn() -> usize + Send + Sync>;

/// Wraps a constant value in a [`ConcurrencyLimit`].
pub fn fixed_limit(concurrency: usize) -> ConcurrencyLimit {
    let concurrency = concurrency.max(1);
    Arc::new(move || concurrency)
}

/// Counting gate whose capacity comes from a [`ConcurrencyLimit`].
struct Gate {
    limit: ConcurrencyLimit,
    active: StdMutex<usize>,
    freed: Notify,
}

impl Gate {
    fn new(limit: ConcurrencyLimit) -> Self {
        Self {
            limit,
            // A std mutex is deliberate: `Permit::drop` must release the slot
            // synchronously, and the critical section never awaits.
            active: StdMutex::new(0),
            freed: Notify::new(),
        }
    }

    /// Takes a slot synchronously; `None` when the gate is at its limit.
    fn try_acquire(self: &Arc<Self>) -> Option<Permit> {
        let limit = (self.limit)().max(1);
        let mut active = self.active.lock().unwrap_or_else(|err| err.into_inner());
        if *active < limit {
            *active += 1;
            return Some(Permit { gate: self.clone() });
        }
        None
    }

    /// Takes a slot, waiting until one is free or `shutdown` is cancelled.
    async fn acquire(self: &Arc<Self>, shutdown: &CancellationToken) -> Option<Permit> {
        loop {
            if let Some(permit) = self.try_acquire() {
                return Some(permit);
            }
            tokio::select! {
                _ = shutdown.cancelled() => return None,
                _ = self.freed.notified() => {}
                _ = tokio::time::sleep(LIMIT_RECHECK) => {}
            }
        }
    }
}

/// An occupied slot in a [`Gate`], released when dropped.
struct Permit {
    gate: Arc<Gate>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut active = self
            .gate
            .active
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        *active = active.saturating_sub(1);
        drop(active);
        self.gate.freed.notify_one();
    }
}

#[derive(Clone)]
pub struct WorkerRunner {
    handles: Arc<QueueHandles>,
    worker: SharedWorker,
    limit: ConcurrencyLimit,
}

impl WorkerRunner {
    /// Creates or reuses the queue resources and wraps the application worker.
    ///
    /// Deliveries are processed one at a time; use
    /// [`connect_with_concurrency`](Self::connect_with_concurrency) to allow
    /// several in parallel.
    pub async fn connect(
        client: JetStreamContext,
        config: QueueConfig,
        worker: impl Worker + 'static,
    ) -> Result<Self> {
        Self::connect_with_concurrency(client, config, worker, 1).await
    }

    /// Same as [`connect`](Self::connect) but processes up to `concurrency`
    /// deliveries at the same time.
    ///
    /// The value is clamped to at least one, so a misconfigured zero degrades
    /// to serial processing instead of stalling the runner.
    pub async fn connect_with_concurrency(
        client: JetStreamContext,
        config: QueueConfig,
        worker: impl Worker + 'static,
        concurrency: usize,
    ) -> Result<Self> {
        Self::connect_with_limit(client, config, worker, fixed_limit(concurrency)).await
    }

    /// Same as [`connect`](Self::connect) but re-reads the concurrency limit
    /// from `limit` before taking every slot.
    pub async fn connect_with_limit(
        client: JetStreamContext,
        config: QueueConfig,
        worker: impl Worker + 'static,
        limit: ConcurrencyLimit,
    ) -> Result<Self> {
        let handles = Arc::new(QueueHandles::create(client, config).await?);
        Ok(Self {
            handles,
            worker: Arc::new(worker),
            limit,
        })
    }

    /// Consumes tasks until `shutdown` is cancelled.
    ///
    /// Each delivery occupies one of `concurrency` slots from the moment it is
    /// pulled until its terminal ack is applied. A slot is acquired *before*
    /// pulling, so when every slot is busy the runner stops fetching and
    /// surplus tasks stay queued in the stream rather than being pulled,
    /// Nak'd and eventually dropped once the delivery budget is exhausted.
    ///
    /// Process shutdown stops accepting new messages but waits (bounded by
    /// [`DRAIN_TIMEOUT`]) for deliveries already in progress to ack.
    ///
    /// # Consumer self-healing
    ///
    /// The stream and its durable consumer are externally deletable state: a
    /// NATS rebuild backed by an ephemeral JetStream volume wipes both, after
    /// which the pull subscription only ever reports "no responders". The
    /// runner therefore never exits on a subscription failure — it re-runs
    /// [`QueueHandles::ensure_worker`] (a get_or_create) and re-subscribes
    /// after [`RECONNECT_DELAY`]. A consumer that still exists keeps its ack
    /// state, so a reconnect never re-delivers acknowledged tasks; one that
    /// was deleted is rebuilt and re-delivers whatever is still queued.
    /// In-flight deliveries are unaffected: they keep running across the
    /// reconnect and ack through their own acker.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<()> {
        let gate = Arc::new(Gate::new(self.limit.clone()));
        let mut in_flight = JoinSet::new();
        let mut established = false;

        'connect: loop {
            if shutdown.is_cancelled() {
                break;
            }
            let consumer = match self.handles.ensure_worker().await {
                Ok(consumer) => consumer,
                Err(err) => {
                    tracing::warn!(
                        err = %err,
                        delay = ?RECONNECT_DELAY,
                        "task consumer unavailable; will retry"
                    );
                    if !sleep_or_cancel(&shutdown, RECONNECT_DELAY).await {
                        break;
                    }
                    continue 'connect;
                }
            };
            // Keep the client-side prefetch window at a single message. The runner
            // must never buffer more deliveries than it has slots for, otherwise
            // buffered messages sit idle burning their `ack_wait` lease while
            // waiting for a free slot.
            let mut messages = match consumer.stream().max_messages_per_batch(1).messages().await {
                Ok(messages) => messages,
                Err(err) => {
                    tracing::warn!(
                        err = %err,
                        delay = ?RECONNECT_DELAY,
                        "failed to subscribe to task consumer; will retry"
                    );
                    if !sleep_or_cancel(&shutdown, RECONNECT_DELAY).await {
                        break;
                    }
                    continue 'connect;
                }
            };
            if established {
                tracing::info!("task queue worker re-connected");
            } else {
                tracing::info!("task queue worker running");
                established = true;
            }

            'consume: loop {
                // Reap finished deliveries so the set does not grow unbounded.
                while in_flight.try_join_next().is_some() {}
                // Acquiring the permit before fetching is the backpressure point.
                let permit = tokio::select! {
                    _ = shutdown.cancelled() => break 'connect,
                    acquired = gate.acquire(&shutdown) => match acquired {
                        Some(permit) => permit,
                        None => break 'connect,
                    },
                };
                let next = tokio::select! {
                    _ = shutdown.cancelled() => break 'consume,
                    next = messages.next() => next,
                };
                let Some(next) = next else {
                    // The subscription ended without reporting an error. Treat it
                    // like any other broken consumer and rebuild instead of
                    // letting the runner exit and silently stop consuming.
                    drop(permit);
                    tracing::warn!(
                        delay = ?RECONNECT_DELAY,
                        "task consumer subscription ended; re-establishing"
                    );
                    if !sleep_or_cancel(&shutdown, RECONNECT_DELAY).await {
                        break 'connect;
                    }
                    continue 'connect;
                };
                match next {
                    Ok(message) => {
                        let runner = self.clone();
                        in_flight.spawn(async move {
                            runner.handle_message(message).await;
                            // Released only after the terminal ack, so the number of
                            // in-flight deliveries never exceeds the configured limit.
                            drop(permit);
                        });
                    }
                    Err(err) => {
                        tracing::warn!(
                            err = %err,
                            delay = ?RECONNECT_DELAY,
                            "task consumer stream error; re-establishing consumer"
                        );
                        drop(permit);
                        // Back off before rebuilding, so a persistently broken
                        // stream cannot turn into a hot reconnect loop.
                        if !sleep_or_cancel(&shutdown, RECONNECT_DELAY).await {
                            break 'connect;
                        }
                        continue 'connect;
                    }
                }
            }
        }

        drain(&mut in_flight).await;
        Ok(())
    }

    async fn handle_message(&self, message: async_nats::jetstream::Message) {
        // JetStream delivery info is the authoritative attempt counter.
        let attempt = message
            .info()
            .ok()
            .map(|info| info.delivered.max(1))
            .unwrap_or(1) as u32;
        let (message, acker) = message.split();
        let acker = Arc::new(acker);
        let task_id = message
            .subject
            .rsplit('.')
            .next()
            .unwrap_or_default()
            .to_string();
        let envelope = match serde_json::from_slice::<TaskEnvelope>(&message.payload) {
            Ok(envelope) => envelope,
            Err(err) => {
                tracing::warn!(task_id = %task_id, err = %err, "invalid task envelope");
                // A delivery that is terminated without success must still be
                // reported, otherwise the producer's observer never learns the
                // task is gone and any execution it tracks stays pending.
                let error = err.to_string();
                self.publish_attempt_failed(&task_id, attempt, "system", &error)
                    .await;
                self.publish_pre_context_terminate(&task_id, attempt, &error)
                    .await;
                let _ = AckAction::Term.apply(&acker).await;
                return;
            }
        };
        // A malformed envelope cannot be retried safely, so reject it.
        let payload = match envelope.decode_payload() {
            Ok(payload) => payload,
            Err(err) => {
                tracing::warn!(task_id = %task_id, err = %err, "invalid task payload");
                let error = err.to_string();
                self.publish_attempt_failed(&task_id, attempt, "system", &error)
                    .await;
                self.publish_pre_context_terminate(&task_id, attempt, &error)
                    .await;
                let _ = AckAction::Term.apply(&acker).await;
                return;
            }
        };

        // Business cancellation is separate from JetStream lease renewal.

        let cancellation = CancellationToken::new();
        let context = TaskContext::new(
            task_id.clone(),
            attempt,
            envelope.execution_deadline_ms(),
            task_queue_core::types::Task { payload },
            self.handles.jetstream.client().clone(),
            ControlEvent::subject(&self.handles.config.name),
            cancellation,
        );
        let renew_acker = acker.clone();
        let renew_cancel = CancellationToken::new();
        let renew_cancel_in_task = renew_cancel.clone();
        let renewer = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(
                task_queue_core::config::LEASE_RENEW_INTERVAL_MS,
            ));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = renew_cancel_in_task.cancelled() => break,
                    _ = ticker.tick() => {
                        if AckAction::Progress.apply(&renew_acker).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        // Notify the producer's observer that this attempt is starting, via
        // the same `{queue}.events` path as attempt_failed (fire-and-forget).
        self.publish_start(&task_id, attempt).await;

        // Progress must continue even when the business heartbeat fails.
        let _started_at_ms = now_ms();
        // The runner keeps a shared clone so the ack path can enforce the
        // "terminate before ack" guarantee after `handle_task` returns: every
        // branch below calls `publish_terminate` (idempotent) before applying
        // its ack action.
        let ack_context = context.clone();
        let result = self.worker.handle_task(context).await;
        renew_cancel.cancel();
        let _ = renewer.await;

        let action = match result {
            // Completed side effects and output are considered accepted.
            Ok(output) => {
                // publish_terminate is idempotent: a worker that already
                // reported its own terminal state is never overwritten.
                if let Err(err) = ack_context
                    .publish_terminate(TaskStatus::Succeeded, output, None)
                    .await
                {
                    tracing::warn!(
                        task_id = %task_id,
                        err = %err,
                        "failed to publish success terminate"
                    );
                }
                AckAction::Ack
            }
            // Guest errors are recoverable until the delivery budget is spent.
            Err(TaskError {
                source: TaskErrorSource::Guest,
                message,
            }) => {
                ack_context
                    .publish_attempt_failed("guest", &message)
                    .await
                    .unwrap_or_else(|err| {
                        tracing::warn!(task_id = %task_id, err = %err, "failed to publish guest attempt_failed");
                    });
                if attempt >= self.handles.config.max_deliver.max(1) as u32 {
                    if let Err(err) = ack_context
                        .publish_terminate(TaskStatus::MaxRetriesExceeded, None, Some(message))
                        .await
                    {
                        tracing::warn!(
                            task_id = %task_id,
                            err = %err,
                            "failed to publish max-retries terminate"
                        );
                    }
                    AckAction::Term
                } else {
                    AckAction::Nak(Some(self.handles.config.backoff_for_attempt(attempt)))
                }
            }
            // Deadlines and infrastructure failures should not spin on retries.
            Err(err) => {
                tracing::warn!(task_id = %task_id, err = %err, "worker system failure");
                let message = err.to_string();
                ack_context
                    .publish_attempt_failed("system", &message)
                    .await
                    .unwrap_or_else(|publish_err| {
                        tracing::warn!(task_id = %task_id, err = %publish_err, "failed to publish system attempt_failed");
                    });
                // A system failure past the execution deadline is reported
                // as an execution timeout; anything else is a plain failure.
                let status = if now_ms() > ack_context.execution_deadline_ms {
                    TaskStatus::ExecutionTimeout
                } else {
                    TaskStatus::Failed
                };
                if let Err(publish_err) = ack_context
                    .publish_terminate(status, None, Some(message))
                    .await
                {
                    tracing::warn!(
                        task_id = %task_id,
                        err = %publish_err,
                        "failed to publish system terminate"
                    );
                }
                AckAction::Term
            }
        };
        // The lease renewal task must stop before applying the terminal ack.
        if let Err(err) = action.apply(&acker).await {
            tracing::warn!(task_id = %task_id, err = %err, "failed to acknowledge task");
        }
    }

    /// Reports a delivery that is being terminated without success to the
    /// producer's observer via `{queue}.events`.
    ///
    /// Every [`AckAction::Term`](AckAction::Term) path must publish this first:
    /// the workqueue message is about to disappear, no other component will
    /// emit a terminal result, and executions tracked on the producer side
    /// would stay pending forever. The event schema matches the
    /// `custom:task-queue` provider's `attempt_failed` contract; publishing is
    /// advisory (core NATS, fire-and-forget) and must never block the ack.
    async fn publish_attempt_failed(&self, task_id: &str, attempt: u32, source: &str, error: &str) {
        let subject = ControlEvent::subject(&self.handles.config.name);
        let value = task_queue_core::events::attempt_failed_event(task_id, attempt, source, error);
        if let Err(err) = task_queue_core::events::publish_control_event(
            &self.handles.jetstream.client(),
            subject,
            value,
        )
        .await
        {
            tracing::warn!(
                task_id = %task_id,
                err = %err,
                "failed to publish attempt_failed event"
            );
        }
    }

    /// Publishes the terminal `complete` event for failures that happen
    /// before a [`TaskContext`] exists (malformed envelope or payload), where
    /// the context-level [`TaskContext::publish_terminate`] is unavailable.
    /// Keeps the runner invariant that every Term path reports a terminal
    /// result; publishing is advisory (core NATS, fire-and-forget).
    async fn publish_pre_context_terminate(&self, task_id: &str, attempt: u32, error: &str) {
        let subject = ControlEvent::subject(&self.handles.config.name);
        let value = task_queue_core::events::complete_event(
            task_id,
            attempt,
            TaskStatus::Failed,
            None,
            Some(error),
        );
        if let Err(err) = task_queue_core::events::publish_control_event(
            &self.handles.jetstream.client(),
            subject,
            value,
        )
        .await
        {
            tracing::warn!(
                task_id = %task_id,
                err = %err,
                "failed to publish pre-context terminate event"
            );
        }
    }

    /// Reports that a delivery is starting execution via `{queue}.events`,
    /// mirroring the host plugin's `on-start` callback. Publishing is
    /// advisory (core NATS, fire-and-forget) and must never block execution.
    async fn publish_start(&self, task_id: &str, attempt: u32) {
        let subject = ControlEvent::subject(&self.handles.config.name);
        let value = task_queue_core::events::start_event(task_id, attempt);
        if let Err(err) = task_queue_core::events::publish_control_event(
            &self.handles.jetstream.client(),
            subject,
            value,
        )
        .await
        {
            tracing::warn!(task_id = %task_id, err = %err, "failed to publish start event");
        }
    }
}

/// Waits for in-flight deliveries to ack after shutdown.
///
/// Deliveries that overrun [`DRAIN_TIMEOUT`] are aborted: they will lose their
/// lease and be redelivered, which is safer than blocking shutdown forever.
async fn drain(in_flight: &mut JoinSet<()>) {
    if in_flight.is_empty() {
        return;
    }
    let pending = in_flight.len();
    tracing::info!(pending, "draining in-flight task deliveries");
    let drained = tokio::time::timeout(DRAIN_TIMEOUT, async {
        while in_flight.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::warn!(
            pending = in_flight.len(),
            "timed out draining in-flight task deliveries; aborting"
        );
        in_flight.abort_all();
        while in_flight.join_next().await.is_some() {}
    }
}

pub fn task_context(worker: impl Worker + 'static) -> SharedWorker {
    Arc::new(worker)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_waits_for_in_flight_deliveries() {
        let mut in_flight = JoinSet::new();
        in_flight.spawn(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
        });
        drain(&mut in_flight).await;
        assert!(in_flight.is_empty());
    }

    #[tokio::test]
    async fn drain_returns_immediately_when_nothing_is_in_flight() {
        let mut in_flight = JoinSet::new();
        drain(&mut in_flight).await;
        assert!(in_flight.is_empty());
    }

    #[tokio::test]
    async fn sleep_or_cancel_returns_false_when_shutdown_is_cancelled() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        assert!(!sleep_or_cancel(&shutdown, Duration::from_secs(60)).await);
    }

    #[tokio::test]
    async fn sleep_or_cancel_returns_true_after_the_delay() {
        let shutdown = CancellationToken::new();
        assert!(sleep_or_cancel(&shutdown, Duration::from_millis(5)).await);
    }
}
