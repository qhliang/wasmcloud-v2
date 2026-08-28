//! Native Rust worker runner.
//!
//! The runner decodes the shared task envelope, renews the JetStream lease,
//! invokes the application worker, and maps success/failure to ack semantics.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use async_nats::jetstream::Context as JetStreamContext;
use futures::StreamExt as _;
use task_queue_core::config::QueueConfig;
use task_queue_core::nats::{AckAction, QueueHandles, now_ms};
use task_queue_core::types::{TaskEnvelope, TaskError, TaskErrorSource};
use task_queue_core::worker::{SharedWorker, TaskContext, Worker};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct WorkerRunner {
    handles: Arc<QueueHandles>,
    worker: SharedWorker,
}

impl WorkerRunner {
    /// Creates or reuses the queue resources and wraps the application worker.
    pub async fn connect(
        client: JetStreamContext,
        config: QueueConfig,
        worker: impl Worker + 'static,
    ) -> Result<Self> {
        let handles = Arc::new(QueueHandles::create(client, config).await?);
        Ok(Self {
            handles,
            worker: Arc::new(worker),
        })
    }

    /// Consumes tasks until `shutdown` is cancelled.
    ///
    /// Messages are processed sequentially; process shutdown stops accepting
    /// new messages but does not force-cancel a call already in progress.
    pub async fn run(&self, shutdown: CancellationToken) -> Result<()> {
        let consumer = self.handles.ensure_worker().await?;
        let mut messages = consumer
            .messages()
            .await
            .context("failed to subscribe to task consumer")?;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                message = messages.next() => match message {
                    Some(Ok(message)) => self.handle_message(message.clone()).await,
                    Some(Err(err)) => {
                        tracing::warn!(err = %err, "task consumer stream error");
                    }
                    None => break,
                }
            }
        }
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
                let _ = AckAction::Term.apply(&acker).await;
                return;
            }
        };
        // A malformed envelope cannot be retried safely, so reject it.
        let payload = match envelope.decode_payload() {
            Ok(payload) => payload,
            Err(err) => {
                tracing::warn!(task_id = %task_id, err = %err, "invalid task payload");
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
            heartbeat_subject(&self.handles.config.name),
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

        // Progress must continue even when the business heartbeat fails.
        let _started_at_ms = now_ms();
        let result = self.worker.handle_task(context).await;
        renew_cancel.cancel();
        let _ = renewer.await;

        let action = match result {
            // Completed side effects and output are considered accepted.
            Ok(_) => AckAction::Ack,
            // Guest errors are recoverable until the delivery budget is spent.
            Err(TaskError {
                source: TaskErrorSource::Guest,
                message: _,
            }) => {
                if attempt >= self.handles.config.max_deliver.max(1) as u32 {
                    AckAction::Term
                } else {
                    AckAction::Nak(Some(self.handles.config.backoff_for_attempt(attempt)))
                }
            }
            // Deadlines and infrastructure failures should not spin on retries.
            Err(err) => {
                tracing::warn!(task_id = %task_id, err = %err, "worker system failure");
                AckAction::Term
            }
        };
        // The lease renewal task must stop before applying the terminal ack.
        if let Err(err) = action.apply(&acker).await {
            tracing::warn!(task_id = %task_id, err = %err, "failed to acknowledge task");
        }
    }
}

pub fn heartbeat_subject(queue: &str) -> String {
    format!("{queue}.heartbeat")
}

pub fn task_context(worker: impl Worker + 'static) -> SharedWorker {
    Arc::new(worker)
}
