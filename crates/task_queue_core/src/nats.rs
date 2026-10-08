//! JetStream resources, metadata store, and ack helpers.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::kv::{Config as KvConfig, UpdateError, UpdateErrorKind};
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy};
use async_nats::jetstream::{AckKind, Context as JetStreamContext};
use bytes::Bytes;

use crate::config::QueueConfig;
use crate::types::TaskMeta;

/// Metadata compare-and-swap attempts before giving up.
///
/// Three is enough to ride out a concurrent writer landing between our read
/// and our write; a fourth attempt means the key is being contended by something
/// other than ordinary task dispatch.
const METADATA_CAS_ATTEMPTS: u32 = 3;

#[derive(Clone)]
pub struct QueueHandles {
    pub config: QueueConfig,
    pub jetstream: JetStreamContext,
    pub task_stream: async_nats::jetstream::stream::Stream,
    pub metadata: async_nats::jetstream::kv::Store,
    pub results: Option<JetStreamContext>,
}

impl QueueHandles {
    /// Creates or reuses the task stream, META KV bucket, and optional result
    /// archive for a queue. Resource names are deterministic per queue.
    pub async fn create(client: JetStreamContext, config: QueueConfig) -> Result<Self> {
        let task_subject = task_filter(&config.name);
        let task_stream = client
            .get_or_create_stream(StreamConfig {
                name: config.name.clone(),
                subjects: vec![task_subject],
                retention: RetentionPolicy::WorkQueue,
                max_message_size: crate::config::MAX_MESSAGE_SIZE,
                ..Default::default()
            })
            .await
            .context("failed to create task stream")?;

        let metadata = client
            .create_key_value(KvConfig {
                bucket: metadata_bucket(&config.name),
                max_value_size: crate::config::MAX_MESSAGE_SIZE,
                history: 1,
                ..Default::default()
            })
            .await
            .context("failed to create task metadata")?;

        let results = if config.results_archive {
            Some(
                client
                    .get_or_create_stream(StreamConfig {
                        name: result_stream_name(&config.name),
                        subjects: vec![result_filter(&config.name)],
                        retention: RetentionPolicy::Limits,
                        max_message_size: crate::config::MAX_MESSAGE_SIZE,
                        ..Default::default()
                    })
                    .await
                    .context("failed to create result archive")?,
            )
        } else {
            None
        };
        let results = results.map(|_| client.clone());

        Ok(Self {
            config,
            jetstream: client,
            task_stream,
            metadata,
            results,
        })
    }

    pub async fn ensure_worker(&self) -> Result<async_nats::jetstream::consumer::PullConsumer> {
        // Homogeneous workers share this durable consumer so JetStream tracks
        // delivery count, ack state, and retry policy once per task.
        let name = worker_consumer_name(&self.config.name);
        let durable_name = name.clone();
        self.task_stream
            .get_or_create_consumer(
                name.as_str(),
                PullConfig {
                    durable_name: Some(durable_name),
                    filter_subject: task_filter(&self.config.name),
                    ack_wait: self.config.ack_wait,
                    max_deliver: self.config.max_deliver,
                    ..Default::default()
                },
            )
            .await
            .context("failed to create worker consumer")
    }

    pub async fn get_metadata(&self, task_id: &str) -> Result<TaskMeta> {
        let key = metadata_key(&self.config.name, task_id);
        let raw = self
            .metadata
            .get(key.as_str())
            .await
            .context("failed to read task metadata")?
            .ok_or_else(|| anyhow::anyhow!("task not found"))?;
        serde_json::from_slice(&raw).context("failed to decode task metadata")
    }

    pub async fn get_metadata_with_revision(
        &self,
        task_id: &str,
    ) -> Result<Option<(u64, TaskMeta)>> {
        // The KV revision is required for compare-and-swap state transitions.
        let key = metadata_key(&self.config.name, task_id);
        let entry = match self.metadata.entry(key.as_str()).await {
            Ok(Some(entry)) => entry,
            _ => return Ok(None),
        };
        let meta = serde_json::from_slice(entry.value.as_ref())
            .context("failed to decode task metadata")?;
        Ok(Some((entry.revision, meta)))
    }

    pub async fn put_metadata(&self, meta: &TaskMeta) -> Result<u64> {
        let raw = serde_json::to_vec(meta).context("failed to encode task metadata")?;
        let key = metadata_key(&self.config.name, &meta.id);
        self.metadata
            .put(key.as_str(), Bytes::from(raw))
            .await
            .context("failed to write task metadata")
    }

    pub async fn update_metadata(&self, meta: &TaskMeta, revision: u64) -> Result<u64> {
        // A revision mismatch prevents a stale worker or producer from
        // overwriting newer queue state.
        let raw = serde_json::to_vec(meta).context("failed to encode task metadata")?;
        let key = metadata_key(&self.config.name, &meta.id);
        self.metadata
            .update(key.as_str(), Bytes::from(raw), revision)
            .await
            .context("failed to update task metadata")
    }

    /// Applies `mutate` to a task's metadata under a KV revision compare-and-swap.
    ///
    /// Read-modify-write against the META bucket has two independent writers for
    /// the same key — the dispatcher marking a task running and a producer
    /// requesting cancellation — so a plain `put_metadata` can silently discard
    /// the other side's field (`cancel_requested` in particular: a cancellation
    /// would be erased by the next dispatch write and the task would run to
    /// completion regardless).
    ///
    /// Only a revision conflict is retried, and that is what makes replaying
    /// `mutate` safe: `WrongLastRevision` means the server rejected the write,
    /// so the mutation provably did not land and can be recomputed against a
    /// fresh snapshot. Any other failure (`TimedOut`, transport, store deleted)
    /// leaves the write outcome unknown, so it is propagated instead of retried —
    /// otherwise a non-idempotent `mutate` (appending to `attempts`, say) would
    /// duplicate its effect whenever the first write did in fact land.
    ///
    /// `mutate` is `FnMut` because it may run once per attempt, letting callers
    /// report which branch a replayed mutation took.
    ///
    /// Returns the revision the winning write produced.
    pub async fn update_metadata_with<F>(&self, task_id: &str, mut mutate: F) -> Result<u64>
    where
        F: FnMut(&mut TaskMeta) -> Result<()>,
    {
        let key = metadata_key(&self.config.name, task_id);
        let mut attempts = 0;
        loop {
            attempts += 1;
            let Some((revision, mut meta)) = self.get_metadata_with_revision(task_id).await? else {
                anyhow::bail!("task not found");
            };
            mutate(&mut meta)?;
            let raw = serde_json::to_vec(&meta).context("failed to encode task metadata")?;
            match self
                .metadata
                .update(key.as_str(), Bytes::from(raw), revision)
                .await
            {
                Ok(next) => return Ok(next),
                Err(err) => {
                    if !is_retryable_update_error(&err, attempts) {
                        return Err(anyhow::Error::new(err))
                            .context("failed to update task metadata");
                    }
                    tracing::debug!(
                        task_id = %task_id,
                        attempts,
                        "task metadata cas conflict; replaying mutation"
                    );
                }
            }
        }
    }

    pub async fn publish_result(&self, raw: Vec<u8>, task_id: &str) -> Result<()> {
        let Some(_stream) = self.results.as_ref() else {
            return Ok(());
        };
        let subject = result_subject(&self.config.name, task_id);
        self.jetstream
            .publish(subject, Bytes::from(raw))
            .await
            .context("failed to publish result")?;
        Ok(())
    }
}

/// Whether a failed CAS write may be retried by replaying the mutation.
///
/// Only a revision conflict qualifies. `WrongLastRevision` is the server
/// rejecting the write outright, which proves the mutation did not land and
/// makes recomputing it against a fresh snapshot safe. Every other kind leaves
/// the outcome unknown — a `TimedOut` write may already have been applied — so
/// replaying could double-apply a non-idempotent mutation (an appended attempt
/// record, say). Those are propagated instead.
fn is_retryable_update_error(err: &UpdateError, attempts: u32) -> bool {
    err.kind() == UpdateErrorKind::WrongLastRevision && attempts < METADATA_CAS_ATTEMPTS
}

#[derive(Debug, Clone)]
pub enum AckAction {
    Ack,
    Progress,
    Nak(Option<Duration>),
    Term,
}

impl AckAction {
    /// Wraps the four JetStream acknowledgement outcomes used by wasm and
    /// native workers.
    pub async fn apply(&self, acker: &async_nats::jetstream::message::Acker) -> anyhow::Result<()> {
        let result = match self {
            Self::Ack => acker.ack_with(AckKind::Ack).await,
            Self::Progress => acker.ack_with(AckKind::Progress).await,
            Self::Nak(delay) => acker.ack_with(AckKind::Nak(*delay)).await,
            Self::Term => acker.ack_with(AckKind::Term).await,
        };
        result.map_err(|err| anyhow::anyhow!("failed to acknowledge task: {err}"))
    }
}

pub fn task_filter(queue: &str) -> String {
    format!("{queue}.tasks.>")
}

pub fn task_subject(queue: &str, task_id: &str) -> String {
    format!("{queue}.tasks.{task_id}")
}

pub fn result_filter(queue: &str) -> String {
    format!("{queue}.results.>")
}

pub fn result_subject(queue: &str, task_id: &str) -> String {
    format!("{queue}.results.{task_id}")
}

pub fn metadata_bucket(queue: &str) -> String {
    format!("{queue}-meta")
}

pub fn result_stream_name(queue: &str) -> String {
    format!("{queue}-results")
}

pub fn worker_consumer_name(queue: &str) -> String {
    format!("{queue}-worker")
}

pub fn metadata_key(queue: &str, task_id: &str) -> String {
    format!("{queue}.{task_id}")
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_keys_are_namespaced_by_queue() {
        assert_eq!(
            metadata_key("agent-task", "0198e57c-0000-7000-8000-000000000000"),
            "agent-task.0198e57c-0000-7000-8000-000000000000"
        );
    }

    #[test]
    fn revision_conflict_is_retryable_until_the_budget_is_spent() {
        let conflict = UpdateError::new(UpdateErrorKind::WrongLastRevision);
        assert!(is_retryable_update_error(&conflict, 1));
        assert!(is_retryable_update_error(
            &conflict,
            METADATA_CAS_ATTEMPTS - 1
        ));
        // The last permitted attempt already used the write; retrying past it
        // would mean mutating without a fresh snapshot.
        assert!(!is_retryable_update_error(&conflict, METADATA_CAS_ATTEMPTS));
    }

    #[test]
    fn unknown_outcome_write_errors_are_never_replayed() {
        // A timeout or transport failure may have applied the write server-side
        // before the client gave up, so the outcome is unknown and replaying a
        // non-idempotent mutation could duplicate it.
        for kind in [
            UpdateErrorKind::TimedOut,
            UpdateErrorKind::Other,
            UpdateErrorKind::InvalidKey,
        ] {
            assert!(
                !is_retryable_update_error(&UpdateError::new(kind), 1),
                "{kind:?} must not be replayed"
            );
        }
    }

    #[test]
    fn resource_names_are_stable() {
        assert_eq!(metadata_bucket("agent-task"), "agent-task-meta");
        assert_eq!(result_stream_name("agent-task"), "agent-task-results");
        assert_eq!(worker_consumer_name("agent-task"), "agent-task-worker");
        assert_eq!(
            task_subject("agent-task", "task-1"),
            "agent-task.tasks.task-1"
        );
        assert_eq!(
            result_subject("agent-task", "task-1"),
            "agent-task.results.task-1"
        );
    }
}
