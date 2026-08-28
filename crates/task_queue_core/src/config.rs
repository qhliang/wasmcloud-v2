//! Queue configuration shared by host and native workers.
//!
//! Defaults are protocol-level safety limits. Host-interface and native
//! clients may override them, but all bindings for one queue must agree on
//! JetStream resource behavior.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};

pub const ACK_WAIT_MS: u64 = 30_000;
pub const LEASE_RENEW_INTERVAL_MS: u64 = 10_000;
pub const DEFAULT_DISPATCH_TIMEOUT_MS: u64 = 600_000;
pub const DEFAULT_EXECUTION_TIMEOUT_MS: u64 = 3_600_000;
pub const DEFAULT_RETRY_BACKOFF_MS: &[u64] = &[1_000, 5_000, 15_000, 60_000];
pub const PAYLOAD_MAX_BYTES: usize = 1_048_576;
pub const HEARTBEAT_MAX_INFO_BYTES: usize = 8_192;
pub const HEARTBEAT_MIN_INTERVAL_MS: u64 = 1_000;
pub const MAX_MESSAGE_SIZE: i32 = PAYLOAD_MAX_BYTES as i32 + 4_096;
pub const MAX_DELIVERIES: i64 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueConfig {
    pub name: String,
    pub ack_wait: Duration,
    pub lease_renew_interval: Duration,
    pub dispatch_timeout: Duration,
    pub execution_timeout: Duration,
    pub max_deliver: i64,
    pub retry_backoff_ms: Vec<u64>,
    pub results_archive: bool,
}

impl QueueConfig {
    /// Creates a valid queue configuration with approved defaults.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ack_wait: Duration::from_millis(ACK_WAIT_MS),
            lease_renew_interval: Duration::from_millis(LEASE_RENEW_INTERVAL_MS),
            dispatch_timeout: Duration::from_millis(DEFAULT_DISPATCH_TIMEOUT_MS),
            execution_timeout: Duration::from_millis(DEFAULT_EXECUTION_TIMEOUT_MS),
            max_deliver: MAX_DELIVERIES,
            retry_backoff_ms: DEFAULT_RETRY_BACKOFF_MS.to_vec(),
            results_archive: true,
        }
    }

    pub fn from_config(config: &HashMap<String, String>) -> Result<Self> {
        // `queue` is deliberately required and is not inferred from workload
        // metadata; different workers may intentionally bind different queues.
        let name = config_value(config, "queue")?;
        validate_queue_name(&name).context("invalid queue name")?;
        Ok(Self {
            name,
            ack_wait: parse_duration_ms(config, "ack-wait-ms", ACK_WAIT_MS)?,
            lease_renew_interval: parse_duration_ms(
                config,
                "lease-renew-interval-ms",
                LEASE_RENEW_INTERVAL_MS,
            )?,
            dispatch_timeout: parse_duration_ms(
                config,
                "default-dispatch-timeout-ms",
                DEFAULT_DISPATCH_TIMEOUT_MS,
            )?,
            execution_timeout: parse_duration_ms(
                config,
                "default-execution-timeout-ms",
                DEFAULT_EXECUTION_TIMEOUT_MS,
            )?,
            max_deliver: parse_i64(config, "max-deliver", MAX_DELIVERIES)?,
            retry_backoff_ms: parse_backoff_ms(
                config,
                "retry-backoff-ms",
                DEFAULT_RETRY_BACKOFF_MS,
            )?,
            results_archive: parse_bool(config, "results-archive", true)?,
        })
    }

    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        // Attempt 1 has already failed, so its retry delay is the first
        // configured backoff entry. Later attempts clamp to the last entry.
        let index = attempt.saturating_sub(1) as usize;
        let millis = self
            .retry_backoff_ms
            .get(index)
            .copied()
            .or_else(|| self.retry_backoff_ms.last().copied())
            .unwrap_or(1_000)
            .min(u64::from(u32::MAX));
        Duration::from_millis(millis)
    }
}

pub fn validate_queue_name(queue: &str) -> Result<()> {
    let mut chars = queue.chars();
    let first = chars.next();
    let valid_first = first
        .map(|ch| ch.is_ascii_alphanumeric())
        .unwrap_or_default();
    let valid_tail = chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-');
    let valid = valid_first && valid_tail && queue.len() <= 64;
    if valid {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "queue name must match ^[A-Za-z0-9][A-Za-z0-9_-]{{0,63}}$"
        ))
    }
}

pub fn config_value(config: &HashMap<String, String>, key: &str) -> Result<String> {
    config
        .get(key)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing required config: '{key}'"))
}

pub fn parse_duration_ms(
    config: &HashMap<String, String>,
    key: &str,
    default: u64,
) -> Result<Duration> {
    parse_u64(config, key, default)
        .map(Duration::from_millis)
        .with_context(|| format!("parse {key}"))
}

pub fn parse_u64(config: &HashMap<String, String>, key: &str, default: u64) -> Result<u64> {
    match config.get(key) {
        Some(raw) => raw
            .parse()
            .map_err(|err| anyhow::anyhow!("invalid {key}: {err}")),
        None => Ok(default),
    }
}

pub fn parse_i64(config: &HashMap<String, String>, key: &str, default: i64) -> Result<i64> {
    match config.get(key) {
        Some(raw) => raw
            .parse()
            .map_err(|err| anyhow::anyhow!("invalid {key}: {err}")),
        None => Ok(default),
    }
}

pub fn parse_bool(config: &HashMap<String, String>, key: &str, default: bool) -> Result<bool> {
    match config.get(key) {
        Some(raw) => raw
            .parse()
            .map_err(|err| anyhow::anyhow!("invalid {key}: {err}")),
        None => Ok(default),
    }
}

pub fn parse_backoff_ms(
    config: &HashMap<String, String>,
    key: &str,
    default: &[u64],
) -> Result<Vec<u64>> {
    match config.get(key) {
        Some(raw) => raw
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::parse)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| anyhow::anyhow!("invalid {key}: {err}")),
        None => Ok(default.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn queue_name_accepts_valid_characters() {
        assert!(validate_queue_name("agent-task").is_ok());
        assert!(validate_queue_name("Agent_2").is_ok());
    }

    #[test]
    fn queue_name_rejects_invalid_names() {
        assert!(validate_queue_name("").is_err());
        assert!(validate_queue_name("-agent").is_err());
        assert!(validate_queue_name("agent.*").is_err());
        assert!(validate_queue_name("agent task").is_err());
        assert!(validate_queue_name(&"a".repeat(65)).is_err());
    }

    #[test]
    fn queue_config_requires_name() {
        let err = QueueConfig::from_config(&HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("missing required config: 'queue'"));
    }

    #[test]
    fn backoff_parses_csv_values() {
        let mut config = HashMap::new();
        config.insert("queue".to_string(), "agent-task".to_string());
        config.insert(
            "retry-backoff-ms".to_string(),
            "1000, 5000,15000 ,60000".to_string(),
        );
        let parsed = parse_backoff_ms(&config, "retry-backoff-ms", DEFAULT_RETRY_BACKOFF_MS)
            .expect("valid backoff");
        assert_eq!(parsed, vec![1_000, 5_000, 15_000, 60_000]);
    }

    #[test]
    fn backoff_uses_attempt_index() {
        let config = QueueConfig::new("agent-task");
        assert_eq!(config.backoff_for_attempt(1).as_millis(), 1_000);
        assert_eq!(config.backoff_for_attempt(2).as_millis(), 5_000);
        assert_eq!(config.backoff_for_attempt(99).as_millis(), 60_000);
    }
}
