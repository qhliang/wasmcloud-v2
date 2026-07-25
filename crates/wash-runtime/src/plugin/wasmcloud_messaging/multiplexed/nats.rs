//! NATS backend for the multiplexed `wasmcloud:messaging` plugin.
//!
//! A [`MsgBackend`]/[`BackendProvider`] pair backed by an `async_nats` client,
//! serving the outbound (consumer) `publish`/`request` path — the same surface
//! the standalone `NatsMessaging` plugin uses.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::plugin::multiplex::BackendProvider;

use super::{BrokerMessage, MsgBackend, MsgId};

/// A NATS-backed [`MsgBackend`]. The provider pools clients by `url`
/// ([`NatsMsgProvider::pool_key`]), so named imports pointing at the same
/// cluster share one connection while imports with distinct urls get
/// independent clients.
pub struct NatsMsgBackend {
    client: Arc<async_nats::Client>,
}

#[async_trait::async_trait]
impl MsgBackend for NatsMsgBackend {
    async fn request(
        &self,
        subject: String,
        body: Vec<u8>,
        timeout_ms: u32,
    ) -> Result<BrokerMessage, String> {
        let timeout = std::time::Duration::from_millis(timeout_ms as u64);
        let resp =
            match tokio::time::timeout(timeout, self.client.request(subject, body.into())).await {
                Ok(Ok(msg)) => msg,
                Ok(Err(e)) => return Err(format!("failed to send request: {e}")),
                Err(_) => return Err(format!("request timed out after {timeout_ms}ms")),
            };
        Ok(BrokerMessage {
            subject: resp.subject.to_string(),
            reply_to: resp.reply.as_ref().map(|r| r.to_string()),
            body: resp.payload.into(),
        })
    }

    async fn publish(&self, msg: BrokerMessage) -> Result<(), String> {
        let result = if let Some(reply_to) = msg.reply_to {
            self.client
                .publish_with_reply(msg.subject, reply_to, msg.body.into())
                .await
        } else {
            self.client.publish(msg.subject, msg.body.into()).await
        };
        result.map_err(|e| format!("failed to send message: {e}"))
    }
}

/// NATS provider, selected by `config.backend = "nats"`. Requires `config.url`
/// (e.g. `nats://127.0.0.1:4222`).
#[derive(Default)]
pub struct NatsMsgProvider;

#[async_trait::async_trait]
impl BackendProvider<MsgId> for NatsMsgProvider {
    fn pool_key(&self, config: &HashMap<String, String>) -> Option<String> {
        config
            .get("url")
            .or_else(|| config.get("nats_url"))
            .cloned()
    }
    fn backend_type(&self) -> &'static str {
        "nats"
    }

    async fn instantiate(&self, config: &HashMap<String, String>) -> anyhow::Result<MsgId> {
        // Support both "url" (upstream) and "nats_url" (backward compat)
        let url = config
            .get("url")
            .or_else(|| config.get("nats_url"))
            .ok_or_else(|| {
                anyhow::anyhow!("nats messaging backend requires 'url' or 'nats_url' config")
            })?;

        let mut opts = async_nats::ConnectOptions::new();

        // Connection timeout
        if let Some(timeout) = config.get("nats_connection_timeout") {
            let secs: u64 = timeout
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid nats_connection_timeout: {e}"))?;
            opts = opts.connection_timeout(Duration::from_secs(secs));
        }

        // Token auth
        if let Some(token) = config.get("nats_token") {
            opts = opts.token(token.clone());
        }
        // Username/password auth
        else if let Some(user) = config.get("nats_user") {
            let password = config
                .get("nats_password")
                .ok_or_else(|| anyhow::anyhow!("nats_user requires nats_password"))?;
            opts = opts.user_and_password(user.clone(), password.clone());
        }

        // TLS
        if let Some(ca_path) = config.get("nats_tls_ca") {
            opts = opts.add_root_certificates(ca_path.into());
        }
        if let (Some(cert), Some(key)) = (config.get("nats_tls_cert"), config.get("nats_tls_key")) {
            opts = opts.add_client_certificate(cert.into(), key.into());
        }

        let client = opts.connect(url).await?;
        Ok(Arc::new(NatsMsgBackend {
            client: Arc::new(client),
        }))
    }
}
