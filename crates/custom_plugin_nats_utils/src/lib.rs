use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Default NATS connection timeout in seconds.
const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 10;

/// Build `async_nats::ConnectOptions` from a config map.
///
/// Supported keys (checked in priority order):
/// - `nats_connection_timeout` — connection timeout in seconds (default: 10)
/// - `nats_jwt` + `nats_nkey_seed` — JWT/NKey authentication
/// - `nats_token` — token authentication
/// - `nats_user` + `nats_password` — username/password authentication
/// - `nats_tls_ca` — path to CA certificate
/// - `nats_tls_cert` + `nats_tls_key` — client certificate and key
pub fn build_nats_connect_options(
    config: &HashMap<String, String>,
) -> anyhow::Result<async_nats::ConnectOptions> {
    let mut opts = async_nats::ConnectOptions::new();

    // Connection timeout
    let timeout = config
        .get("nats_connection_timeout")
        .map(|v| v.parse::<u64>())
        .transpose()
        .map_err(|e| anyhow::anyhow!("invalid nats_connection_timeout: {e}"))?
        .unwrap_or(DEFAULT_CONNECTION_TIMEOUT_SECS);
    opts = opts.connection_timeout(Duration::from_secs(timeout));

    // JWT/NKey auth (highest priority)
    if let Some(jwt) = config.get("nats_jwt") {
        let seed = config
            .get("nats_nkey_seed")
            .ok_or_else(|| anyhow::anyhow!("nats_jwt requires nats_nkey_seed"))?;
        let kp = Arc::new(
            nkeys::KeyPair::from_seed(seed)
                .map_err(|e| anyhow::anyhow!("invalid nkey seed: {e}"))?,
        );
        let jwt = jwt.clone();
        opts = opts.jwt(jwt, move |nonce| {
            let kp = kp.clone();
            async move { kp.sign(&nonce).map_err(async_nats::AuthError::new) }
        });
    } else if let Some(token) = config.get("nats_token") {
        // Token auth
        opts = opts.token(token.clone());
    } else if let Some(user) = config.get("nats_user") {
        // Username/password auth
        let password = config
            .get("nats_password")
            .ok_or_else(|| anyhow::anyhow!("nats_user requires nats_password"))?;
        opts = opts.user_and_password(user.clone(), password.clone());
    }

    // TLS options
    if let Some(ca_path) = config.get("nats_tls_ca") {
        opts = opts.add_root_certificates(ca_path.into());
    }
    if let (Some(cert_path), Some(key_path)) =
        (config.get("nats_tls_cert"), config.get("nats_tls_key"))
    {
        opts = opts.add_client_certificate(cert_path.into(), key_path.into());
    }

    Ok(opts)
}
