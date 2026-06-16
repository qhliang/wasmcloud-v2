//! # Mail Host Plugin (Resource-based)
//!
//! Provides `custom:mail/sender@0.1.0` interface for sending emails via SMTP
//! and reading emails via IMAP.
//!
//! Two config sources with priority:
//! 1. Wasm dynamic config (passed via resource constructor)
//! 2. Static interface config (fallback from wasmcloud config)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use mailparse::MailHeaderMap;
use tokio::sync::RwLock;
use tracing::debug;
use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::WorkloadItem;
use wash_runtime::plugin::config::{resolve_field, resolve_optional_field};
use wash_runtime::plugin::{HostPlugin, find_interface};
use wash_runtime::wit::{WitInterface, WitWorld};
use wasmtime::component::Resource;

mod bindings {
    wasmtime::component::bindgen!({
        world: "mail",
        imports: {
            default: async | trappable | tracing,
        },
        with: {
            "custom:mail/sender.mail-client": super::MailClientHandle,
        },
    });
}

use bindings::custom::mail::types::{MailConfig, MailError};

const PLUGIN_ID: &str = "plugin-mail";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Host-side state for a mail-client resource instance.
pub struct MailClientHandle {
    config: PluginConfig,
}

/// SMTP + IMAP configuration for a workload
#[derive(Clone, Debug)]
struct PluginConfig {
    smtp_host: String,
    smtp_port: u16,
    username: String,
    password: String,
    default_from: String,
    imap_host: Option<String>,
    imap_port: u16,
}

/// Per-component data.
struct ComponentData {
    /// Static interface config from wasmcloud config (fallback source)
    interface_config: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Plugin struct
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Mail {
    tracker: Arc<RwLock<HashMap<String, ComponentData>>>,
}

impl Default for Mail {
    fn default() -> Self {
        Self::new()
    }
}

impl Mail {
    pub fn new() -> Self {
        Self {
            tracker: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// ---------------------------------------------------------------------------
// WIT types::Host
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::mail::types::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::Host — empty (resource lives in HostMailClient)
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::mail::sender::Host for ActiveCtx<'a> {}

// ---------------------------------------------------------------------------
// WIT sender::HostMailClient — resource constructor + methods
// ---------------------------------------------------------------------------

impl<'a> bindings::custom::mail::sender::HostMailClient for ActiveCtx<'a> {
    async fn new(
        &mut self,
        config: Option<MailConfig>,
    ) -> wasmtime::Result<Resource<MailClientHandle>> {
        let Some(plugin) = self.get_plugin::<Mail>(PLUGIN_ID) else {
            return Err(wasmtime::Error::msg("mail plugin not available"));
        };

        let component_id: Arc<str> = self.component_id.clone();
        let lock = plugin.tracker.read().await;
        let Some(data) = lock.get(component_id.as_ref()) else {
            return Err(wasmtime::Error::msg("component not tracked"));
        };

        // Resolve required fields: smtp-host, username, password, default-from
        let smtp_host = match resolve_field(
            config.as_ref().map(|c| c.smtp_host.clone()),
            &data.interface_config,
            "smtp-host",
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(wasmtime::Error::msg(
                    "missing smtp-host: provide via constructor or interface config",
                ));
            }
        };

        let username = match resolve_field(
            config.as_ref().map(|c| c.username.clone()),
            &data.interface_config,
            "username",
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(wasmtime::Error::msg(
                    "missing username: provide via constructor or interface config",
                ));
            }
        };

        let password = match resolve_field(
            config.as_ref().map(|c| c.password.clone()),
            &data.interface_config,
            "password",
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(wasmtime::Error::msg(
                    "missing password: provide via constructor or interface config",
                ));
            }
        };

        let default_from = match resolve_field(
            config.as_ref().map(|c| c.default_from.clone()),
            &data.interface_config,
            "default-from",
        ) {
            Ok(v) => v,
            Err(_) => {
                return Err(wasmtime::Error::msg(
                    "missing default-from: provide via constructor or interface config",
                ));
            }
        };

        // Resolve optional fields: smtp-port, imap-host
        let smtp_port: u16 = resolve_optional_field(
            config
                .as_ref()
                .and_then(|c| c.smtp_port.map(|p| p.to_string())),
            &data.interface_config,
            "smtp-port",
        )
        .and_then(|v| v.parse().ok())
        .unwrap_or(465);

        let imap_host = resolve_optional_field(
            config.as_ref().and_then(|c| c.imap_host.clone()),
            &data.interface_config,
            "imap-host",
        );

        let imap_port: u16 = resolve_optional_field(None, &data.interface_config, "imap-port")
            .and_then(|v| v.parse().ok())
            .unwrap_or(993);

        drop(lock);

        let plugin_config = PluginConfig {
            smtp_host,
            smtp_port,
            username,
            password,
            default_from,
            imap_host,
            imap_port,
        };

        debug!(
            smtp_host = %plugin_config.smtp_host,
            smtp_port = plugin_config.smtp_port,
            imap_host = ?plugin_config.imap_host,
            default_from = %plugin_config.default_from,
            "Mail client resource created"
        );

        let handle = MailClientHandle {
            config: plugin_config,
        };

        let resource = self.table.push(handle)?;
        Ok(resource)
    }

    async fn send_mail(
        &mut self,
        client: Resource<MailClientHandle>,
        to: String,
        subject: String,
        body_text: Option<String>,
        body_html: Option<String>,
        cc: Option<String>,
        bcc: Option<String>,
    ) -> wasmtime::Result<Result<(), MailError>> {
        let handle = self.table.get(&client)?;

        // Parse from address
        let from_mailbox: Mailbox = match handle.config.default_from.parse() {
            Ok(m) => m,
            Err(e) => {
                return Ok(Err(MailError::InvalidAddress(format!(
                    "invalid from address '{}': {}",
                    handle.config.default_from, e
                ))));
            }
        };

        // Parse to addresses
        let to_addrs = match parse_addresses(&to) {
            Ok(addrs) => addrs,
            Err(e) => return Ok(Err(MailError::InvalidAddress(e))),
        };

        // Build message
        let mut builder = Message::builder().from(from_mailbox).subject(&subject);

        for addr in to_addrs {
            builder = builder.to(addr);
        }

        if let Some(ref cc_str) = cc
            && !cc_str.is_empty()
        {
            match parse_addresses(cc_str) {
                Ok(addrs) => {
                    for addr in addrs {
                        builder = builder.cc(addr);
                    }
                }
                Err(e) => return Ok(Err(MailError::InvalidAddress(e))),
            }
        }

        if let Some(ref bcc_str) = bcc
            && !bcc_str.is_empty()
        {
            match parse_addresses(bcc_str) {
                Ok(addrs) => {
                    for addr in addrs {
                        builder = builder.bcc(addr);
                    }
                }
                Err(e) => return Ok(Err(MailError::InvalidAddress(e))),
            }
        }

        // Build email body
        let email = match (body_text, body_html) {
            (Some(text), Some(html)) => builder.multipart(
                lettre::message::MultiPart::alternative()
                    .singlepart(
                        lettre::message::SinglePart::builder()
                            .header(lettre::message::header::ContentType::TEXT_PLAIN)
                            .body(text),
                    )
                    .singlepart(
                        lettre::message::SinglePart::builder()
                            .header(lettre::message::header::ContentType::TEXT_HTML)
                            .body(html),
                    ),
            ),
            (Some(text), None) => builder.body(text),
            (None, Some(html)) => builder.body(html),
            (None, None) => {
                return Ok(Err(MailError::InvalidAddress(
                    "either body-text or body-html must be provided".to_string(),
                )));
            }
        };

        let email = match email {
            Ok(msg) => msg,
            Err(e) => {
                return Ok(Err(MailError::SendFailed(format!(
                    "failed to build email: {}",
                    e
                ))));
            }
        };

        debug!(
            to = %to,
            subject = %subject,
            "Sending email via SMTP"
        );

        // Build SMTP transport
        let creds = Credentials::new(
            handle.config.username.clone(),
            handle.config.password.clone(),
        );

        let mailer =
            match AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&handle.config.smtp_host) {
                Ok(m) => m,
                Err(_) => {
                    match AsyncSmtpTransport::<Tokio1Executor>::relay(&handle.config.smtp_host) {
                        Ok(m) => m,
                        Err(e) => {
                            return Ok(Err(MailError::Internal(format!(
                                "failed to create SMTP transport: {e}"
                            ))));
                        }
                    }
                }
            }
            .port(handle.config.smtp_port)
            .credentials(creds)
            .build();

        match mailer.send(email).await {
            Ok(_) => {
                debug!("Email sent successfully");
                Ok(Ok(()))
            }
            Err(e) => {
                debug!(error = %e, "Failed to send email");
                Ok(Err(MailError::SendFailed(format!(
                    "smtp send failed: {}",
                    e
                ))))
            }
        }
    }

    async fn list_mails(
        &mut self,
        client: Resource<MailClientHandle>,
        mailbox: Option<String>,
        search_criteria: Option<String>,
        limit: Option<u32>,
    ) -> wasmtime::Result<Result<String, MailError>> {
        let handle = self.table.get(&client)?;

        let mut session = match connect_imap(&handle.config).await {
            Ok(s) => s,
            Err(e) => return Ok(Err(MailError::Internal(e))),
        };

        let mailbox_name = mailbox.as_deref().unwrap_or("INBOX");
        let criteria = search_criteria.as_deref().unwrap_or("ALL");
        let max_results = limit.unwrap_or(20);

        session.select(mailbox_name).await.map_err(|e| {
            MailError::Internal(format!("failed to select mailbox '{mailbox_name}': {e}"))
        })?;

        let uids = session
            .uid_search(criteria)
            .await
            .map_err(|e| MailError::Internal(format!("IMAP search failed: {e}")))?;

        let mut results = Vec::new();
        let uid_vec: Vec<u32> = uids.iter().copied().collect();
        // Take only the last N UIDs (most recent)
        let start = uid_vec.len().saturating_sub(max_results as usize);
        let selected_uids = uid_vec.get(start..).unwrap_or_default();

        if !selected_uids.is_empty() {
            let uid_set = selected_uids
                .iter()
                .map(|u| u.to_string())
                .collect::<Vec<_>>()
                .join(",");
            let messages = session
                .uid_fetch(&uid_set, "(UID ENVELOPE FLAGS)")
                .await
                .map_err(|e| MailError::Internal(format!("IMAP fetch failed: {e}")))?;

            use futures::TryStreamExt;
            let msgs: Vec<_> = messages
                .try_collect()
                .await
                .map_err(|e| MailError::Internal(format!("IMAP fetch collect failed: {e}")))?;

            for msg in msgs {
                let uid = msg.uid.map(|u| u.to_string()).unwrap_or_default();
                let subject = msg
                    .envelope()
                    .and_then(|e| e.subject.as_ref())
                    .map(|s| String::from_utf8_lossy(s).to_string())
                    .unwrap_or_default();
                let from = msg
                    .envelope()
                    .and_then(|e| e.from.as_ref().and_then(|v| v.first()))
                    .map(|addr| {
                        let name = addr
                            .name
                            .as_ref()
                            .map(|n| format!("{} ", String::from_utf8_lossy(n)));
                        let mbx = addr
                            .mailbox
                            .as_ref()
                            .map(|m| String::from_utf8_lossy(m).to_string())
                            .unwrap_or_default();
                        let host = addr
                            .host
                            .as_ref()
                            .map(|h| format!("@{}", String::from_utf8_lossy(h)))
                            .unwrap_or_default();
                        format!("{}{}{}", name.unwrap_or_default(), mbx, host)
                    })
                    .unwrap_or_default();
                let date = msg
                    .envelope()
                    .and_then(|e| e.date.as_ref())
                    .map(|d| String::from_utf8_lossy(d).to_string())
                    .unwrap_or_default();
                let flags: Vec<String> = msg.flags().map(|f| format!("{f:?}")).collect();

                results.push(serde_json::json!({
                    "uid": uid,
                    "subject": subject,
                    "from": from,
                    "date": date,
                    "flags": flags,
                }));
            }
        }

        let _ = session.logout().await;

        match serde_json::to_string(&results) {
            Ok(json) => Ok(Ok(json)),
            Err(e) => Ok(Err(MailError::Internal(format!(
                "failed to serialize mail list: {e}"
            )))),
        }
    }

    async fn get_mail(
        &mut self,
        client: Resource<MailClientHandle>,
        message_id: String,
        mailbox: Option<String>,
    ) -> wasmtime::Result<Result<String, MailError>> {
        let handle = self.table.get(&client)?;

        let mut session = match connect_imap(&handle.config).await {
            Ok(s) => s,
            Err(e) => return Ok(Err(MailError::Internal(e))),
        };

        let mailbox_name = mailbox.as_deref().unwrap_or("INBOX");

        session.select(mailbox_name).await.map_err(|e| {
            MailError::Internal(format!("failed to select mailbox '{mailbox_name}': {e}"))
        })?;

        let messages = session
            .uid_fetch(&message_id, "(UID BODY.PEEK[])")
            .await
            .map_err(|e| MailError::Internal(format!("IMAP fetch failed: {e}")))?;

        use futures::TryStreamExt;
        let msgs: Vec<_> = messages
            .try_collect()
            .await
            .map_err(|e| MailError::Internal(format!("IMAP fetch collect failed: {e}")))?;

        let _ = session.logout().await;

        let msg = match msgs.first() {
            Some(m) => m,
            None => {
                return Ok(Err(MailError::Internal(format!(
                    "message '{message_id}' not found"
                ))));
            }
        };

        let body = match msg.body() {
            Some(b) => b,
            None => {
                return Ok(Err(MailError::Internal(
                    "message has no body content".to_string(),
                )));
            }
        };

        let parsed = mailparse::parse_mail(body)
            .map_err(|e| MailError::Internal(format!("failed to parse email: {e}")))?;

        let uid = msg.uid.map(|u| u.to_string()).unwrap_or_default();

        // Extract headers
        let subject = parsed
            .headers
            .get_first_value("Subject")
            .unwrap_or_default();
        let from = parsed.headers.get_first_value("From").unwrap_or_default();
        let to = parsed.headers.get_first_value("To").unwrap_or_default();
        let date = parsed.headers.get_first_value("Date").unwrap_or_default();

        // Extract body text and HTML
        let mut body_text = None;
        let mut body_html = None;
        let mut content_type = "text/plain".to_string();

        extract_bodies(&parsed, &mut body_text, &mut body_html, &mut content_type);

        let result = serde_json::json!({
            "uid": uid,
            "subject": subject,
            "from": from,
            "to": to,
            "date": date,
            "body_text": body_text,
            "body_html": body_html,
            "content_type": content_type,
        });

        match serde_json::to_string(&result) {
            Ok(json) => Ok(Ok(json)),
            Err(e) => Ok(Err(MailError::Internal(format!(
                "failed to serialize mail: {e}"
            )))),
        }
    }

    async fn stop(
        &mut self,
        _client: Resource<MailClientHandle>,
    ) -> wasmtime::Result<Result<(), MailError>> {
        // No background tasks to stop; no-op
        debug!("Mail client stop() called (no-op)");
        Ok(Ok(()))
    }

    async fn drop(&mut self, rep: Resource<MailClientHandle>) -> wasmtime::Result<()> {
        if let Ok(_handle) = self.table.delete(rep) {
            debug!("Mail client resource dropped");
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse comma-separated email addresses into a Vec of Mailbox.
fn parse_addresses(addrs: &str) -> Result<Vec<Mailbox>, String> {
    let mut result = Vec::new();
    for addr in addrs.split(',') {
        let addr = addr.trim();
        if addr.is_empty() {
            continue;
        }
        let mailbox: Mailbox = addr.parse().map_err(|e: lettre::address::AddressError| {
            format!("invalid address '{}': {}", addr, e)
        })?;
        result.push(mailbox);
    }
    if result.is_empty() {
        return Err("no valid addresses provided".to_string());
    }
    Ok(result)
}

/// Connect to an IMAP server via TLS and authenticate.
async fn connect_imap(
    config: &PluginConfig,
) -> Result<async_imap::Session<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>, String> {
    let imap_host = match &config.imap_host {
        Some(h) => h.clone(),
        None => return Err("IMAP host not configured".to_string()),
    };

    let addr = format!("{imap_host}:{}", config.imap_port);
    let tcp_stream = tokio::net::TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("failed to connect to IMAP server '{addr}': {e}"))?;

    let root_cert_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let client_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();
    let config_ref = Arc::new(client_config);
    let server_name = rustls_pki_types::ServerName::try_from(imap_host.clone())
        .map_err(|e| format!("invalid IMAP server name '{imap_host}': {e}"))?;
    let connector = tokio_rustls::TlsConnector::from(config_ref);
    let tls_stream = connector
        .connect(server_name, tcp_stream)
        .await
        .map_err(|e| format!("TLS handshake failed for IMAP: {e}"))?;

    let client = async_imap::Client::new(tls_stream);
    let session = client
        .login(&config.username, &config.password)
        .await
        .map_err(|(e, _)| format!("IMAP login failed: {e}"))?;

    Ok(session)
}

/// Recursively extract text and HTML bodies from a parsed email.
fn extract_bodies(
    part: &mailparse::ParsedMail,
    text: &mut Option<String>,
    html: &mut Option<String>,
    content_type: &mut String,
) {
    let ct = part
        .headers
        .get_first_value("Content-Type")
        .unwrap_or_default();
    let ct_lower = ct.to_lowercase();

    if ct_lower.starts_with("text/plain") {
        if let Ok(body) = part.get_body() {
            *text = Some(body);
            *content_type = "text/plain".to_string();
        }
    } else if ct_lower.starts_with("text/html") {
        if let Ok(body) = part.get_body() {
            *html = Some(body);
            *content_type = "text/html".to_string();
        }
    } else if ct_lower.starts_with("multipart/") {
        for subpart in &part.subparts {
            extract_bodies(subpart, text, html, content_type);
        }
    }
}

// ---------------------------------------------------------------------------
// HostPlugin Implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl HostPlugin for Mail {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            exports: HashSet::from([WitInterface::from("custom:mail/sender@0.1.0")]),
            ..Default::default()
        }
    }

    async fn on_workload_item_bind<'a>(
        &self,
        component_handle: &mut WorkloadItem<'a>,
        interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        let Some(interface) = find_interface(&interfaces, "custom", "mail") else {
            return Ok(());
        };

        let interface_config = interface.config.clone();

        bindings::custom::mail::types::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::mail::sender::add_to_linker::<_, SharedCtx>(
            component_handle.linker(),
            extract_active_ctx,
        )?;

        let WorkloadItem::Component(ch) = component_handle else {
            return Ok(());
        };

        let component_id = ch.id().to_string();
        debug!(component_id = %component_id, "Mail plugin bound to component");

        self.tracker
            .write()
            .await
            .insert(component_id, ComponentData { interface_config });

        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        self.tracker.write().await.remove(workload_id);
        debug!(workload_id = %workload_id, "Mail plugin unbound");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_id() {
        let plugin = Mail::new();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_default() {
        let plugin = Mail::default();
        assert_eq!(plugin.id(), PLUGIN_ID);
    }

    #[test]
    fn test_parse_addresses_valid() {
        let addrs = parse_addresses("a@b.com, c@d.com").unwrap();
        assert_eq!(addrs.len(), 2);
    }

    #[test]
    fn test_parse_addresses_single() {
        let addrs = parse_addresses("test@example.com").unwrap();
        assert_eq!(addrs.len(), 1);
    }

    #[test]
    fn test_parse_addresses_invalid() {
        let result = parse_addresses("not-an-email");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_addresses_empty() {
        let result = parse_addresses("");
        assert!(result.is_err());
    }
}
