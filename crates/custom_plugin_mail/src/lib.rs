//! # Mail Host Plugin
//!
//! This module implements a wasmCloud host plugin that provides
//! `custom:mail/sender@0.1.0` interface for sending emails via SMTP
//! and reading emails via IMAP.

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
use wash_runtime::plugin::HostPlugin;
use wash_runtime::wit::{WitInterface, WitWorld};

mod bindings {
    wasmtime::component::bindgen!({
        world: "mail",
        imports: {
            default: async | trappable | tracing,
        },
    });
}

use bindings::custom::mail::types::MailError;

const PLUGIN_ID: &str = "plugin-mail";

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

/// Extract and validate config from interface config.
fn extract_config(interface: &WitInterface) -> Result<PluginConfig, String> {
    let smtp_host = interface
        .config
        .get("smtp-host")
        .cloned()
        .unwrap_or_default();
    if smtp_host.is_empty() {
        return Err("missing required config: 'smtp-host'".to_string());
    }

    let smtp_port: u16 = interface
        .config
        .get("smtp-port")
        .and_then(|v| v.parse().ok())
        .unwrap_or(465);

    let username = interface
        .config
        .get("username")
        .cloned()
        .unwrap_or_default();
    if username.is_empty() {
        return Err("missing required config: 'username'".to_string());
    }

    let password = interface
        .config
        .get("password")
        .cloned()
        .unwrap_or_default();
    if password.is_empty() {
        return Err("missing required config: 'password'".to_string());
    }

    let default_from = interface
        .config
        .get("default-from")
        .cloned()
        .unwrap_or_default();
    if default_from.is_empty() {
        return Err("missing required config: 'default-from'".to_string());
    }

    let imap_host = interface.config.get("imap-host").cloned();
    let imap_port: u16 = interface
        .config
        .get("imap-port")
        .and_then(|v| v.parse().ok())
        .unwrap_or(993);

    Ok(PluginConfig {
        smtp_host,
        smtp_port,
        username,
        password,
        default_from,
        imap_host,
        imap_port,
    })
}

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
) -> Result<async_imap::Session<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>, String>
{
    let imap_host = match &config.imap_host {
        Some(h) => h.clone(),
        None => return Err("IMAP host not configured".to_string()),
    };

    let addr = format!("{imap_host}:{}", config.imap_port);
    let tcp_stream = tokio::net::TcpStream::connect(&addr)
        .await
        .map_err(|e| format!("failed to connect to IMAP server '{addr}': {e}"))?;

    let root_cert_store = rustls::RootCertStore::from_iter(
        webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
    );
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

/// Mail host plugin for sending emails via SMTP and reading via IMAP.
#[derive(Clone, Default)]
pub struct Mail {
    configs: Arc<RwLock<HashMap<String, PluginConfig>>>,
}

impl Mail {
    /// Create a new Mail plugin.
    /// Configuration is provided per-workload via interface config.
    pub fn new() -> Self {
        Self {
            configs: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// ============================================================================
// Sender Interface Implementation
// ============================================================================

impl<'a> bindings::custom::mail::sender::Host for ActiveCtx<'a> {
    async fn send_mail(
        &mut self,
        to: String,
        subject: String,
        body_text: Option<String>,
        body_html: Option<String>,
        cc: Option<String>,
        bcc: Option<String>,
    ) -> wasmtime::Result<Result<(), MailError>> {
        let Some(plugin) = self.get_plugin::<Mail>(PLUGIN_ID) else {
            return Ok(Err(MailError::Internal(
                "Mail plugin not available".to_string(),
            )));
        };

        let workload_id = self.workload_id.as_ref().to_string();

        let config = {
            let configs = plugin.configs.read().await;
            match configs.get(&workload_id).cloned() {
                Some(c) => c,
                None => {
                    return Ok(Err(MailError::ConfigError(format!(
                        "no mail config found for workload '{}'",
                        workload_id
                    ))));
                }
            }
        };

        // Parse from address
        let from_mailbox: Mailbox = match config.default_from.parse() {
            Ok(m) => m,
            Err(e) => {
                return Ok(Err(MailError::InvalidAddress(format!(
                    "invalid from address '{}': {}",
                    config.default_from, e
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

        if let Some(ref cc_str) = cc {
            if !cc_str.is_empty() {
                match parse_addresses(cc_str) {
                    Ok(addrs) => {
                        for addr in addrs {
                            builder = builder.cc(addr);
                        }
                    }
                    Err(e) => return Ok(Err(MailError::InvalidAddress(e))),
                }
            }
        }

        if let Some(ref bcc_str) = bcc {
            if !bcc_str.is_empty() {
                match parse_addresses(bcc_str) {
                    Ok(addrs) => {
                        for addr in addrs {
                            builder = builder.bcc(addr);
                        }
                    }
                    Err(e) => return Ok(Err(MailError::InvalidAddress(e))),
                }
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
            workload_id = %workload_id,
            to = %to,
            subject = %subject,
            "Sending email via SMTP"
        );

        // Build SMTP transport
        let creds = Credentials::new(config.username.clone(), config.password.clone());

        let mailer = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&config.smtp_host)
            .unwrap_or_else(|_| {
                AsyncSmtpTransport::<Tokio1Executor>::relay(&config.smtp_host)
                    .expect("failed to create SMTP transport")
            })
            .port(config.smtp_port)
            .credentials(creds)
            .build();

        match mailer.send(email).await {
            Ok(_) => {
                debug!(
                    workload_id = %workload_id,
                    "Email sent successfully"
                );
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
        mailbox: Option<String>,
        search_criteria: Option<String>,
        limit: Option<u32>,
    ) -> wasmtime::Result<Result<String, MailError>> {
        let Some(plugin) = self.get_plugin::<Mail>(PLUGIN_ID) else {
            return Ok(Err(MailError::Internal(
                "Mail plugin not available".to_string(),
            )));
        };

        let workload_id = self.workload_id.as_ref().to_string();

        let config = {
            let configs = plugin.configs.read().await;
            match configs.get(&workload_id).cloned() {
                Some(c) => c,
                None => {
                    return Ok(Err(MailError::ConfigError(format!(
                        "no mail config found for workload '{}'",
                        workload_id
                    ))));
                }
            }
        };

        let mut session = match connect_imap(&config).await {
            Ok(s) => s,
            Err(e) => return Ok(Err(MailError::Internal(e))),
        };

        let mailbox_name = mailbox.as_deref().unwrap_or("INBOX");
        let criteria = search_criteria.as_deref().unwrap_or("ALL");
        let max_results = limit.unwrap_or(20);

        session
            .select(mailbox_name)
            .await
            .map_err(|e| MailError::Internal(format!("failed to select mailbox '{mailbox_name}': {e}")))?;

        let uids = session
            .uid_search(criteria)
            .await
            .map_err(|e| MailError::Internal(format!("IMAP search failed: {e}")))?;

        let mut results = Vec::new();
        let uid_vec: Vec<u32> = uids.iter().copied().collect();
        // Take only the last N UIDs (most recent)
        let start = uid_vec.len().saturating_sub(max_results as usize);
        let selected_uids = &uid_vec[start..];

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
        message_id: String,
        mailbox: Option<String>,
    ) -> wasmtime::Result<Result<String, MailError>> {
        let Some(plugin) = self.get_plugin::<Mail>(PLUGIN_ID) else {
            return Ok(Err(MailError::Internal(
                "Mail plugin not available".to_string(),
            )));
        };

        let workload_id = self.workload_id.as_ref().to_string();

        let config = {
            let configs = plugin.configs.read().await;
            match configs.get(&workload_id).cloned() {
                Some(c) => c,
                None => {
                    return Ok(Err(MailError::ConfigError(format!(
                        "no mail config found for workload '{}'",
                        workload_id
                    ))));
                }
            }
        };

        let mut session = match connect_imap(&config).await {
            Ok(s) => s,
            Err(e) => return Ok(Err(MailError::Internal(e))),
        };

        let mailbox_name = mailbox.as_deref().unwrap_or("INBOX");

        session
            .select(mailbox_name)
            .await
            .map_err(|e| MailError::Internal(format!("failed to select mailbox '{mailbox_name}': {e}")))?;

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
        let from = parsed
            .headers
            .get_first_value("From")
            .unwrap_or_default();
        let to = parsed.headers.get_first_value("To").unwrap_or_default();
        let date = parsed
            .headers
            .get_first_value("Date")
            .unwrap_or_default();

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

// ============================================================================
// HostPlugin Implementation
// ============================================================================

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
        let mail_interface = interfaces
            .iter()
            .find(|i| i.namespace == "custom" && i.package == "mail");

        let Some(interface) = mail_interface else {
            return Ok(());
        };

        let workload_id = component_handle.workload_id().to_string();

        let config = match extract_config(interface) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(
                    workload_id = %workload_id,
                    "Mail plugin config validation failed: {}", e
                );
                return Ok(());
            }
        };

        debug!(
            workload_id = %workload_id,
            smtp_host = %config.smtp_host,
            smtp_port = config.smtp_port,
            imap_host = ?config.imap_host,
            default_from = %config.default_from,
            "Configuring Mail plugin for workload"
        );

        {
            let mut configs = self.configs.write().await;
            configs.insert(workload_id.clone(), config);
        }

        let linker = component_handle.linker();
        bindings::custom::mail::sender::add_to_linker::<_, SharedCtx>(linker, extract_active_ctx)?;

        debug!("Mail plugin bound to workload '{workload_id}'");
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: HashSet<WitInterface>,
    ) -> anyhow::Result<()> {
        {
            let mut configs = self.configs.write().await;
            configs.remove(workload_id);
        }
        debug!("Mail plugin unbound from workload '{workload_id}'");
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
    fn test_extract_config_valid() {
        let mut config = HashMap::new();
        config.insert("smtp-host".to_string(), "smtp.example.com".to_string());
        config.insert("smtp-port".to_string(), "587".to_string());
        config.insert("username".to_string(), "user@example.com".to_string());
        config.insert("password".to_string(), "secret".to_string());
        config.insert(
            "default-from".to_string(),
            "noreply@example.com".to_string(),
        );

        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "plugin-mail".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config,
            name: None,
        };

        let cfg = extract_config(&interface).unwrap();
        assert_eq!(cfg.smtp_host, "smtp.example.com");
        assert_eq!(cfg.smtp_port, 587);
        assert_eq!(cfg.username, "user@example.com");
        assert!(cfg.imap_host.is_none());
        assert_eq!(cfg.imap_port, 993);
    }

    #[test]
    fn test_extract_config_with_imap() {
        let mut config = HashMap::new();
        config.insert("smtp-host".to_string(), "smtp.example.com".to_string());
        config.insert("username".to_string(), "user".to_string());
        config.insert("password".to_string(), "pass".to_string());
        config.insert("default-from".to_string(), "from@example.com".to_string());
        config.insert("imap-host".to_string(), "imap.example.com".to_string());
        config.insert("imap-port".to_string(), "993".to_string());

        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "plugin-mail".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config,
            name: None,
        };

        let cfg = extract_config(&interface).unwrap();
        assert_eq!(cfg.imap_host.as_deref(), Some("imap.example.com"));
        assert_eq!(cfg.imap_port, 993);
    }

    #[test]
    fn test_extract_config_default_port() {
        let mut config = HashMap::new();
        config.insert("smtp-host".to_string(), "smtp.example.com".to_string());
        config.insert("username".to_string(), "user".to_string());
        config.insert("password".to_string(), "pass".to_string());
        config.insert("default-from".to_string(), "from@example.com".to_string());

        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "plugin-mail".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config,
            name: None,
        };

        let cfg = extract_config(&interface).unwrap();
        assert_eq!(cfg.smtp_port, 465);
    }

    #[test]
    fn test_extract_config_missing_host() {
        let interface = WitInterface {
            namespace: "custom".to_string(),
            package: "plugin-mail".to_string(),
            interfaces: HashSet::new(),
            version: None,
            config: HashMap::new(),
            name: None,
        };

        let result = extract_config(&interface);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("smtp-host"));
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
