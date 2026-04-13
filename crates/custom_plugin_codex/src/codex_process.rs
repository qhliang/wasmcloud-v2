//! Subprocess management, JSONL parsing, and binary download for the Codex CLI.

use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::Deserialize;
use tracing::{debug, instrument, warn};

// ---------------------------------------------------------------------------
// JSONL event types (from codex exec --json output)
// ---------------------------------------------------------------------------

/// Token usage reported in `turn.completed` events.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct CodexUsage {
    pub input_tokens: u64,
    #[serde(default)]
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
}

/// A JSONL event emitted by `codex exec --json`.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum CodexJsonlEvent {
    #[serde(rename = "thread.started")]
    ThreadStarted { thread_id: String },
    #[serde(rename = "turn.started")]
    TurnStarted,
    #[serde(rename = "item.started")]
    ItemStarted { item: serde_json::Value },
    #[serde(rename = "item.completed")]
    ItemCompleted { item: serde_json::Value },
    #[serde(rename = "turn.completed")]
    TurnCompleted { usage: CodexUsage },
    #[serde(rename = "turn.failed")]
    TurnFailed { error: serde_json::Value },
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(rename = "exec.approval.request")]
    ExecApprovalRequest { item: serde_json::Value },
}

/// Token usage accumulator for a session.
#[derive(Clone, Debug, Default)]
pub struct TokenUsageAccum {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
}

impl TokenUsageAccum {
    pub fn add(&mut self, usage: &CodexUsage) {
        self.input_tokens += usage.input_tokens;
        self.cached_input_tokens += usage.cached_input_tokens;
        self.output_tokens += usage.output_tokens;
    }
}

// ---------------------------------------------------------------------------
// Subprocess spawning
// ---------------------------------------------------------------------------

/// Configuration for spawning a codex subprocess.
#[derive(Clone, Debug)]
pub struct CodexSpawnConfig {
    pub binary_path: PathBuf,
    pub model: String,
    pub api_token: String,
    pub base_url: Option<String>,
    pub project_dir: PathBuf,
}

/// Spawn `codex exec --json` as a subprocess and return the child handle and stdin.
#[instrument(skip(config))]
pub fn spawn_codex_exec(
    config: &CodexSpawnConfig,
    prompt: &str,
    session_id: Option<&str>,
) -> anyhow::Result<(tokio::process::Child, tokio::process::ChildStdin)> {
    let mut cmd = tokio::process::Command::new(&config.binary_path);

    if let Some(sid) = session_id {
        cmd.arg("exec").arg("resume").arg(sid);
    } else {
        cmd.arg("exec");
    }

    cmd.arg("--json")
        .arg("--model")
        .arg(&config.model)
        .arg("--skip-git-repo-check")
        .arg("--color")
        .arg("never")
        .arg(prompt);

    cmd.current_dir(&config.project_dir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .stdin(std::process::Stdio::piped())
        .env("CODEX_API_KEY", &config.api_token);

    if let Some(ref base_url) = config.base_url {
        cmd.env("OPENAI_BASE_URL", base_url);
    }

    debug!(
        binary = %config.binary_path.display(),
        model = %config.model,
        project_dir = %config.project_dir.display(),
        session_id = session_id.unwrap_or("none"),
        "Spawning codex exec subprocess (approval mode)"
    );

    let mut child = cmd.spawn().context("failed to spawn codex exec subprocess")?;

    let stdin = child.stdin.take().context("codex process has no stdin")?;

    Ok((child, stdin))
}

// ---------------------------------------------------------------------------
// JSONL reader background task
// ---------------------------------------------------------------------------

/// Read JSONL output from codex stdout, parse events, and push to a shared buffer.
pub async fn read_jsonl_output(
    stdout: tokio::process::ChildStdout,
    sender: tokio::sync::mpsc::UnboundedSender<CodexJsonlEvent>,
) {
    use tokio::io::{AsyncBufReadExt, BufReader};

    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        match serde_json::from_str::<CodexJsonlEvent>(&line) {
            Ok(event) => {
                // debug!(event_type = ?std::mem::discriminant(&event), "Parsed JSONL event");
                if sender.send(event).is_err() {
                    break;
                }
            }
            Err(e) => {
                warn!(error = %e, line = %line, "Failed to parse JSONL event from codex");
            }
        }
    }

    debug!("Codex JSONL reader finished (stdout closed)");
}

// ---------------------------------------------------------------------------
// Binary download
// ---------------------------------------------------------------------------

/// Detect the platform-specific tarball URL for the latest codex release.
pub fn detect_platform_tarball_url() -> Option<String> {
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;

    let target = match (os, arch) {
        ("linux", "x86_64") => "codex-x86_64-unknown-linux-musl",
        ("linux", "aarch64") => "codex-aarch64-unknown-linux-musl",
        ("macos", "x86_64") => "codex-x86_64-apple-darwin",
        ("macos", "aarch64") => "codex-aarch64-apple-darwin",
        _ => return None,
    };

    Some(format!(
        "https://github.com/openai/codex/releases/latest/download/{target}.tar.gz"
    ))
}

/// Download the codex binary from GitHub releases and extract it to `target_path`.
pub async fn download_codex_binary(target_path: &Path) -> anyhow::Result<()> {
    let url = detect_platform_tarball_url().ok_or_else(|| {
        anyhow::anyhow!(
            "unsupported platform (os={}, arch={}) for codex binary download",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;

    debug!(url = %url, "Downloading codex binary");

    let response = reqwest::get(&url)
        .await
        .context("failed to download codex binary")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "failed to download codex binary: HTTP {}",
            response.status()
        );
    }

    let bytes = response
        .bytes()
        .await
        .context("failed to read codex binary response body")?;

    // Ensure parent directory exists
    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent)
            .context("failed to create codex binary parent directory")?;
    }

    // Decompress tar.gz and extract the codex binary
    let tarball = flate2::read::GzDecoder::new(&bytes[..]);
    let mut archive = tar::Archive::new(tarball);

    for entry in archive
        .entries()
        .context("failed to read tar archive entries")?
    {
        let mut entry = entry.context("failed to read tar entry")?;
        let path = entry.path().context("failed to get tar entry path")?;
        // Look for any file named "codex" or starting with "codex-"
        if path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("codex"))
        {
            entry
                .unpack(target_path)
                .context("failed to extract codex binary")?;

            // Make executable
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(target_path, std::fs::Permissions::from_mode(0o755))
                    .context("failed to set codex binary permissions")?;
            }

            debug!(path = %target_path.display(), "Codex binary downloaded and extracted");
            return Ok(());
        }
    }

    anyhow::bail!("codex binary not found in downloaded archive")
}

/// Ensure the codex binary exists at the configured path, downloading if necessary.
pub async fn ensure_codex_binary(binary_path: &Path) -> anyhow::Result<()> {
    if binary_path.exists() {
        debug!(path = %binary_path.display(), "Codex binary already exists");
        return Ok(());
    }

    debug!(
        path = %binary_path.display(),
        "Codex binary not found, downloading..."
    );
    download_codex_binary(binary_path).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_platform_tarball_url() {
        let url = detect_platform_tarball_url();
        // Should return a URL on supported platforms (macOS/Linux)
        if cfg!(target_os = "macos") || cfg!(target_os = "linux") {
            assert!(url.is_some());
            let url = url.unwrap();
            assert!(url.starts_with("https://github.com/openai/codex/releases/"));
            assert!(url.ends_with(".tar.gz"));
        }
    }

    #[test]
    fn test_parse_thread_started() {
        let json =
            r#"{"type":"thread.started","thread_id":"0199a213-81c0-7800-8aa1-bbab2a035a53"}"#;
        let event: CodexJsonlEvent = serde_json::from_str(json).unwrap();
        match event {
            CodexJsonlEvent::ThreadStarted { thread_id } => {
                assert_eq!(thread_id, "0199a213-81c0-7800-8aa1-bbab2a035a53");
            }
            _ => panic!("expected ThreadStarted"),
        }
    }

    #[test]
    fn test_parse_turn_started() {
        let json = r#"{"type":"turn.started"}"#;
        let event: CodexJsonlEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(event, CodexJsonlEvent::TurnStarted));
    }

    #[test]
    fn test_parse_item_started() {
        let json = r#"{"type":"item.started","item":{"id":"item_1","type":"command_execution","command":"bash -lc ls","status":"in_progress"}}"#;
        let event: CodexJsonlEvent = serde_json::from_str(json).unwrap();
        match event {
            CodexJsonlEvent::ItemStarted { item } => {
                assert_eq!(item["id"], "item_1");
            }
            _ => panic!("expected ItemStarted"),
        }
    }

    #[test]
    fn test_parse_item_completed() {
        let json = r#"{"type":"item.completed","item":{"id":"item_3","type":"agent_message","text":"Repo contains docs, sdk, and examples directories."}}"#;
        let event: CodexJsonlEvent = serde_json::from_str(json).unwrap();
        match event {
            CodexJsonlEvent::ItemCompleted { item } => {
                assert_eq!(item["type"], "agent_message");
                assert_eq!(
                    item["text"],
                    "Repo contains docs, sdk, and examples directories."
                );
            }
            _ => panic!("expected ItemCompleted"),
        }
    }

    #[test]
    fn test_parse_turn_completed() {
        let json = r#"{"type":"turn.completed","usage":{"input_tokens":24763,"cached_input_tokens":24448,"output_tokens":122}}"#;
        let event: CodexJsonlEvent = serde_json::from_str(json).unwrap();
        match event {
            CodexJsonlEvent::TurnCompleted { usage } => {
                assert_eq!(usage.input_tokens, 24763);
                assert_eq!(usage.cached_input_tokens, 24448);
                assert_eq!(usage.output_tokens, 122);
            }
            _ => panic!("expected TurnCompleted"),
        }
    }

    #[test]
    fn test_token_usage_accum() {
        let mut accum = TokenUsageAccum::default();
        accum.add(&CodexUsage {
            input_tokens: 100,
            cached_input_tokens: 50,
            output_tokens: 200,
        });
        accum.add(&CodexUsage {
            input_tokens: 300,
            cached_input_tokens: 0,
            output_tokens: 400,
        });
        assert_eq!(accum.input_tokens, 400);
        assert_eq!(accum.cached_input_tokens, 50);
        assert_eq!(accum.output_tokens, 600);
    }

    #[test]
    fn test_parse_error_event() {
        let json = r#"{"type":"error","message":"Missing environment variable: `GLM_API_TOKEN`."}"#;
        let event: CodexJsonlEvent = serde_json::from_str(json).unwrap();
        match event {
            CodexJsonlEvent::Error { message } => {
                assert!(message.contains("GLM_API_TOKEN"));
            }
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn test_parse_turn_failed() {
        let json = r#"{"type":"turn.failed","error":{"message":"Missing environment variable: `GLM_API_TOKEN`."}}"#;
        let event: CodexJsonlEvent = serde_json::from_str(json).unwrap();
        match event {
            CodexJsonlEvent::TurnFailed { error } => {
                assert_eq!(
                    error.get("message").and_then(|v| v.as_str()),
                    Some("Missing environment variable: `GLM_API_TOKEN`.")
                );
            }
            _ => panic!("expected TurnFailed"),
        }
    }
}
