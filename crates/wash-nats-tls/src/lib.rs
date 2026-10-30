//! NATS TLS 配置解析（宿主插件共用）。
//!
//! ## 为什么需要
//! `hostInterfaces[].config` 里的 `nats_tls_ca` / `nats_tls_cert` /
//! `nats_tls_key` 原本只能填**文件路径**。但 WebUI 的 Secret 存的是**证书内容**
//! （PEM），直接填进去会被当成路径去 `File::open`，连接必然失败。
//!
//! 这里把两种取值都支持：
//! - 值以 `-----BEGIN ...` 开头 ⇒ **PEM 内容**（在内存里构建 rustls 配置，不落盘）；
//! - 否则 ⇒ **文件路径**（保持旧行为，走 async-nats 自带的路径 API）。
//!
//! 三个键可以混用（例如 CA 用内容、客户端证书用挂载路径）。
//!
//! ⚠️ 走「内容」分支时，`async-nats` 会用我们提供的 `ClientConfig` **完全跳过**
//! 它自己的根证书加载（见 async-nats `tls.rs::config_tls`）。所以没配 CA 时
//! 这里要自己加载系统根证书，否则访问公网 NATS 会验签失败。
//!
//! ⚠️ 客户端证书与私钥必须**同时**提供：只给证书会得到明确报错，只给私钥
//! 则忽略（与 async-nats 的路径行为保持一致）。
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use async_nats::ConnectOptions;
use async_nats::rustls::pki_types::pem::PemObject;
use async_nats::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use async_nats::rustls::{ClientConfig, RootCertStore};

/// CA 证书（可选）。
pub const CA_KEY: &str = "nats_tls_ca";
/// 客户端证书（可选，必须与私钥成对）。
pub const CERT_KEY: &str = "nats_tls_cert";
/// 客户端私钥（可选，必须与证书成对）。
pub const KEY_KEY: &str = "nats_tls_key";

/// 一个 TLS 取值的来源。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TlsValue {
    /// 文件路径（旧行为）。
    Path(PathBuf),
    /// PEM 文本（来自 Secret / ConfigMap）。
    Pem(String),
}

/// 判断一个值是不是 PEM 内容。
///
/// ⚠️ 只认 `-----BEGIN ` 前缀：路径里不可能出现它，而 PEM 一定以它开头
/// （允许前面有空白/换行，Secret 里常见）。
pub fn looks_like_pem(value: &str) -> bool {
    value.trim_start().starts_with("-----BEGIN ")
}

/// 把配置值分类成「路径」或「内容」。
pub fn classify(value: &str) -> TlsValue {
    let trimmed = value.trim();
    if looks_like_pem(trimmed) {
        TlsValue::Pem(trimmed.to_string())
    } else {
        TlsValue::Path(PathBuf::from(trimmed))
    }
}

/// 把 TLS 配置应用到 `ConnectOptions` 上。
///
/// 行为：
/// 1. 三个键都没配（或全空）⇒ 原样返回，不加任何 TLS 选项；
/// 2. 全是路径 ⇒ 走 async-nats 的 `add_root_certificates` /
///    `add_client_certificate`（与改造前完全一致）；
/// 3. 含至少一个 PEM 内容 ⇒ 构建内存 `ClientConfig`（`require_tls(true)`）。
pub fn apply_tls(config: &HashMap<String, String>, opts: ConnectOptions) -> Result<ConnectOptions> {
    let ca = read_key(config, CA_KEY);
    let cert = read_key(config, CERT_KEY);
    let key = read_key(config, KEY_KEY);

    if ca.is_none() && cert.is_none() && key.is_none() {
        return Ok(opts);
    }

    let any_pem = [ca, cert].into_iter().flatten().any(looks_like_pem);

    if !any_pem {
        // 旧路径分支：保持与改造前逐字节一致的行为。
        let mut opts = opts;
        if let Some(ca) = ca {
            opts = opts.add_root_certificates(PathBuf::from(ca.trim()));
        }
        if let (Some(cert), Some(key)) = (cert, key) {
            opts =
                opts.add_client_certificate(PathBuf::from(cert.trim()), PathBuf::from(key.trim()));
        }
        return Ok(opts);
    }

    let cfg = build_client_config(ca, cert, key)?;
    Ok(opts.require_tls(true).tls_client_config(cfg))
}

/// 从配置里取一个非空值（去首尾空白）。
fn read_key<'a>(config: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    config.get(key).map(|s| s.trim()).filter(|s| !s.is_empty())
}

/// 构建内存 TLS 配置（内容与路径可混用）。
fn build_client_config(
    ca: Option<&str>,
    cert: Option<&str>,
    key: Option<&str>,
) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();
    match ca {
        Some(value) => {
            for der in read_certs(value).context("failed to load nats_tls_ca")? {
                roots
                    .add(der)
                    .context("nats_tls_ca contains an invalid certificate")?;
            }
        }
        None => {
            // ⚠️ 给了 ClientConfig 后 async-nats 不再自己加载系统根证书，
            // 这里补上，保持「不配 CA 时仍信任公共 CA」的旧语义。
            let native = rustls_native_certs::load_native_certs();
            if !native.errors.is_empty() {
                let errs = native
                    .errors
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; ");
                bail!("failed to load platform root certificates: {errs}");
            }
            roots.add_parsable_certificates(native.certs);
        }
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    match (cert, key) {
        (Some(_), None) => {
            bail!("{CERT_KEY} is set but {KEY_KEY} is missing — both must be provided together")
        }
        (Some(cert), Some(key)) => {
            let chain = read_certs(cert).context("failed to load nats_tls_cert")?;
            let key = read_key_der(key).context("failed to load nats_tls_key")?;
            Ok(builder
                .with_client_auth_cert(chain, key)
                .context("nats_tls_cert / nats_tls_key are not a valid certificate/key pair")?)
        }
        _ => Ok(builder.with_no_client_auth()),
    }
}

/// 读出一个取值对应的原始字节（内容直接用，路径读文件）。
fn read_bytes(value: &str) -> Result<Vec<u8>> {
    match classify(value) {
        TlsValue::Pem(text) => Ok(text.into_bytes()),
        TlsValue::Path(path) => read_file(&path),
    }
}

fn read_file(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))
}

fn read_certs(value: &str) -> Result<Vec<CertificateDer<'static>>> {
    let bytes = read_bytes(value)?;
    let certs = CertificateDer::pem_slice_iter(&bytes)
        .collect::<Result<Vec<_>, _>>()
        .context("value is not valid PEM certificate data")?;
    if certs.is_empty() {
        bail!("no PEM certificate found");
    }
    Ok(certs)
}

fn read_key_der(value: &str) -> Result<PrivateKeyDer<'static>> {
    let bytes = read_bytes(value)?;
    PrivateKeyDer::from_pem_slice(&bytes).context("value is not valid PEM private key data")
}

#[cfg(test)]
mod tests {
    use super::*;

    const CERT_PEM: &str = include_str!("../testdata/cert.pem");
    const KEY_PEM: &str = include_str!("../testdata/key.pem");

    fn cfg(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn detects_pem_content() {
        assert!(looks_like_pem(
            "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----"
        ));
        // Secret 里常有前导换行/空格。
        assert!(looks_like_pem("\n  -----BEGIN PRIVATE KEY-----\nabc"));
        assert!(!looks_like_pem("/etc/nats/ca.pem"));
        assert!(!looks_like_pem(""));
    }

    #[test]
    fn classify_path_vs_content() {
        assert_eq!(
            classify(" /etc/nats/ca.pem "),
            TlsValue::Path(PathBuf::from("/etc/nats/ca.pem"))
        );
        assert!(matches!(classify(CERT_PEM), TlsValue::Pem(_)));
    }

    #[test]
    fn no_tls_keys_leaves_options_untouched() {
        let opts = apply_tls(&cfg(&[]), ConnectOptions::new()).expect("no-op");
        // 无法直接读 ConnectOptions 内部字段；这里只要求不报错。
        drop(opts);
    }

    #[test]
    fn builds_config_from_pem_contents() {
        let cfg = cfg(&[(CA_KEY, CERT_PEM), (CERT_KEY, CERT_PEM), (KEY_KEY, KEY_PEM)]);
        let built = build_client_config(
            cfg.get(CA_KEY).map(String::as_str),
            cfg.get(CERT_KEY).map(String::as_str),
            cfg.get(KEY_KEY).map(String::as_str),
        );
        assert!(built.is_ok(), "should build client config: {built:?}");
    }

    #[test]
    fn ca_only_builds_config() {
        assert!(build_client_config(Some(CERT_PEM), None, None).is_ok());
    }

    #[test]
    fn cert_without_key_is_an_error() {
        let err = build_client_config(Some(CERT_PEM), Some(CERT_PEM), None)
            .expect_err("must require key");
        assert!(err.to_string().contains(KEY_KEY), "{err}");
    }

    #[test]
    fn invalid_pem_is_rejected_with_context() {
        let err = build_client_config(
            Some("-----BEGIN CERTIFICATE-----\nnot base64\n"),
            None,
            None,
        )
        .expect_err("must reject invalid PEM");
        assert!(format!("{err:#}").contains(CA_KEY), "{err:#}");
    }
}
