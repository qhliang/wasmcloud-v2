//! Cloudflare Workers KV backend for the multiplexed keyvalue plugin.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use crate::plugin::multiplex::BackendProvider;
use super::{KeyResponse, KvBackend, KvId, StoreError};

/// A Cloudflare Workers KV [`KvBackend`]. Each namespace maps to a Cloudflare
/// KV namespace; values are stored as UTF-8 strings, with binary values
/// transparently base64-encoded.
pub struct CloudflareKvBackend {
    client: reqwest::Client,
    account_id: String,
    namespace_id: String,
    api_token: String,
}

impl CloudflareKvBackend {
    fn err(e: impl std::fmt::Display) -> StoreError {
        StoreError::Other(format!("Cloudflare KV error: {e}"))
    }

    fn base_url(&self) -> String {
        format!(
            "https://api.cloudflare.com/client/v4/accounts/{}/storage/kv/namespaces/{}",
            self.account_id, self.namespace_id
        )
    }

    async fn request(&self, method: reqwest::Method, url: &str, body: Option<String>) -> Result<reqwest::Response, StoreError> {
        let mut req = self
            .client
            .request(method, url)
            .header("Authorization", format!("Bearer {}", self.api_token));
        if let Some(b) = body {
            req = req.header("Content-Type", "application/octet-stream").body(b);
        }
        req.send().await.map_err(Self::err)
    }
}

#[async_trait::async_trait]
impl KvBackend for CloudflareKvBackend {
    async fn open(&self, _identifier: &str) -> Result<(), StoreError> {
        // Cloudflare KV namespaces are pre-created
        Ok(())
    }

    async fn get(&self, _bucket: &str, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let url = format!("{}/values/{key}", self.base_url());
        match self.request(reqwest::Method::GET, &url, None).await {
            Ok(resp) if resp.status().is_success() => {
                let bytes = resp.bytes().await.map_err(Self::err)?;
                if bytes.is_empty() {
                    return Ok(Some(vec![]));
                }
                // Try base64 decode for binary values
                match base64::engine::general_purpose::STANDARD.decode(&bytes) {
                    Ok(decoded) => Ok(Some(decoded)),
                    Err(_) => Ok(Some(bytes.to_vec())),
                }
            }
            Ok(resp) if resp.status().as_u16() == 404 => Ok(None),
            Ok(resp) => Err(Self::err(format!("HTTP {}", resp.status()))),
            Err(e) => Err(e),
        }
    }

    async fn set(&self, _bucket: &str, key: &str, value: Vec<u8>) -> Result<(), StoreError> {
        let url = format!("{}/values/{key}", self.base_url());
        // Base64-encode binary values so they survive the text-only KV API
        let is_binary = !value.iter().all(|b| b.is_ascii() && *b >= 32);
        let body = if is_binary {
            base64::engine::general_purpose::STANDARD.encode(&value)
        } else {
            String::from_utf8_lossy(&value).to_string()
        };
        match self.request(reqwest::Method::PUT, &url, Some(body)).await {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) => Err(Self::err(format!("HTTP {}", resp.status()))),
            Err(e) => Err(e),
        }
    }

    async fn delete(&self, _bucket: &str, key: &str) -> Result<(), StoreError> {
        let url = format!("{}/values/{key}", self.base_url());
        match self.request(reqwest::Method::DELETE, &url, None).await {
            Ok(resp) if resp.status().is_success() => Ok(()),
            Ok(resp) if resp.status().as_u16() == 404 => Ok(()),
            Ok(resp) => Err(Self::err(format!("HTTP {}", resp.status()))),
            Err(e) => Err(e),
        }
    }

    async fn exists(&self, _bucket: &str, key: &str) -> Result<bool, StoreError> {
        // Cloudflare KV list API to check existence efficiently
        let url = format!("{}/keys?limit=1&prefix={key}", self.base_url());
        match self.request(reqwest::Method::GET, &url, None).await {
            Ok(resp) if resp.status().is_success() => {
                let body: serde_json::Value =
                    serde_json::from_slice(&resp.bytes().await.map_err(Self::err)?)
                        .map_err(|e| Self::err(e))?;
                let results = body["result"].as_array().map(|a| a.len()).unwrap_or(0);
                Ok(results > 0)
            }
            Ok(_) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn list_keys(
        &self,
        _bucket: &str,
        _cursor: Option<u64>,
    ) -> Result<KeyResponse, StoreError> {
        let url = format!("{}/keys", self.base_url());
        match self.request(reqwest::Method::GET, &url, None).await {
            Ok(resp) if resp.status().is_success() => {
                let body: serde_json::Value =
                    serde_json::from_slice(&resp.bytes().await.map_err(Self::err)?)
                        .map_err(|e| Self::err(e))?;
                let keys: Vec<String> = body["result"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .filter_map(|v| v["name"].as_str().map(String::from))
                    .collect();
                Ok(KeyResponse {
                    keys,
                    cursor: None, // Cloudflare KV list is not paginated this way
                })
            }
            Ok(resp) => Err(Self::err(format!("HTTP {}", resp.status()))),
            Err(e) => Err(e),
        }
    }

    async fn increment(&self, bucket: &str, key: &str, delta: i64) -> Result<i64, StoreError> {
        // Cloudflare KV does not have atomic increment.
        // Use a read-modify-write with base64-encoded i64 value.
        let current = self.get(bucket, key).await?;
        let value = match current {
            Some(bytes) if bytes.len() == 8 => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes);
                i64::from_le_bytes(arr)
            }
            Some(_) => 0,
            None => 0,
        };
        let new = value.wrapping_add(delta).min(i64::MAX);
        self.set(bucket, key, new.to_le_bytes().to_vec()).await?;
        Ok(new)
    }

    async fn get_many(
        &self,
        bucket: &str,
        keys: Vec<String>,
    ) -> Result<Vec<Option<(String, Vec<u8>)>>, StoreError> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            let val = self.get(bucket, &key).await?;
            results.push(val.map(|v| (key.clone(), v)));
        }
        Ok(results)
    }

    async fn set_many(
        &self,
        bucket: &str,
        key_values: Vec<(String, Vec<u8>)>,
    ) -> Result<(), StoreError> {
        for (key, value) in key_values {
            self.set(bucket, &key, value).await?;
        }
        Ok(())
    }

    async fn delete_many(&self, bucket: &str, keys: Vec<String>) -> Result<(), StoreError> {
        for key in keys {
            self.delete(bucket, &key).await?;
        }
        Ok(())
    }
}

/// Provider for [`CloudflareKvBackend`], selected by `config.backend = "cloudflare"`.
#[derive(Default)]
pub struct CloudflareKvProvider;

#[async_trait::async_trait]
impl BackendProvider<KvId> for CloudflareKvProvider {
    fn backend_type(&self) -> &'static str {
        "cloudflare"
    }

    async fn instantiate(&self, config: &HashMap<String, String>) -> anyhow::Result<KvId> {
        let account_id = config
            .get("account_id")
            .ok_or_else(|| anyhow::anyhow!("cloudflare keyvalue backend requires 'account_id' config"))?;
        let api_token = config
            .get("api_token")
            .ok_or_else(|| anyhow::anyhow!("cloudflare keyvalue backend requires 'api_token' config"))?;
        let namespace_id = config
            .get("namespace_id")
            .ok_or_else(|| anyhow::anyhow!("cloudflare keyvalue backend requires 'namespace_id' config"))?;

        Ok(Arc::new(CloudflareKvBackend {
            client: reqwest::Client::new(),
            account_id: account_id.clone(),
            namespace_id: namespace_id.clone(),
            api_token: api_token.clone(),
        }))
    }
}
