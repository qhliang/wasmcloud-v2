//! S3 [`BlobBackend`] for the multiplexed blobstore plugin.
//!
//! Backed by OpenDAL's S3 service, so any S3-compatible object store works —
//! AWS S3, MinIO, Cloudflare R2, ... — via the `backend: s3` interface-config
//! key. All other config keys are passed straight to OpenDAL (`bucket`,
//! `endpoint`, `access_key_id`, `secret_access_key`, `region`, ...). Non-S3
//! OpenDAL schemes are deliberately rejected here; the in-memory, filesystem,
//! and NATS backends live in their own providers.
//!
//! Containers map to `<name>/` prefixes and objects to `<container>/<object>`
//! keys, mirroring the object layout of the standalone `custom_plugin_blobstore`
//! backend.

use std::collections::HashMap;
use std::sync::Arc;

use opendal::{Error, ErrorKind, Operator, Scheme};
use tracing::warn;

use crate::plugin::multiplex::{BACKEND_CONFIG_KEY, BackendProvider};

use super::{
    BlobBackend, BlobBackendError, BlobId, BlobResult, ContainerInfo, ObjectInfo, clamp_range,
};

/// An OpenDAL-backed [`BlobBackend`].
pub struct OpenDalBlobBackend {
    op: Operator,
}

impl OpenDalBlobBackend {
    /// The prefix under which a container's objects live.
    fn container_prefix(&self, name: &str) -> String {
        format!("{name}/")
    }

    /// The object key for `(container, object)`.
    fn object_key(&self, container: &str, object: &str) -> String {
        format!("{container}/{object}")
    }

    /// Whether an OpenDAL error means the path does not exist.
    fn not_found(e: &Error) -> bool {
        e.kind() == ErrorKind::NotFound
    }
}

#[async_trait::async_trait]
impl BlobBackend for OpenDalBlobBackend {
    async fn create_container(&self, name: &str) -> BlobResult<()> {
        // Object stores have no real directories; the trailing-slash marker is
        // best-effort and not required for object reads/writes to succeed.
        if let Err(e) = self.op.create_dir(&self.container_prefix(name)).await {
            warn!(
                error = %e,
                container = %name,
                "failed to create container marker; continuing"
            );
        }
        Ok(())
    }

    async fn get_container(&self, name: &str) -> BlobResult<()> {
        if self.container_exists(name).await? {
            Ok(())
        } else {
            Err(BlobBackendError::NoSuchContainer(name.to_string()))
        }
    }

    async fn delete_container(&self, name: &str) -> BlobResult<()> {
        match self.op.remove_all(&self.container_prefix(name)).await {
            Ok(()) => Ok(()),
            Err(e) if Self::not_found(&e) => Ok(()),
            Err(e) => Err(BlobBackendError::other(e)),
        }
    }

    async fn container_exists(&self, name: &str) -> BlobResult<bool> {
        Ok(self
            .op
            .exists(&self.container_prefix(name))
            .await
            .unwrap_or(false))
    }

    async fn container_info(&self, name: &str) -> BlobResult<ContainerInfo> {
        if !self.container_exists(name).await? {
            return Err(BlobBackendError::NoSuchContainer(name.to_string()));
        }
        // Object stores do not expose a container creation time.
        Ok(ContainerInfo {
            name: name.to_string(),
            created_at: 0,
        })
    }

    async fn clear_container(&self, name: &str) -> BlobResult<()> {
        let prefix = self.container_prefix(name);
        match self.op.remove_all(&prefix).await {
            Ok(()) => {}
            Err(e) if Self::not_found(&e) => {}
            Err(e) => return Err(BlobBackendError::other(e)),
        }
        // Re-create the marker so the container still exists afterwards.
        self.create_container(name).await
    }

    async fn get_data(
        &self,
        container: &str,
        object: &str,
        start: u64,
        end: u64,
    ) -> BlobResult<Vec<u8>> {
        let data = match self.op.read(&self.object_key(container, object)).await {
            Ok(data) => data.to_vec(),
            Err(e) if Self::not_found(&e) => {
                return Err(BlobBackendError::NoSuchObject(object.to_string()));
            }
            Err(e) => return Err(BlobBackendError::other(e)),
        };
        let range = clamp_range(start, end, data.len());
        Ok(data.get(range).unwrap_or_default().to_vec())
    }

    async fn write_data(&self, container: &str, object: &str, data: Vec<u8>) -> BlobResult<()> {
        // Object stores need no pre-created container directory, so unlike the
        // filesystem backend there is no existence check before writing.
        self.op
            .write(&self.object_key(container, object), data)
            .await
            .map(|_| ())
            .map_err(BlobBackendError::other)
    }

    async fn list_objects(&self, container: &str) -> BlobResult<Vec<String>> {
        let entries = match self.op.list(&self.container_prefix(container)).await {
            Ok(entries) => entries,
            Err(e) if Self::not_found(&e) => {
                return Err(BlobBackendError::NoSuchContainer(container.to_string()));
            }
            Err(e) => return Err(BlobBackendError::other(e)),
        };
        Ok(entries
            .into_iter()
            // Directory markers (`<name>/`, mode dir) are not objects.
            .filter(|e| e.metadata().mode().is_file())
            .map(|e| e.name().to_string())
            .collect())
    }

    async fn delete_object(&self, container: &str, object: &str) -> BlobResult<()> {
        match self.op.delete(&self.object_key(container, object)).await {
            Ok(()) => Ok(()),
            Err(e) if Self::not_found(&e) => Ok(()),
            Err(e) => Err(BlobBackendError::other(e)),
        }
    }

    async fn delete_objects(&self, container: &str, objects: &[String]) -> BlobResult<()> {
        for object in objects {
            self.delete_object(container, object).await?;
        }
        Ok(())
    }

    async fn has_object(&self, container: &str, object: &str) -> BlobResult<bool> {
        // Only a regular object counts; a directory marker is not an object.
        Ok(
            match self.op.stat(&self.object_key(container, object)).await {
                Ok(meta) => meta.mode().is_file(),
                Err(_) => false,
            },
        )
    }

    async fn object_info(&self, container: &str, object: &str) -> BlobResult<ObjectInfo> {
        let meta = match self.op.stat(&self.object_key(container, object)).await {
            Ok(meta) => meta,
            Err(e) if Self::not_found(&e) => {
                return Err(BlobBackendError::NoSuchObject(object.to_string()));
            }
            Err(e) => return Err(BlobBackendError::other(e)),
        };
        if !meta.mode().is_file() {
            return Err(BlobBackendError::NoSuchObject(object.to_string()));
        }
        Ok(ObjectInfo {
            name: object.to_string(),
            container: container.to_string(),
            created_at: meta.last_modified().map_or(0, |ts| ts.timestamp() as u64),
            size: meta.content_length(),
        })
    }

    async fn copy_object(
        &self,
        src_container: &str,
        src_object: &str,
        dest_container: &str,
        dest_object: &str,
    ) -> BlobResult<()> {
        // Read-copy-write rather than `Operator::copy`: not every OpenDAL
        // backend implements copy (memory notably does not), and this keeps the
        // semantics identical across backends.
        let data = self
            .get_data(src_container, src_object, 0, u64::MAX)
            .await?;
        self.write_data(dest_container, dest_object, data).await
    }
}

/// Provider for [`OpenDalBlobBackend`], selected by `config.backend` (default
/// `"s3"`). Only the S3 scheme is accepted — all non-S3 OpenDAL schemes are
/// rejected — and config keys except `backend` are passed to OpenDAL.
pub struct OpenDalBlobProvider;

#[async_trait::async_trait]
impl BackendProvider<BlobId> for OpenDalBlobProvider {
    fn pool_key(&self, _config: &HashMap<String, String>) -> Option<String> {
        None
    }

    fn backend_type(&self) -> &'static str {
        "s3"
    }

    async fn instantiate(&self, config: &HashMap<String, String>) -> anyhow::Result<BlobId> {
        let backend = config
            .get(BACKEND_CONFIG_KEY)
            .map(String::as_str)
            .unwrap_or("s3");
        if backend != "s3" {
            return Err(anyhow::anyhow!(
                "the OpenDAL blobstore provider only supports the 's3' backend, got '{backend}'"
            ));
        }
        let iter = config
            .iter()
            .filter(|(k, _)| k.as_str() != BACKEND_CONFIG_KEY)
            .map(|(k, v)| (k.clone(), v.clone()));
        let op = Operator::via_iter(Scheme::S3, iter)
            .map_err(|e| anyhow::anyhow!("failed to create OpenDAL S3 operator: {e}"))?;
        Ok(Arc::new(OpenDalBlobBackend { op }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(backend: &str) -> HashMap<String, String> {
        HashMap::from([(BACKEND_CONFIG_KEY.to_string(), backend.to_string())])
    }

    /// Non-S3 OpenDAL schemes are rejected at instantiation.
    #[tokio::test]
    async fn instantiate_rejects_non_s3_backends() {
        let provider = OpenDalBlobProvider;
        for backend in ["memory", "fs", "webdav", "ftp", "nats", "azblob", "gcs"] {
            let err = match provider.instantiate(&config(backend)).await {
                Ok(_) => panic!("non-s3 backend '{backend}' should be rejected"),
                Err(e) => e,
            };
            assert!(
                err.to_string().contains("only supports the 's3' backend"),
                "unexpected error: {err}"
            );
        }
    }

    /// A default `backend` (no config key) resolves to s3, and an explicit
    /// `backend: s3` builds an operator without touching the network. S3
    /// requires a `bucket`, which also proves remaining config keys are
    /// forwarded to OpenDAL.
    #[tokio::test]
    async fn instantiate_s3_backend_succeeds() {
        let provider = OpenDalBlobProvider;
        // No `backend` key at all: defaults to s3.
        let defaulted = HashMap::from([
            ("bucket".to_string(), "test-bucket".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
        ]);
        provider
            .instantiate(&defaulted)
            .await
            .expect("default backend should be s3");
        // Explicit `backend: s3` plus a bucket; operator construction does not
        // contact the service.
        let configured = HashMap::from([
            (BACKEND_CONFIG_KEY.to_string(), "s3".to_string()),
            ("bucket".to_string(), "test-bucket".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
        ]);
        provider
            .instantiate(&configured)
            .await
            .expect("s3 backend with bucket should instantiate");
    }
}
