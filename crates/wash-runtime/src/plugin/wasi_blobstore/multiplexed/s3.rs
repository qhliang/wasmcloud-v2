use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use super::{BlobBackend, BlobBackendError, BlobId, BlobResult, ContainerInfo, ObjectInfo};
use crate::plugin::multiplex::BackendProvider;
use opendal::Operator;
use opendal::Scheme;

#[allow(dead_code)]
pub struct OpenDalBlobBackend {
    _op: Operator,
}

#[async_trait::async_trait]
impl BlobBackend for OpenDalBlobBackend {
    async fn create_container(&self, _name: &str) -> BlobResult<()> {
        Ok(())
    }
    async fn get_container(&self, name: &str) -> BlobResult<()> {
        Err(BlobBackendError::Other(name.to_string()))
    }
    async fn delete_container(&self, _name: &str) -> BlobResult<()> {
        Ok(())
    }
    async fn container_exists(&self, _name: &str) -> BlobResult<bool> {
        Ok(false)
    }
    async fn container_info(&self, name: &str) -> BlobResult<ContainerInfo> {
        Ok(ContainerInfo {
            name: name.to_string(),
            created_at: 0,
        })
    }
    async fn clear_container(&self, _name: &str) -> BlobResult<()> {
        Ok(())
    }
    async fn get_data(&self, _c: &str, object: &str, _s: u64, _e: u64) -> BlobResult<Vec<u8>> {
        Err(BlobBackendError::Other(object.to_string()))
    }
    async fn write_data(&self, _c: &str, _o: &str, _d: Vec<u8>) -> BlobResult<()> {
        Ok(())
    }
    async fn list_objects(&self, _container: &str) -> BlobResult<Vec<String>> {
        Ok(vec![])
    }
    async fn delete_object(&self, _c: &str, _o: &str) -> BlobResult<()> {
        Ok(())
    }
    async fn delete_objects(&self, _c: &str, _o: &[String]) -> BlobResult<()> {
        Ok(())
    }
    async fn has_object(&self, _c: &str, _o: &str) -> BlobResult<bool> {
        Ok(false)
    }
    async fn object_info(&self, name: &str, object: &str) -> BlobResult<ObjectInfo> {
        Err(BlobBackendError::Other(format!("{name}/{object}")))
    }
    async fn copy_object(&self, _s: &str, _so: &str, _d: &str, _do_: &str) -> BlobResult<()> {
        Ok(())
    }
}

#[allow(dead_code)]
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
        let backend = config.get("backend").map(String::as_str).unwrap_or("s3");
        let iter = config
            .iter()
            .filter(|(k, _)| k.as_str() != "backend")
            .map(|(k, v)| (k.clone(), v.clone()));
        let scheme = Scheme::from_str(backend).map_err(|e| anyhow::anyhow!("{e}"))?;
        let _op = Operator::via_iter(scheme, iter).map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(Arc::new(OpenDalBlobBackend { _op }))
    }
}
