use std::sync::Arc;

use anyhow::Context;

pub struct S3LightProvider;

impl objstore::ObjStoreProvider for S3LightProvider {
    fn kind(&self) -> &str {
        "s3"
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore, anyhow::Error> {
        let config = crate::S3ObjStoreConfig::from_uri(url.as_str())
            .context("Failed to parse S3 object store configuration from URI")?;
        let store = crate::S3ObjStore::new(config)?;
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
