use std::sync::Arc;

use objstore::{ObjStoreError, Result};

use crate::S3ObjStore;

#[derive(Clone, Debug, Default)]
pub struct S3LightProvider {
    _private: (),
}

impl S3LightProvider {
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl objstore::ObjStoreProvider for S3LightProvider {
    type Config = crate::S3ObjStoreConfig;

    fn kind(&self) -> &'static str {
        S3ObjStore::KIND
    }

    fn url_scheme(&self) -> &str {
        "s3"
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore> {
        let config = crate::S3ObjStoreConfig::from_uri(url.as_str()).map_err(|source| {
            ObjStoreError::InvalidConfig {
                message: "failed to parse S3 object store configuration from URI".to_string(),
                source: Some(source.into()),
            }
        })?;
        let store = crate::S3ObjStore::new(config)?;
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
