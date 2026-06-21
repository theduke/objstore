use std::sync::Arc;

use objstore::{ObjStoreError, Result};

use crate::{LogFsObjStore, LogFsObjStoreConfig};

#[derive(Clone, Debug, Default)]
pub struct LogFsProvider {
    _private: (),
}

impl LogFsProvider {
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl objstore::ObjStoreProvider for LogFsProvider {
    type Config = LogFsObjStoreConfig;

    fn kind(&self) -> &'static str {
        LogFsObjStore::KIND
    }

    fn url_scheme(&self) -> &str {
        LogFsObjStoreConfig::URI_SCHEME
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore> {
        let config =
            LogFsObjStoreConfig::from_url(url).map_err(|source| ObjStoreError::InvalidConfig {
                message: "failed to parse logfs object store configuration from URI".to_string(),
                source: Some(source.into()),
            })?;
        let store = LogFsObjStore::new(config)?;
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
