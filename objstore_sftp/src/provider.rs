use std::sync::Arc;

use anyhow::Context;

use crate::{SftpObjStore, SftpObjStoreConfig};

#[derive(Clone, Debug, Default)]
pub struct SftpProvider {
    _private: (),
}

impl SftpProvider {
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl objstore::ObjStoreProvider for SftpProvider {
    type Config = SftpObjStoreConfig;

    fn kind(&self) -> &'static str {
        SftpObjStore::KIND
    }

    fn url_scheme(&self) -> &str {
        "sftp"
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore, anyhow::Error> {
        let config = SftpObjStoreConfig::from_uri(url.as_str())
            .context("Failed to parse SFTP object store configuration from URI")?;
        let store = SftpObjStore::new(config)?;
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
