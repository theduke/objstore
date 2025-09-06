use std::sync::Arc;

use crate::{FtpObjStore, FtpObjStoreConfig};

#[derive(Clone, Debug, Default)]
pub struct FtpProvider {
    _private: (),
}

impl FtpProvider {
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl objstore::ObjStoreProvider for FtpProvider {
    type Config = FtpObjStoreConfig;

    fn kind(&self) -> &'static str {
        FtpObjStore::KIND
    }

    fn url_scheme(&self) -> &str {
        "ftp"
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore, anyhow::Error> {
        let config = FtpObjStoreConfig::from_url(url)?;
        let store = FtpObjStore::new(config)?;
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
