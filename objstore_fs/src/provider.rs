use std::sync::Arc;

use objstore::{ObjStoreError, Result};

#[derive(Clone, Debug, Default)]
pub struct FsProvider {
    _private: (),
}

impl FsProvider {
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl objstore::ObjStoreProvider for FsProvider {
    type Config = crate::FsObjStoreConfig;

    fn kind(&self) -> &'static str {
        crate::FsObjStore::KIND
    }

    fn url_scheme(&self) -> &str {
        "fs"
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore> {
        if url.scheme() != self.url_scheme() {
            return Err(ObjStoreError::InvalidConfig {
                message: format!(
                    "invalid scheme: expected '{}', got '{}'",
                    self.url_scheme(),
                    url.scheme()
                ),
                source: None,
            });
        }

        let config = crate::FsObjStoreConfig {
            path: url.path().into(),
        };
        let store = crate::FsObjStore::new(config)?;
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
