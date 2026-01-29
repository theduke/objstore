use std::sync::Arc;

use anyhow::Context;

use crate::{GithubObjStore, GithubObjStoreConfig};

#[derive(Clone, Debug, Default)]
pub struct GithubProvider {
    _private: (),
}

impl GithubProvider {
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl objstore::ObjStoreProvider for GithubProvider {
    type Config = GithubObjStoreConfig;

    fn kind(&self) -> &'static str {
        GithubObjStore::KIND
    }

    fn url_scheme(&self) -> &str {
        GithubObjStoreConfig::URI_SCHEME
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore, anyhow::Error> {
        if url.scheme() != self.url_scheme() {
            return Err(anyhow::anyhow!(
                "invalid scheme: expected '{}', got '{}'",
                self.url_scheme(),
                url.scheme()
            ));
        }
        let config = GithubObjStoreConfig::from_uri(url.as_str())
            .context("failed to parse github object store configuration from URI")?;
        let store = GithubObjStore::new(config)?;
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
