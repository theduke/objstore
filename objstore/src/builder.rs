use std::sync::Arc;

use crate::{ObjStoreProvider, store::DynObjStore};

#[derive(Clone, Debug)]
pub struct ObjStoreBuilder {
    providers: Vec<Arc<dyn ObjStoreProvider>>,
}

impl Default for ObjStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjStoreBuilder {
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    pub fn register_provider<P: ObjStoreProvider + 'static>(&mut self, provider: P) {
        self.providers.push(Arc::new(provider));
    }

    pub fn with_provider(mut self, provider: Arc<dyn ObjStoreProvider>) -> Self {
        self.providers.push(provider);
        self
    }

    pub fn build(&self, uri: &str) -> Result<DynObjStore, anyhow::Error> {
        let url = url::Url::parse(uri).map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;

        for provider in &self.providers {
            if provider.url_scheme() == url.scheme() {
                return provider.build(&url);
            }
        }
        Err(anyhow::anyhow!(
            "No suitable provider found for URI: {}",
            url
        ))
    }
}
