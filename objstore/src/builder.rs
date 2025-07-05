use crate::{ObjStoreProvider, store::DynObjStore};

pub struct ObjStoreBuilder {
    providers: Vec<Box<dyn ObjStoreProvider>>,
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
        self.providers.push(Box::new(provider));
    }

    pub fn with_provider(mut self, provider: Box<dyn ObjStoreProvider>) -> Self {
        self.providers.push(provider);
        self
    }

    pub fn build(&self, uri: &str) -> Result<DynObjStore, anyhow::Error> {
        let url = url::Url::parse(uri).map_err(|e| anyhow::anyhow!("Invalid URL: {}", e))?;

        for provider in &self.providers {
            if provider.kind() == url.scheme() {
                return provider.build(&url);
            }
        }
        Err(anyhow::anyhow!(
            "No suitable provider found for URI: {}",
            url
        ))
    }
}
