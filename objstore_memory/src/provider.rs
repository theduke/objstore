use std::sync::Arc;

use objstore::ObjStoreProvider;

use crate::MemoryObjStore;

#[derive(Clone, Debug, Default)]
pub struct MemoryProvider {
    _private: (),
}

impl MemoryProvider {
    pub const fn new() -> Self {
        Self { _private: () }
    }
}

impl ObjStoreProvider for MemoryProvider {
    type Config = ();

    fn kind(&self) -> &'static str {
        MemoryObjStore::KIND
    }

    fn url_scheme(&self) -> &str {
        "memory"
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore, anyhow::Error> {
        if url.scheme() != self.url_scheme() {
            return Err(anyhow::anyhow!(
                "Invalid scheme: expected '{}', got '{}'",
                self.url_scheme(),
                url.scheme()
            ));
        }

        let store = crate::MemoryObjStore::new();
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
