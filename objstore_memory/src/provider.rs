use std::sync::Arc;

use objstore::ObjStoreProvider;

pub struct MemoryProvider;

impl ObjStoreProvider for MemoryProvider {
    fn kind(&self) -> &str {
        "memory"
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore, anyhow::Error> {
        if url.scheme() != self.kind() {
            return Err(anyhow::anyhow!(
                "Invalid scheme: expected '{}', got '{}'",
                self.kind(),
                url.scheme()
            ));
        }

        let store = crate::MemoryObjStore::new();
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
