use std::sync::Arc;

pub struct FsProvider;

impl objstore::ObjStoreProvider for FsProvider {
    fn kind(&self) -> &str {
        "fs"
    }

    fn build(&self, url: &url::Url) -> Result<objstore::DynObjStore, anyhow::Error> {
        if url.scheme() != self.kind() {
            return Err(anyhow::anyhow!(
                "Invalid scheme: expected '{}', got '{}'",
                self.kind(),
                url.scheme()
            ));
        }

        let config = crate::FsObjStoreConfig {
            path: url.path().into(),
        };
        let store = crate::FsObjStore::new(config)?;
        Ok(Arc::new(store) as objstore::DynObjStore)
    }
}
