use bytes::Bytes;

use crate::{
    Copy, DownloadUrlArgs, KeyMetaPage, KeyPage, ListArgs, ObjStore, ObjectMeta, Put, ValueStream,
};

/// Wrapper for an object stores that logs operations with the `tracing` crate.
///
/// * All get operations will be logged at the `TRACE` level
///   (get metadata, get keys, listing)
/// * All put/delete operations will be logged at the `TRACE` level on start of the operation
///   and at the `DEBUG` level on completion.
/// * All errors will be logged at the `ERROR` level
#[derive(Debug)]
pub struct TracedObjStore<S> {
    name: String,
    inner: S,
}

impl<S> TracedObjStore<S> {
    /// Creates a new `TracedObjStore` with the given name and inner object store.
    ///
    /// All logs will contain the name of the store.
    pub fn new(name: impl Into<String>, inner: S) -> Self {
        Self {
            name: name.into(),
            inner,
        }
    }
}

#[async_trait::async_trait]
impl<S> ObjStore for TracedObjStore<S>
where
    S: ObjStore + Send + Sync,
{
    fn kind(&self) -> &str {
        self.inner.kind()
    }

    fn safe_uri(&self) -> &url::Url {
        self.inner.safe_uri()
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        tracing::debug!("Performing healthcheck on object store: {}", self.kind());
        match self.inner.healthcheck().await {
            Ok(_) => {
                tracing::debug!(store = &self.name, "healthcheck::ok");
                Ok(())
            }
            Err(e) => {
                tracing::error!(store=&self.name, error=%e, "healthcheck::failed");
                Err(e)
            }
        }
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        match self.inner.meta(key).await {
            Ok(meta) => {
                tracing::trace!(store = &self.name, key, ?meta, "get_meta");
                Ok(meta)
            }
            Err(e) => {
                tracing::error!(store = &self.name, key, error=%e, "Failed to get metadata");
                Err(e)
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        match self.inner.get(key).await {
            Ok(Some(value)) => {
                tracing::trace!(store = &self.name, key, "get::ok");
                Ok(Some(value))
            }
            Ok(None) => {
                tracing::trace!(store = &self.name, key, "get::not_found");
                Ok(None)
            }
            Err(e) => {
                tracing::error!(store = &self.name, key, error=%e, "get::failed");
                Err(e)
            }
        }
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        match self.inner.get_stream(key).await {
            Ok(Some(value)) => {
                tracing::trace!(store = &self.name, key, "get_stream::ok");
                Ok(Some(value))
            }
            Ok(None) => {
                tracing::trace!(store = &self.name, key, "get_stream::not_found");
                Ok(None)
            }
            Err(e) => {
                tracing::error!(store = &self.name, key, error=%e, "get_stream::failed");
                Err(e)
            }
        }
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        match self.inner.get_with_meta(key).await {
            Ok(Some((value, meta))) => {
                tracing::trace!(store = &self.name, key, ?meta, "get_with_meta::ok");
                Ok(Some((value, meta)))
            }
            Ok(None) => {
                tracing::trace!(store = &self.name, key, "get_with_meta::not_found");
                Ok(None)
            }
            Err(e) => {
                tracing::error!(store = &self.name, key, error=%e, "get_with_meta::failed");
                Err(e)
            }
        }
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        match self.inner.get_stream_with_meta(key).await {
            Ok(Some((meta, value))) => {
                tracing::trace!(store = &self.name, key, ?meta, "get_stream_with_meta::ok");
                Ok(Some((meta, value)))
            }
            Ok(None) => {
                tracing::trace!(store = &self.name, key, "get_stream_with_meta::not_found");
                Ok(None)
            }
            Err(e) => {
                tracing::error!(store = &self.name, key, error=%e, "get_stream_with_meta::failed");
                Err(e)
            }
        }
    }
    async fn generate_download_url(
        &self,
        args: DownloadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        match self.inner.generate_download_url(args).await {
            Ok(Some(url)) => {
                tracing::trace!(store = &self.name, %url, "generate_download_url::ok");
                Ok(Some(url))
            }
            Ok(None) => {
                tracing::warn!(
                    store = &self.name,
                    "generate_download_url::failed - store does not support download URLs"
                );
                Ok(None)
            }
            Err(e) => {
                tracing::error!(store = &self.name, error=%e, "generate_download_url::failed");
                Err(e)
            }
        }
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta, anyhow::Error> {
        let key = put.key.clone();
        tracing::trace!(store = &self.name, key, "put::start");
        match self.inner.send_put(put).await {
            Ok(out) => {
                tracing::debug!(store = &self.name, key, "put::ok");
                Ok(out)
            }
            Err(e) => {
                tracing::error!(store = &self.name, key, error=%e, "put::failed");
                Err(e)
            }
        }
    }

    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        tracing::trace!(
            store = &self.name,
            src = &copy.source_key,
            dest = &copy.target_key,
            "copy::start"
        );
        match self.inner.send_copy(copy).await {
            Ok(out) => {
                tracing::debug!(store = &self.name, "copy::ok");
                Ok(out)
            }
            Err(e) => {
                tracing::error!(store = &self.name, error = %e, "copy::failed");
                Err(e)
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        tracing::trace!(store = &self.name, key, "delete::start");
        match self.inner.delete(key).await {
            Ok(_) => {
                tracing::debug!(store = &self.name, key, "delete::ok");
                Ok(())
            }
            Err(e) => {
                tracing::error!(store = &self.name, key, error=%e, "delete::failed");
                Err(e)
            }
        }
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error> {
        tracing::trace!(store = &self.name, prefix, "delete_prefix::start");
        match self.inner.delete_prefix(prefix).await {
            Ok(_) => {
                tracing::debug!(store = &self.name, prefix, "delete_prefix::ok");
                Ok(())
            }
            Err(e) => {
                tracing::error!(store = &self.name, prefix, error=%e, "delete_prefix::failed");
                Err(e)
            }
        }
    }

    async fn list(&self, args: ListArgs) -> Result<KeyMetaPage, anyhow::Error> {
        match self.inner.list(args).await {
            Ok(page) => {
                tracing::trace!(store = &self.name, ?page, "list::ok");
                Ok(page)
            }
            Err(e) => {
                tracing::error!(store = &self.name, error=%e, "list::failed");
                Err(e)
            }
        }
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        match self.inner.list_keys(args).await {
            Ok(page) => {
                tracing::trace!(store = &self.name, ?page, "list_keys::ok");
                Ok(page)
            }
            Err(e) => {
                tracing::error!(store = &self.name, error=%e, "list_keys::failed");
                Err(e)
            }
        }
    }
}
