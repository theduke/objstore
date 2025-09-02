use std::sync::Arc;

use anyhow::Context as _;
use bytes::Bytes;

use crate::{
    Conditions, Copy, DataSource, DownloadUrlArgs, KeyPage, KeyStream, ListArgs, MetaStream,
    ObjectMeta, ObjectMetaPage, Put, ValueStream,
};
use futures::{StreamExt as _, TryStreamExt as _, stream};

/// Abstraction for a generic key-value store.
#[async_trait::async_trait]
pub trait ObjStore: Send + Sync + std::fmt::Debug {
    /// Get a descriptive name for backend implementation.
    ///
    /// eg: "memory", "s3", ...
    fn kind(&self) -> &str;

    /// Get a "safe" URI for the store, which does not include any sensitive information
    /// like api keys.
    fn safe_uri(&self) -> &url::Url;

    /// Checks if the store is usable.
    ///
    /// May perform upstream service requests to validate connectivity and credentials.
    async fn healthcheck(&self) -> Result<(), anyhow::Error>;

    /// Get metadata for a given key.
    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error>;

    /// Get the value for a given key.
    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error>;

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error>;

    /// Get both the value and metadata for a given key.
    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error>;

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error>;

    /// Generate a download URL for a given key.
    ///
    /// NOTE: Must return `Ok(None)` if the store does not support download URLs!
    async fn generate_download_url(
        &self,
        args: DownloadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error>;

    /// Store a value under a given key.
    async fn send_put(&self, put: Put) -> Result<ObjectMeta, anyhow::Error>;

    /// Copy an existing object to a new key.
    ///
    /// May apply server-side copy optimizations and respects `Conditions`.
    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error>;

    /// Delete a key from the store.
    async fn delete(&self, key: &str) -> Result<(), anyhow::Error>;

    /// Delete all keys with a given prefix.
    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error>;

    /// List keys in the store.
    ///
    /// In contrast to [`Self::list`], this returns only the keys, not their metadata.
    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error>;

    /// List all the keys, optionally filtered by a prefix.
    ///
    /// NOTE: this method will paginate through all keys, and accumulates
    /// the results in memory.
    ///
    /// Use with caution.
    async fn list_all_keys(&self, prefix: &str) -> Result<Vec<String>, anyhow::Error> {
        let args = ListArgs::new().with_prefix(prefix);
        self.list_keys_stream(args)
            .map_ok(|v| v.items)
            .try_concat()
            .await
    }

    fn list_keys_stream<'a>(&'a self, args: ListArgs) -> KeyStream<'a> {
        let init = Some(args.clone());
        let page_stream = stream::try_unfold(init, move |state| async move {
            if let Some(args) = state {
                let page = self.list_keys(args.clone()).await?;
                let next = page
                    .next_cursor
                    .as_ref()
                    .map(|c| args.clone().with_cursor(c.clone()));
                Ok(Some((page, next)))
            } else {
                Ok(None)
            }
        });
        Box::pin(page_stream)
    }

    /// List metadata for a given key.
    ///
    /// The arguments allow for prefix filtering, pagination, and limiting
    /// the number of results.
    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error>;

    /// Streaming variant of [`list`]: pages through [`Self::list`] and yields each metadata page (`ObjectMetaPage`).
    ///
    /// This default method repeatedly calls `list` to page through all results lazily.
    fn list_stream(&self, args: ListArgs) -> MetaStream
    where
        Self: Sized + Clone + 'static,
    {
        let store = self.clone();
        let init = Some(args.clone());
        let page_stream = stream::try_unfold(init, move |state| {
            let store = store.clone();
            async move {
                if let Some(args) = state {
                    let page = store.list(args.clone()).await?;
                    let next = page
                        .next_cursor
                        .as_ref()
                        .map(|c| args.clone().with_cursor(c.clone()));
                    Ok(Some((page, next)))
                } else {
                    Ok(None)
                }
            }
        });
        Box::pin(page_stream)
    }

    /// Purge all keys in the store.
    async fn purge_all(&self) -> Result<(), anyhow::Error> {
        self.delete_prefix("").await
    }

    /// Get a JSON value from the store.
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, anyhow::Error>
    where
        Self: Sized,
    {
        match self.get(key).await {
            Ok(Some(data)) => {
                let jd = &mut serde_json::Deserializer::from_slice(&data);
                let out =
                    serde_path_to_error::deserialize(jd).context("could not deserialize JSON")?;

                Ok(Some(out))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[async_trait::async_trait]
impl<K: ObjStore> ObjStore for Arc<K> {
    fn kind(&self) -> &str {
        self.as_ref().kind()
    }

    fn safe_uri(&self) -> &url::Url {
        self.as_ref().safe_uri()
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        self.as_ref().healthcheck().await
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        self.as_ref().meta(key).await
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        self.as_ref().get(key).await
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        self.as_ref().get_stream(key).await
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        self.as_ref().get_with_meta(key).await
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        self.as_ref().get_stream_with_meta(key).await
    }

    async fn generate_download_url(
        &self,
        args: DownloadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        self.as_ref().generate_download_url(args).await
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta, anyhow::Error> {
        self.as_ref().send_put(put).await
    }
    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        self.as_ref().send_copy(copy).await
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        self.as_ref().delete(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error> {
        self.as_ref().delete_prefix(prefix).await
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error> {
        self.as_ref().list(args).await
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        self.as_ref().list_keys(args).await
    }
}

pub type DynObjStore = Arc<dyn ObjStore>;

#[async_trait::async_trait]
impl ObjStore for DynObjStore {
    fn kind(&self) -> &str {
        self.as_ref().kind()
    }

    fn safe_uri(&self) -> &url::Url {
        self.as_ref().safe_uri()
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        self.as_ref().healthcheck().await
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        self.as_ref().meta(key).await
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        self.as_ref().get(key).await
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        self.as_ref().get_stream(key).await
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        self.as_ref().get_with_meta(key).await
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        self.as_ref().get_stream_with_meta(key).await
    }

    async fn generate_download_url(
        &self,
        args: DownloadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        self.as_ref().generate_download_url(args).await
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta, anyhow::Error> {
        self.as_ref().send_put(put).await
    }
    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        self.as_ref().send_copy(copy).await
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        self.as_ref().delete(key).await
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error> {
        self.as_ref().list(args).await
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        self.as_ref().list_keys(args).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error> {
        self.as_ref().delete_prefix(prefix).await
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, anyhow::Error> {
        match self.get(key).await {
            Ok(Some(data)) => {
                let jd = &mut serde_json::Deserializer::from_slice(&data);
                let out =
                    serde_path_to_error::deserialize(jd).context("could not deserialize JSON")?;

                Ok(Some(out))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub struct PutBuilder<'a, S> {
    store: &'a S,
    key: String,
    conditions: Conditions,
    /// Specifies the MIME type of the data.
    mime_type: Option<String>,
}

impl<'a, S: ObjStore> PutBuilder<'a, S>
where
    S: ObjStore,
{
    pub fn build(self, data: impl Into<DataSource>) -> Put {
        let mut put = Put::new(self.key, data.into());
        put.conditions = self.conditions;
        put.mime_type = self.mime_type;
        put
    }

    pub async fn json<T: serde::Serialize>(self, data: &T) -> Result<ObjectMeta, anyhow::Error> {
        let data = serde_json::to_vec(data).context("could not serialize JSON data for put")?;
        let store = self.store;
        let put = self.build(DataSource::Data(Bytes::from(data)));
        store.send_put(put).await
    }

    pub async fn send(self, data: impl Into<DataSource>) -> Result<ObjectMeta, anyhow::Error> {
        let store = self.store;
        let put = self.build(data);
        store.send_put(put).await
    }

    pub async fn text(self, text: impl Into<String>) -> Result<ObjectMeta, anyhow::Error> {
        let data = Bytes::from(text.into());
        self.send(DataSource::Data(data)).await
    }

    pub async fn bytes(self, data: impl Into<Bytes>) -> Result<ObjectMeta, anyhow::Error> {
        self.send(DataSource::Data(data.into())).await
    }

    pub async fn stream<D, E>(
        self,
        stream: impl futures::Stream<Item = Result<D, E>> + Send + 'static,
    ) -> Result<ObjectMeta, anyhow::Error>
    where
        Bytes: From<D>,
        anyhow::Error: From<E>,
        E: Send + 'static,
    {
        let stream: ValueStream = stream
            .map_ok(|item: D| Bytes::from(item))
            .map_err(anyhow::Error::from)
            .boxed();

        self.send(DataSource::Stream(stream)).await
    }
}

/// Builder for a copy request from one key to another, respecting conditions.
pub struct CopyBuilder<'a, S> {
    store: &'a S,
    src: String,
    dest: String,
    conditions: Conditions,
}

impl<'a, S: ObjStore> CopyBuilder<'a, S>
where
    S: ObjStore,
{
    /// Construct the underlying `Copy` request.
    pub fn build(&self) -> Copy {
        let mut copy = Copy::new(self.src.clone(), self.dest.clone());
        copy.conditions = self.conditions.clone();
        copy
    }

    /// Execute the copy request.
    pub async fn send(self) -> Result<ObjectMeta, anyhow::Error> {
        let mut copy = Copy::new(self.src.clone(), self.dest.clone());
        copy.conditions = self.conditions.clone();
        self.store.send_copy(copy).await
    }
}

pub trait ObjStoreExt: ObjStore
where
    Self: Sized,
{
    fn put(&self, key: &str) -> PutBuilder<'_, Self> {
        PutBuilder {
            store: self,
            key: key.to_string(),
            conditions: Conditions::default(),
            mime_type: None,
        }
    }
    /// Begin a copy operation from `src` to `dest`, allows setting conditions.
    fn copy(&self, src: &str, dest: &str) -> CopyBuilder<'_, Self> {
        CopyBuilder {
            store: self,
            src: src.to_string(),
            dest: dest.to_string(),
            conditions: Conditions::default(),
        }
    }
}

impl<S: ObjStore> ObjStoreExt for S {}
