use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::{
    ByteRange, Conditions, Copy, DataSource, DownloadUrlArgs, KeyPage, KeyStream, ListArgs,
    MetaStream, ObjStoreError, ObjectMeta, ObjectMetaPage, Put, Result, SizedValueStream,
    UploadUrlArgs, ValueStream,
};
use futures::{TryStreamExt as _, stream};

/// Abstraction for a generic key-value store.
#[async_trait::async_trait]
pub trait ObjStore: Send + Sync + std::fmt::Debug {
    /// Get the store as [`std::any::Any`] for downcasting.
    fn as_any(&self) -> &dyn std::any::Any
    where
        Self: Sized + 'static,
    {
        self
    }

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
    async fn healthcheck(&self) -> Result<()>;

    /// Get metadata for a given key.
    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>>;

    /// Build a streaming read, optionally restricted to a byte range.
    fn build_stream(&self, key: &str) -> GetStreamBuilder<'_, Self>
    where
        Self: Sized,
    {
        GetStreamBuilder::new(self, key)
    }

    /// Get the value for a given key.
    async fn get(&self, key: &str) -> Result<Option<Bytes>>;

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>>;

    /// Stream an end-exclusive byte range from an object.
    ///
    /// Bounded ranges are clamped at EOF. A start at exact EOF returns an
    /// empty stream; a start beyond EOF or an end before the start returns
    /// [`ObjStoreError::InvalidRequest`]. `Ok(None)` means the object does not
    /// exist. Implementations must begin reading at the requested offset rather
    /// than consuming the object from byte zero.
    async fn get_range_stream(&self, key: &str, range: ByteRange) -> Result<Option<ValueStream>>;

    /// Collect an end-exclusive byte range into memory.
    async fn get_range(&self, key: &str, range: ByteRange) -> Result<Option<Bytes>> {
        match self.get_range_stream(key, range).await? {
            Some(stream) => Ok(Some(stream.try_collect::<BytesMut>().await?.freeze())),
            None => Ok(None),
        }
    }

    /// Get both the value and metadata for a given key.
    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>>;

    async fn get_stream_with_meta(&self, key: &str) -> Result<Option<(ObjectMeta, ValueStream)>>;

    /// Generate a download URL for a given key.
    ///
    /// NOTE: Must return `Ok(None)` if the store does not support download URLs!
    async fn generate_download_url(&self, args: DownloadUrlArgs) -> Result<Option<url::Url>>;

    /// Generate a presigned upload URL for a given key.
    ///
    /// The client can PUT the object directly to the returned URL without sending
    /// credentials. NOTE: Must return `Ok(None)` if the store does not support upload URLs!
    async fn generate_upload_url(&self, args: UploadUrlArgs) -> Result<Option<url::Url>>;

    /// Store a value under a given key.
    async fn send_put(&self, put: Put) -> Result<ObjectMeta>;

    /// Copy an existing object to a new key.
    ///
    /// May apply server-side copy optimizations and respects `Conditions`.
    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta>;

    /// Delete a key from the store.
    async fn delete(&self, key: &str) -> Result<()>;

    /// Delete all keys with a given prefix.
    async fn delete_prefix(&self, prefix: &str) -> Result<()>;

    /// List keys in the store.
    ///
    /// In contrast to [`Self::list`], this returns only the keys, not their metadata.
    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage>;

    /// List all the keys, optionally filtered by a prefix.
    ///
    /// NOTE: this method will paginate through all keys, and accumulates
    /// the results in memory.
    ///
    /// Use with caution.
    async fn list_all_keys(&self, prefix: &str) -> Result<Vec<String>> {
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
    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage>;

    /// Streaming variant of [`Self::list`]: pages through [`Self::list`] and yields each metadata page (`ObjectMetaPage`).
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
    async fn purge_all(&self) -> Result<()> {
        self.delete_prefix("").await
    }

    /// Get a JSON value from the store.
    async fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>>
    where
        Self: Sized,
    {
        match self.get(key).await {
            Ok(Some(data)) => {
                let jd = &mut serde_json::Deserializer::from_slice(&data);
                let out = serde_path_to_error::deserialize(jd).map_err(|source| {
                    ObjStoreError::ContentDeserialization {
                        key: key.to_string(),
                        format: "json".to_string(),
                        source: Some(Box::new(source)),
                    }
                })?;

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

    async fn healthcheck(&self) -> Result<()> {
        self.as_ref().healthcheck().await
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>> {
        self.as_ref().meta(key).await
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        self.as_ref().get(key).await
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>> {
        self.as_ref().get_stream(key).await
    }

    async fn get_range_stream(&self, key: &str, range: ByteRange) -> Result<Option<ValueStream>> {
        self.as_ref().get_range_stream(key, range).await
    }

    async fn get_range(&self, key: &str, range: ByteRange) -> Result<Option<Bytes>> {
        self.as_ref().get_range(key, range).await
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>> {
        self.as_ref().get_with_meta(key).await
    }

    async fn get_stream_with_meta(&self, key: &str) -> Result<Option<(ObjectMeta, ValueStream)>> {
        self.as_ref().get_stream_with_meta(key).await
    }

    async fn generate_download_url(&self, args: DownloadUrlArgs) -> Result<Option<url::Url>> {
        self.as_ref().generate_download_url(args).await
    }

    async fn generate_upload_url(&self, args: UploadUrlArgs) -> Result<Option<url::Url>> {
        self.as_ref().generate_upload_url(args).await
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta> {
        self.as_ref().send_put(put).await
    }
    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta> {
        self.as_ref().send_copy(copy).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.as_ref().delete(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        self.as_ref().delete_prefix(prefix).await
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage> {
        self.as_ref().list(args).await
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage> {
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

    async fn healthcheck(&self) -> Result<()> {
        self.as_ref().healthcheck().await
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>> {
        self.as_ref().meta(key).await
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        self.as_ref().get(key).await
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>> {
        self.as_ref().get_stream(key).await
    }

    async fn get_range_stream(&self, key: &str, range: ByteRange) -> Result<Option<ValueStream>> {
        self.as_ref().get_range_stream(key, range).await
    }

    async fn get_range(&self, key: &str, range: ByteRange) -> Result<Option<Bytes>> {
        self.as_ref().get_range(key, range).await
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>> {
        self.as_ref().get_with_meta(key).await
    }

    async fn get_stream_with_meta(&self, key: &str) -> Result<Option<(ObjectMeta, ValueStream)>> {
        self.as_ref().get_stream_with_meta(key).await
    }

    async fn generate_download_url(&self, args: DownloadUrlArgs) -> Result<Option<url::Url>> {
        self.as_ref().generate_download_url(args).await
    }

    async fn generate_upload_url(&self, args: UploadUrlArgs) -> Result<Option<url::Url>> {
        self.as_ref().generate_upload_url(args).await
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta> {
        self.as_ref().send_put(put).await
    }
    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta> {
        self.as_ref().send_copy(copy).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.as_ref().delete(key).await
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage> {
        self.as_ref().list(args).await
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage> {
        self.as_ref().list_keys(args).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        self.as_ref().delete_prefix(prefix).await
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        match self.get(key).await {
            Ok(Some(data)) => {
                let jd = &mut serde_json::Deserializer::from_slice(&data);
                let out = serde_path_to_error::deserialize(jd).map_err(|source| {
                    ObjStoreError::ContentDeserialization {
                        key: key.to_string(),
                        format: "json".to_string(),
                        source: Some(Box::new(source)),
                    }
                })?;

                Ok(Some(out))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

/// Builder for streaming all or part of an object.
pub struct GetStreamBuilder<'a, S: ?Sized> {
    store: &'a S,
    key: String,
    range: Option<ByteRange>,
}

impl<'a, S> GetStreamBuilder<'a, S>
where
    S: ObjStore + ?Sized,
{
    fn new(store: &'a S, key: &str) -> Self {
        Self {
            store,
            key: key.to_string(),
            range: None,
        }
    }

    /// Set the inclusive start offset. If omitted, reading starts at byte zero.
    pub fn with_start(mut self, start: u64) -> Self {
        self.range.get_or_insert(ByteRange::from_offset(0)).start = start;
        self
    }

    /// Set the exclusive end offset. If omitted, reading continues through EOF.
    pub fn with_end(mut self, end: u64) -> Self {
        self.range.get_or_insert(ByteRange::from_offset(0)).end = Some(end);
        self
    }

    /// Set both offsets from a [`ByteRange`].
    pub fn with_range(mut self, range: ByteRange) -> Self {
        self.range = Some(range);
        self
    }

    /// Open the configured byte stream.
    pub async fn send(self) -> Result<Option<ValueStream>> {
        match self.range {
            Some(range) => self.store.get_range_stream(&self.key, range).await,
            None => self.store.get_stream(&self.key).await,
        }
    }

    /// Open the configured byte stream together with full-object metadata.
    pub async fn send_with_meta(self) -> Result<Option<(ObjectMeta, ValueStream)>> {
        match self.range {
            None => self.store.get_stream_with_meta(&self.key).await,
            Some(range) => {
                let meta = match self.store.meta(&self.key).await? {
                    Some(meta) => meta,
                    None => return Ok(None),
                };
                match self.store.get_range_stream(&self.key, range).await? {
                    Some(stream) => Ok(Some((meta, stream))),
                    None => Ok(None),
                }
            }
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

    pub async fn json<T: serde::Serialize>(self, data: &T) -> Result<ObjectMeta> {
        let data = serde_json::to_vec(data).map_err(|source| ObjStoreError::InvalidRequest {
            message: "could not serialize JSON data for put".to_string(),
            source: Some(source.into()),
        })?;
        let store = self.store;
        let put = self.build(DataSource::Data(Bytes::from(data)));
        store.send_put(put).await
    }

    pub async fn send(self, data: impl Into<DataSource>) -> Result<ObjectMeta> {
        let store = self.store;
        let put = self.build(data);
        store.send_put(put).await
    }

    pub async fn text(self, text: impl Into<String>) -> Result<ObjectMeta> {
        let data = Bytes::from(text.into());
        self.send(DataSource::Data(data)).await
    }

    pub async fn bytes(self, data: impl Into<Bytes>) -> Result<ObjectMeta> {
        self.send(DataSource::Data(data.into())).await
    }

    pub async fn stream(self, stream: SizedValueStream) -> Result<ObjectMeta> {
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
    pub async fn send(self) -> Result<ObjectMeta> {
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
