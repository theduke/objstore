use bytes::Bytes;
use futures::TryStreamExt as _;

use crate::{
    Copy, DownloadUrlArgs, KeyPage, ListArgs, ObjStore, ObjStoreError, ObjectMeta, ObjectMetaPage,
    Put, Resource, Result, UploadUrlArgs, ValueStream,
};

/// Wrapper that scopes all object store operations to a fixed key prefix.
#[derive(Clone, Debug)]
pub struct PrefixObjStore<S> {
    prefix: String,
    inner: S,
}

impl<S> PrefixObjStore<S> {
    /// Creates a new prefixed object store.
    ///
    /// The configured prefix is normalized to avoid duplicate leading/trailing `/`
    /// separators, so `"tenant-a"` and `"/tenant-a/"` behave the same.
    pub fn new(prefix: impl AsRef<str>, inner: S) -> Self {
        Self {
            prefix: normalize_prefix(prefix.as_ref()),
            inner,
        }
    }

    fn trim_joined_suffix<'a>(&self, key: &'a str) -> &'a str {
        if self.prefix.is_empty() {
            key
        } else {
            key.trim_start_matches('/')
        }
    }

    fn prepend_prefix(&self, key: &str) -> String {
        let key = self.trim_joined_suffix(key);

        if self.prefix.is_empty() {
            key.to_owned()
        } else if key.is_empty() {
            self.prefix.clone()
        } else {
            format!("{}{}", self.prefix, key)
        }
    }

    fn strip_prefix(&self, key: &str) -> Result<String> {
        if self.prefix.is_empty() {
            return Ok(key.to_owned());
        }

        key.strip_prefix(&self.prefix)
            .map(|suffix| suffix.trim_start_matches('/').to_owned())
            .ok_or_else(|| ObjStoreError::Internal {
                message: format!("wrapped store returned key outside prefix: {key}"),
                source: None,
            })
    }

    fn strip_prefix_owned(&self, key: String) -> std::result::Result<String, String> {
        if self.prefix.is_empty() {
            return Ok(key);
        }

        match key.strip_prefix(&self.prefix) {
            Some(suffix) => Ok(suffix.trim_start_matches('/').to_owned()),
            None => Err(key),
        }
    }

    fn map_resource(&self, resource: Resource) -> Resource {
        match resource {
            Resource::Object { key } => match self.strip_prefix_owned(key) {
                Ok(key) => Resource::Object { key },
                Err(key) => Resource::Object { key },
            },
            Resource::Prefix { prefix } => match self.strip_prefix_owned(prefix) {
                Ok(prefix) => Resource::Prefix { prefix },
                Err(prefix) => Resource::Prefix { prefix },
            },
            resource => resource,
        }
    }

    fn map_key_lossy(&self, key: String) -> String {
        match self.strip_prefix_owned(key) {
            Ok(key) => key,
            Err(key) => key,
        }
    }

    fn map_error(&self, err: ObjStoreError) -> ObjStoreError {
        match err {
            ObjStoreError::ObjectNotFound { key, source } => match self.strip_prefix_owned(key) {
                Ok(key) => ObjStoreError::ObjectNotFound { key, source },
                Err(key) => ObjStoreError::Internal {
                    message: format!("wrapped store returned object error outside prefix: {key}"),
                    source: Some(ObjStoreError::ObjectNotFound { key, source }.into()),
                },
            },
            ObjStoreError::AlreadyExists { resource, source } => ObjStoreError::AlreadyExists {
                resource: self.map_resource(resource),
                source,
            },
            ObjStoreError::PreconditionFailed {
                operation,
                resource,
                source,
            } => ObjStoreError::PreconditionFailed {
                operation,
                resource: resource.map(|resource| self.map_resource(resource)),
                source,
            },
            ObjStoreError::PermissionDenied {
                operation,
                resource,
                source,
            } => ObjStoreError::PermissionDenied {
                operation,
                resource: resource.map(|resource| self.map_resource(resource)),
                source,
            },
            ObjStoreError::Unauthenticated {
                operation,
                resource,
                source,
            } => ObjStoreError::Unauthenticated {
                operation,
                resource: resource.map(|resource| self.map_resource(resource)),
                source,
            },
            ObjStoreError::Backend {
                backend,
                operation,
                mut details,
                source,
            } => ObjStoreError::Backend {
                backend,
                operation,
                details: {
                    details.resource = details.resource.map(|resource| self.map_resource(resource));
                    details
                },
                source,
            },
            ObjStoreError::InvalidMetadata {
                key,
                message,
                source,
            } => ObjStoreError::InvalidMetadata {
                key: self.map_key_lossy(key),
                message,
                source,
            },
            ObjStoreError::ContentDeserialization {
                key,
                format,
                source,
            } => ObjStoreError::ContentDeserialization {
                key: self.map_key_lossy(key),
                format,
                source,
            },
            err => err,
        }
    }

    fn map_list_args(&self, mut args: ListArgs) -> ListArgs {
        match args.prefix().map(str::to_owned) {
            Some(prefix) => args.set_prefix(self.prepend_prefix(&prefix)),
            None if !self.prefix.is_empty() => args.set_prefix(self.prefix.clone()),
            None => {}
        }

        if let Some(cursor) = args.cursor().map(str::to_owned) {
            args = args.with_cursor(self.prepend_prefix(&cursor));
        }

        args
    }

    fn map_meta(&self, mut meta: ObjectMeta) -> Result<ObjectMeta> {
        meta.key = self.strip_prefix(&meta.key)?;
        Ok(meta)
    }

    fn map_meta_page(&self, mut page: ObjectMetaPage) -> Result<ObjectMetaPage> {
        page.items = page
            .items
            .into_iter()
            .map(|item| self.map_meta(item))
            .collect::<Result<_, _>>()?;

        page.next_cursor = page
            .next_cursor
            .map(|cursor| self.strip_prefix(&cursor))
            .transpose()?;

        page.prefixes = page
            .prefixes
            .map(|prefixes| {
                prefixes
                    .into_iter()
                    .map(|prefix| self.strip_prefix(&prefix))
                    .collect::<Result<_, _>>()
            })
            .transpose()?;

        Ok(page)
    }

    fn map_key_page(&self, mut page: KeyPage) -> Result<KeyPage> {
        page.items = page
            .items
            .into_iter()
            .map(|key| self.strip_prefix(&key))
            .collect::<Result<_, _>>()?;

        page.next_cursor = page
            .next_cursor
            .map(|cursor| self.strip_prefix(&cursor))
            .transpose()?;

        Ok(page)
    }

    fn map_stream_errors(&self, stream: ValueStream) -> ValueStream {
        let prefix = PrefixObjStore {
            prefix: self.prefix.clone(),
            inner: (),
        };
        Box::pin(stream.map_err(move |err| prefix.map_error(err)))
    }
}

fn normalize_prefix(prefix: &str) -> String {
    let prefix = prefix.trim_matches('/');

    if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    }
}

#[async_trait::async_trait]
impl<S> ObjStore for PrefixObjStore<S>
where
    S: ObjStore + Send + Sync,
{
    fn kind(&self) -> &str {
        self.inner.kind()
    }

    fn safe_uri(&self) -> &url::Url {
        self.inner.safe_uri()
    }

    async fn healthcheck(&self) -> Result<()> {
        self.inner
            .healthcheck()
            .await
            .map_err(|err| self.map_error(err))
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>> {
        self.inner
            .meta(&self.prepend_prefix(key))
            .await
            .map_err(|err| self.map_error(err))?
            .map(|meta| self.map_meta(meta))
            .transpose()
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        self.inner
            .get(&self.prepend_prefix(key))
            .await
            .map_err(|err| self.map_error(err))
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>> {
        self.inner
            .get_stream(&self.prepend_prefix(key))
            .await
            .map_err(|err| self.map_error(err))
            .map(|stream| stream.map(|stream| self.map_stream_errors(stream)))
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>> {
        self.inner
            .get_with_meta(&self.prepend_prefix(key))
            .await
            .map_err(|err| self.map_error(err))?
            .map(|(value, meta)| self.map_meta(meta).map(|meta| (value, meta)))
            .transpose()
    }

    async fn get_stream_with_meta(&self, key: &str) -> Result<Option<(ObjectMeta, ValueStream)>> {
        self.inner
            .get_stream_with_meta(&self.prepend_prefix(key))
            .await
            .map_err(|err| self.map_error(err))?
            .map(|(meta, value)| self.map_meta(meta).map(|meta| (meta, value)))
            .map(|result| result.map(|(meta, stream)| (meta, self.map_stream_errors(stream))))
            .transpose()
    }

    async fn generate_download_url(&self, mut args: DownloadUrlArgs) -> Result<Option<url::Url>> {
        args.key = self.prepend_prefix(&args.key);
        self.inner
            .generate_download_url(args)
            .await
            .map_err(|err| self.map_error(err))
    }

    async fn generate_upload_url(&self, mut args: UploadUrlArgs) -> Result<Option<url::Url>> {
        args.key = self.prepend_prefix(&args.key);
        self.inner
            .generate_upload_url(args)
            .await
            .map_err(|err| self.map_error(err))
    }

    async fn send_put(&self, mut put: Put) -> Result<ObjectMeta> {
        put.key = self.prepend_prefix(&put.key);
        let meta = self
            .inner
            .send_put(put)
            .await
            .map_err(|err| self.map_error(err))?;
        self.map_meta(meta)
    }

    async fn send_copy(&self, mut copy: Copy) -> Result<ObjectMeta> {
        copy.source_key = self.prepend_prefix(&copy.source_key);
        copy.target_key = self.prepend_prefix(&copy.target_key);
        let meta = self
            .inner
            .send_copy(copy)
            .await
            .map_err(|err| self.map_error(err))?;
        self.map_meta(meta)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner
            .delete(&self.prepend_prefix(key))
            .await
            .map_err(|err| self.map_error(err))
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        self.inner
            .delete_prefix(&self.prepend_prefix(prefix))
            .await
            .map_err(|err| self.map_error(err))
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage> {
        let page = self
            .inner
            .list(self.map_list_args(args))
            .await
            .map_err(|err| self.map_error(err))?;
        self.map_meta_page(page)
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage> {
        let page = self
            .inner
            .list_keys(self.map_list_args(args))
            .await
            .map_err(|err| self.map_error(err))?;
        self.map_key_page(page)
    }
}
