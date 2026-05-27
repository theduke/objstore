use bytes::Bytes;

use crate::{
    Copy, DownloadUrlArgs, KeyPage, ListArgs, ObjStore, ObjectMeta, ObjectMetaPage, Put,
    UploadUrlArgs, ValueStream,
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

    fn strip_prefix(&self, key: &str) -> Result<String, anyhow::Error> {
        if self.prefix.is_empty() {
            return Ok(key.to_owned());
        }

        key.strip_prefix(&self.prefix)
            .map(|suffix| suffix.trim_start_matches('/').to_owned())
            .ok_or_else(|| anyhow::anyhow!("wrapped store returned key outside prefix: {key}"))
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

    fn map_meta(&self, mut meta: ObjectMeta) -> Result<ObjectMeta, anyhow::Error> {
        meta.key = self.strip_prefix(&meta.key)?;
        Ok(meta)
    }

    fn map_meta_page(&self, mut page: ObjectMetaPage) -> Result<ObjectMetaPage, anyhow::Error> {
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

    fn map_key_page(&self, mut page: KeyPage) -> Result<KeyPage, anyhow::Error> {
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

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        self.inner.healthcheck().await
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        self.inner
            .meta(&self.prepend_prefix(key))
            .await?
            .map(|meta| self.map_meta(meta))
            .transpose()
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        self.inner.get(&self.prepend_prefix(key)).await
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        self.inner.get_stream(&self.prepend_prefix(key)).await
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        self.inner
            .get_with_meta(&self.prepend_prefix(key))
            .await?
            .map(|(value, meta)| self.map_meta(meta).map(|meta| (value, meta)))
            .transpose()
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        self.inner
            .get_stream_with_meta(&self.prepend_prefix(key))
            .await?
            .map(|(meta, value)| self.map_meta(meta).map(|meta| (meta, value)))
            .transpose()
    }

    async fn generate_download_url(
        &self,
        mut args: DownloadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        args.key = self.prepend_prefix(&args.key);
        self.inner.generate_download_url(args).await
    }

    async fn generate_upload_url(
        &self,
        mut args: UploadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        args.key = self.prepend_prefix(&args.key);
        self.inner.generate_upload_url(args).await
    }

    async fn send_put(&self, mut put: Put) -> Result<ObjectMeta, anyhow::Error> {
        put.key = self.prepend_prefix(&put.key);
        let meta = self.inner.send_put(put).await?;
        self.map_meta(meta)
    }

    async fn send_copy(&self, mut copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        copy.source_key = self.prepend_prefix(&copy.source_key);
        copy.target_key = self.prepend_prefix(&copy.target_key);
        let meta = self.inner.send_copy(copy).await?;
        self.map_meta(meta)
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        self.inner.delete(&self.prepend_prefix(key)).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error> {
        self.inner.delete_prefix(&self.prepend_prefix(prefix)).await
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error> {
        let page = self.inner.list(self.map_list_args(args)).await?;
        self.map_meta_page(page)
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        let page = self.inner.list_keys(self.map_list_args(args)).await?;
        self.map_key_page(page)
    }
}
