mod provider;

pub use self::provider::MemoryProvider;

use std::{collections::BTreeMap, sync::Arc};

use bytes::{Bytes, BytesMut};
use futures::TryStreamExt as _;
use time::OffsetDateTime;
use tokio::sync::RwLock;

use objstore::{
    Copy, DataSource, DownloadUrlArgs, KeyMetaPage, KeyPage, ListArgs, ObjStore, ObjectMeta, Put,
    ValueStream,
};
use url::Url;

/// In-memory [`ObjStore`] implementation.
///
/// Supports concurrent access.
#[derive(Clone)]
pub struct MemoryObjStore {
    state: State,
    safe_uri: Url,
}

impl std::fmt::Debug for MemoryObjStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore").finish()
    }
}

#[derive(Clone)]
struct Item {
    data: Bytes,
    meta: ObjectMeta,
}

#[derive(Clone)]
struct State {
    data: Arc<RwLock<BTreeMap<String, Item>>>,
}

impl MemoryObjStore {
    /// The kind of this object store (see [`ObjStore::kind`]).
    pub const KIND: &'static str = "objstore.memory";

    pub fn new() -> Self {
        Self {
            safe_uri: Url::parse("memory://").expect("Invalid URL for MemoryObjStore"),
            state: State {
                data: Arc::new(RwLock::new(BTreeMap::new())),
            },
        }
    }
}

impl Default for MemoryObjStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl ObjStore for MemoryObjStore {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn safe_uri(&self) -> &Url {
        &self.safe_uri
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        let meta = self
            .state
            .data
            .read()
            .await
            .get(key)
            .map(|item| item.meta.clone());
        Ok(meta)
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        let bytes = self
            .state
            .data
            .read()
            .await
            .get(key)
            .map(|item| item.data.clone());
        Ok(bytes)
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        if let Some(value) = self.get(key).await? {
            let stream = futures::stream::once(async move { Ok(value) });
            Ok(Some(Box::pin(stream)))
        } else {
            Ok(None)
        }
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        match self.state.data.read().await.get(key).cloned() {
            Some(item) => Ok(Some((item.data, item.meta))),
            None => Ok(None),
        }
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        if let Some((data, meta)) = self.get_with_meta(key).await? {
            let stream = futures::stream::once(async move { Ok(data) });
            Ok(Some((meta, Box::pin(stream))))
        } else {
            Ok(None)
        }
    }

    async fn generate_download_url(
        &self,
        _args: DownloadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        Ok(None)
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta, anyhow::Error> {
        use sha2::Digest;

        let value = match put.data {
            DataSource::Data(bytes) => bytes,
            DataSource::Stream(stream) => {
                let data = stream.try_collect::<BytesMut>().await?;
                data.freeze()
            }
        };

        let digest = sha2::Sha256::digest(&value);

        // Use the sha256 hash as the etag.
        let etag = format!("sha256:{digest:x}");

        let now = OffsetDateTime::now_utc();
        let mut meta = ObjectMeta::new(put.key.clone());
        meta.size = Some(value.len() as u64);
        meta.etag = Some(etag.clone());
        meta.created_at = Some(now);
        meta.updated_at = Some(now);
        meta.hash_sha256 = Some(digest.into());

        self.state.data.write().await.insert(
            put.key,
            Item {
                data: value,
                meta: meta.clone(),
            },
        );
        Ok(meta)
    }

    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        // Load source item
        let item = {
            let data_read = self.state.data.read().await;
            // Check source exists

            // TODO: support conditions

            data_read
                .get(&copy.source_key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("source key '{}' not found", copy.source_key))?
        };
        // Create new metadata for destination
        let mut meta = item.meta.clone();
        meta.key = copy.target_key.clone();
        let now = OffsetDateTime::now_utc();
        meta.created_at = Some(now);
        meta.updated_at = Some(now);
        // Insert copied data
        self.state.data.write().await.insert(
            copy.target_key.clone(),
            Item {
                data: item.data,
                meta: meta.clone(),
            },
        );
        Ok(meta)
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        self.state.data.write().await.remove(key);
        Ok(())
    }

    async fn list(&self, args: ListArgs) -> Result<KeyMetaPage, anyhow::Error> {
        let data = self.state.data.read().await;

        let limit = args.limit().unwrap_or(1_000) as usize;

        let prefix = args.prefix().unwrap_or_default().to_owned();

        let items: Vec<ObjectMeta> = {
            let iter = data
                .range(prefix.clone()..)
                .take_while(|(key, _value)| key.starts_with(&prefix));

            if let Some(cursor) = args.cursor() {
                let cursor = cursor.to_owned();
                iter.skip_while(|(key, _value)| key <= &&cursor)
                    .take(limit)
                    .map(|(_key, item)| item.meta.clone())
                    .collect()
            } else {
                iter.take(limit)
                    .map(|(_key, item)| item.meta.clone())
                    .collect()
            }
        };

        Ok(KeyMetaPage {
            next_cursor: items.last().map(|item| item.key().to_owned()),
            // FIXME: implement args.delimiter() based prefix detection
            prefixes: None,
            items,
        })
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        let items = self.list(args).await?;
        let page = KeyPage {
            items: items
                .items
                .into_iter()
                .map(|item| item.key().to_owned())
                .collect(),
            next_cursor: items.next_cursor,
        };
        Ok(page)
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error> {
        self.state
            .data
            .write()
            .await
            .retain(|key, _value| !key.starts_with(prefix));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_kv_memory() {
        objstore_test::test_objstore(&MemoryObjStore::new()).await;
    }
}
