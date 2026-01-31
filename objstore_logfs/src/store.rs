use std::{collections::BTreeSet, io::Write as _, sync::Arc};

use anyhow::anyhow;
use bytes::Bytes;
use futures::StreamExt;
use logfs::{Journal2, KeyMeta, LogFs, LogFsError};
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use url::Url;

use objstore::{
    Copy, DataSource, DownloadUrlArgs, KeyPage, ListArgs, ObjStore, ObjectMeta, ObjectMetaPage,
    Put, UploadUrlArgs, ValueStream,
};

use crate::LogFsObjStoreConfig;

#[derive(Clone)]
pub struct LogFsObjStore {
    state: Arc<State>,
}

struct State {
    log: LogFs<Journal2>,
    safe_uri: Url,
}

impl std::fmt::Debug for LogFsObjStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogFsObjStore")
            .field("safe_uri", &self.state.safe_uri)
            .finish()
    }
}

impl LogFsObjStore {
    pub const KIND: &'static str = "objstore.logfs";

    pub fn new(config: LogFsObjStoreConfig) -> Result<Self, anyhow::Error> {
        let log_config = config.to_logfs_config();
        dbg!(&log_config);
        let log = LogFs::open(log_config).map_err(map_logfs_err)?;
        let safe_uri = config.safe_uri()?;

        Ok(Self {
            state: Arc::new(State { log, safe_uri }),
        })
    }

    fn key_meta_to_object_meta(key: String, meta: KeyMeta) -> ObjectMeta {
        let mut obj = ObjectMeta::new(key);
        obj.size = Some(meta.size);
        if let Some(chunk_size) = meta.chunk_size {
            obj.extra
                .insert("chunk_size".to_string(), serde_json::json!(chunk_size));
        }
        obj
    }

    async fn with_log<F, R>(&self, func: F) -> Result<R, anyhow::Error>
    where
        F: FnOnce(LogFs<Journal2>) -> Result<R, LogFsError> + Send + 'static,
        R: Send + 'static,
    {
        let log = self.state.log.clone();
        task::spawn_blocking(move || func(log))
            .await
            .map_err(|err| anyhow!("logfs blocking task failed: {err}"))?
            .map_err(map_logfs_err)
    }

    async fn list_raw(
        &self,
        args: ListArgs,
    ) -> Result<(Vec<ObjectMeta>, Option<String>, Option<Vec<String>>), anyhow::Error> {
        let prefix = args.prefix().map(|p| p.to_string()).unwrap_or_default();
        let limit = args.limit().unwrap_or(1_000) as usize;
        let cursor = args.cursor().map(|c| c.to_string());
        let delimiter = args.delimiter().map(|d| d.to_string());

        self.with_log(move |log| {
            let mut keys = if prefix.is_empty() {
                log.paths_range(String::new()..)?
            } else {
                log.paths_range(prefix.clone()..)?
            };

            if let Some(cursor) = &cursor {
                keys.retain(|key| key > cursor);
            }

            if !prefix.is_empty() {
                keys.retain(|key| key.starts_with(&prefix));
            }

            let mut truncated = false;
            let mut last_processed = None;
            let mut items = Vec::new();
            let mut directories: BTreeSet<String> = BTreeSet::new();
            let mut processed = 0usize;

            for key in keys.into_iter() {
                processed += 1;
                last_processed = Some(key.clone());

                if let Some(delim) = delimiter.as_deref()
                    && !delim.is_empty()
                {
                    let stripped = key
                        .strip_prefix(&prefix)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| key.clone());
                    if let Some(idx) = stripped.find(delim) {
                        let dir = &stripped[..idx];
                        let mut full = prefix.clone();
                        full.push_str(dir);
                        directories.insert(full);
                        if processed >= limit {
                            truncated = true;
                            break;
                        }
                        continue;
                    }
                }

                let key_meta = match log.get_meta(&key)? {
                    Some(meta) => meta,
                    None => continue,
                };
                let meta = Self::key_meta_to_object_meta(key.clone(), key_meta);
                items.push(meta);

                if processed >= limit {
                    truncated = true;
                    break;
                }
            }

            let directories = if directories.is_empty() {
                None
            } else {
                Some(directories.into_iter().collect())
            };

            let next_cursor = if truncated {
                last_processed
            } else {
                items.last().map(|item| item.key.clone())
            };

            Ok((items, next_cursor, directories))
        })
        .await
    }

    async fn spawn_reader_stream(&self, key: String) -> Result<Option<ValueStream>, anyhow::Error> {
        let log = self.state.log.clone();
        let (ready_tx, ready_rx) = oneshot::channel::<Result<bool, LogFsError>>();
        let (tx, rx) = mpsc::channel::<Result<Bytes, LogFsError>>(8);

        task::spawn_blocking(move || {
            let path = key.clone();
            match log.get_chunks(&path) {
                Ok(mut reader) => {
                    let _ = ready_tx.send(Ok(true));
                    for chunk in reader.by_ref() {
                        let chunk = chunk.map(Bytes::from);
                        if tx.blocking_send(chunk).is_err() {
                            break;
                        }
                    }
                }
                Err(LogFsError::NotFound { .. }) => {
                    let _ = ready_tx.send(Ok(false));
                }
                Err(err) => {
                    let _ = ready_tx.send(Err(err));
                }
            }
        });

        match ready_rx
            .await
            .map_err(|err| anyhow!("logfs reader coordination failed: {err}"))?
        {
            Ok(true) => {
                let stream = futures::stream::unfold(rx, |mut rx| async {
                    rx.recv()
                        .await
                        .map(|item| (item.map_err(map_logfs_err), rx))
                });
                Ok(Some(Box::pin(stream)))
            }
            Ok(false) => Ok(None),
            Err(err) => Err(map_logfs_err(err)),
        }
    }
}

#[async_trait::async_trait]
impl ObjStore for LogFsObjStore {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn safe_uri(&self) -> &Url {
        &self.state.safe_uri
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        self.with_log(|log| {
            log.superblock()?;
            Ok(())
        })
        .await
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        let key = key.to_string();
        self.with_log(move |log| match log.get_meta(&key)? {
            Some(meta) => Ok(Some(Self::key_meta_to_object_meta(key, meta))),
            None => Ok(None),
        })
        .await
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        let key = key.to_string();
        let data = self.with_log(move |log| log.get(&key)).await?;
        Ok(data.map(Bytes::from))
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        self.spawn_reader_stream(key.to_string()).await
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        let key = key.to_string();
        self.with_log(move |log| {
            let data = match log.get(&key)? {
                Some(data) => data,
                None => return Ok(None),
            };
            let meta = match log.get_meta(&key)? {
                Some(meta) => Self::key_meta_to_object_meta(key.clone(), meta),
                None => return Ok(None),
            };
            Ok(Some((Bytes::from(data), meta)))
        })
        .await
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        if let Some(meta) = self.meta(key).await?
            && let Some(stream) = self.get_stream(key).await?
        {
            return Ok(Some((meta, stream)));
        }
        Ok(None)
    }

    async fn generate_download_url(
        &self,
        _args: DownloadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        Ok(None)
    }

    async fn generate_upload_url(
        &self,
        _args: UploadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        Ok(None)
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta, anyhow::Error> {
        let key = put.key.clone();
        match put.data {
            DataSource::Data(bytes) => {
                let data = bytes.to_vec();
                self.with_log(move |log| {
                    dbg!("pre insert");
                    log.insert(key.clone(), data)?;
                    dbg!("post insert");
                    let meta = log
                        .get_meta(&key)?
                        .ok_or_else(|| LogFsError::NotFound { path: key.clone() })?;
                    dbg!("post get_meta", &meta);
                    Ok(Self::key_meta_to_object_meta(key, meta))
                })
                .await
            }
            DataSource::Stream(mut stream) => {
                let log = self.state.log.clone();
                let key_clone = key.clone();
                let (tx, rx) = mpsc::channel::<Bytes>(8);
                let writer_handle =
                    task::spawn_blocking(move || -> Result<ObjectMeta, LogFsError> {
                        let mut rx = rx;
                        let mut writer = log.insert_writer(key_clone.clone())?;
                        while let Some(chunk) = rx.blocking_recv() {
                            writer.write_all(&chunk)?;
                        }
                        writer.finish()?;
                        let meta =
                            log.get_meta(&key_clone)?
                                .ok_or_else(|| LogFsError::NotFound {
                                    path: key_clone.clone(),
                                })?;
                        Ok(Self::key_meta_to_object_meta(key_clone, meta))
                    });

                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    tx.send(chunk)
                        .await
                        .map_err(|_| anyhow!("logfs writer task dropped receiver"))?;
                }
                drop(tx);

                writer_handle
                    .await
                    .map_err(|err| anyhow!("logfs writer task failed: {err}"))?
                    .map_err(map_logfs_err)
            }
        }
    }

    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        self.with_log(move |log| {
            let data = log
                .get(&copy.source_key)?
                .ok_or_else(|| LogFsError::NotFound {
                    path: copy.source_key.clone(),
                })?;
            log.insert(copy.target_key.clone(), data)?;
            let meta = log
                .get_meta(&copy.target_key)?
                .ok_or_else(|| LogFsError::NotFound {
                    path: copy.target_key.clone(),
                })?;
            Ok(Self::key_meta_to_object_meta(copy.target_key, meta))
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        let key = key.to_string();
        self.with_log(move |log| {
            log.remove(&key)?;
            Ok(())
        })
        .await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error> {
        let prefix = prefix.to_string();
        self.with_log(move |log| {
            log.remove_prefix(&prefix)?;
            Ok(())
        })
        .await
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error> {
        let (items, next_cursor, prefixes) = self.list_raw(args).await?;
        Ok(ObjectMetaPage {
            items,
            next_cursor,
            prefixes,
        })
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        let page = self.list(args).await?;
        Ok(KeyPage {
            next_cursor: page.next_cursor,
            items: page.items.into_iter().map(|meta| meta.key).collect(),
        })
    }
}

fn map_logfs_err(err: LogFsError) -> anyhow::Error {
    anyhow!(err)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use objstore::wrapper::trace::TracedObjStore;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[test_log::test]
    async fn test_logfs_store() {
        let dir = tempfile::tempdir().unwrap();
        let crypto = crate::LogFsCryptoConfig {
            key: "hello123".to_string(),
            salt: b"saltysalt".to_vec(),
            iterations: NonZeroU32::new(1).unwrap(),
        };
        let config = LogFsObjStoreConfig::new(dir.path().join("store.log"))
            .with_allow_create(true)
            .with_crypto(crypto);
        let store = LogFsObjStore::new(config).unwrap();

        let traced_store = TracedObjStore::new("logfs", store);

        objstore_test::test_objstore(&traced_store).await;
    }
}
