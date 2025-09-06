use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context as _, anyhow};
use bytes::{Bytes, BytesMut};
use futures::TryStreamExt;
use objstore::{
    Copy, DataSource, DownloadUrlArgs, KeyPage, ListArgs, ObjStore, ObjectMeta, ObjectMetaPage,
    Put, ValueStream,
};
use russh_sftp::{
    client::{SftpSession, error::Error as SftpError, fs::Metadata},
    protocol::StatusCode,
};
use time::OffsetDateTime;
use url::Url;

use crate::{SftpObjStoreConfig, pool::SftpPool};

#[derive(Clone)]
pub struct SftpObjStore {
    state: Arc<State>,
}

impl std::fmt::Debug for SftpObjStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpObjStore").finish()
    }
}

struct State {
    root: String,
    safe_uri: Url,
    pool: SftpPool,
}

impl SftpObjStore {
    /// The kind of this object store (see [`ObjStore::kind`]).
    pub const KIND: &'static str = "objstore.sftp";

    pub fn new(config: SftpObjStoreConfig) -> Result<Self, anyhow::Error> {
        let root = if let Some(prefix) = config.path_prefix {
            let mut p = prefix;
            if !p.starts_with('/') {
                p = format!("/{p}");
            }
            if !p.ends_with('/') {
                p.push('/');
            }
            p
        } else {
            "/".to_string()
        };

        let safe_uri_str = format!(
            "sftp://{}@{}:{}{}",
            config.username, config.host, config.port, root
        );
        let safe_uri = Url::parse(&safe_uri_str).context("failed to build safe uri")?;

        Ok(Self {
            state: Arc::new(State {
                root,
                safe_uri,
                pool: SftpPool::new(
                    config.host,
                    config.port,
                    config.username,
                    config.password,
                    config.pool_size,
                ),
            }),
        })
    }

    fn build_path(&self, key: &str) -> String {
        let key = key.trim_start_matches('/');
        if self.state.root.ends_with('/') {
            format!("{}{}", self.state.root, key)
        } else {
            format!("{}/{}", self.state.root, key)
        }
    }
}

fn meta_from_attrs(key: String, attrs: Metadata) -> ObjectMeta {
    let mut meta = ObjectMeta::new(key);
    if let Some(size) = attrs.size {
        meta.size = Some(size);
    }
    if let Some(mtime) = attrs.mtime {
        if let Ok(ts) = OffsetDateTime::from_unix_timestamp(mtime as i64) {
            meta.updated_at = Some(ts);
        }
    }
    meta
}

async fn collect_all(
    sftp: &SftpSession,
    start_remote: String,
    start_key: String,
    out: &mut Vec<(String, Metadata)>,
) -> Result<(), SftpError> {
    let mut stack = vec![(start_remote, start_key)];
    while let Some((remote, key_prefix)) = stack.pop() {
        let dir = match sftp.read_dir(remote.clone()).await {
            Ok(d) => d,
            Err(SftpError::Status(s)) => match s.status_code {
                StatusCode::Eof => {
                    continue;
                }
                StatusCode::NoSuchFile => {
                    continue;
                }
                _ => {
                    return Err(SftpError::Status(s));
                }
            },
            Err(err) => return Err(err),
        };
        for entry in dir {
            let name = entry.file_name();
            let meta = entry.metadata();
            let new_remote = if remote.ends_with('/') {
                format!("{}{}", remote, name)
            } else {
                format!("{}/{}", remote, name)
            };
            let new_key = if key_prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", key_prefix, name)
            };
            if meta.file_type().is_dir() {
                stack.push((new_remote, new_key));
            } else {
                out.push((new_key, meta));
            }
        }
    }
    Ok(())
}

async fn create_dir_all(sftp: &SftpSession, dir: &Path) -> Result<(), SftpError> {
    let mut current = PathBuf::new();
    for component in dir.components() {
        current.push(component.as_os_str());
        if current.as_os_str() == "/" {
            continue;
        }
        let path = current.to_string_lossy().to_string();
        tracing::trace!(path, "creating directory");
        match sftp.create_dir(path.clone()).await {
            Ok(_) => {}
            Err(SftpError::Status(status)) => {
                if status.status_code == StatusCode::Failure {
                    match sftp.metadata(path.clone()).await {
                        Ok(meta) if meta.file_type().is_dir() => {}
                        Ok(_) => return Err(SftpError::Status(status)),
                        Err(e) => return Err(e),
                    }
                } else {
                    return Err(SftpError::Status(status));
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl ObjStore for SftpObjStore {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn safe_uri(&self) -> &Url {
        &self.state.safe_uri
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        self.state
            .pool
            .with_sftp(|s| Box::pin(s.read_dir(self.state.root.clone())))
            .await?;
        Ok(())
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        let path = self.build_path(key);
        match self
            .state
            .pool
            .with_sftp(|s| Box::pin(s.metadata(path.clone())))
            .await
        {
            Ok(attrs) => Ok(Some(meta_from_attrs(key.to_string(), attrs))),
            Err(e) => {
                if let Some(SftpError::Status(status)) = e.downcast_ref::<SftpError>() {
                    if status.status_code == StatusCode::NoSuchFile {
                        return Ok(None);
                    }
                }
                Err(e)
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        let path = self.build_path(key);
        match self
            .state
            .pool
            .with_sftp(|s| Box::pin(s.read(path.clone())))
            .await
        {
            Ok(buf) => Ok(Some(Bytes::from(buf))),
            Err(e) => {
                if let Some(SftpError::Status(status)) = e.downcast_ref::<SftpError>() {
                    if status.status_code == StatusCode::NoSuchFile {
                        return Ok(None);
                    }
                }
                Err(e)
            }
        }
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        if let Some(data) = self.get(key).await? {
            let stream = futures::stream::once(async move { Ok(data) });
            Ok(Some(Box::pin(stream)))
        } else {
            Ok(None)
        }
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        let (data, meta) = tokio::try_join!(self.get(key), self.meta(key))?;
        match (data, meta) {
            (Some(d), Some(m)) => Ok(Some((d, m))),
            _ => Ok(None),
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
    ) -> Result<Option<Url>, anyhow::Error> {
        Ok(None)
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta, anyhow::Error> {
        let path = self.build_path(&put.key);

        let data: Bytes = match put.data {
            DataSource::Data(b) => b,
            DataSource::Stream(mut stream) => {
                let mut buf = BytesMut::new();
                while let Some(chunk) = stream.try_next().await? {
                    buf.extend_from_slice(&chunk);
                }
                buf.freeze()
            }
        };

        let attrs = self
            .state
            .pool
            .with_sftp(|sftp| {
                let path = path.clone();
                let data = data.clone();
                let key = put.key.clone();
                Box::pin(async move {
                    if let Some(parent) = Path::new(&path).parent() {
                        create_dir_all(&sftp, parent).await?;
                    }
                    tracing::trace!(key = %key, path, size = data.len(), "uploading file");
                    sftp.write(path.clone(), &data).await?;
                    sftp.metadata(path).await
                })
            })
            .await?;

        Ok(meta_from_attrs(put.key, attrs))
    }

    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        let data = self
            .get(&copy.source_key)
            .await?
            .ok_or_else(|| anyhow!("source key not found"))?;
        let mut put = Put::new(copy.target_key.clone(), DataSource::Data(data));
        put.conditions = copy.conditions;
        self.send_put(put).await
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        let path = self.build_path(key);
        match self
            .state
            .pool
            .with_sftp(|s| Box::pin(s.remove_file(path.clone())))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(SftpError::Status(status)) = e.downcast_ref::<SftpError>() {
                    if status.status_code == StatusCode::NoSuchFile {
                        return Ok(());
                    }
                }
                Err(e)
            }
        }
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error> {
        let keys = self.list_all_keys(prefix).await?;
        for key in keys {
            let _ = self.delete(&key).await;
        }
        Ok(())
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        let meta = self.list(args).await?;
        Ok(KeyPage {
            items: meta.items.into_iter().map(|m| m.key).collect(),
            next_cursor: meta.next_cursor,
        })
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error> {
        let entries: Vec<(String, Metadata)> = self
            .state
            .pool
            .with_sftp(|sftp| {
                let start = self.state.root.trim_end_matches('/').to_string();
                Box::pin(async move {
                    let mut out = Vec::new();
                    collect_all(sftp, start, "".to_string(), &mut out).await?;
                    Ok(out)
                })
            })
            .await?;
        let mut entries = entries;
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        if let Some(prefix) = args.prefix() {
            entries.retain(|(k, _)| k.starts_with(prefix));
        }
        if let Some(cursor) = args.cursor() {
            entries.retain(|(k, _)| k.as_str() > cursor);
        }
        let limit = args.limit().unwrap_or(1000) as usize;
        let has_more = entries.len() > limit;
        let items: Vec<ObjectMeta> = entries
            .into_iter()
            .take(limit)
            .map(|(k, m)| meta_from_attrs(k, m))
            .collect();
        let next_cursor = if has_more {
            items.last().map(|m| m.key.clone())
        } else {
            None
        };
        Ok(ObjectMetaPage {
            items,
            next_cursor,
            prefixes: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use anyhow::bail;

    use super::*;

    fn load_test_config() -> Result<Option<SftpObjStoreConfig>, anyhow::Error> {
        const ENV_VAR: &str = "SFTP_TEST_URI";
        let Ok(var) = std::env::var(ENV_VAR) else {
            if std::env::var("TEST_STRICT").is_ok() {
                bail!("missing required environment variable: {ENV_VAR}");
            } else {
                eprintln!(
                    "skipping s3 tests due to missing config - set TEST_STRICT=1 env var to require the test"
                );
                return Ok(None);
            }
        };

        let config = SftpObjStoreConfig::from_uri(&var)?;
        Ok(Some(config))
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_sftp_client() -> Result<(), anyhow::Error> {
        let Some(config) = load_test_config()? else {
            return Ok(());
        };
        let store = SftpObjStore::new(config)?;

        objstore_test::test_objstore(&store).await;

        Ok(())
    }
}
