use std::borrow::Cow;
use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt as _};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, PrimitiveDateTime, format_description};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use objstore::{
    Copy, DataSource, DownloadUrlArgs, KeyPage, ListArgs, ObjStore, ObjectMeta, ObjectMetaPage,
    Put, ValueStream,
};
use suppaftp::{
    Status,
    tokio::AsyncFtpStream,
    types::{FileType, FtpError},
};
use url::Url;

use crate::FtpObjStoreConfig;

/// FTP-based object store implementation.
#[derive(Clone, Debug)]
pub struct FtpObjStore {
    state: Arc<State>,
}

#[derive(Debug)]
struct State {
    safe_uri: Url,
    host: String,
    port: u16,
    user: String,
    password: String,
    secure: bool,
    path_prefix: Option<String>,
}

impl FtpObjStore {
    /// The kind of this object store (see [`ObjStore::kind`]).
    pub const KIND: &'static str = "objstore.ftp";

    pub fn new(config: FtpObjStoreConfig) -> Result<Self, anyhow::Error> {
        let scheme = if config.secure { "ftps" } else { "ftp" };
        let mut safe_uri = Url::parse(&format!("{scheme}://{}:{}", config.host, config.port))
            .context("failed to build safe-url")?;
        if let Some(prefix) = &config.path_prefix {
            let mut path = String::from("/");
            path.push_str(prefix.trim_matches('/'));
            safe_uri.set_path(&path);
        }
        Ok(Self {
            state: Arc::new(State {
                safe_uri,
                host: config.host,
                port: config.port,
                user: config.user,
                password: config.password,
                secure: config.secure,
                path_prefix: config.path_prefix,
            }),
        })
    }

    fn build_path<'a>(&self, key: &'a str) -> Cow<'a, str> {
        let key = key.trim_start_matches('/');
        match &self.state.path_prefix {
            Some(prefix) if !prefix.is_empty() => {
                Cow::Owned(format!("/{}/{}", prefix.trim_matches('/'), key))
            }
            _ => Cow::Owned(format!("/{key}")),
        }
    }

    async fn connect(&self) -> Result<AsyncFtpStream, anyhow::Error> {
        if self.state.secure {
            anyhow::bail!("ftps not supported in this build");
        }
        let addr = format!("{}:{}", self.state.host, self.state.port);
        tracing::debug!(addr, "connecting to ftp server");
        let mut ftp = AsyncFtpStream::connect(addr).await?;
        tracing::debug!(
            user = self.state.user,
            password = self.state.password,
            "starting ftp login"
        );
        dbg!(&self.state);
        ftp.login(&self.state.user, &self.state.password).await?;
        ftp.transfer_type(FileType::Binary).await?;
        Ok(ftp)
    }

    fn parse_mlst(&self, key: String, line: &str) -> ObjectMeta {
        let fmt = format_description::parse("[year][month][day][hour][minute][second]")
            .expect("valid format description");
        let (facts, _name) = line.split_once(' ').unwrap_or((line, ""));
        let mut size = None;
        let mut mtime = None;
        for fact in facts.split(';') {
            if let Some((k, v)) = fact.split_once('=') {
                match k.to_ascii_lowercase().as_str() {
                    "size" => {
                        size = v.parse().ok();
                    }
                    "modify" => {
                        if let Ok(dt) = PrimitiveDateTime::parse(v, &fmt) {
                            mtime = Some(dt.assume_utc());
                        }
                    }
                    _ => {}
                }
            }
        }
        let mut meta = ObjectMeta::new(key);
        meta.size = size;
        meta.updated_at = mtime;
        meta.created_at = mtime;
        meta
    }
}

#[async_trait::async_trait]
impl ObjStore for FtpObjStore {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn safe_uri(&self) -> &Url {
        &self.state.safe_uri
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        let mut ftp = self.connect().await?;
        ftp.noop().await?;
        ftp.quit().await.ok();
        Ok(())
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        let mut ftp = self.connect().await?;
        let path = self.build_path(key);
        match ftp.mlst(Some(path.as_ref())).await {
            Ok(line) => {
                let meta = self.parse_mlst(key.to_string(), &line);
                ftp.quit().await.ok();
                Ok(Some(meta))
            }
            Err(FtpError::UnexpectedResponse(resp)) if resp.status == Status::FileUnavailable => {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        let mut ftp = self.connect().await?;
        let path = self.build_path(key);
        match ftp.retr_as_stream(path.as_ref()).await {
            Ok(mut reader) => {
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await?;
                ftp.finalize_retr_stream(reader).await?;
                ftp.quit().await.ok();
                Ok(Some(Bytes::from(buf)))
            }
            Err(FtpError::UnexpectedResponse(resp)) if resp.status == Status::FileUnavailable => {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        if let Some(bytes) = self.get(key).await? {
            let stream = futures::stream::once(async move { Ok(bytes) }).boxed();
            Ok(Some(stream))
        } else {
            Ok(None)
        }
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        if let Some(data) = self.get(key).await? {
            let meta = self.meta(key).await?.unwrap_or_else(|| {
                let mut m = ObjectMeta::new(key.to_string());
                m.size = Some(data.len() as u64);
                let now = OffsetDateTime::now_utc();
                m.created_at = Some(now);
                m.updated_at = Some(now);
                let sha = Sha256::digest(&data);
                m.hash_sha256 = Some(sha.into());
                m
            });
            Ok(Some((data, meta)))
        } else {
            Ok(None)
        }
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        if let Some((data, meta)) = self.get_with_meta(key).await? {
            let stream = futures::stream::once(async move { Ok(data) }).boxed();
            Ok(Some((meta, stream)))
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
        let mut ftp = self.connect().await?;
        let path = self.build_path(&put.key);
        let now = OffsetDateTime::now_utc();
        let mut meta = ObjectMeta::new(put.key.clone());
        meta.created_at = Some(now);
        meta.updated_at = Some(now);
        match put.data {
            DataSource::Data(bytes) => {
                let mut cursor = std::io::Cursor::new(bytes.clone());
                ftp.put_file(path.as_ref(), &mut cursor).await?;
                let sha = Sha256::digest(&bytes);
                meta.size = Some(bytes.len() as u64);
                meta.hash_sha256 = Some(sha.into());
            }
            DataSource::Stream(mut stream) => {
                let mut writer = ftp.put_with_stream(path.as_ref()).await?;
                let mut hasher = Sha256::new();
                let mut size = 0u64;
                while let Some(chunk) = stream.try_next().await? {
                    size += chunk.len() as u64;
                    hasher.update(&chunk);
                    writer.write_all(&chunk).await?;
                }
                ftp.finalize_put_stream(writer).await?;
                meta.size = Some(size);
                meta.hash_sha256 = Some(hasher.finalize().into());
            }
        }
        ftp.quit().await.ok();
        Ok(meta)
    }

    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        let data = self
            .get(&copy.source_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source key '{}' not found", copy.source_key))?;
        let put = Put::new(copy.target_key.clone(), DataSource::Data(data));
        self.send_put(put).await
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        let mut ftp = self.connect().await?;
        let path = self.build_path(key);
        match ftp.rm(path.as_ref()).await {
            Ok(_) => {
                ftp.quit().await.ok();
                Ok(())
            }
            Err(FtpError::UnexpectedResponse(resp)) if resp.status == Status::FileUnavailable => {
                Ok(())
            }
            Err(e) => Err(e.into()),
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
        let page = self.list(args).await?;
        let items = page.items.iter().map(|m| m.key.clone()).collect();
        Ok(KeyPage {
            items,
            next_cursor: page.next_cursor,
        })
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error> {
        let limit = args.limit().unwrap_or(1_000) as usize;
        let prefix = args.prefix().unwrap_or("");
        let (dir, filter) = match prefix.rsplit_once('/') {
            Some((d, f)) => (d, Some(f.to_string())),
            None => (
                "",
                if prefix.is_empty() {
                    None
                } else {
                    Some(prefix.to_string())
                },
            ),
        };
        let mut ftp = self.connect().await?;
        let dir_path = self.build_path(dir);
        let names = ftp.nlst(Some(dir_path.as_ref())).await.unwrap_or_default();
        let mut items = Vec::new();
        for name in names {
            let name = name.trim().to_string();
            if let Some(f) = &filter {
                if !name.starts_with(f) {
                    continue;
                }
            }
            let key = if dir.is_empty() {
                name.clone()
            } else {
                format!("{dir}/{name}")
            };
            if let Ok(line) = ftp.mlst(Some(self.build_path(&key).as_ref())).await {
                let meta = self.parse_mlst(key, &line);
                items.push(meta);
            }
            if items.len() >= limit {
                break;
            }
        }
        ftp.quit().await.ok();
        Ok(ObjectMetaPage {
            items,
            next_cursor: None,
            prefixes: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load_test_config() -> Result<Option<FtpObjStoreConfig>, anyhow::Error> {
        const ENV_VAR: &str = "FTP_TEST_URI";
        let Ok(var) = std::env::var(ENV_VAR) else {
            eprintln!("skipping ftp tests due to missing config - set {ENV_VAR} to run");
            return Ok(None);
        };
        dbg!(&var);
        let config = FtpObjStoreConfig::from_uri(&var)?;
        Ok(Some(config))
    }

    #[tokio::test]
    #[test_log::test]
    async fn test_ftp_store() {
        let config = if let Some(config) = load_test_config().unwrap() {
            config
        } else {
            return;
        };
        tracing::info!(?config, "loaded ftp test config");
        let store = FtpObjStore::new(config).expect("failed to create ftp store");
        objstore_test::test_objstore(&store).await;
    }
}
