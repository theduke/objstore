mod provider;

pub use self::provider::FsProvider;

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _};
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt as _};

use objstore::{
    Copy, DataSource, DownloadUrlArgs, KeyPage, ListArgs, ObjStore, ObjStoreError, ObjectMeta,
    ObjectMetaPage, Operation, Put, Result, UploadUrlArgs, ValueStream,
};
use sha2::Digest;
use url::Url;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FsObjStoreConfig {
    path: PathBuf,
}

impl FsObjStoreConfig {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[derive(Clone, Debug)]
pub struct FsObjStore {
    state: Arc<State>,
}

#[derive(Debug)]
struct State {
    safe_uri: Url,
    root: PathBuf,
}

impl FsObjStore {
    /// The kind of this object store (see [`ObjStore::kind`]).
    pub const KIND: &'static str = "objstore.fs";

    pub fn new(config: FsObjStoreConfig) -> Result<Self> {
        let root = config.path.clone();
        std::fs::create_dir_all(&root).map_err(|source| ObjStoreError::Io {
            operation: Operation::Build,
            source: Some(source.into()),
        })?;

        let safe_uri = Url::parse(&format!("file://{}", root.display())).map_err(|source| {
            ObjStoreError::InvalidConfig {
                message: "failed to build safe-uri".to_string(),
                source: Some(source.into()),
            }
        })?;

        Ok(Self {
            state: Arc::new(State { safe_uri, root }),
        })
    }

    fn key_path(&self, key: &str) -> PathBuf {
        self.state.root.join(key)
    }
}

fn meta_from_fs_meta(key: String, fs_meta: std::fs::Metadata) -> ObjectMeta {
    let mut meta = ObjectMeta::new(key);
    meta.size = Some(fs_meta.len());
    meta.created_at = fs_meta.created().ok().map(OffsetDateTime::from);
    meta.updated_at = fs_meta.modified().ok().map(OffsetDateTime::from);

    meta
}

fn io_error(operation: Operation, source: std::io::Error) -> ObjStoreError {
    ObjStoreError::Io {
        operation,
        source: Some(source.into()),
    }
}

async fn list_dir_rec(
    path: &Path,
    cursor: Option<&str>,
    limit: usize,
    prefix_filter: Option<&str>,
    current_path: &str,
    items: &mut Vec<ObjectMeta>,
    directories: &mut Option<Vec<String>>,
) -> Result<Option<()>> {
    let f = async {
        let mut iter = match tokio::fs::read_dir(path).await {
            Ok(iter) => iter,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(Operation::List, err)),
        };

        while let Some(entry) = iter
            .next_entry()
            .await
            .map_err(|err| io_error(Operation::List, err))?
        {
            let meta = entry
                .metadata()
                .await
                .map_err(|err| io_error(Operation::List, err))?;
            let key = entry.file_name().to_string_lossy().to_string();

            if let Some(prefix) = &prefix_filter
                && !key.starts_with(prefix)
            {
                continue;
            }

            if !meta.is_file() {
                if meta.is_dir() {
                    if let Some(directories) = directories {
                        directories.push(key.clone());
                    }

                    let cpath = if current_path.is_empty() {
                        key
                    } else {
                        format!("{current_path}/{key}")
                    };
                    list_dir_rec(
                        &entry.path(),
                        cursor,
                        limit,
                        None,
                        &cpath,
                        items,
                        directories,
                    )
                    .await?;
                    continue;
                } else {
                    continue;
                }
            }

            if let Some(cursor) = cursor
                && (key.as_str() <= cursor || key.as_str() == cursor)
            {
                continue;
            }

            let full_key = if current_path.is_empty() {
                key
            } else {
                format!("{current_path}/{key}")
            };
            items.push(meta_from_fs_meta(full_key, meta));

            if items.len() >= limit {
                break;
            }
        }

        Ok(Some(()))
    };

    Box::pin(f).await
}

async fn list_dir(
    path: &Path,
    cursor: Option<&str>,
    limit: usize,
    prefix_filter: Option<&str>,
    current_path: &str,
    flat: bool,
) -> Result<(Vec<ObjectMeta>, Option<Vec<String>>)> {
    let mut items = Vec::new();
    let mut directories = if flat { None } else { Some(Vec::new()) };
    list_dir_rec(
        path,
        cursor,
        limit,
        prefix_filter,
        current_path,
        &mut items,
        &mut directories,
    )
    .await?;

    let mut keys = HashSet::new();

    items.retain(|item| {
        if keys.insert(item.key().to_owned()) {
            true
        } else {
            // If the key is already in the set, we skip it.
            false
        }
    });

    Ok((items, directories))
}

#[async_trait::async_trait]
impl ObjStore for FsObjStore {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn safe_uri(&self) -> &Url {
        &self.state.safe_uri
    }

    async fn healthcheck(&self) -> Result<()> {
        Ok(())
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let path = self.key_path(key);
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(Some(meta_from_fs_meta(key.to_string(), meta))),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(io_error(Operation::Meta, err)),
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let path = self.key_path(key);
        let data = match tokio::fs::read(&path).await {
            Ok(data) => Some(data.into()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(io_error(Operation::Get, err)),
        };
        Ok(data)
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>> {
        let path = self.key_path(key);
        match tokio::fs::File::open(&path).await {
            Ok(file) => {
                let stream = tokio_util::io::ReaderStream::new(file)
                    .map_ok(Bytes::from)
                    .map_err(|source| ObjStoreError::Io {
                        operation: Operation::GetStream,
                        source: Some(source.into()),
                    })
                    .boxed();
                Ok(Some(stream))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(io_error(Operation::GetStream, err)),
        }
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>> {
        let mut f = match tokio::fs::File::open(self.key_path(key)).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(Operation::Get, err)),
        };
        let fs_meta = match f.metadata().await {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(Operation::Get, err)),
        };
        let mut buf = Vec::with_capacity(fs_meta.len() as usize);
        f.read_to_end(&mut buf)
            .await
            .map_err(|err| io_error(Operation::Get, err))?;

        let meta = meta_from_fs_meta(key.to_string(), fs_meta);
        Ok(Some((buf.into(), meta)))
    }

    async fn get_stream_with_meta(&self, key: &str) -> Result<Option<(ObjectMeta, ValueStream)>> {
        let path = self.key_path(key);
        let f = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(Operation::GetStream, err)),
        };
        let fs_meta = match f.metadata().await {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(io_error(Operation::GetStream, err)),
        };
        let stream = tokio_util::io::ReaderStream::new(f)
            .map_ok(Bytes::from)
            .map_err(|source| ObjStoreError::Io {
                operation: Operation::GetStream,
                source: Some(source.into()),
            })
            .boxed();

        let meta = meta_from_fs_meta(key.to_string(), fs_meta);
        Ok(Some((meta, stream)))
    }

    async fn generate_download_url(&self, _args: DownloadUrlArgs) -> Result<Option<url::Url>> {
        Ok(None)
    }

    async fn generate_upload_url(&self, _args: UploadUrlArgs) -> Result<Option<url::Url>> {
        Ok(None)
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta> {
        let path = self.key_path(&put.key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| io_error(Operation::Put, err))?;
        }

        match put.data {
            DataSource::Data(value) => {
                tokio::fs::write(&path, &value)
                    .await
                    .map_err(|err| io_error(Operation::Put, err))?;
            }
            DataSource::Stream(sized) => {
                let mut stream = sized.into_stream();
                let mut file = tokio::fs::File::create(&path)
                    .await
                    .map_err(|err| io_error(Operation::Put, err))?;

                while let Some(chunk) = stream.next().await {
                    file.write_all(&chunk?)
                        .await
                        .map_err(|err| io_error(Operation::Put, err))?;
                }

                file.sync_all()
                    .await
                    .map_err(|err| io_error(Operation::Put, err))?;
            }
        }

        let fs_meta = tokio::fs::metadata(&path)
            .await
            .map_err(|err| io_error(Operation::Put, err))?;
        let meta = meta_from_fs_meta(put.key, fs_meta);

        Ok(meta)
    }

    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta> {
        let src_path = self.key_path(&copy.source_key);
        let dst_path = self.key_path(&copy.target_key);
        // If requested, ensure destination does not exist

        // TODO: conditions support

        if let Some(parent) = dst_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|err| io_error(Operation::Copy, err))?;
        }
        // Perform file copy
        match tokio::fs::copy(&src_path, &dst_path).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(ObjStoreError::object_not_found(copy.source_key));
            }
            Err(err) => return Err(io_error(Operation::Copy, err)),
        }
        // Build metadata from filesystem and compute hash
        let fs_meta = tokio::fs::metadata(&dst_path)
            .await
            .map_err(|err| io_error(Operation::Copy, err))?;
        let data = tokio::fs::read(&dst_path)
            .await
            .map_err(|err| io_error(Operation::Copy, err))?;
        let mut meta = meta_from_fs_meta(copy.target_key.clone(), fs_meta);
        // Compute sha256 hash of copied data
        let digest = sha2::Sha256::digest(&data);
        meta.hash_sha256 = Some(digest.into());
        Ok(meta)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.key_path(key);
        tokio::fs::remove_file(&path)
            .await
            .map_err(|err| io_error(Operation::Delete, err))?;
        Ok(())
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage> {
        let limit = args.limit().unwrap_or(10_000) as usize;

        // Must compute the prefix as a parent directory.

        let (path, key_path, prefix) = if let Some(prefix) = args.prefix() {
            match prefix.rsplit_once('/') {
                Some((main, rest)) => (self.key_path(main), main, Some(rest)),
                None => (self.state.root.clone(), "", Some(prefix)),
            }
        } else {
            (self.state.root.clone(), "", None)
        };

        let flat = if let Some(delim) = args.delimiter() {
            if delim == "/" {
                true
            } else {
                return Err(ObjStoreError::InvalidRequest {
                    message: "the fs store only supports '/' as a delimiter".to_string(),
                    source: None,
                });
            }
        } else {
            false
        };

        let (items, directories) =
            list_dir(&path, args.cursor(), limit, prefix, key_path, flat).await?;

        Ok(ObjectMetaPage {
            next_cursor: items.last().map(|item| item.key().to_owned()),
            items,
            prefixes: directories,
        })
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage> {
        let meta_items = self.list(args).await?;
        let items = meta_items.items.into_iter().map(|item| item.key).collect();
        let page = KeyPage {
            items,
            next_cursor: meta_items.next_cursor,
        };
        Ok(page)
    }

    async fn list_all_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let args = ListArgs::new().with_prefix(prefix).with_limit(u64::MAX);
        let meta_items = self.list(args).await?;
        let keys = meta_items
            .items
            .into_iter()
            .map(|item| item.key)
            .collect::<Vec<_>>();
        Ok(keys)
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<()> {
        let path = self.key_path(prefix);

        // check if dir or file
        let meta = match tokio::fs::metadata(&path).await {
            Ok(meta) => meta,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(io_error(Operation::DeletePrefix, err)),
        };

        let res = if meta.is_dir() {
            tokio::fs::remove_dir_all(&path).await
        } else {
            tokio::fs::remove_file(&path).await
        };
        match res {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_error(Operation::DeletePrefix, err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_kv_fs() {
        let dir = tempfile::tempdir().unwrap();
        let config = FsObjStoreConfig::new(dir.path().to_owned());
        let store = FsObjStore::new(config).unwrap();

        objstore_test::test_objstore(&store).await;
    }
}
