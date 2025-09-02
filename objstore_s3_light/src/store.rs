use std::{borrow::Cow, sync::Arc, time::Duration};

use anyhow::Context;
use bytes::Bytes;
use futures::TryStreamExt as _;
use http::StatusCode;
use reqwest::{Client, Url};
use rusty_s3::{Bucket, S3Action, actions::ListObjectsV2Response};

use bytes::{BufMut, BytesMut};
use futures::StreamExt;
use http::header::{CONTENT_TYPE, ETAG};
use rusty_s3::actions::{CompleteMultipartUpload, CreateMultipartUpload, UploadPart};
use time::OffsetDateTime;

use objstore::{
    Copy, DataSource, DownloadUrlArgs, ObjectMetaPage, KeyPage, ListArgs, ObjStore, ObjectMeta, Put,
    ValueStream,
};

use crate::{
    S3ObjStoreConfig,
    util::{apply_condition_headers, parse_object_headers},
};

/// A lightweight S3-compatible object store.
///
/// Implements the [`KVStore`] trait.
#[derive(Clone, Debug)]
pub struct S3ObjStore {
    state: Arc<State>,
}

#[derive(Debug)]
struct State {
    safe_uri: Url,
    creds: rusty_s3::Credentials,
    bucket: Bucket,
    path_prefix: Option<String>,
    client: Client,
}

impl S3ObjStore {
    /// The kind of this object store (see [`ObjStore::kind`]).
    pub const KIND: &'static str = "objstore.s3-light";

    const DURATION: Duration = Duration::from_secs(180);
    /// Chunk size for multipart upload (minimum 5 MiB per part).
    const PART_SIZE: usize = 8 * 1024 * 1024;

    fn default_client() -> Client {
        Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest client")
    }

    pub fn new(config: S3ObjStoreConfig) -> Result<Self, anyhow::Error> {
        let client = Self::default_client();
        Self::new_with_client(config, client)
    }

    pub fn new_with_client(
        config: S3ObjStoreConfig,
        client: Client,
    ) -> Result<Self, anyhow::Error> {
        let path_prefix = if let Some(prefix) = &config.path_prefix {
            let prefix = prefix.trim_end_matches('/');
            if prefix.is_empty() {
                None
            } else {
                let mut prefix = prefix.to_string();
                prefix.push('/');
                Some(prefix)
            }
        } else {
            None
        };

        let safe_uri = format!(
            "s3://{}/{}",
            config.url.host_str().context("missing host in URL")?,
            config.bucket
        )
        .parse::<Url>()
        .context("failed to build safe-url")?;

        Ok(Self {
            state: Arc::new(State {
                safe_uri,
                creds: config.build_credentials(),
                bucket: config.build_bucket()?,
                path_prefix,
                client,
            }),
        })
    }

    fn build_key<'a>(&self, key: &'a str) -> Cow<'a, str> {
        let key = key.trim_start_matches('/');

        match &self.state.path_prefix {
            Some(prefix) => {
                // The constructor ensures that the prefix ends with a slash.
                debug_assert!(prefix.ends_with('/'));
                Cow::Owned(format!("{prefix}{key}"))
            }
            None => Cow::Borrowed(key),
        }
    }

    fn prune_key_prefix(&self, key: String) -> String {
        match &self.state.path_prefix {
            Some(prefix) => match key.strip_prefix(prefix) {
                Some(suffix) => suffix.to_string(),
                None => key,
            },
            None => key,
        }
    }

    async fn error_for_status(res: reqwest::Response) -> Result<reqwest::Response, anyhow::Error> {
        if res.status().is_success() {
            Ok(res)
        } else {
            let status = res.status();
            let body = res.text().await.context("failed to read response body")?;
            Err(anyhow::anyhow!("S3 request failed: {}: {}", status, body))
        }
    }

    pub async fn head_object(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        let s3_key = self.build_key(key);
        let url = self
            .state
            .bucket
            .head_object(Some(&self.state.creds), &s3_key)
            .sign(Self::DURATION);
        tracing::trace!(%s3_key, %url, "sending head_object request to s3");

        let res = self.state.client.head(url).send().await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let res = Self::error_for_status(res).await?;
        let head = parse_object_headers(key.to_owned(), res.headers())?;

        Ok(Some(head))
    }

    pub async fn get_object_response(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, reqwest::Response)>, anyhow::Error> {
        let s3_key = self.build_key(key);
        tracing::trace!(%s3_key, "loading key from s3");
        let url = self
            .state
            .bucket
            .get_object(Some(&self.state.creds), &s3_key)
            .sign(std::time::Duration::from_secs(60 * 60));

        let res = self.state.client.get(url).send().await?;
        tracing::trace!(?res, "response for get_object request");
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let res = Self::error_for_status(res).await?;

        let head = parse_object_headers(key.to_owned(), res.headers())?;

        Ok(Some((head, res)))
    }

    pub async fn get_object(
        &self,
        key: &str,
    ) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        match self.get_object_response(key).await? {
            Some((head, res)) => {
                let bytes = res.bytes().await.context("failed to read response body")?;
                Ok(Some((bytes, head)))
            }
            None => Ok(None),
        }
    }

    fn generate_download_url(&self, args: DownloadUrlArgs) -> Result<Url, anyhow::Error> {
        let s3_key = self.build_key(&args.key);

        let url = self
            .state
            .bucket
            .get_object(Some(&self.state.creds), &s3_key)
            .sign(args.valid_for);

        Ok(url)
    }

    pub async fn put_object(&self, mut put: Put) -> Result<ObjectMeta, anyhow::Error> {
        // If the payload is a stream, use multipart upload for resilience and large sizes.

        let mut data = DataSource::Data(Bytes::new());
        std::mem::swap(&mut data, &mut put.data);

        let data = match data {
            DataSource::Data(bytes) => bytes,
            DataSource::Stream(stream) => {
                return self.multipart_upload(put, stream).await;
            }
        };

        // Simple upload for buffered data.
        let s3_key = self.build_key(&put.key);
        let mut action = self
            .state
            .bucket
            .put_object(Some(&self.state.creds), &s3_key);
        apply_condition_headers(action.headers_mut(), put.conditions)?;
        // forward MIME type header if set
        if let Some(ct) = &put.mime_type {
            action.headers_mut().insert(CONTENT_TYPE.to_string(), ct);
        }
        let url = action.sign(Self::DURATION);

        let body = data;

        let res = self.state.client.put(url).body(body).send().await?;
        let res = Self::error_for_status(res).await?;
        let meta = parse_object_headers(put.key, res.headers())?;
        Ok(meta)
    }

    async fn multipart_upload(
        &self,
        put: Put,
        mut stream: ValueStream,
    ) -> Result<ObjectMeta, anyhow::Error> {
        // initiate multipart upload
        let s3_key = self.build_key(&put.key);
        let mut create = self
            .state
            .bucket
            .create_multipart_upload(Some(&self.state.creds), &s3_key);
        apply_condition_headers(create.headers_mut(), put.conditions)?;
        // forward MIME type header if set
        if let Some(ct) = &put.mime_type {
            create.headers_mut().insert(CONTENT_TYPE.to_string(), ct);
        }
        let url = create.sign(Self::DURATION);
        let resp = self.state.client.post(url).send().await?;
        let resp = Self::error_for_status(resp).await?;
        let body = resp
            .text()
            .await
            .context("reading multipart create response")?;
        let multipart = CreateMultipartUpload::parse_response(&body)
            .context("parsing multipart create response")?;
        let upload_id = multipart.upload_id();

        // upload parts
        let mut part_number = 1u16;
        let mut etags = Vec::new();
        let mut buffer = BytesMut::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.put_slice(&chunk);
            if buffer.len() >= Self::PART_SIZE {
                let upload = UploadPart::new(
                    &self.state.bucket,
                    Some(&self.state.creds),
                    &s3_key,
                    part_number,
                    upload_id,
                );
                let url = upload.sign(Self::DURATION);
                let data = buffer.split().freeze();
                let res = self.state.client.put(url).body(data).send().await?;
                let res = Self::error_for_status(res).await?;
                let etag = res
                    .headers()
                    .get(ETAG)
                    .context("missing ETag for multipart part")?
                    .to_str()?
                    .trim_matches('"')
                    .to_string();
                etags.push(etag);
                part_number += 1;
            }
        }
        // final part (include empty buffer for empty stream)
        if !buffer.is_empty() || etags.is_empty() {
            let upload = UploadPart::new(
                &self.state.bucket,
                Some(&self.state.creds),
                &s3_key,
                part_number,
                upload_id,
            );
            let url = upload.sign(Self::DURATION);
            let data = buffer.freeze();
            let res = self.state.client.put(url).body(data).send().await?;
            let res = Self::error_for_status(res).await?;
            let etag = res
                .headers()
                .get(ETAG)
                .context("missing ETag for multipart last part")?
                .to_str()?
                .trim_matches('"')
                .to_string();
            etags.push(etag);
        }

        // complete multipart upload
        let complete = CompleteMultipartUpload::new(
            &self.state.bucket,
            Some(&self.state.creds),
            &s3_key,
            upload_id,
            etags.iter().map(|s| s.as_str()),
        );
        let url = complete.sign(Self::DURATION);
        let body = complete.body();
        let resp = self.state.client.post(url).body(body).send().await?;
        Self::error_for_status(resp).await?;
        // fetch metadata after completion
        let meta = self
            .head_object(&put.key)
            .await?
            .context("failed to fetch object metadata after multipart upload")?;
        Ok(meta)
    }

    pub async fn delete_object(&self, key: &str) -> Result<(), anyhow::Error> {
        let url = self
            .state
            .bucket
            .delete_object(Some(&self.state.creds), &self.build_key(key))
            .sign(Self::DURATION);

        let res = self.state.client.delete(url).send().await?;
        Self::error_for_status(res).await?;

        Ok(())
    }

    pub async fn list_objects(
        &self,
        args: ListArgs,
    ) -> Result<ListObjectsV2Response, anyhow::Error> {
        let mut prep = self.state.bucket.list_objects_v2(Some(&self.state.creds));

        let prefix = if let Some(prefix) = args.prefix() {
            Some(self.build_key(prefix).into_owned())
        } else {
            self.state.path_prefix.clone()
        };
        if let Some(delimiter) = args.delimiter() {
            prep.with_delimiter(delimiter);
        }
        if let Some(prefix) = &prefix
            && !prefix.is_empty()
        {
            prep.with_prefix(prefix);
        }
        if let Some(cursor) = args.cursor() {
            prep.with_continuation_token(cursor);
        }
        if let Some(limit) = args.limit() {
            let limit: usize = limit.try_into().context("limit is too large")?;
            prep.with_max_keys(limit);
        }

        let url = prep.sign(Self::DURATION);
        tracing::trace!(?prefix, %url, "listing objects in s3");
        let res = self.state.client.get(url).send().await?;
        let res = Self::error_for_status(res).await?;

        let body = res.text().await?;
        let mut data = rusty_s3::actions::ListObjectsV2::parse_response(&body)?;

        for content in &mut data.contents {
            // Need to urldecode the key.
            if let Ok(key) = percent_encoding::percent_decode_str(&content.key).decode_utf8() {
                // TODO: propagate error?
                content.key = key.into_owned();
            }

            let key = std::mem::take(&mut content.key);
            content.key = self.prune_key_prefix(key);
        }

        Ok(data)
    }

    fn list_to_metas(&self, list: ListObjectsV2Response) -> Result<Vec<ObjectMeta>, anyhow::Error> {
        list.contents
            .into_iter()
            .map(|o| -> Result<ObjectMeta, anyhow::Error> {
                let key = self.prune_key_prefix(o.key);
                let mut meta = ObjectMeta::new(key);
                let updated_at = OffsetDateTime::parse(
                    &o.last_modified,
                    &time::format_description::well_known::Iso8601::DEFAULT,
                )?;

                meta.etag = Some(o.etag.trim_matches('"').trim().to_string());
                meta.size = Some(o.size);
                // FIXME: created at
                meta.created_at = None;
                meta.updated_at = Some(updated_at);

                // Extract MD5 hash from ETag when it's a simple hex string
                if let Some(etag_val) = &meta.etag {
                    let tag = etag_val.trim_matches('"');
                    if tag.len() == 32 && tag.chars().all(|c| c.is_ascii_hexdigit()) {
                        let mut arr = [0u8; 16];
                        for i in 0..16 {
                            arr[i] =
                                u8::from_str_radix(&tag[i * 2..i * 2 + 2], 16).unwrap_or_default();
                        }
                        meta.hash_md5 = Some(arr);
                    }
                }

                Ok(meta)
            })
            .collect::<Result<Vec<_>, _>>()
    }

    pub async fn delete_all(&self, prefix: &str) -> Result<(), anyhow::Error> {
        // Since S3 does not have a "delete prefix" operation, we need to
        // emulate it by first listing all the keys, and then deleting them.

        let mut cursor = None;
        loop {
            let mut args = ListArgs::new().with_cursor_opt(cursor);
            if !prefix.is_empty() {
                args = args.with_prefix(prefix);
            }

            let list = self.list_objects(args.clone()).await?;
            let next_cursor = list.next_continuation_token;

            // TODO: multi-delete fails with "unsupported search parameters" error
            // Currently not working...
            // let objs: Vec<_> = list
            //     .contents
            //     .into_iter()
            //     .map(|o| ObjectIdentifier {
            //         key: o.key,
            //         version_id: None,
            //     })
            //     .collect();
            //
            // let del = self
            //     .state
            //     .bucket
            //     .delete_objects(Some(&self.state.creds), objs.iter());
            // let url = del.sign(Self::DURATION);
            // let (body, _md5) = del.body_with_md5();
            // let res = self.state.client.delete(url).body(body).send().await?;
            // Self::error_for_status(res).await?;

            for obj in list.contents {
                let key = obj.key;
                self.delete_object(&key).await?;
            }

            if let Some(next_cursor) = next_cursor {
                cursor = Some(next_cursor);
            } else {
                break;
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjStore for S3ObjStore {
    fn kind(&self) -> &'static str {
        Self::KIND
    }

    fn safe_uri(&self) -> &Url {
        &self.state.safe_uri
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        self.head_object("/__healthcheck/i-do-not-exist").await?;
        Ok(())
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        match self.head_object(key).await? {
            Some(h) => Ok(Some(h)),
            None => Ok(None),
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        match self.get_object(key).await? {
            Some((bytes, _)) => Ok(Some(bytes)),
            None => Ok(None),
        }
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        match self.get_object_response(key).await? {
            Some((_, res)) => {
                let stream = res.bytes_stream().map_err(anyhow::Error::from);
                Ok(Some(Box::pin(stream)))
            }
            None => Ok(None),
        }
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        match self.get_object(key).await? {
            Some((bytes, meta)) => Ok(Some((bytes, meta))),
            None => Ok(None),
        }
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        match self.get_object_response(key).await? {
            Some((meta, res)) => {
                let stream = res.bytes_stream().map_err(anyhow::Error::from);
                Ok(Some((meta, Box::pin(stream))))
            }
            None => Ok(None),
        }
    }

    async fn generate_download_url(
        &self,
        args: DownloadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        let url = Self::generate_download_url(self, args)?;
        Ok(Some(url))
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta, anyhow::Error> {
        self.put_object(put).await
    }

    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        let s3_key = self.build_key(&copy.target_key);
        let mut b = self
            .state
            .bucket
            .put_object(Some(&self.state.creds), &s3_key);

        let source_path = format!(
            "/{}/{}",
            self.state.bucket.name(),
            copy.source_key.trim_start_matches('/')
        );
        b.headers_mut().insert("x-amz-copy-source", source_path);
        apply_condition_headers(b.headers_mut(), copy.conditions)?;

        // FIXME: add conditions
        let url = b.sign(Self::DURATION);

        let res = self.state.client.put(url).send().await?;
        let res = Self::error_for_status(res).await?;

        let meta = parse_object_headers(copy.target_key, res.headers())?;
        Ok(meta)
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        self.delete_object(key).await
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error> {
        let delim = args.delimiter().unwrap_or_default().to_string();
        let mut list = self.list_objects(args).await?;
        let cursor = list.next_continuation_token.take();

        let prefixes: Vec<String> = list
            .common_prefixes
            .drain(..)
            .map(|p| {
                let val = percent_encoding::percent_decode_str(&p.prefix)
                    .decode_utf8()
                    .with_context(|| format!("failed to decode prefix: '{}'", p.prefix))?;

                Ok(val.trim_end_matches(&delim).to_owned())
            })
            .collect::<Result<Vec<String>, anyhow::Error>>()?;
        let prefixes = if prefixes.is_empty() {
            None
        } else {
            Some(prefixes)
        };

        let items = self.list_to_metas(list)?;
        Ok(ObjectMetaPage {
            items,
            next_cursor: cursor,
            prefixes,
        })
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        let list = self.list_objects(args).await?;
        tracing::trace!(?list, "listing keys");
        let items = list.contents.into_iter().map(|o| o.key).collect();
        tracing::trace!(?items, "listed keys");
        Ok(KeyPage {
            items,
            next_cursor: list.next_continuation_token,
        })
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error> {
        self.delete_all(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use anyhow::bail;
    use http::HeaderMap;

    use crate::S3ObjStoreConfig;

    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use sha2::{Digest, Sha256};

    fn test_strict() -> bool {
        std::env::var("TEST_STRICT").is_ok()
    }

    fn load_test_config() -> Result<Option<S3ObjStoreConfig>, anyhow::Error> {
        const ENV_VAR: &str = "S3_TEST_URI";
        let Ok(var) = std::env::var(ENV_VAR) else {
            if test_strict() {
                bail!("missing required environment variable: {ENV_VAR}");
            } else {
                eprintln!(
                    "skipping s3 tests due to missing config - set TEST_STRICT=1 env var to require the test"
                );
                return Ok(None);
            }
        };

        let config = S3ObjStoreConfig::from_uri(&var)?;
        Ok(Some(config))
    }

    #[test]
    fn test_parse_objectmeta_headers() {
        let mut map = HeaderMap::new();
        map.insert(
            "Last-Modified",
            "Tue, 15 Nov 1994 12:45:26 GMT".parse().unwrap(),
        );
        map.insert("Content-Length", "1234".parse().unwrap());

        let meta = parse_object_headers("key".to_string(), &map).unwrap();
        assert_eq!(meta.size, Some(1234));
        assert_eq!(
            meta.updated_at
                .unwrap()
                .format(&time::format_description::well_known::Rfc2822)
                .unwrap(),
            "Tue, 15 Nov 1994 12:45:26 +0000",
        );

        // Test MD5 and SHA256 hash extraction
        // MD5 of empty body is d41d8cd98f00b204e9800998ecf8427e (base64: 1B2M2Y8AsgTpgAmY7PhCfg==)
        map.insert("Content-MD5", "1B2M2Y8AsgTpgAmY7PhCfg==".parse().unwrap());
        // SHA256 of empty body
        let sha_raw = format!("{:x}", Sha256::digest(b""));
        map.insert("x-amz-meta-sha256", sha_raw.parse().unwrap());
        let meta = parse_object_headers("key".to_string(), &map).unwrap();
        // verify MD5 bytes
        let md5_bytes = STANDARD.decode("1B2M2Y8AsgTpgAmY7PhCfg==").unwrap();
        let mut md5_arr = [0u8; 16];
        md5_arr.copy_from_slice(&md5_bytes);
        assert_eq!(meta.hash_md5, Some(md5_arr));
        // verify SHA256 bytes
        let sha_expected = Sha256::digest(b"");
        assert_eq!(meta.hash_sha256, Some(sha_expected.into()));
    }

    #[tokio::test]
    async fn test_s3_light() {
        let config = if let Some(config) = load_test_config().unwrap() {
            config
        } else {
            return;
        };

        let store = S3ObjStore::new(config.clone()).expect("failed to create s3 kv store");

        // Test with prefix.
        objstore_test::test_objstore(&store).await;

        // Test with without.
        let config = S3ObjStoreConfig {
            path_prefix: None,
            ..config
        };
        let store = S3ObjStore::new(config).expect("failed to create s3 kv store");
        objstore_test::test_objstore(&store).await;
    }
}
