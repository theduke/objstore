use std::{borrow::Cow, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::TryStreamExt as _;
use http::StatusCode;
use reqwest::{Client, RequestBuilder, Url};
use rusty_s3::{Bucket, Map, S3Action, actions::ListObjectsV2Response};

use bytes::{BufMut, BytesMut};
use futures::StreamExt;
use http::header::CONTENT_LENGTH;
use http::header::{CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_TYPE, ETAG};
use rusty_s3::actions::{
    AbortMultipartUpload, CompleteMultipartUpload, CreateMultipartUpload, UploadPart,
};
use time::OffsetDateTime;

use objstore::{
    Conditions, Copy, DataSource, DownloadUrlArgs, KeyPage, ListArgs, ObjStore, ObjStoreError,
    ObjectMeta, ObjectMetaPage, Operation, Put, Resource, Result as ObjStoreResult, UploadUrlArgs,
    ValueStream,
};

use crate::{
    S3ObjStoreConfig,
    util::{
        apply_condition_headers, apply_copy_source_condition_headers, insert_signed_header,
        parse_copy_object_result, parse_object_headers, parse_s3_error_response,
    },
};

/// A lightweight S3-compatible object store.
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
    fetch_metadata_after_put: bool,
    client: Client,
}

struct MultipartUploadState {
    key: String,
    s3_key: String,
    upload_id: String,
    conditions: Conditions,
    mime_type: Option<String>,
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

    fn dispatch_error(operation: Operation, source: reqwest::Error) -> ObjStoreError {
        if source.is_timeout() {
            ObjStoreError::Timeout {
                operation,
                source: Some(source.into()),
            }
        } else {
            ObjStoreError::Dispatch {
                operation,
                source: source.into(),
            }
        }
    }

    fn response_error(
        operation: Operation,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> ObjStoreError {
        ObjStoreError::Response {
            operation,
            source: source.into(),
        }
    }

    fn invalid_request(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> ObjStoreError {
        ObjStoreError::InvalidRequest {
            message: message.into(),
            source: Some(source.into()),
        }
    }

    pub fn new(config: S3ObjStoreConfig) -> ObjStoreResult<Self> {
        let client = Self::default_client();
        Self::new_with_client(config, client)
    }

    pub fn new_with_client(config: S3ObjStoreConfig, client: Client) -> ObjStoreResult<Self> {
        let path_prefix = if let Some(prefix) = &config.path_prefix {
            let prefix = prefix.trim_matches('/');
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
            config
                .url
                .host_str()
                .ok_or_else(|| ObjStoreError::InvalidConfig {
                    message: "missing host in URL".to_string(),
                    source: None,
                })?,
            config.bucket
        )
        .parse::<Url>()
        .map_err(|source| ObjStoreError::InvalidConfig {
            message: "failed to build safe-url".to_string(),
            source: Some(source.into()),
        })?;

        Ok(Self {
            state: Arc::new(State {
                safe_uri,
                creds: config.build_credentials(),
                bucket: config.build_bucket()?,
                path_prefix,
                fetch_metadata_after_put: config.fetch_metadata_after_put,
                client,
            }),
        })
    }

    /// Create the configured bucket using a signed S3 PUT request.
    pub async fn bucket_create(&self) -> ObjStoreResult<()> {
        let action = self.state.bucket.create_bucket(&self.state.creds);
        let url = action.sign(Self::DURATION);

        let res = self
            .state
            .client
            .put(url)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Put, source))?;
        Self::error_for_status(
            res,
            Operation::Put,
            Some(Resource::Bucket {
                bucket: self.state.bucket.name().to_string(),
            }),
        )
        .await?;

        Ok(())
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

    fn with_signed_headers(mut req: RequestBuilder, headers: &Map<'_>) -> RequestBuilder {
        for (key, value) in headers.iter() {
            req = req.header(key, value);
        }
        req
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

    fn normalize_list_response(&self, data: &mut ListObjectsV2Response) {
        for content in &mut data.contents {
            if let Ok(key) = percent_encoding::percent_decode_str(&content.key).decode_utf8() {
                content.key = key.into_owned();
            }

            let key = std::mem::take(&mut content.key);
            content.key = self.prune_key_prefix(key);
        }
        for prefix in &mut data.common_prefixes {
            if let Ok(value) = percent_encoding::percent_decode_str(&prefix.prefix).decode_utf8() {
                prefix.prefix = value.into_owned();
            }

            let value = std::mem::take(&mut prefix.prefix);
            prefix.prefix = self.prune_key_prefix(value);
        }
    }

    fn classify_s3_error(
        status: StatusCode,
        headers: &http::HeaderMap,
        body: &[u8],
        operation: Operation,
        resource: Option<Resource>,
    ) -> ObjStoreError {
        let parsed = parse_s3_error_response(body);
        let code = parsed.as_ref().and_then(|err| err.code.clone());
        let message = parsed
            .as_ref()
            .and_then(|err| err.message.clone())
            .or_else(|| {
                let text = String::from_utf8_lossy(body).trim().to_string();
                (!text.is_empty()).then_some(text)
            });
        let request_id = parsed
            .as_ref()
            .and_then(|err| err.request_id.clone())
            .or_else(|| {
                headers
                    .get("x-amz-request-id")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
            });
        let extended_request_id = parsed
            .as_ref()
            .and_then(|err| err.extended_request_id.clone())
            .or_else(|| {
                headers
                    .get("x-amz-id-2")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string)
            });

        match status {
            StatusCode::UNAUTHORIZED => ObjStoreError::Unauthenticated { source: None },
            StatusCode::FORBIDDEN => ObjStoreError::PermissionDenied {
                operation,
                resource,
                source: None,
            },
            StatusCode::NOT_FOUND => match resource.clone() {
                Some(Resource::Bucket { bucket }) => ObjStoreError::BucketNotFound {
                    bucket,
                    source: None,
                },
                Some(Resource::Object { key }) => {
                    ObjStoreError::ObjectNotFound { key, source: None }
                }
                _ => ObjStoreError::Backend {
                    backend: Self::KIND,
                    operation,
                    resource,
                    code,
                    status: Some(status.as_u16()),
                    message,
                    request_id,
                    extended_request_id,
                    source: None,
                },
            },
            StatusCode::PRECONDITION_FAILED => ObjStoreError::PreconditionFailed {
                operation,
                resource,
                source: None,
            },
            StatusCode::CONFLICT => {
                if matches!(
                    code.as_deref(),
                    Some("BucketAlreadyExists" | "BucketAlreadyOwnedByYou")
                ) {
                    if let Some(Resource::Bucket { bucket }) = resource.clone() {
                        return ObjStoreError::AlreadyExists {
                            resource: Resource::Bucket { bucket },
                            source: None,
                        };
                    }
                }
                ObjStoreError::Backend {
                    backend: Self::KIND,
                    operation,
                    resource,
                    code,
                    status: Some(status.as_u16()),
                    message,
                    request_id,
                    extended_request_id,
                    source: None,
                }
            }
            StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => ObjStoreError::Timeout {
                operation,
                source: None,
            },
            _ => ObjStoreError::Backend {
                backend: Self::KIND,
                operation,
                resource,
                code,
                status: Some(status.as_u16()),
                message,
                request_id,
                extended_request_id,
                source: None,
            },
        }
    }

    async fn error_for_status(
        res: reqwest::Response,
        operation: Operation,
        resource: Option<Resource>,
    ) -> ObjStoreResult<reqwest::Response> {
        if res.status().is_success() {
            Ok(res)
        } else {
            let status = res.status();
            let headers = res.headers().clone();
            let body = res
                .bytes()
                .await
                .map_err(|source| Self::response_error(operation, source))?;
            Err(Self::classify_s3_error(
                status, &headers, &body, operation, resource,
            ))
        }
    }

    fn error_from_success_body(
        body: &[u8],
        operation: Operation,
        resource: Option<Resource>,
    ) -> ObjStoreResult<()> {
        let Some(err) = parse_s3_error_response(body) else {
            return Ok(());
        };

        Err(ObjStoreError::Backend {
            backend: Self::KIND,
            operation,
            resource,
            code: err.code.clone(),
            status: None,
            message: err.message.clone(),
            request_id: err.request_id,
            extended_request_id: err.extended_request_id,
            source: None,
        })
    }

    async fn ensure_bucket_exists(&self) -> ObjStoreResult<()> {
        let action = self.state.bucket.head_bucket(Some(&self.state.creds));
        let url = action.sign(Self::DURATION);

        let res = self
            .state
            .client
            .head(url)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Healthcheck, source))?;
        if res.status() == StatusCode::NOT_FOUND {
            return Err(ObjStoreError::bucket_not_found(self.state.bucket.name()));
        }
        Self::error_for_status(
            res,
            Operation::Healthcheck,
            Some(Resource::Bucket {
                bucket: self.state.bucket.name().to_string(),
            }),
        )
        .await?;
        Ok(())
    }

    fn etag_from_headers(headers: &http::HeaderMap) -> ObjStoreResult<Option<String>> {
        headers
            .get(ETAG)
            .map(|v| {
                Ok(v.to_str()
                    .map_err(|source| ObjStoreError::InvalidMetadata {
                        key: "<response>".to_string(),
                        message: "invalid etag header".to_string(),
                        source: Some(source.into()),
                    })?
                    .trim_matches('"')
                    .trim()
                    .to_string())
            })
            .transpose()
    }

    async fn metadata_after_write(
        &self,
        key: &str,
        fallback: ObjectMeta,
        context: &'static str,
    ) -> ObjStoreResult<ObjectMeta> {
        if self.state.fetch_metadata_after_put {
            self.head_object(key)
                .await?
                .ok_or_else(|| ObjStoreError::Backend {
                    backend: Self::KIND,
                    operation: Operation::Meta,
                    resource: Some(Resource::Object {
                        key: key.to_string(),
                    }),
                    code: None,
                    status: None,
                    message: Some(context.to_string()),
                    request_id: None,
                    extended_request_id: None,
                    source: None,
                })
        } else {
            Ok(fallback)
        }
    }

    pub async fn head_object(&self, key: &str) -> ObjStoreResult<Option<ObjectMeta>> {
        let s3_key = self.build_key(key);
        let url = self
            .state
            .bucket
            .head_object(Some(&self.state.creds), &s3_key)
            .sign(Self::DURATION);
        tracing::trace!(%s3_key, %url, "sending head_object request to s3");

        let res = self
            .state
            .client
            .head(url)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Meta, source))?;
        if res.status() == StatusCode::NOT_FOUND {
            self.ensure_bucket_exists().await?;
            return Ok(None);
        }
        let res = Self::error_for_status(
            res,
            Operation::Meta,
            Some(Resource::Object {
                key: key.to_string(),
            }),
        )
        .await?;
        let head = parse_object_headers(key.to_owned(), res.headers())?;

        Ok(Some(head))
    }

    pub async fn get_object_response(
        &self,
        key: &str,
    ) -> ObjStoreResult<Option<(ObjectMeta, reqwest::Response)>> {
        let s3_key = self.build_key(key);
        tracing::trace!(%s3_key, "loading key from s3");
        let url = self
            .state
            .bucket
            .get_object(Some(&self.state.creds), &s3_key)
            .sign(std::time::Duration::from_secs(60 * 60));

        let res = self
            .state
            .client
            .get(url)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Get, source))?;
        tracing::trace!(?res, "response for get_object request");
        if res.status() == StatusCode::NOT_FOUND {
            self.ensure_bucket_exists().await?;
            return Ok(None);
        }
        let res = Self::error_for_status(
            res,
            Operation::Get,
            Some(Resource::Object {
                key: key.to_string(),
            }),
        )
        .await?;

        let head = parse_object_headers(key.to_owned(), res.headers())?;

        Ok(Some((head, res)))
    }

    pub async fn get_object(&self, key: &str) -> ObjStoreResult<Option<(Bytes, ObjectMeta)>> {
        match self.get_object_response(key).await? {
            Some((head, res)) => {
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|source| Self::response_error(Operation::Get, source))?;
                Ok(Some((bytes, head)))
            }
            None => Ok(None),
        }
    }

    fn generate_download_url(&self, args: DownloadUrlArgs) -> ObjStoreResult<Url> {
        let s3_key = self.build_key(&args.key);

        let url = self
            .state
            .bucket
            .get_object(Some(&self.state.creds), &s3_key)
            .sign(args.valid_for);

        Ok(url)
    }

    fn presign_upload_url(&self, args: UploadUrlArgs) -> ObjStoreResult<Url> {
        let s3_key = self.build_key(&args.key);
        let mut action = self
            .state
            .bucket
            .put_object(Some(&self.state.creds), &s3_key);

        if let Some(ct) = &args.content_type {
            insert_signed_header(action.headers_mut(), CONTENT_TYPE.as_str(), ct.clone());
        }
        if let Some(v) = &args.content_disposition {
            insert_signed_header(
                action.headers_mut(),
                CONTENT_DISPOSITION.as_str(),
                v.clone(),
            );
        }
        if let Some(v) = &args.content_encoding {
            insert_signed_header(action.headers_mut(), CONTENT_ENCODING.as_str(), v.clone());
        }
        if let Some(v) = &args.cache_control {
            insert_signed_header(action.headers_mut(), CACHE_CONTROL.as_str(), v.clone());
        }
        for (k, v) in &args.metadata {
            let name = format!("x-amz-meta-{}", k.to_lowercase().replace('_', "-"));
            insert_signed_header(action.headers_mut(), name, v.clone());
        }

        let url = action.sign(args.valid_for);
        Ok(url)
    }

    pub async fn put_object(&self, mut put: Put) -> ObjStoreResult<ObjectMeta> {
        let mut data = DataSource::Data(Bytes::new());
        std::mem::swap(&mut data, &mut put.data);

        let data = match data {
            DataSource::Data(bytes) => bytes,
            DataSource::Stream(sized) => {
                // Use a single PUT with Content-Length when the stream length is known
                // and fits in one part, otherwise fall back to multipart upload.
                if let Some(size) = sized.size()
                    && size <= Self::PART_SIZE as u64
                {
                    return self.single_put_stream(put, sized.into_stream(), size).await;
                }
                return self.put_stream(put, sized.into_stream()).await;
            }
        };

        self.put_bytes(put, data).await
    }

    async fn put_bytes(&self, put: Put, data: Bytes) -> ObjStoreResult<ObjectMeta> {
        let s3_key = self.build_key(&put.key);
        let mut action = self
            .state
            .bucket
            .put_object(Some(&self.state.creds), &s3_key);
        apply_condition_headers(action.headers_mut(), put.conditions).map_err(|source| {
            Self::invalid_request("failed to format put condition headers", source)
        })?;
        // forward MIME type header if set
        if let Some(ct) = &put.mime_type {
            insert_signed_header(action.headers_mut(), CONTENT_TYPE.as_str(), ct.as_str());
        }
        let headers = action.headers_mut().clone();
        let url = action.sign(Self::DURATION);

        let size = data.len() as u64;
        let body = data;

        let res = Self::with_signed_headers(self.state.client.put(url), &headers)
            .body(body)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Put, source))?;
        let res = Self::error_for_status(
            res,
            Operation::Put,
            Some(Resource::Object {
                key: put.key.clone(),
            }),
        )
        .await?;

        let mut fallback = ObjectMeta::new(put.key.clone());
        fallback.size = Some(size);
        fallback.mime_type = put.mime_type;
        fallback.etag = Self::etag_from_headers(res.headers())?;

        self.metadata_after_write(
            &put.key,
            fallback,
            "failed to fetch object metadata after put",
        )
        .await
    }

    async fn single_put_stream(
        &self,
        put: Put,
        stream: ValueStream,
        size: u64,
    ) -> ObjStoreResult<ObjectMeta> {
        let s3_key = self.build_key(&put.key);
        let mut action = self
            .state
            .bucket
            .put_object(Some(&self.state.creds), &s3_key);
        apply_condition_headers(action.headers_mut(), put.conditions).map_err(|source| {
            Self::invalid_request("failed to format put condition headers", source)
        })?;
        if let Some(ct) = &put.mime_type {
            action.headers_mut().insert(CONTENT_TYPE.to_string(), ct);
        }
        action
            .headers_mut()
            .insert(CONTENT_LENGTH.to_string(), size.to_string());
        let headers = action.headers_mut().clone();
        let url = action.sign(Self::DURATION);

        let body = reqwest::Body::wrap_stream(stream.map(|r| r.map_err(std::io::Error::other)));

        let res = Self::with_signed_headers(self.state.client.put(url), &headers)
            .body(body)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Put, source))?;
        let res = Self::error_for_status(
            res,
            Operation::Put,
            Some(Resource::Object {
                key: put.key.clone(),
            }),
        )
        .await?;

        let mut fallback = ObjectMeta::new(put.key.clone());
        fallback.size = Some(size);
        fallback.mime_type = put.mime_type;
        fallback.etag = Self::etag_from_headers(res.headers())?;

        self.metadata_after_write(
            &put.key,
            fallback,
            "failed to fetch object metadata after put",
        )
        .await
    }

    async fn put_stream(
        &self,
        mut put: Put,
        mut stream: ValueStream,
    ) -> ObjStoreResult<ObjectMeta> {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.is_empty() {
                continue;
            }
            return self.multipart_upload(put, stream, chunk).await;
        }

        put.data = DataSource::Data(Bytes::new());
        self.put_bytes(put, Bytes::new()).await
    }

    async fn multipart_upload(
        &self,
        put: Put,
        stream: ValueStream,
        first_chunk: Bytes,
    ) -> ObjStoreResult<ObjectMeta> {
        // initiate multipart upload
        let s3_key = self.build_key(&put.key).into_owned();
        let mut create = self
            .state
            .bucket
            .create_multipart_upload(Some(&self.state.creds), &s3_key);
        // forward MIME type header if set
        if let Some(ct) = &put.mime_type {
            insert_signed_header(create.headers_mut(), CONTENT_TYPE.as_str(), ct.as_str());
        }
        let headers = create.headers_mut().clone();
        let url = create.sign(Self::DURATION);
        let resp = Self::with_signed_headers(self.state.client.post(url), &headers)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Put, source))?;
        let resp = Self::error_for_status(
            resp,
            Operation::Put,
            Some(Resource::Object {
                key: put.key.clone(),
            }),
        )
        .await?;
        let body = resp
            .text()
            .await
            .map_err(|source| Self::response_error(Operation::Put, source))?;
        let multipart = CreateMultipartUpload::parse_response(&body)
            .map_err(|source| Self::response_error(Operation::Put, source))?;
        let upload_id = multipart.upload_id();

        let upload = MultipartUploadState {
            key: put.key,
            s3_key: s3_key.clone(),
            upload_id: upload_id.to_string(),
            conditions: put.conditions,
            mime_type: put.mime_type,
        };

        let upload_result = self
            .multipart_upload_after_create(upload, stream, first_chunk)
            .await;

        if upload_result.is_err() {
            let abort = AbortMultipartUpload::new(
                &self.state.bucket,
                Some(&self.state.creds),
                &s3_key,
                upload_id,
            );
            let url = abort.sign(Self::DURATION);
            let _ = self.state.client.delete(url).send().await;
        }

        upload_result
    }

    async fn multipart_upload_after_create(
        &self,
        upload: MultipartUploadState,
        mut stream: ValueStream,
        first_chunk: Bytes,
    ) -> ObjStoreResult<ObjectMeta> {
        let MultipartUploadState {
            key,
            s3_key,
            upload_id,
            conditions,
            mime_type,
        } = upload;

        // upload parts
        let mut part_number = 1u16;
        let mut etags = Vec::new();
        let mut total_size = 0u64;
        let mut buffer = BytesMut::new();
        buffer.put_slice(&first_chunk);

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.put_slice(&chunk);
            if buffer.len() >= Self::PART_SIZE {
                let upload = UploadPart::new(
                    &self.state.bucket,
                    Some(&self.state.creds),
                    &s3_key,
                    part_number,
                    &upload_id,
                );
                let url = upload.sign(Self::DURATION);
                let data = buffer.split().freeze();
                total_size += data.len() as u64;
                let res = self
                    .state
                    .client
                    .put(url)
                    .body(data)
                    .send()
                    .await
                    .map_err(|source| Self::dispatch_error(Operation::Put, source))?;
                let res = Self::error_for_status(
                    res,
                    Operation::Put,
                    Some(Resource::Object { key: key.clone() }),
                )
                .await?;
                let etag = res
                    .headers()
                    .get(ETAG)
                    .ok_or_else(|| ObjStoreError::InvalidMetadata {
                        key: key.clone(),
                        message: "missing ETag for multipart part".to_string(),
                        source: None,
                    })?
                    .to_str()
                    .map_err(|source| ObjStoreError::InvalidMetadata {
                        key: key.clone(),
                        message: "invalid ETag for multipart part".to_string(),
                        source: Some(source.into()),
                    })?
                    .trim_matches('"')
                    .to_string();
                etags.push(etag);
                part_number += 1;
            }
        }
        // final part
        if !buffer.is_empty() {
            let upload = UploadPart::new(
                &self.state.bucket,
                Some(&self.state.creds),
                &s3_key,
                part_number,
                &upload_id,
            );
            let url = upload.sign(Self::DURATION);
            let data = buffer.freeze();
            total_size += data.len() as u64;
            let res = self
                .state
                .client
                .put(url)
                .body(data)
                .send()
                .await
                .map_err(|source| Self::dispatch_error(Operation::Put, source))?;
            let res = Self::error_for_status(
                res,
                Operation::Put,
                Some(Resource::Object { key: key.clone() }),
            )
            .await?;
            let etag = res
                .headers()
                .get(ETAG)
                .ok_or_else(|| ObjStoreError::InvalidMetadata {
                    key: key.clone(),
                    message: "missing ETag for multipart last part".to_string(),
                    source: None,
                })?
                .to_str()
                .map_err(|source| ObjStoreError::InvalidMetadata {
                    key: key.clone(),
                    message: "invalid ETag for multipart last part".to_string(),
                    source: Some(source.into()),
                })?
                .trim_matches('"')
                .to_string();
            etags.push(etag);
        }

        // complete multipart upload
        let mut complete = CompleteMultipartUpload::new(
            &self.state.bucket,
            Some(&self.state.creds),
            &s3_key,
            &upload_id,
            etags.iter().map(|s| s.as_str()),
        );
        apply_condition_headers(complete.headers_mut(), conditions).map_err(|source| {
            Self::invalid_request(
                "failed to format multipart complete condition headers",
                source,
            )
        })?;
        let headers = complete.headers_mut().clone();
        let url = complete.sign(Self::DURATION);
        let body = complete.body();
        let resp = Self::with_signed_headers(self.state.client.post(url), &headers)
            .body(body)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Put, source))?;
        let resp = Self::error_for_status(
            resp,
            Operation::Put,
            Some(Resource::Object { key: key.clone() }),
        )
        .await?;
        let body = resp
            .bytes()
            .await
            .map_err(|source| Self::response_error(Operation::Put, source))?;
        Self::error_from_success_body(
            &body,
            Operation::Put,
            Some(Resource::Object { key: key.clone() }),
        )?;

        let mut fallback = ObjectMeta::new(key.clone());
        fallback.size = Some(total_size);
        fallback.mime_type = mime_type;

        self.metadata_after_write(
            &key,
            fallback,
            "failed to fetch object metadata after multipart upload",
        )
        .await
    }

    pub async fn delete_object(&self, key: &str) -> ObjStoreResult<()> {
        let url = self
            .state
            .bucket
            .delete_object(Some(&self.state.creds), &self.build_key(key))
            .sign(Self::DURATION);

        let res = self
            .state
            .client
            .delete(url)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Delete, source))?;
        Self::error_for_status(
            res,
            Operation::Delete,
            Some(Resource::Object {
                key: key.to_string(),
            }),
        )
        .await?;

        Ok(())
    }

    pub async fn list_objects(&self, args: ListArgs) -> ObjStoreResult<ListObjectsV2Response> {
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
            let limit: usize = limit
                .try_into()
                .map_err(|source| Self::invalid_request("list limit is too large", source))?;
            prep.with_max_keys(limit);
        }

        let url = prep.sign(Self::DURATION);
        tracing::trace!(?prefix, %url, "listing objects in s3");
        let res = self
            .state
            .client
            .get(url)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::List, source))?;
        let res = Self::error_for_status(
            res,
            Operation::List,
            prefix.map(|prefix| Resource::Prefix { prefix }),
        )
        .await?;

        let body = res
            .text()
            .await
            .map_err(|source| Self::response_error(Operation::List, source))?;
        let mut data = rusty_s3::actions::ListObjectsV2::parse_response(&body)
            .map_err(|source| Self::response_error(Operation::List, source))?;
        self.normalize_list_response(&mut data);

        Ok(data)
    }

    fn list_to_metas(&self, list: ListObjectsV2Response) -> ObjStoreResult<Vec<ObjectMeta>> {
        list.contents
            .into_iter()
            .map(|o| -> ObjStoreResult<ObjectMeta> {
                let key = self.prune_key_prefix(o.key);
                let mut meta = ObjectMeta::new(key.clone());
                let updated_at = OffsetDateTime::parse(
                    &o.last_modified,
                    &time::format_description::well_known::Iso8601::DEFAULT,
                )
                .map_err(|source| ObjStoreError::InvalidMetadata {
                    key: key.clone(),
                    message: "failed to parse S3 list LastModified value".to_string(),
                    source: Some(source.into()),
                })?;

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
            .collect::<ObjStoreResult<Vec<_>>>()
    }

    pub async fn delete_all(&self, prefix: &str) -> ObjStoreResult<()> {
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

    async fn healthcheck(&self) -> ObjStoreResult<()> {
        self.ensure_bucket_exists().await?;
        Ok(())
    }

    async fn meta(&self, key: &str) -> ObjStoreResult<Option<ObjectMeta>> {
        match self.head_object(key).await? {
            Some(h) => Ok(Some(h)),
            None => Ok(None),
        }
    }

    async fn get(&self, key: &str) -> ObjStoreResult<Option<Bytes>> {
        match self.get_object(key).await? {
            Some((bytes, _)) => Ok(Some(bytes)),
            None => Ok(None),
        }
    }

    async fn get_stream(&self, key: &str) -> ObjStoreResult<Option<ValueStream>> {
        match self.get_object_response(key).await? {
            Some((_, res)) => {
                let stream = res
                    .bytes_stream()
                    .map_err(|source| Self::response_error(Operation::GetStream, source));
                Ok(Some(Box::pin(stream)))
            }
            None => Ok(None),
        }
    }

    async fn get_with_meta(&self, key: &str) -> ObjStoreResult<Option<(Bytes, ObjectMeta)>> {
        match self.get_object(key).await? {
            Some((bytes, meta)) => Ok(Some((bytes, meta))),
            None => Ok(None),
        }
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> ObjStoreResult<Option<(ObjectMeta, ValueStream)>> {
        match self.get_object_response(key).await? {
            Some((meta, res)) => {
                let stream = res
                    .bytes_stream()
                    .map_err(|source| Self::response_error(Operation::GetStream, source));
                Ok(Some((meta, Box::pin(stream))))
            }
            None => Ok(None),
        }
    }

    async fn generate_download_url(
        &self,
        args: DownloadUrlArgs,
    ) -> ObjStoreResult<Option<url::Url>> {
        let url = Self::generate_download_url(self, args)?;
        Ok(Some(url))
    }

    async fn generate_upload_url(&self, args: UploadUrlArgs) -> ObjStoreResult<Option<url::Url>> {
        let url = Self::presign_upload_url(self, args)?;
        Ok(Some(url))
    }

    async fn send_put(&self, put: Put) -> ObjStoreResult<ObjectMeta> {
        Ok(self.put_object(put).await?)
    }

    async fn send_copy(&self, copy: Copy) -> ObjStoreResult<ObjectMeta> {
        let source_key = copy.source_key;
        let target_key = copy.target_key;
        let s3_key = self.build_key(&target_key);
        let mut b = self
            .state
            .bucket
            .put_object(Some(&self.state.creds), &s3_key);

        let s3_source_key = self.build_key(&source_key).into_owned();
        // Percent-encode each path segment but preserve '/' separators so
        // internal slashes in object keys are not encoded.
        let encoded_key = s3_source_key
            .split('/')
            .map(|seg| {
                percent_encoding::utf8_percent_encode(seg, percent_encoding::NON_ALPHANUMERIC)
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("/");
        let source_path = format!(
            "/{}/{}",
            self.state.bucket.name(),
            encoded_key.trim_start_matches('/')
        );
        insert_signed_header(b.headers_mut(), "x-amz-copy-source", source_path);
        apply_copy_source_condition_headers(b.headers_mut(), copy.conditions).map_err(
            |source| {
                Self::invalid_request("failed to format copy source condition headers", source)
            },
        )?;

        let headers = b.headers_mut().clone();
        let url = b.sign(Self::DURATION);

        let res = Self::with_signed_headers(self.state.client.put(url), &headers)
            .send()
            .await
            .map_err(|source| Self::dispatch_error(Operation::Copy, source))?;
        let res = Self::error_for_status(
            res,
            Operation::Copy,
            Some(Resource::Object {
                key: source_key.clone(),
            }),
        )
        .await?;
        let body = res
            .bytes()
            .await
            .map_err(|source| Self::response_error(Operation::Copy, source))?;
        Self::error_from_success_body(
            &body,
            Operation::Copy,
            Some(Resource::Object {
                key: source_key.clone(),
            }),
        )?;

        let fallback = parse_copy_object_result(target_key.clone(), &body)?
            .unwrap_or_else(|| ObjectMeta::new(target_key.clone()));

        Ok(self
            .metadata_after_write(
                &target_key,
                fallback,
                "failed to fetch object metadata after copy",
            )
            .await?)
    }

    async fn delete(&self, key: &str) -> ObjStoreResult<()> {
        self.delete_object(key).await?;
        Ok(())
    }

    async fn list(&self, args: ListArgs) -> ObjStoreResult<ObjectMetaPage> {
        let delim = args.delimiter().unwrap_or_default().to_string();
        let mut list = self.list_objects(args).await?;
        let cursor = list.next_continuation_token.take();

        let prefixes: Vec<String> = list
            .common_prefixes
            .drain(..)
            .map(|p| Ok(p.prefix.trim_end_matches(&delim).to_owned()))
            .collect::<ObjStoreResult<Vec<String>>>()?;
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

    async fn list_keys(&self, args: ListArgs) -> ObjStoreResult<KeyPage> {
        let list = self.list_objects(args).await?;
        tracing::trace!(?list, "listing keys");
        let items = list.contents.into_iter().map(|o| o.key).collect();
        tracing::trace!(?items, "listed keys");
        Ok(KeyPage {
            items,
            next_cursor: list.next_continuation_token,
        })
    }

    async fn delete_prefix(&self, prefix: &str) -> ObjStoreResult<()> {
        self.delete_all(prefix).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use http::HeaderMap;
    use objstore::{Conditions, MatchValue, ObjStoreExt};
    use rusty_s3::{Credentials, UrlStyle as RustyUrlStyle};

    use crate::{S3ObjStoreConfig, util::error_from_success_response_body};

    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use sha2::{Digest, Sha256};

    fn read_create_bucket() -> bool {
        std::env::var("TEST_CREATE_BUCKET").ok().unwrap_or_default() == "1"
    }

    fn test_strict() -> bool {
        std::env::var("TEST_STRICT").is_ok()
    }

    fn load_test_config() -> ObjStoreResult<Option<S3ObjStoreConfig>> {
        const ENV_VAR: &str = "S3_TEST_URI";
        let Ok(var) = std::env::var(ENV_VAR) else {
            if test_strict() {
                return Err(ObjStoreError::InvalidConfig {
                    message: format!("missing required environment variable: {ENV_VAR}"),
                    source: None,
                });
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

    async fn ensure_test_bucket(store: &S3ObjStore) {
        if read_create_bucket() {
            let _ = store.bucket_create().await;
        }
    }

    #[test]
    fn classify_s3_precondition_failed() {
        let err = S3ObjStore::classify_s3_error(
            StatusCode::PRECONDITION_FAILED,
            &HeaderMap::new(),
            br#"<Error><Code>PreconditionFailed</Code><Message>condition failed</Message></Error>"#,
            Operation::Put,
            Some(Resource::Object {
                key: "existing-key".to_string(),
            }),
        );

        match err {
            ObjStoreError::PreconditionFailed {
                operation,
                resource,
                ..
            } => {
                assert_eq!(operation, Operation::Put);
                assert_eq!(
                    resource,
                    Some(Resource::Object {
                        key: "existing-key".to_string()
                    })
                );
            }
            other => panic!("expected PreconditionFailed, got {other:?}"),
        }
    }

    #[test]
    fn classify_s3_permission_denied() {
        let err = S3ObjStore::classify_s3_error(
            StatusCode::FORBIDDEN,
            &HeaderMap::new(),
            br#"<Error><Code>AccessDenied</Code><Message>denied</Message></Error>"#,
            Operation::List,
            Some(Resource::Bucket {
                bucket: "private-bucket".to_string(),
            }),
        );

        match err {
            ObjStoreError::PermissionDenied {
                operation,
                resource,
                ..
            } => {
                assert_eq!(operation, Operation::List);
                assert_eq!(
                    resource,
                    Some(Resource::Bucket {
                        bucket: "private-bucket".to_string()
                    })
                );
            }
            other => panic!("expected PermissionDenied, got {other:?}"),
        }
    }

    #[test]
    fn classify_s3_backend_preserves_status_code_and_request_id() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-request-id", "request-123".parse().unwrap());
        headers.insert("x-amz-id-2", "extended-456".parse().unwrap());

        let err = S3ObjStore::classify_s3_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &headers,
            br#"<Error><Code>InternalError</Code><Message>failed</Message></Error>"#,
            Operation::Copy,
            None,
        );

        match err {
            ObjStoreError::Backend {
                backend,
                operation,
                code,
                status,
                message,
                request_id,
                extended_request_id,
                ..
            } => {
                assert_eq!(backend, S3ObjStore::KIND);
                assert_eq!(operation, Operation::Copy);
                assert_eq!(code.as_deref(), Some("InternalError"));
                assert_eq!(status, Some(500));
                assert_eq!(message.as_deref(), Some("failed"));
                assert_eq!(request_id.as_deref(), Some("request-123"));
                assert_eq!(extended_request_id.as_deref(), Some("extended-456"));
            }
            other => panic!("expected Backend, got {other:?}"),
        }
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

    #[test]
    fn test_put_signed_headers_are_lowercase_and_replayed() {
        let bucket = Bucket::new(
            "https://s3.example.com".parse().unwrap(),
            RustyUrlStyle::Path,
            "bucket",
            "auto",
        )
        .unwrap();
        let creds = Credentials::new("key", "secret");
        let mut action = bucket.put_object(Some(&creds), "key");

        let mut conditions = Conditions::new();
        conditions.if_match = Some(MatchValue::Tags(vec!["etag".to_string()]));
        apply_condition_headers(action.headers_mut(), conditions).unwrap();
        insert_signed_header(action.headers_mut(), "Content-Type", "application/zip");

        let headers = action.headers_mut().clone();
        let signed_url = action.sign(S3ObjStore::DURATION);
        let signed_headers = signed_url
            .query_pairs()
            .find(|(name, _)| name == "X-Amz-SignedHeaders")
            .map(|(_, value)| value.into_owned())
            .unwrap();

        assert_eq!(signed_headers, "content-type;host;if-match");

        let request = S3ObjStore::with_signed_headers(Client::new().put(signed_url), &headers)
            .build()
            .unwrap();
        assert_eq!(
            request.headers().get("content-type").unwrap(),
            "application/zip"
        );
        assert_eq!(request.headers().get("if-match").unwrap(), "\"etag\"");
    }

    #[test]
    fn test_put_if_not_exists_signs_if_none_match() {
        let bucket = Bucket::new(
            "https://s3.example.com".parse().unwrap(),
            RustyUrlStyle::Path,
            "bucket",
            "auto",
        )
        .unwrap();
        let creds = Credentials::new("key", "secret");
        let mut action = bucket.put_object(Some(&creds), "key");

        apply_condition_headers(action.headers_mut(), Conditions::new().if_not_exists()).unwrap();

        let headers = action.headers_mut().clone();
        let signed_url = action.sign(S3ObjStore::DURATION);
        let signed_headers = signed_url
            .query_pairs()
            .find(|(name, _)| name == "X-Amz-SignedHeaders")
            .map(|(_, value)| value.into_owned())
            .unwrap();

        assert_eq!(signed_headers, "host;if-none-match");

        let request = S3ObjStore::with_signed_headers(Client::new().put(signed_url), &headers)
            .build()
            .unwrap();
        assert_eq!(request.headers().get("if-none-match").unwrap(), "*");
        assert!(!request.headers().contains_key("if-match"));
    }

    #[test]
    fn test_copy_signed_headers_are_lowercase_and_replayed() {
        let bucket = Bucket::new(
            "https://s3.example.com".parse().unwrap(),
            RustyUrlStyle::Path,
            "bucket",
            "auto",
        )
        .unwrap();
        let creds = Credentials::new("key", "secret");
        let mut action = bucket.put_object(Some(&creds), "target");

        insert_signed_header(
            action.headers_mut(),
            "X-Amz-Copy-Source",
            "/bucket/source%20key",
        );
        apply_copy_source_condition_headers(
            action.headers_mut(),
            Conditions::new().if_unmodified_since(OffsetDateTime::UNIX_EPOCH),
        )
        .unwrap();

        let headers = action.headers_mut().clone();
        let signed_url = action.sign(S3ObjStore::DURATION);
        let signed_headers = signed_url
            .query_pairs()
            .find(|(name, _)| name == "X-Amz-SignedHeaders")
            .map(|(_, value)| value.into_owned())
            .unwrap();

        assert_eq!(
            signed_headers,
            "host;x-amz-copy-source;x-amz-copy-source-if-unmodified-since"
        );

        let request = S3ObjStore::with_signed_headers(Client::new().put(signed_url), &headers)
            .build()
            .unwrap();
        assert_eq!(
            request.headers().get("x-amz-copy-source").unwrap(),
            "/bucket/source%20key"
        );
        assert!(
            request
                .headers()
                .contains_key("x-amz-copy-source-if-unmodified-since")
        );
    }

    #[test]
    fn test_presigned_upload_url_signs_normalized_headers() {
        let config = S3ObjStoreConfig {
            url: "https://s3.example.com".parse().unwrap(),
            bucket: "bucket".to_string(),
            region: "auto".to_string(),
            path_style: crate::UrlStyle::Path,
            fetch_metadata_after_put: true,
            key: "key".to_string(),
            secret: "secret".to_string(),
            token: None,
            path_prefix: None,
        };
        let store = S3ObjStore::new(config).unwrap();

        let mut args = UploadUrlArgs::new("key", S3ObjStore::DURATION);
        args.content_type = Some("application/zip".to_string());
        args.content_disposition = Some("attachment".to_string());
        args.content_encoding = Some("gzip".to_string());
        args.cache_control = Some("max-age=60".to_string());
        args.metadata
            .insert("Sha256_Checksum".to_string(), "abc".to_string());

        let signed_url = store.presign_upload_url(args).unwrap();
        let signed_headers = signed_url
            .query_pairs()
            .find(|(name, _)| name == "X-Amz-SignedHeaders")
            .map(|(_, value)| value.into_owned())
            .unwrap();

        assert_eq!(
            signed_headers,
            "cache-control;content-disposition;content-encoding;content-type;host;x-amz-meta-sha256-checksum"
        );
    }

    #[test]
    fn test_path_prefix_is_normalized_and_pruned_from_list_results() {
        let config = S3ObjStoreConfig {
            url: "https://s3.example.com".parse().unwrap(),
            bucket: "bucket".to_string(),
            region: "auto".to_string(),
            path_style: crate::UrlStyle::Path,
            fetch_metadata_after_put: false,
            key: "key".to_string(),
            secret: "secret".to_string(),
            token: None,
            path_prefix: Some("/tenant/".to_string()),
        };
        let store = S3ObjStore::new(config).unwrap();

        assert_eq!(store.build_key("/file.txt"), "tenant/file.txt");

        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Contents>
                    <Key>tenant%2Fnested%2Ffile%20name.txt</Key>
                    <LastModified>2024-01-01T00:00:00.000Z</LastModified>
                    <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>
                    <Size>123</Size>
                    <StorageClass>STANDARD</StorageClass>
                </Contents>
                <CommonPrefixes>
                    <Prefix>tenant%2Fnested%2Fdir%2F</Prefix>
                </CommonPrefixes>
            </ListBucketResult>"#;
        let mut list = rusty_s3::actions::ListObjectsV2::parse_response(xml).unwrap();
        store.normalize_list_response(&mut list);

        assert_eq!(list.contents[0].key, "nested/file name.txt");
        assert_eq!(list.common_prefixes[0].prefix, "nested/dir/");
    }

    #[test]
    fn test_multipart_success_response_error_body_is_reported() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>EntityTooSmall</Code>
                <Message>Your proposed upload is smaller than the minimum allowed object size.</Message>
            </Error>"#;

        let err = error_from_success_response_body(body).unwrap_err();
        let err = err.to_string();
        assert!(err.contains("EntityTooSmall"), "unexpected error: {err}");
        assert!(
            err.contains("minimum allowed object size"),
            "unexpected error: {err}"
        );

        let ok = br#"<CompleteMultipartUploadResult>
            <Location>http://example.com/bucket/key</Location>
            <Bucket>bucket</Bucket>
            <Key>key</Key>
            <ETag>&quot;etag&quot;</ETag>
        </CompleteMultipartUploadResult>"#;
        error_from_success_response_body(ok).unwrap();
    }

    #[test]
    fn test_copy_success_response_error_body_is_reported_before_metadata() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>
            <Error>
                <Code>InternalError</Code>
                <Message>copy failed after response started</Message>
            </Error>"#;

        let err = error_from_success_response_body(body).unwrap_err();
        let err = err.to_string();
        assert!(err.contains("InternalError"), "unexpected error: {err}");
        assert!(err.contains("copy failed"), "unexpected error: {err}");
    }

    #[test]
    fn test_parse_copy_object_result() {
        let body = br#"<CopyObjectResult>
            <LastModified>2024-01-01T00:00:00.000Z</LastModified>
            <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>
        </CopyObjectResult>"#;

        let meta = parse_copy_object_result("target".to_string(), body)
            .unwrap()
            .expect("copy result should parse");
        assert_eq!(meta.key, "target");
        assert_eq!(
            meta.etag.as_deref(),
            Some("d41d8cd98f00b204e9800998ecf8427e")
        );
        assert!(meta.updated_at.is_some());
    }

    #[test]
    fn test_complete_multipart_signs_conditions() {
        let bucket = Bucket::new(
            "https://s3.example.com".parse().unwrap(),
            RustyUrlStyle::Path,
            "bucket",
            "auto",
        )
        .unwrap();
        let creds = Credentials::new("key", "secret");
        let etags = ["etag"];
        let mut complete = CompleteMultipartUpload::new(
            &bucket,
            Some(&creds),
            "key",
            "upload-id",
            etags.iter().copied(),
        );

        apply_condition_headers(complete.headers_mut(), Conditions::new().if_not_exists()).unwrap();

        let headers = complete.headers_mut().clone();
        let signed_url = complete.sign(S3ObjStore::DURATION);
        let signed_headers = signed_url
            .query_pairs()
            .find(|(name, _)| name == "X-Amz-SignedHeaders")
            .map(|(_, value)| value.into_owned())
            .unwrap();

        assert_eq!(signed_headers, "host;if-none-match");

        let request = S3ObjStore::with_signed_headers(Client::new().post(signed_url), &headers)
            .body(complete.body())
            .build()
            .unwrap();
        assert_eq!(request.headers().get("if-none-match").unwrap(), "*");
    }

    #[tokio::test]
    async fn test_s3_light() {
        let config = if let Some(config) = load_test_config().unwrap() {
            config
        } else {
            return;
        };

        let store = S3ObjStore::new(config.clone()).expect("failed to create s3 kv store");
        ensure_test_bucket(&store).await;

        // Test with prefix.
        objstore_test::test_objstore(&store).await;
        objstore_test::test_empty_stream_put(&store, "empty-stream").await;

        // Test with without.
        let config = S3ObjStoreConfig {
            path_prefix: None,
            ..config
        };
        let store = S3ObjStore::new(config).expect("failed to create s3 kv store");
        objstore_test::test_objstore(&store).await;
        objstore_test::test_empty_stream_put(&store, "empty-stream").await;
    }

    #[tokio::test]
    async fn test_s3_write_metadata_fetch_can_be_disabled() {
        let config = if let Some(config) = load_test_config().unwrap() {
            config
        } else {
            return;
        };
        let config = S3ObjStoreConfig {
            path_prefix: None,
            fetch_metadata_after_put: false,
            ..config
        };
        let store = S3ObjStore::new(config).expect("failed to create s3 kv store");
        ensure_test_bucket(&store).await;

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let source = format!("regression-fetch-meta-source-{nanos}");
        let target = format!("regression-fetch-meta-target-{nanos}");
        let value = Bytes::from(format!("metadata fetch regression {nanos}"));
        let len = value.len() as u64;

        let put_meta = store
            .send_put(Put::new(&source, value.clone()))
            .await
            .expect("put should succeed");
        assert_eq!(put_meta.size, Some(len));
        assert!(
            put_meta.updated_at.is_none(),
            "disabled metadata fetch should not HEAD after PUT"
        );

        let copy_meta = store
            .send_copy(objstore::Copy::new(&source, &target))
            .await
            .expect("copy should succeed");
        assert_eq!(copy_meta.key, target);
        assert!(
            copy_meta.size.is_none(),
            "CopyObject response does not include object size without a HEAD"
        );

        assert_eq!(store.get(&target).await.unwrap().unwrap(), value);

        store
            .delete(&source)
            .await
            .expect("failed to clean up source");
        store
            .delete(&target)
            .await
            .expect("failed to clean up target");
    }

    #[tokio::test]
    async fn test_s3_healthcheck_errors_for_missing_bucket() {
        let config = if let Some(config) = load_test_config().unwrap() {
            config
        } else {
            return;
        };
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let store = S3ObjStore::new(S3ObjStoreConfig {
            bucket: format!("missing-bucket-{nanos}"),
            ..config
        })
        .expect("failed to create s3 kv store");

        let err = store
            .healthcheck()
            .await
            .expect_err("missing bucket should fail healthcheck");
        match err {
            ObjStoreError::BucketNotFound { bucket, .. } => {
                assert!(bucket.starts_with("missing-bucket-"));
            }
            other => panic!("expected BucketNotFound for missing S3 bucket, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_s3_multipart_if_not_exists_does_not_overwrite() {
        use objstore::SizedValueStream;

        let config = if let Some(config) = load_test_config().unwrap() {
            config
        } else {
            return;
        };
        let config = S3ObjStoreConfig {
            path_prefix: None,
            ..config
        };
        let store = S3ObjStore::new(config).expect("failed to create s3 kv store");
        ensure_test_bucket(&store).await;

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key = format!("regression-multipart-if-not-exists-{nanos}");
        let original = Bytes::from_static(b"original");
        store.put(&key).bytes(original.clone()).await.unwrap();

        let chunks = vec![
            Ok(Bytes::from(vec![b'a'; S3ObjStore::PART_SIZE])),
            Ok(Bytes::from_static(b"tail")),
        ];
        let stream: ValueStream = futures::stream::iter(chunks).boxed();
        let mut put = Put::new(
            &key,
            DataSource::Stream(SizedValueStream::new_without_size(stream)),
        );
        put.conditions = Conditions::new().if_not_exists();

        store
            .send_put(put)
            .await
            .expect_err("multipart if-not-exists should fail for an existing object");
        let got = store.get(&key).await.unwrap().unwrap();
        assert_eq!(
            got, original,
            "failed conditional multipart must not overwrite"
        );

        store
            .delete(&key)
            .await
            .expect("failed to clean up test object");
    }

    /// Regression test for the header-handling fix: a sized stream upload that
    /// fits in a single part must take the single-PUT path and actually send
    /// the signed `Content-Type` and `Content-Length` headers to S3.
    ///
    /// Before the fix the headers were added to the signed action but never
    /// copied onto the outgoing request, so S3 never saw the MIME type and
    /// stream uploads always fell back to multipart.
    #[tokio::test]
    async fn test_s3_single_put_stream_headers() {
        use objstore::SizedValueStream;

        let config = if let Some(config) = load_test_config().unwrap() {
            config
        } else {
            return;
        };

        // No prefix: keeps the key predictable for cleanup.
        let config = S3ObjStoreConfig {
            path_prefix: None,
            ..config
        };
        let store = S3ObjStore::new(config).expect("failed to create s3 kv store");
        ensure_test_bucket(&store).await;

        // Unique key derived from the current time to avoid collisions across runs.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let key = format!("regression-single-put-stream-{nanos}");

        // Small payload well below PART_SIZE (8 MiB) to force the single-PUT path.
        let value: Bytes = Bytes::from(format!("{{\"id\":{nanos},\"msg\":\"sized-stream-put\"}}"));
        let len = value.len() as u64;
        let mime = "application/json".to_string();

        let stream: ValueStream =
            futures::stream::once(std::future::ready(Ok(value.clone()))).boxed();

        let mut put = Put::new(&key, DataSource::Stream(SizedValueStream::new(stream, len)));
        put.mime_type = Some(mime.clone());
        store
            .send_put(put)
            .await
            .expect("single PUT stream upload should succeed");

        let (got, meta) = store
            .get_with_meta(&key)
            .await
            .expect("get_with_meta should not error")
            .expect("uploaded object should exist");

        assert_eq!(
            got, value,
            "retrieved value should match the uploaded payload"
        );
        assert_eq!(
            meta.size,
            Some(len),
            "Content-Length should have been sent and honored on the single PUT"
        );
        assert_eq!(
            meta.mime_type,
            Some(mime),
            "signed Content-Type header should have reached S3"
        );

        store
            .delete(&key)
            .await
            .expect("failed to clean up test object");
    }
}
