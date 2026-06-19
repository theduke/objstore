use std::borrow::Cow;

use anyhow::{Context as _, anyhow};
use base64::Engine as _;
use http::HeaderMap;
use objstore::{Conditions, ObjectMeta};
use quick_xml::de::from_reader;
use serde::Deserialize;
use time::OffsetDateTime;

pub(crate) fn insert_signed_header<'a>(
    headers: &mut rusty_s3::Map<'a>,
    name: impl AsRef<str>,
    value: impl Into<Cow<'a, str>>,
) {
    headers.insert(name.as_ref().to_ascii_lowercase(), value);
}

/// See <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html>
pub fn parse_object_headers(key: String, headers: &HeaderMap) -> Result<ObjectMeta, anyhow::Error> {
    let last_modified = if let Some(v) = headers.get(http::header::LAST_MODIFIED) {
        let raw = v.to_str().with_context(|| {
            format!(
                "invalid last-modified header: {}",
                String::from_utf8_lossy(v.as_bytes()),
            )
        })?;

        OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc2822).with_context(
            || {
                format!(
                    "failed to parse last-modified header: '{}'",
                    String::from_utf8_lossy(v.as_bytes()),
                )
            },
        )?
    } else {
        tracing::warn!("missing last-modified header in response headers");
        OffsetDateTime::UNIX_EPOCH
    };

    let size = if let Some(v) = headers.get(http::header::CONTENT_LENGTH) {
        v.to_str()
            .context("invalid content-length header")?
            .parse::<u64>()
            .context("invalid content-length header")?
    } else {
        tracing::trace!("missing content-length header in response headers");
        0
    };

    let etag = if let Some(v) = headers.get(http::header::ETAG) {
        let v = v
            .to_str()
            .context("invalid etag header")?
            .trim_matches('"')
            .trim()
            .to_string();
        Some(v)
    } else {
        None
    };

    let mut meta = ObjectMeta::new(key);
    meta.etag = etag;
    meta.size = Some(size);
    meta.created_at = None;
    meta.updated_at = Some(last_modified);
    // Extract content type if available
    if let Some(v) = headers.get(http::header::CONTENT_TYPE) {
        let ct = v
            .to_str()
            .context("invalid content-type header")?
            .to_string();
        meta.mime_type = Some(ct);
    }
    // Extract MD5 hash from Content-MD5 header (base64-encoded)
    if let Some(v) = headers.get("Content-MD5") {
        let raw = v.to_str().context("invalid content-md5 header")?;
        let bytes = base64::prelude::BASE64_STANDARD
            .decode(raw)
            .context("invalid base64 content-md5 header")?;
        if bytes.len() == 16 {
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&bytes);
            meta.hash_md5 = Some(arr);
        } else {
            tracing::warn!(len = bytes.len(), "unexpected Content-MD5 length");
        }
    }
    // Extract SHA256 hash from x-amz-meta-sha256 header (hex-encoded)
    if let Some(v) = headers.get("x-amz-meta-sha256") {
        let raw = v.to_str().context("invalid x-amz-meta-sha256 header")?;
        if raw.len() == 64 && raw.chars().all(|c| c.is_ascii_hexdigit()) {
            let mut arr = [0u8; 32];
            for i in 0..32 {
                arr[i] = u8::from_str_radix(&raw[i * 2..i * 2 + 2], 16)
                    .context("invalid hex in x-amz-meta-sha256 header")?;
            }
            meta.hash_sha256 = Some(arr);
        } else {
            tracing::warn!(header = raw, "unexpected x-amz-meta-sha256 format");
        }
    }

    Ok(meta)
}

#[derive(Debug, Deserialize)]
#[serde(rename = "Error")]
struct S3ErrorResponse {
    #[serde(rename = "Code")]
    code: Option<String>,
    #[serde(rename = "Message")]
    message: Option<String>,
}

pub fn error_from_success_response_body(body: &[u8]) -> Result<(), anyhow::Error> {
    let Ok(err) = from_reader::<_, S3ErrorResponse>(body) else {
        return Ok(());
    };

    if err.code.is_none() && err.message.is_none() {
        return Ok(());
    }

    let code = err.code.as_deref().unwrap_or("Unknown");
    let message = err.message.as_deref().unwrap_or("S3 returned an error");
    Err(anyhow!("S3 request failed: {code}: {message}"))
}

#[derive(Debug, Deserialize)]
#[serde(rename = "CopyObjectResult")]
struct CopyObjectResult {
    #[serde(rename = "ETag")]
    etag: Option<String>,
    #[serde(rename = "LastModified")]
    last_modified: Option<String>,
}

pub fn parse_copy_object_result(
    key: String,
    body: &[u8],
) -> Result<Option<ObjectMeta>, anyhow::Error> {
    let Ok(result) = from_reader::<_, CopyObjectResult>(body) else {
        return Ok(None);
    };

    if result.etag.is_none() && result.last_modified.is_none() {
        return Ok(None);
    }

    let mut meta = ObjectMeta::new(key);
    meta.etag = result
        .etag
        .map(|etag| etag.trim_matches('"').trim().to_string());
    meta.updated_at = result
        .last_modified
        .map(|raw| {
            OffsetDateTime::parse(
                &raw,
                &time::format_description::well_known::Iso8601::DEFAULT,
            )
            .with_context(|| format!("failed to parse copy LastModified value: '{raw}'"))
        })
        .transpose()?;

    Ok(Some(meta))
}

pub fn apply_condition_headers(
    headers: &mut rusty_s3::Map,
    mut conditions: Conditions,
) -> Result<(), time::error::Format> {
    conditions.sanitize();

    if let Some(if_match) = &conditions.if_match {
        match if_match {
            objstore::MatchValue::Any => {
                insert_signed_header(headers, "if-match", "*");
            }
            objstore::MatchValue::Tags(tags) => {
                assert!(
                    !tags.is_empty(),
                    "if-match tags cannot be empty due to sanitize()"
                );

                let mut value = String::new();
                for (index, tag) in tags.iter().enumerate() {
                    if index > 0 {
                        value.push_str(", ");
                    }
                    value.push('"');
                    value.push_str(tag);
                    value.push('"');
                }
                insert_signed_header(headers, "if-match", value);
            }
        }
    }
    if let Some(if_none_match) = &conditions.if_none_match {
        match if_none_match {
            objstore::MatchValue::Any => {
                insert_signed_header(headers, "if-none-match", "*");
            }
            objstore::MatchValue::Tags(tags) => {
                assert!(
                    !tags.is_empty(),
                    "if-none-match tags cannot be empty due to sanitize()"
                );

                let mut value = String::new();
                for (index, tag) in tags.iter().enumerate() {
                    if index > 0 {
                        value.push_str(", ");
                    }
                    value.push('"');
                    value.push_str(tag);
                    value.push('"');
                }
                insert_signed_header(headers, "if-none-match", value);
            }
        }
    }
    if let Some(date) = &conditions.if_modified_since {
        let value = date
            .to_offset(time::UtcOffset::UTC)
            .format(&time::format_description::well_known::Rfc2822)?;

        insert_signed_header(headers, "if-modified-since", value);
    }

    if let Some(date) = &conditions.if_unmodified_since {
        let value = date
            .to_offset(time::UtcOffset::UTC)
            .format(&time::format_description::well_known::Rfc2822)?;

        insert_signed_header(headers, "if-unmodified-since", value);
    }

    Ok(())
}

/// Apply S3 CopyObject source conditions using x-amz-copy-source-if-* headers.
/// These are distinct from the normal If-* headers used for PutObject or destination conditions.
/// See <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html>.
pub fn apply_copy_source_condition_headers(
    headers: &mut rusty_s3::Map,
    mut conditions: Conditions,
) -> anyhow::Result<()> {
    conditions.sanitize();

    if let Some(if_match) = &conditions.if_match {
        match if_match {
            objstore::MatchValue::Any => {
                insert_signed_header(headers, "x-amz-copy-source-if-match", "*");
            }
            objstore::MatchValue::Tags(tags) => {
                if tags.is_empty() {
                    return Err(anyhow!("if-match tags cannot be empty due to sanitize()"));
                }

                let mut value = String::new();
                for (index, tag) in tags.iter().enumerate() {
                    if index > 0 {
                        value.push_str(", ");
                    }
                    value.push('"');
                    value.push_str(tag);
                    value.push('"');
                }
                insert_signed_header(headers, "x-amz-copy-source-if-match", value);
            }
        }
    }
    if let Some(if_none_match) = &conditions.if_none_match {
        match if_none_match {
            objstore::MatchValue::Any => {
                insert_signed_header(headers, "x-amz-copy-source-if-none-match", "*");
            }
            objstore::MatchValue::Tags(tags) => {
                if tags.is_empty() {
                    return Err(anyhow!(
                        "if-none-match tags cannot be empty due to sanitize()"
                    ));
                }

                let mut value = String::new();
                for (index, tag) in tags.iter().enumerate() {
                    if index > 0 {
                        value.push_str(", ");
                    }
                    value.push('"');
                    value.push_str(tag);
                    value.push('"');
                }
                insert_signed_header(headers, "x-amz-copy-source-if-none-match", value);
            }
        }
    }
    if let Some(date) = &conditions.if_modified_since {
        let value = date
            .to_offset(time::UtcOffset::UTC)
            .format(&time::format_description::well_known::Rfc2822)?;

        insert_signed_header(headers, "x-amz-copy-source-if-modified-since", value);
    }

    if let Some(date) = &conditions.if_unmodified_since {
        let value = date
            .to_offset(time::UtcOffset::UTC)
            .format(&time::format_description::well_known::Rfc2822)?;

        insert_signed_header(headers, "x-amz-copy-source-if-unmodified-since", value);
    }

    Ok(())
}
