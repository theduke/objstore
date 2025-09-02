use anyhow::Context as _;
use base64::Engine as _;
use http::HeaderMap;
use objstore::{Conditions, ObjectMeta};
use time::OffsetDateTime;

/// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
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

pub fn apply_condition_headers(
    headers: &mut rusty_s3::Map,
    mut conditions: Conditions,
) -> Result<(), time::error::Format> {
    conditions.sanitize();

    if let Some(if_match) = &conditions.if_match {
        match if_match {
            objstore::MatchValue::Any => {
                headers.insert("If-Match", "*");
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
                headers.insert("If-Match", value);
            }
        }
    }
    if let Some(if_none_match) = &conditions.if_none_match {
        match if_none_match {
            objstore::MatchValue::Any => {
                headers.insert("If-None-Match", "*");
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
                headers.insert("If-None-Match", value);
            }
        }
    }
    if let Some(date) = &conditions.if_modified_since {
        let value = date
            .to_offset(time::UtcOffset::UTC)
            .format(&time::format_description::well_known::Rfc2822)?;

        headers.insert("If-Modified-Since", value);
    }

    if let Some(date) = &conditions.if_unmodified_since {
        let value = date
            .to_offset(time::UtcOffset::UTC)
            .format(&time::format_description::well_known::Rfc2822)?;

        headers.insert("If-Unmodified-Since", value);
    }

    Ok(())
}
