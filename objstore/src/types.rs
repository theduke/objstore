use std::collections::HashMap;

use bytes::Bytes;
use time::OffsetDateTime;

/// Byte stream.
pub type ValueStream = futures::stream::BoxStream<'static, Result<Bytes, anyhow::Error>>;

/// Stream of key-name pages (as returned by `list_keys`).
pub type KeyStream<'a> = futures::stream::BoxStream<'a, Result<KeyPage, anyhow::Error>>;

/// Stream of metadata pages (as returned by `list`).
pub type MetaStream = futures::stream::BoxStream<'static, Result<ObjectMetaPage, anyhow::Error>>;

/// Key metadata.
///
/// Fields are private for forwards compatibility.
///
/// Use [`ObjectMeta::into_parts`] to get a struct with public fields.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ObjectMeta {
    pub key: String,
    pub etag: Option<String>,
    pub size: Option<u64>,
    pub created_at: Option<OffsetDateTime>,
    pub updated_at: Option<OffsetDateTime>,
    pub hash_md5: Option<[u8; 16]>,
    pub hash_sha256: Option<[u8; 32]>,
    /// Optional MIME content type of the object.
    pub mime_type: Option<String>,

    pub extra: HashMap<String, serde_json::Value>,
}

impl ObjectMeta {
    pub fn new(key: String) -> Self {
        Self {
            key,
            etag: None,
            size: None,
            created_at: None,
            updated_at: None,
            hash_md5: None,
            hash_sha256: None,
            mime_type: None,
            extra: HashMap::new(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    /// Round the timestamps to the nearest second.
    ///
    /// Useful for normalizing timestamps due to differing precisions in the backend.
    pub fn round_timestamps_second(&mut self) {
        if let Some(ts) = self.created_at.as_mut() {
            if let Ok(new) = ts.replace_millisecond(0) {
                *ts = new;
            }
        }
        if let Some(ts) = self.updated_at.as_mut() {
            if let Ok(new) = ts.replace_millisecond(0) {
                *ts = new;
            }
        }
    }

    /// Round the timestamps to the nearest minute.
    ///
    /// Useful for normalizing timestamps due to differing precisions in the backend.
    pub fn round_timestamps_minute(&mut self) {
        if let Some(ts) = self.created_at.as_mut() {
            if let Ok(new1) = ts.replace_millisecond(0) {
                if let Ok(new) = new1.replace_minute(0) {
                    *ts = new;
                }
            }
        }
        if let Some(ts) = self.updated_at.as_mut() {
            if let Ok(new) = ts.replace_millisecond(0) {
                if let Ok(new) = new.replace_minute(0) {
                    *ts = new;
                }
            }
        }
    }

    pub fn with_rounded_timestamps_minute(mut self) -> Self {
        self.round_timestamps_minute();
        self
    }
}

#[derive(Clone, Debug)]
pub struct ObjectMetaPage {
    pub items: Vec<ObjectMeta>,
    pub next_cursor: Option<String>,

    pub prefixes: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct KeyPage {
    pub items: Vec<String>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ListArgs {
    prefix: Option<String>,
    limit: Option<u64>,
    cursor: Option<String>,
    delimiter: Option<String>,
}

impl ListArgs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    pub fn set_prefix(&mut self, prefix: impl Into<String>) {
        let prefix = prefix.into();
        if !prefix.is_empty() {
            self.prefix = Some(prefix);
        } else {
            self.prefix = None;
        }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        if !prefix.is_empty() {
            self.prefix = Some(prefix);
        }
        self
    }

    pub fn delimiter(&self) -> Option<&str> {
        self.delimiter.as_deref()
    }

    pub fn set_delimiter(&mut self, delimiter: impl Into<String>) {
        let delimiter = delimiter.into();
        if !delimiter.is_empty() {
            self.delimiter = Some(delimiter);
        } else {
            self.delimiter = None;
        }
    }

    pub fn with_delimiter(mut self, delimiter: impl Into<String>) -> Self {
        self.set_delimiter(delimiter);
        self
    }

    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    pub fn set_limit(&mut self, limit: u64) {
        if limit > 0 {
            self.limit = Some(limit);
        } else {
            self.limit = None;
        }
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.set_limit(limit);
        self
    }

    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    pub fn with_cursor_opt(mut self, cursor: Option<String>) -> Self {
        self.cursor = cursor;
        self
    }
}

pub enum DataSource {
    Data(Bytes),
    Stream(ValueStream),
}

impl std::fmt::Debug for DataSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Data(_) => f.write_str("DataSource::Data(...)"),
            Self::Stream(_) => f.write_str("DataSource::Stream(...)"),
        }
    }
}

impl From<Bytes> for DataSource {
    fn from(data: Bytes) -> Self {
        Self::Data(data)
    }
}

impl From<ValueStream> for DataSource {
    fn from(stream: ValueStream) -> Self {
        Self::Stream(stream)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectMatch {
    Any,
    Items(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchValue {
    Any,
    Tags(Vec<String>),
}

impl MatchValue {
    pub fn any() -> Self {
        Self::Any
    }

    pub fn tags(tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut clean_tags = Vec::new();
        for tag in tags {
            let tag = tag.into();
            if !tag.trim().is_empty() {
                clean_tags.push(tag);
            }
        }
        if clean_tags.is_empty() {
            Self::Any
        } else {
            Self::Tags(clean_tags)
        }
    }

    pub fn is_any(&self) -> bool {
        matches!(self, Self::Any)
    }

    pub fn as_tags(&self) -> Option<&[String]> {
        if let Self::Tags(tags) = self {
            Some(tags)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct Conditions {
    pub if_match: Option<MatchValue>,
    pub if_none_match: Option<MatchValue>,
    pub if_modified_since: Option<OffsetDateTime>,
    pub if_unmodified_since: Option<OffsetDateTime>,
}

impl Conditions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn if_not_exists(mut self) -> Self {
        self.if_match = Some(MatchValue::Any);
        self
    }

    pub fn if_match_any(mut self) -> Self {
        self.if_match = Some(MatchValue::Any);
        self
    }

    pub fn if_match_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut clean_tags = Vec::<String>::new();
        for tag in tags {
            let tag = tag.into();
            if tag == "*" {
                return self.if_match_any();
            }
            if tag.trim().is_empty() {
                continue; // Skip empty tags
            }

            clean_tags.push(tag);
        }

        if !clean_tags.is_empty() {
            self.if_match = Some(MatchValue::Tags(clean_tags));
        }
        self
    }

    pub fn if_none_match_tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut clean_tags = Vec::<String>::new();
        for tag in tags {
            let tag = tag.into();
            if tag == "*" {
                self.if_match = Some(MatchValue::Any);
                return self;
            }
            if tag.trim().is_empty() {
                continue; // Skip empty tags
            }
            clean_tags.push(tag);
        }

        if !clean_tags.is_empty() {
            self.if_none_match = Some(MatchValue::Tags(clean_tags));
        }
        self
    }

    pub fn if_unmodified_since(mut self, value: OffsetDateTime) -> Self {
        self.if_unmodified_since = Some(value);
        self
    }

    pub fn sanitize(&mut self) {
        if let Some(MatchValue::Tags(tags)) = &mut self.if_match {
            tags.retain(|tag| !tag.trim().is_empty());
            let has_any = tags.iter().any(|tag| tag == "*");
            if has_any {
                self.if_match = Some(MatchValue::Any);
            } else if !tags.is_empty() {
                self.if_match = Some(MatchValue::Tags(tags.clone()));
            } else {
                self.if_match = None;
            }
        }

        if let Some(MatchValue::Tags(tags)) = &mut self.if_none_match {
            tags.retain(|tag| !tag.trim().is_empty());
            let has_any = tags.iter().any(|tag| tag == "*");
            if has_any {
                self.if_match = Some(MatchValue::Any);
                self.if_none_match = None;
            } else if !tags.is_empty() {
                self.if_none_match = Some(MatchValue::Tags(tags.clone()));
            } else {
                self.if_none_match = None;
            }
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct Put {
    pub key: String,
    pub data: DataSource,
    pub conditions: Conditions,
    /// Optional MIME type to associate with the object.
    pub mime_type: Option<String>,
}

/// Request to copy an object from one key to another.
#[derive(Debug)]
#[non_exhaustive]
pub struct Copy {
    /// Source key to copy from.
    pub source_key: String,
    /// Destination key to copy to.
    pub target_key: String,
    /// Conditions to apply to the copy operation.
    pub conditions: Conditions,
    // TODO: add source/target bucket support?
}

impl Copy {
    /// Create a new copy request from `src` to `dest`.
    pub fn new(src: impl Into<String>, dest: impl Into<String>) -> Self {
        Self {
            source_key: src.into(),
            target_key: dest.into(),
            conditions: Conditions::default(),
        }
    }
}

impl Put {
    pub fn new(key: impl Into<String>, data: impl Into<DataSource>) -> Self {
        Self {
            key: key.into(),
            data: data.into(),
            conditions: Conditions::default(),
            mime_type: None,
        }
    }
}

/// Arguments for generating a download URL for an object.
#[derive(Debug)]
#[non_exhaustive]
pub struct DownloadUrlArgs {
    pub key: String,

    pub valid_for: std::time::Duration,

    pub response_content_type: Option<String>,
    pub response_content_disposition: Option<String>,
    pub response_content_encoding: Option<String>,
    pub response_content_language: Option<String>,
    pub response_cache_control: Option<String>,
}

impl DownloadUrlArgs {
    pub fn new(key: impl Into<String>, valid_for: std::time::Duration) -> Self {
        Self {
            key: key.into(),
            valid_for,
            response_content_type: None,
            response_content_disposition: None,
            response_content_encoding: None,
            response_content_language: None,
            response_cache_control: None,
        }
    }
}
