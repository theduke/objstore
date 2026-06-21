use std::{error::Error as StdError, fmt};

pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;
pub type Result<T, E = ObjStoreError> = std::result::Result<T, E>;

#[derive(Debug)]
#[non_exhaustive]
pub enum ObjStoreError {
    ObjectNotFound {
        key: String,
        source: Option<BoxError>,
    },
    BucketNotFound {
        bucket: String,
        source: Option<BoxError>,
    },
    ProviderNotFound {
        scheme: String,
    },
    AlreadyExists {
        resource: Resource,
        source: Option<BoxError>,
    },
    PreconditionFailed {
        operation: Operation,
        resource: Option<Resource>,
        source: Option<BoxError>,
    },
    Unauthenticated {
        source: Option<BoxError>,
    },
    PermissionDenied {
        operation: Operation,
        resource: Option<Resource>,
        source: Option<BoxError>,
    },
    Unsupported {
        operation: Operation,
        source: Option<BoxError>,
    },
    InvalidConfig {
        message: String,
        source: Option<BoxError>,
    },
    InvalidRequest {
        message: String,
        source: Option<BoxError>,
    },
    InvalidMetadata {
        key: String,
        message: String,
        source: Option<BoxError>,
    },
    JsonContentDeserialization {
        key: String,
        source: BoxError,
    },
    Io {
        operation: Operation,
        source: std::io::Error,
    },
    Timeout {
        operation: Operation,
        source: Option<BoxError>,
    },
    Dispatch {
        operation: Operation,
        source: BoxError,
    },
    Response {
        operation: Operation,
        source: BoxError,
    },
    Backend {
        backend: &'static str,
        operation: Operation,
        resource: Option<Resource>,
        code: Option<String>,
        status: Option<u16>,
        message: Option<String>,
        request_id: Option<String>,
        extended_request_id: Option<String>,
        source: Option<BoxError>,
    },
    Internal {
        message: String,
        source: Option<BoxError>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Operation {
    Unknown,
    Build,
    Healthcheck,
    Meta,
    Get,
    GetStream,
    Put,
    Copy,
    Delete,
    DeletePrefix,
    List,
    ListKeys,
    GenerateDownloadUrl,
    GenerateUploadUrl,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Resource {
    Store,
    Bucket { bucket: String },
    Object { key: String },
    Prefix { prefix: String },
    Provider { scheme: String },
}

impl ObjStoreError {
    pub fn object_not_found(key: impl Into<String>) -> Self {
        Self::ObjectNotFound {
            key: key.into(),
            source: None,
        }
    }

    pub fn bucket_not_found(bucket: impl Into<String>) -> Self {
        Self::BucketNotFound {
            bucket: bucket.into(),
            source: None,
        }
    }

    pub fn provider_not_found(scheme: impl Into<String>) -> Self {
        Self::ProviderNotFound {
            scheme: scheme.into(),
        }
    }

    pub fn unsupported(operation: Operation) -> Self {
        Self::Unsupported {
            operation,
            source: None,
        }
    }

    pub fn backend(
        backend: &'static str,
        operation: Operation,
        source: impl Into<BoxError>,
    ) -> Self {
        Self::Backend {
            backend,
            operation,
            resource: None,
            code: None,
            status: None,
            message: None,
            request_id: None,
            extended_request_id: None,
            source: Some(source.into()),
        }
    }

    pub fn with_source(mut self, source: impl Into<BoxError>) -> Self {
        let source = Some(source.into());
        match &mut self {
            Self::ObjectNotFound { source: field, .. }
            | Self::BucketNotFound { source: field, .. }
            | Self::AlreadyExists { source: field, .. }
            | Self::PreconditionFailed { source: field, .. }
            | Self::Unauthenticated { source: field }
            | Self::PermissionDenied { source: field, .. }
            | Self::Unsupported { source: field, .. }
            | Self::InvalidConfig { source: field, .. }
            | Self::InvalidRequest { source: field, .. }
            | Self::InvalidMetadata { source: field, .. }
            | Self::Timeout { source: field, .. }
            | Self::Backend { source: field, .. }
            | Self::Internal { source: field, .. } => *field = source,
            Self::JsonContentDeserialization { .. }
            | Self::ProviderNotFound { .. }
            | Self::Io { .. }
            | Self::Dispatch { .. }
            | Self::Response { .. } => {}
        }
        self
    }
}

impl fmt::Display for ObjStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObjectNotFound { key, .. } => write!(f, "object not found: {key}"),
            Self::BucketNotFound { bucket, .. } => write!(f, "bucket not found: {bucket}"),
            Self::ProviderNotFound { scheme } => {
                write!(f, "provider not found for URL scheme: {scheme}")
            }
            Self::AlreadyExists { resource, .. } => {
                write!(f, "resource already exists: {resource}")
            }
            Self::PreconditionFailed { operation, .. } => {
                write!(f, "precondition failed while {operation}")
            }
            Self::Unauthenticated { .. } => write!(f, "authentication failed"),
            Self::PermissionDenied { operation, .. } => {
                write!(f, "permission denied while {operation}")
            }
            Self::Unsupported { operation, .. } => {
                write!(f, "operation is not supported: {operation}")
            }
            Self::InvalidConfig { message, .. } => write!(f, "invalid configuration: {message}"),
            Self::InvalidRequest { message, .. } => write!(f, "invalid request: {message}"),
            Self::InvalidMetadata { key, message, .. } => {
                write!(f, "invalid metadata for {key}: {message}")
            }
            Self::JsonContentDeserialization { key, .. } => {
                write!(f, "could not deserialize JSON content for {key}")
            }
            Self::Io { operation, .. } => write!(f, "I/O error while {operation}"),
            Self::Timeout { operation, .. } => write!(f, "request timed out while {operation}"),
            Self::Dispatch { operation, .. } => write!(f, "dispatch failed while {operation}"),
            Self::Response { operation, .. } => {
                write!(f, "backend response could not be handled while {operation}")
            }
            Self::Backend {
                backend,
                operation,
                code,
                message,
                ..
            } => {
                if let (Some(code), Some(message)) = (code, message) {
                    write!(
                        f,
                        "{backend} backend failed while {operation}: {code}: {message}"
                    )
                } else if let Some(code) = code {
                    write!(f, "{backend} backend failed while {operation}: {code}")
                } else if let Some(message) = message {
                    write!(f, "{backend} backend failed while {operation}: {message}")
                } else {
                    write!(f, "{backend} backend failed while {operation}")
                }
            }
            Self::Internal { message, .. } => {
                write!(f, "internal objstore invariant violated: {message}")
            }
        }
    }
}

impl StdError for ObjStoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::ObjectNotFound { source, .. }
            | Self::BucketNotFound { source, .. }
            | Self::AlreadyExists { source, .. }
            | Self::PreconditionFailed { source, .. }
            | Self::Unauthenticated { source }
            | Self::PermissionDenied { source, .. }
            | Self::Unsupported { source, .. }
            | Self::InvalidConfig { source, .. }
            | Self::InvalidRequest { source, .. }
            | Self::InvalidMetadata { source, .. }
            | Self::Timeout { source, .. }
            | Self::Backend { source, .. }
            | Self::Internal { source, .. } => source.as_deref().map(|source| source as _),
            Self::JsonContentDeserialization { source, .. } => Some(&**source),
            Self::Io { source, .. } => Some(source),
            Self::Dispatch { source, .. } | Self::Response { source, .. } => Some(&**source),
            Self::ProviderNotFound { .. } => None,
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Unknown => "unknown operation",
            Self::Build => "build store",
            Self::Healthcheck => "healthcheck",
            Self::Meta => "get metadata",
            Self::Get => "get object",
            Self::GetStream => "stream object",
            Self::Put => "put object",
            Self::Copy => "copy object",
            Self::Delete => "delete object",
            Self::DeletePrefix => "delete prefix",
            Self::List => "list objects",
            Self::ListKeys => "list keys",
            Self::GenerateDownloadUrl => "generate download URL",
            Self::GenerateUploadUrl => "generate upload URL",
        };
        f.write_str(label)
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store => f.write_str("store"),
            Self::Bucket { bucket } => write!(f, "bucket {bucket}"),
            Self::Object { key } => write!(f, "object {key}"),
            Self::Prefix { prefix } => write!(f, "prefix {prefix}"),
            Self::Provider { scheme } => write!(f, "provider {scheme}"),
        }
    }
}

impl From<std::io::Error> for ObjStoreError {
    fn from(source: std::io::Error) -> Self {
        Self::Io {
            operation: Operation::Unknown,
            source,
        }
    }
}
