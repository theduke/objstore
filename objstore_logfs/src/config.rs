use std::{num::NonZeroU32, path::PathBuf};

use anyhow::{Context as _, anyhow, bail};
use base64::Engine as _;
use logfs::{ConfigBuilder, CryptoConfig, LogConfig};
use serde::{Deserialize, Serialize};
use url::Url;
use zeroize::Zeroizing;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LogFsCryptoConfig {
    pub key: String,
    #[serde(with = "serde_bytes")]
    pub salt: Vec<u8>,
    pub iterations: NonZeroU32,
}

impl LogFsCryptoConfig {
    pub fn into_crypto(self) -> CryptoConfig {
        CryptoConfig {
            key: Zeroizing::new(self.key),
            salt: Zeroizing::new(self.salt),
            iterations: self.iterations,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LogFsObjStoreConfig {
    pub path: PathBuf,
    #[serde(default)]
    pub allow_create: bool,
    #[serde(default)]
    pub raw_mode: bool,
    pub offset: Option<u64>,
    #[serde(default)]
    pub readonly: bool,
    pub default_chunk_size: Option<u32>,
    pub partial_index_write_interval: Option<u64>,
    pub full_index_write_interval: Option<u64>,
    pub crypto: Option<LogFsCryptoConfig>,
}

impl LogFsObjStoreConfig {
    pub const URI_SCHEME: &'static str = "logfs";

    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            allow_create: false,
            raw_mode: false,
            offset: None,
            readonly: false,
            default_chunk_size: None,
            partial_index_write_interval: None,
            full_index_write_interval: None,
            crypto: None,
        }
    }

    pub fn with_allow_create(mut self, allow: bool) -> Self {
        self.allow_create = allow;
        self
    }

    pub fn with_readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    pub fn with_offset(mut self, offset: impl Into<Option<u64>>) -> Self {
        self.offset = offset.into();
        self
    }

    pub fn with_raw_mode(mut self, raw: bool) -> Self {
        self.raw_mode = raw;
        self
    }

    pub fn with_default_chunk_size(mut self, chunk_size: Option<u32>) -> Self {
        self.default_chunk_size = chunk_size;
        self
    }

    pub fn with_crypto(mut self, crypto: LogFsCryptoConfig) -> Self {
        self.crypto = Some(crypto);
        self
    }

    pub(crate) fn to_logfs_config(&self) -> LogConfig {
        let mut builder = ConfigBuilder::new(self.path.clone());
        dbg!(&self);
        if self.raw_mode {
            builder = builder.raw_mode();
        }
        builder = builder.offset(self.offset);
        if self.allow_create {
            builder = builder.allow_create();
        }
        if let Some(ref crypto) = self.crypto {
            builder = builder.crypto(crypto.clone().into_crypto());
        }
        if let Some(chunk_size) = self.default_chunk_size {
            builder = builder.default_chunk_size(chunk_size);
        }
        if let Some(full_interval) = self.full_index_write_interval {
            builder = builder.full_index_write_interval(full_interval);
        }
        builder = builder.readonly(self.readonly);

        let mut config = builder.build();
        if let Some(partial_interval) = self.partial_index_write_interval {
            config.partial_index_write_interval = partial_interval;
        }
        config
    }

    pub fn safe_uri(&self) -> Result<Url, anyhow::Error> {
        let path = if self.path.is_absolute() {
            self.path.clone()
        } else {
            std::env::current_dir()
                .context("failed to determine current working directory")?
                .join(&self.path)
        };
        let file_url = Url::from_file_path(&path).map_err(|_| {
            anyhow!(
                "failed to construct file url from path '{}': path must be absolute",
                path.display()
            )
        })?;
        let file_str = file_url.to_string();
        let safe_str = file_str
            .strip_prefix("file:")
            .map(|rest| format!("{}:{}", Self::URI_SCHEME, rest))
            .ok_or_else(|| anyhow!("expected file:// URL for path"))?;
        Url::parse(&safe_str).map_err(|err| anyhow!("failed to parse logfs safe URI: {err}"))
    }

    pub fn from_url(url: &Url) -> Result<Self, anyhow::Error> {
        if url.scheme() != Self::URI_SCHEME {
            bail!(
                "invalid scheme: expected '{}', got '{}'",
                Self::URI_SCHEME,
                url.scheme()
            );
        }

        let prefix = format!("{}:", Self::URI_SCHEME);
        let file_str = url
            .as_str()
            .strip_prefix(&prefix)
            .map(|rest| format!("file:{}", rest))
            .ok_or_else(|| anyhow!("invalid logfs url: expected '{}' prefix", prefix))?;
        let file_url = Url::parse(&file_str)
            .map_err(|err| anyhow!("failed to parse translated file url: {err}"))?;
        let path = file_url
            .to_file_path()
            .map_err(|_| anyhow!("invalid path in logfs url: '{}'", url))?;

        let mut config = Self::new(path);
        let mut crypto_key: Option<String> = None;
        let mut crypto_salt: Option<Vec<u8>> = None;
        let mut crypto_iterations: Option<NonZeroU32> = None;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "allow_create" => config.allow_create = parse_bool(&value)?,
                "readonly" => config.readonly = parse_bool(&value)?,
                "raw" | "raw_mode" => config.raw_mode = parse_bool(&value)?,
                "offset" => {
                    config.offset = Some(
                        value
                            .parse::<u64>()
                            .with_context(|| format!("invalid offset '{}': expected u64", value))?,
                    )
                }
                "chunk_size" | "default_chunk_size" => {
                    config.default_chunk_size =
                        Some(value.parse::<u32>().with_context(|| {
                            format!("invalid chunk size '{}': expected u32", value)
                        })?)
                }
                "partial_index_interval" => {
                    config.partial_index_write_interval =
                        Some(value.parse::<u64>().with_context(|| {
                            format!("invalid partial index interval '{}': expected u64", value)
                        })?)
                }
                "full_index_interval" => {
                    config.full_index_write_interval =
                        Some(value.parse::<u64>().with_context(|| {
                            format!("invalid full index interval '{}': expected u64", value)
                        })?)
                }
                "crypto_key" => {
                    crypto_key = Some(value.to_string());
                }
                "crypto_salt_b64" | "crypto_salt" => {
                    let engine = base64::engine::general_purpose::STANDARD;
                    let decoded = engine.decode(value.as_ref()).with_context(|| {
                        format!("invalid base64 salt '{}': expected valid base64", value)
                    })?;
                    crypto_salt = Some(decoded);
                }
                "crypto_iterations" => {
                    let parsed = value.parse::<u32>().with_context(|| {
                        format!("invalid crypto iterations '{}': expected u32", value)
                    })?;
                    crypto_iterations = Some(NonZeroU32::new(parsed).ok_or_else(|| {
                        anyhow!("crypto iterations must be non-zero: '{}'", value)
                    })?);
                }
                other => {
                    bail!(
                        "unsupported logfs query parameter '{}': value '{}'",
                        other,
                        value
                    );
                }
            }
        }

        match (crypto_key, crypto_salt, crypto_iterations) {
            (None, None, None) => {}
            (Some(key), Some(salt), Some(iterations)) => {
                config.crypto = Some(LogFsCryptoConfig {
                    key,
                    salt,
                    iterations,
                });
            }
            _ => {
                bail!(
                    "invalid crypto configuration: expected crypto_key, crypto_salt, and crypto_iterations"
                );
            }
        }

        Ok(config)
    }
}

fn parse_bool(value: &str) -> Result<bool, anyhow::Error> {
    match value {
        "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        other => Err(anyhow!(
            "invalid bool value '{}': expected one of [true,false,1,0,on,off,yes,no]",
            other
        )),
    }
}
