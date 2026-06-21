use objstore::{ObjStoreError, Result};
use rusty_s3::Bucket;
use url::Url;

fn default_fetch_metadata_after_put() -> bool {
    true
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum UrlStyle {
    /// Requests will use "path-style" url: i.e:
    /// `https://s3.<region>.amazonaws.com/<bucket>/<key>`.
    ///
    /// This style should be considered deprecated and is **NOT RECOMMENDED**.
    /// Check [Amazon S3 Path Deprecation Plan](https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/)
    /// for more informations.
    Path,
    /// Requests will use "virtual-hosted-style" urls, i.e:
    /// `https://<bucket>.s3.<region>.amazonaws.com/<key>`.
    VirtualHost,
}

impl UrlStyle {
    fn to_rusty(self) -> rusty_s3::UrlStyle {
        match self {
            Self::Path => rusty_s3::UrlStyle::Path,
            Self::VirtualHost => rusty_s3::UrlStyle::VirtualHost,
        }
    }
}

impl From<UrlStyle> for rusty_s3::UrlStyle {
    fn from(style: UrlStyle) -> Self {
        style.to_rusty()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct S3ObjStoreConfig {
    pub url: Url,
    pub bucket: String,
    pub region: String,
    pub path_style: UrlStyle,
    #[serde(default = "default_fetch_metadata_after_put")]
    pub fetch_metadata_after_put: bool,

    pub key: String,
    pub secret: String,
    // TODO: what is token for?
    pub token: Option<String>,

    pub path_prefix: Option<String>,
}

impl S3ObjStoreConfig {
    pub(crate) const URI_SCHEME: &'static str = "s3";

    const QUERY_STYLE: &'static str = "style";
    const QUERY_REGION: &'static str = "region";
    const QUERY_PREFIX: &'static str = "prefix";
    const QUERY_TOKEN: &'static str = "token";
    const QUERY_FETCH_METADATA_AFTER_PUT: &'static str = "fetch_metadata_after_put";
    const QUERY_ENDPOINT_PATH: &'static str = "endpoint_path";

    pub fn validate(&self) -> Result<()> {
        if !(self.url.scheme() == "http" || self.url.scheme() == "https") {
            return Err(ObjStoreError::InvalidConfig {
                message: format!(
                    "invalid URL scheme: expected http or https, got '{}'",
                    self.url.scheme()
                ),
                source: None,
            });
        }
        if self.bucket.trim().is_empty() {
            return Err(ObjStoreError::InvalidConfig {
                message: "bucket name must not be empty".to_string(),
                source: None,
            });
        }
        if self.key.trim().is_empty() {
            return Err(ObjStoreError::InvalidConfig {
                message: "access key ID must not be empty".to_string(),
                source: None,
            });
        }
        if self.secret.trim().is_empty() {
            return Err(ObjStoreError::InvalidConfig {
                message: "secret access key must not be empty".to_string(),
                source: None,
            });
        }

        Ok(())
    }

    pub fn build_uri(&self) -> Result<String> {
        let host = self
            .url
            .host_str()
            .ok_or_else(|| ObjStoreError::InvalidConfig {
                message: "invalid URL: missing host".to_string(),
                source: None,
            })?;
        let port = self
            .url
            .port()
            .map(|port| format!(":{port}"))
            .unwrap_or_default();
        let mut url = format!("{}://{}{}/{}", Self::URI_SCHEME, host, port, self.bucket)
            .parse::<Url>()
            .map_err(|source| ObjStoreError::InvalidConfig {
                message: "failed to build S3 object store URI".to_string(),
                source: Some(source.into()),
            })?;
        url.set_username(&self.key)
            .map_err(|_| ObjStoreError::InvalidConfig {
                message: "failed to set access key in URI".to_string(),
                source: None,
            })?;
        url.set_password(Some(&self.secret))
            .map_err(|_| ObjStoreError::InvalidConfig {
                message: "failed to set secret key in URI".to_string(),
                source: None,
            })?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair(
                Self::QUERY_STYLE,
                match self.path_style {
                    UrlStyle::Path => "path",
                    UrlStyle::VirtualHost => "virtual",
                },
            );
            pairs.append_pair(Self::QUERY_REGION, &self.region);
            let endpoint_path = self.url.path();
            if !endpoint_path.is_empty() && endpoint_path != "/" {
                pairs.append_pair(Self::QUERY_ENDPOINT_PATH, endpoint_path);
            }
            if self.url.scheme() == "http" {
                pairs.append_key_only("insecure");
            }
            if !self.fetch_metadata_after_put {
                pairs.append_pair(Self::QUERY_FETCH_METADATA_AFTER_PUT, "false");
            }
            if let Some(prefix) = &self.path_prefix {
                pairs.append_pair(Self::QUERY_PREFIX, prefix);
            }
            if let Some(token) = &self.token {
                pairs.append_pair(Self::QUERY_TOKEN, token);
            }

            pairs.finish();
        }

        Ok(url.to_string())
    }

    pub(crate) fn build_bucket(&self) -> Result<Bucket> {
        Bucket::new(
            self.url.clone(),
            self.path_style.to_rusty(),
            self.bucket.clone(),
            self.region.clone(),
        )
        .map_err(|source| ObjStoreError::InvalidConfig {
            message: "could not build rusty_s3 bucket".to_string(),
            source: Some(source.into()),
        })
    }

    pub(crate) fn build_credentials(&self) -> rusty_s3::Credentials {
        if let Some(token) = &self.token {
            rusty_s3::Credentials::new_with_token(&self.key, &self.secret, token)
        } else {
            rusty_s3::Credentials::new(&self.key, &self.secret)
        }
    }

    pub fn from_uri(uri: &str) -> Result<Self> {
        let url = uri
            .parse::<Url>()
            .map_err(|source| ObjStoreError::InvalidConfig {
                message: format!("invalid URL '{uri}'"),
                source: Some(source.into()),
            })?;
        if url.scheme() != Self::URI_SCHEME {
            return Err(ObjStoreError::InvalidConfig {
                message: format!(
                    "invalid scheme: expected '{}', got '{}'",
                    Self::URI_SCHEME,
                    url.scheme()
                ),
                source: None,
            });
        }

        let query_pairs = url.query_pairs().collect::<Vec<_>>();

        let region = query_pairs
            .iter()
            .find(|(k, _)| k == Self::QUERY_REGION)
            .map(|(_, v)| v.to_string());

        let key = percent_encoding::percent_decode_str(url.username())
            .decode_utf8()
            .map_err(|source| ObjStoreError::InvalidConfig {
                message: "invalid percent-encoded access key in URI".to_string(),
                source: Some(source.into()),
            })?
            .into_owned();
        let secret = url.password().ok_or_else(|| ObjStoreError::InvalidConfig {
            message: "invalid url: expected '<key>:<secret>@<host>'".to_string(),
            source: None,
        })?;
        let secret = percent_encoding::percent_decode_str(secret)
            .decode_utf8()
            .map_err(|source| ObjStoreError::InvalidConfig {
                message: "invalid percent-encoded secret key in URI".to_string(),
                source: Some(source.into()),
            })?
            .into_owned();

        let mut path_segs = url
            .path_segments()
            .ok_or_else(|| ObjStoreError::InvalidConfig {
                message: format!(
                    "invalid URL '{url}': must contain bucket name as first path segment"
                ),
                source: None,
            })?;

        let path_style = {
            let raw = query_pairs
                .iter()
                .find(|(k, _)| k == Self::QUERY_STYLE)
                .map(|(_, v)| v)
                .ok_or_else(|| ObjStoreError::InvalidConfig {
                    message: "invalid url: missing ?style=[path|domain]".to_string(),
                    source: None,
                })?;
            match raw.as_ref() {
                "path" => UrlStyle::Path,
                "domain" | "virtual" => UrlStyle::VirtualHost,
                _ => {
                    return Err(ObjStoreError::InvalidConfig {
                        message: format!(
                            "invalid style: expected 'path' / 'domain' / 'virtual', got '{raw}'"
                        ),
                        source: None,
                    });
                }
            }
        };

        let bucket = path_segs
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ObjStoreError::InvalidConfig {
                message: format!(
                    "invalid URL '{url}': must contain bucket name as first path segment"
                ),
                source: None,
            })?
            .to_string();

        let path_prefix = {
            let raw = query_pairs
                .iter()
                .find(|(k, _)| k == Self::QUERY_PREFIX)
                .map(|(_, v)| v.as_ref())
                .filter(|s| !s.is_empty());
            raw.map(|prefix| prefix.to_string())
        };

        let token = query_pairs
            .iter()
            .find(|(k, _)| k == Self::QUERY_TOKEN)
            .map(|(_, v)| v.to_string())
            .filter(|s| !s.is_empty());

        let fetch_metadata_after_put = query_pairs
            .iter()
            .find(|(k, _)| k == Self::QUERY_FETCH_METADATA_AFTER_PUT)
            .map(|(_, v)| match v.as_ref() {
                "true" | "1" => Ok(true),
                "false" | "0" => Ok(false),
                other => Err(ObjStoreError::InvalidConfig {
                    message: format!(
                        "invalid fetch_metadata_after_put: expected true/false, got '{other}'"
                    ),
                    source: None,
                }),
            })
            .transpose()?
            .unwrap_or(true);

        let region = region.unwrap_or_else(|| "auto".to_string());

        let insecure = query_pairs.iter().any(|(k, _)| k == "insecure");
        let scheme = if insecure { "http" } else { "https" };
        let endpoint_path = query_pairs
            .iter()
            .find(|(k, _)| k == Self::QUERY_ENDPOINT_PATH)
            .map(|(_, v)| v.as_ref())
            .filter(|s| !s.is_empty());

        // Since the scheme can not be modified, must construct a new raw url.
        let port = if let Some(port) = url.port() {
            format!(":{port}")
        } else {
            String::new()
        };

        let target_url = format!(
            "{}://{}{}",
            scheme,
            url.host_str().ok_or_else(|| ObjStoreError::InvalidConfig {
                message: "invalid URL: missing host".to_string(),
                source: None,
            })?,
            port,
        )
        .parse::<Url>()
        .map_err(|source| ObjStoreError::InvalidConfig {
            message: "failed to build S3 endpoint URL".to_string(),
            source: Some(source.into()),
        })?;
        let mut target_url = target_url;
        if let Some(endpoint_path) = endpoint_path {
            target_url.set_path(endpoint_path);
        }

        let config = crate::S3ObjStoreConfig {
            url: target_url,
            bucket,
            region,
            path_style,
            fetch_metadata_after_put,
            key,
            secret,
            token,
            path_prefix,
        };

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_parse_uri() {
        {
            let uri1 = "s3://user:pw@host:9000/bucket?style=path";
            let config1 = S3ObjStoreConfig::from_uri(uri1).unwrap();
            assert_eq!(
                config1,
                S3ObjStoreConfig {
                    url: Url::parse("https://host:9000").unwrap(),
                    bucket: "bucket".to_string(),
                    region: "auto".to_string(),
                    path_style: UrlStyle::Path,
                    fetch_metadata_after_put: true,
                    key: "user".to_string(),
                    secret: "pw".to_string(),
                    token: None,
                    path_prefix: None,
                }
            );
        }

        {
            let uri2 = "s3://user:pw@host:9000/bucket?style=path&fetch_metadata_after_put=false";
            let config2 = S3ObjStoreConfig::from_uri(uri2).unwrap();
            assert_eq!(config2.fetch_metadata_after_put, false);

            let uri2_roundtrip = config2.build_uri().unwrap();
            assert!(uri2_roundtrip.contains("fetch_metadata_after_put=false"));
        }

        {
            let json = r#"{
                "url":"https://host:9000",
                "bucket":"bucket",
                "region":"auto",
                "path_style":"Path",
                "key":"user",
                "secret":"pw",
                "token":null,
                "path_prefix":null
            }"#;
            let config: S3ObjStoreConfig = serde_json::from_str(json).unwrap();
            assert!(config.fetch_metadata_after_put);
        }

        {
            let config = S3ObjStoreConfig {
                url: Url::parse("http://host:9000/base/path/").unwrap(),
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                path_style: UrlStyle::VirtualHost,
                fetch_metadata_after_put: false,
                key: "user:name".to_string(),
                secret: "pw/@:".to_string(),
                token: Some("session/token".to_string()),
                path_prefix: Some("/tenant/path/".to_string()),
            };

            let uri = config.build_uri().unwrap();
            let roundtrip = S3ObjStoreConfig::from_uri(&uri).unwrap();
            assert_eq!(roundtrip, config);
        }
    }
}
