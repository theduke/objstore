use anyhow::{Context as _, bail};
use rusty_s3::Bucket;
use url::Url;

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

    pub fn validate(&self) -> Result<(), anyhow::Error> {
        if !(self.url.scheme() == "http" || self.url.scheme() == "https") {
            bail!(
                "Invalid URL scheme: expected http or https, got '{}'",
                self.url.scheme()
            );
        }
        if self.bucket.trim().is_empty() {
            bail!("Bucket name must not be empty");
        }
        if self.key.trim().is_empty() {
            bail!("Access key ID must not be empty");
        }
        if self.secret.trim().is_empty() {
            bail!("Secret access key must not be empty");
        }

        Ok(())
    }

    pub fn build_uri(&self) -> Result<String, anyhow::Error> {
        let uri = format!(
            "{}://{}:{}@{}/{}",
            Self::URI_SCHEME,
            self.key,
            self.secret,
            self.url.host_str().context("Invalid URL: missing host")?,
            self.bucket,
        );
        let mut url = uri.parse::<Url>()?;
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

    pub(crate) fn build_bucket(&self) -> Result<Bucket, anyhow::Error> {
        Bucket::new(
            self.url.clone(),
            self.path_style.to_rusty(),
            self.bucket.clone(),
            self.region.clone(),
        )
        .context("could not build rusty_s3 bucket")
    }

    pub(crate) fn build_credentials(&self) -> rusty_s3::Credentials {
        if let Some(token) = &self.token {
            rusty_s3::Credentials::new_with_token(&self.key, &self.secret, token)
        } else {
            rusty_s3::Credentials::new(&self.key, &self.secret)
        }
    }

    pub fn from_uri(uri: &str) -> Result<Self, anyhow::Error> {
        let url = uri
            .parse::<Url>()
            .map_err(|e| anyhow::anyhow!("Invalid URL '{}': {}", uri, e))?;
        if url.scheme() != Self::URI_SCHEME {
            return Err(anyhow::anyhow!(
                "Invalid scheme: expected '{}', got '{}'",
                Self::URI_SCHEME,
                url.scheme()
            ));
        }

        let query_pairs = url.query_pairs().collect::<Vec<_>>();

        let region = query_pairs
            .iter()
            .find(|(k, _)| k == Self::QUERY_REGION)
            .map(|(_, v)| v.to_string());

        let key = url.authority();
        let (key, secret) = if let Some((auth, _)) = key.split_once('@') {
            let (user, pass) = auth.split_once(':').ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid authority format: expected 'user:pass@', got '{}'",
                    auth
                )
            })?;

            (user.to_string(), pass.to_string())
        } else {
            bail!("Invalid url: expected '<key>:<secret>@<host>'")
        };

        let mut path_segs = url.path_segments().ok_or_else(|| {
            anyhow::anyhow!(
                "Invalid URL '{}': must contain bucket name as first path segment",
                url
            )
        })?;

        let path_style = {
            let raw = query_pairs
                .iter()
                .find(|(k, _)| k == Self::QUERY_STYLE)
                .map(|(_, v)| v)
                .context("invalid url: missing ?style=[path|domain]")?;
            match raw.as_ref() {
                "path" => UrlStyle::Path,
                "domain" | "virtual" => UrlStyle::VirtualHost,
                _ => bail!(
                    "invalid style: expected 'path' / 'domain' / 'virtual', got '{}'",
                    raw
                ),
            }
        };

        let bucket = path_segs
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid URL '{}': must contain bucket name as first path segment",
                    url
                )
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

        let region = region.unwrap_or_else(|| "auto".to_string());

        let insecure = query_pairs.iter().any(|(k, _)| k == "insecure");
        let scheme = if insecure { "http" } else { "https" };

        // Since the scheme can not be modified, must construct a new raw url.
        let port = if let Some(port) = url.port() {
            format!(":{port}")
        } else {
            String::new()
        };

        let target_url = format!(
            "{}://{}{}",
            scheme,
            url.host_str().context("Invalid URL: missing host")?,
            port,
        )
        .parse::<Url>()?;

        let config = crate::S3ObjStoreConfig {
            url: target_url,
            bucket,
            region,
            path_style,
            key,
            secret,
            token: None,
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
                    key: "user".to_string(),
                    secret: "pw".to_string(),
                    token: None,
                    path_prefix: None,
                }
            );
        }
    }
}
