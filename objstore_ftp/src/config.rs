use anyhow::{Context, bail};
use url::Url;

/// Configuration for the FTP object store.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct FtpObjStoreConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub secure: bool,
    /// Optional prefix prepended to all keys.
    pub path_prefix: Option<String>,
}

impl FtpObjStoreConfig {
    pub const FTP_SCHEME: &'static str = "ftp";
    pub const FTPS_SCHEME: &'static str = "ftps";
    const DEFAULT_FTP_PORT: u16 = 21;

    /// Parse configuration from a URL.
    pub fn from_url(url: &Url) -> Result<Self, anyhow::Error> {
        let scheme = url.scheme();
        let secure = match scheme {
            Self::FTP_SCHEME => false,
            Self::FTPS_SCHEME => true,
            _ => bail!("Invalid scheme: expected ftp or ftps, got '{scheme}'"),
        };

        let host = url
            .host_str()
            .context("Invalid URL: missing host")?
            .to_string();
        let port = url
            .port_or_known_default()
            .unwrap_or(Self::DEFAULT_FTP_PORT);
        let user = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();
        let path_prefix = {
            let path = url.path().trim_matches('/');
            if path.is_empty() {
                None
            } else {
                Some(path.to_string())
            }
        };

        Ok(Self {
            host,
            port,
            user,
            password,
            secure,
            path_prefix,
        })
    }

    pub fn from_uri(uri: &str) -> Result<Self, anyhow::Error> {
        let url = Url::parse(uri)?;
        Self::from_url(&url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ftp_parse_uri() {
        {
            let uri1 = "ftp://user:password@127.0.0.1";
            let config1 = FtpObjStoreConfig::from_uri(uri1).unwrap();
            assert_eq!(
                config1,
                FtpObjStoreConfig {
                    host: "127.0.0.1".to_string(),
                    port: 21,
                    user: "user".to_string(),
                    password: "password".to_string(),
                    secure: false,
                    path_prefix: None,
                }
            );
        }
    }
}
