use anyhow::Context as _;
use url::Url;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SftpObjStoreConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub path_prefix: Option<String>,
    /// Maximum number of concurrent SFTP sessions per SSH connection.
    /// Defaults to 4 if not specified.
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
}

impl SftpObjStoreConfig {
    pub fn from_uri(uri: &str) -> Result<Self, anyhow::Error> {
        let url = Url::parse(uri)?;
        if url.scheme() != "sftp" {
            return Err(anyhow::anyhow!("invalid scheme: {}", url.scheme()));
        }
        let host = url.host_str().context("missing host in url")?.to_string();
        let port = url.port().unwrap_or(22);
        let username = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();
        let path = url.path().trim_start_matches('/').to_string();
        let path_prefix = if path.is_empty() { None } else { Some(path) };
        Ok(Self {
            host,
            port,
            username,
            password,
            path_prefix,
            pool_size: default_pool_size(),
        })
    }
}

fn default_pool_size() -> usize {
    4
}
