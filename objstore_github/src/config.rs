use anyhow::{Context as _, bail};
use serde::{Deserialize, Serialize};
use url::Url;

/// Configuration for the GitHub-backed object store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GithubObjStoreConfig {
    pub host: String,
    pub owner: String,
    pub repo: String,
    pub token: String,
    pub branch: Option<String>,
    pub path_prefix: Option<String>,
    pub api_base: Url,
    pub raw_base: Url,
}

impl GithubObjStoreConfig {
    pub const URI_SCHEME: &'static str = "github";

    const QUERY_BRANCH: &'static str = "branch";
    const QUERY_PREFIX: &'static str = "prefix";
    const QUERY_API_BASE: &'static str = "api_base";
    const QUERY_RAW_BASE: &'static str = "raw_base";

    pub fn validate(&self) -> Result<(), anyhow::Error> {
        if self.token.trim().is_empty() {
            bail!("api token must not be empty");
        }
        if self.host.trim().is_empty() {
            bail!("host must not be empty");
        }
        if self.owner.trim().is_empty() {
            bail!("repository owner must not be empty");
        }
        if self.repo.trim().is_empty() {
            bail!("repository name must not be empty");
        }
        Ok(())
    }

    pub fn build_uri(&self) -> Result<String, anyhow::Error> {
        let mut url = Url::parse(&format!("{}://{}", Self::URI_SCHEME, self.host))
            .with_context(|| format!("invalid host: {}", self.host))?;

        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("cannot set path segments"))?;
            segments.clear();
            segments.push(&self.owner);
            segments.push(&self.repo);
        }
        url.set_username(&self.token)
            .map_err(|_| anyhow::anyhow!("failed to set token in URI"))?;

        {
            let mut pairs = url.query_pairs_mut();
            if let Some(branch) = &self.branch
                && !branch.trim().is_empty()
            {
                pairs.append_pair(Self::QUERY_BRANCH, branch);
            }
            if let Some(prefix) = &self.path_prefix
                && !prefix.trim().is_empty()
            {
                pairs.append_pair(Self::QUERY_PREFIX, prefix);
            }

            let (default_api, default_raw) = default_endpoints(&self.host)?;
            if self.api_base != default_api {
                pairs.append_pair(Self::QUERY_API_BASE, self.api_base.as_str());
            }
            if self.raw_base != default_raw {
                pairs.append_pair(Self::QUERY_RAW_BASE, self.raw_base.as_str());
            }
        }

        Ok(url.to_string())
    }

    pub fn build_safe_uri(&self) -> Result<Url, anyhow::Error> {
        let mut url = Url::parse(&format!("{}://{}", Self::URI_SCHEME, self.host))
            .with_context(|| format!("invalid host: {}", self.host))?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("cannot set path segments"))?;
            segments.clear();
            segments.push(&self.owner);
            segments.push(&self.repo);
        }

        {
            let mut pairs = url.query_pairs_mut();
            if let Some(branch) = &self.branch
                && !branch.trim().is_empty()
            {
                pairs.append_pair(Self::QUERY_BRANCH, branch);
            }
            if let Some(prefix) = &self.path_prefix
                && !prefix.trim().is_empty()
            {
                pairs.append_pair(Self::QUERY_PREFIX, prefix);
            }
            let (default_api, default_raw) = default_endpoints(&self.host)?;
            if self.api_base != default_api {
                pairs.append_pair(Self::QUERY_API_BASE, self.api_base.as_str());
            }
            if self.raw_base != default_raw {
                pairs.append_pair(Self::QUERY_RAW_BASE, self.raw_base.as_str());
            }
        }

        Ok(url)
    }

    /// Parse a GitHub object store URI into a [`GithubObjStoreConfig`].
    ///
    /// # Format
    /// `github://<token>@<host>/<owner>/<repo>[?branch=<branch>&prefix=<path_prefix>&api_base=<api_base_url>&raw_base=<raw_base_url>]`
    ///
    /// # Examples
    /// * `github://ghp_example@github.com/my-org/my-repo`
    /// * `github://token@github.example.com/team/app?branch=main&prefix=data`
    /// * `github://token@custom.host/org/repo?api_base=https%3A%2F%2Fapi.custom.host%2F&raw_base=https%3A%2F%2Fraw.custom.host%2F`
    ///
    /// # Parameters
    /// * `token`: GitHub personal access token placed in the username section before `@`.
    /// * `host`: Domain providing the GitHub API (for example `github.com` or an enterprise host).
    /// * `owner`: Repository owner taken from the first path segment.
    /// * `repo`: Repository name taken from the second path segment.
    /// * `branch` (optional): Query parameter `branch` selecting the target branch.
    /// * `path_prefix` (optional): Query parameter `prefix` applied to all object keys.
    /// * `api_base` (optional): Query parameter `api_base` overriding the default API endpoint.
    /// * `raw_base` (optional): Query parameter `raw_base` overriding the default raw content endpoint.
    pub fn from_uri(uri: &str) -> Result<Self, anyhow::Error> {
        let url =
            Url::parse(uri).with_context(|| format!("invalid URL '{}': failed to parse", uri))?;
        if url.scheme() != Self::URI_SCHEME {
            bail!(
                "invalid scheme: expected '{}', got '{}'",
                Self::URI_SCHEME,
                url.scheme()
            );
        }

        let token = url.username();
        if token.is_empty() {
            bail!("github uri must contain an api token in the username field");
        }
        let token = token.to_string();

        let host = url
            .host_str()
            .context("github uri must include a host (e.g. github.com)")?
            .to_string();

        let mut path_segments = url
            .path_segments()
            .ok_or_else(|| anyhow::anyhow!("github uri must include path segments"))?;
        let owner = path_segments
            .next()
            .filter(|seg| !seg.is_empty())
            .ok_or_else(|| anyhow::anyhow!("github uri must include repository owner"))?
            .to_string();
        let repo = path_segments
            .next()
            .filter(|seg| !seg.is_empty())
            .ok_or_else(|| anyhow::anyhow!("github uri must include repository name"))?
            .to_string();

        let query_pairs: Vec<_> = url.query_pairs().collect();
        let branch = query_pairs
            .iter()
            .find(|(k, _)| k == Self::QUERY_BRANCH)
            .map(|(_, v)| v.to_string())
            .filter(|v| !v.trim().is_empty());
        let path_prefix = query_pairs
            .iter()
            .find(|(k, _)| k == Self::QUERY_PREFIX)
            .map(|(_, v)| v.to_string())
            .filter(|v| !v.trim().is_empty());

        let api_base = query_pairs
            .iter()
            .find(|(k, _)| k == Self::QUERY_API_BASE)
            .map(|(_, v)| v.to_string());
        let raw_base = query_pairs
            .iter()
            .find(|(k, _)| k == Self::QUERY_RAW_BASE)
            .map(|(_, v)| v.to_string());

        let (default_api, default_raw) = default_endpoints(&host)?;
        let api_base = match api_base {
            Some(url) => {
                normalize_base_url(Url::parse(&url).context("invalid api_base override")?)?
            }
            None => default_api,
        };
        let raw_base = match raw_base {
            Some(url) => {
                normalize_base_url(Url::parse(&url).context("invalid raw_base override")?)?
            }
            None => default_raw,
        };

        let config = GithubObjStoreConfig {
            host,
            owner,
            repo,
            token,
            branch,
            path_prefix,
            api_base,
            raw_base,
        };

        config.validate()?;
        Ok(config)
    }
}

fn normalize_base_url(mut url: Url) -> Result<Url, anyhow::Error> {
    if url.cannot_be_a_base() {
        bail!("url '{}' cannot be used as a base", url);
    }
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path().trim_end_matches('/')));
    }
    Ok(url)
}

fn default_endpoints(host: &str) -> Result<(Url, Url), anyhow::Error> {
    if host.eq_ignore_ascii_case("github.com") {
        let api = Url::parse("https://api.github.com/").expect("valid github api url");
        let raw = Url::parse("https://raw.githubusercontent.com/").expect("valid github raw url");
        Ok((api, raw))
    } else {
        let api = Url::parse(&format!("https://{host}/api/v3/"))
            .with_context(|| format!("failed to build api base url for host '{host}'"))?;
        let raw = Url::parse(&format!("https://{host}/raw/"))
            .with_context(|| format!("failed to build raw base url for host '{host}'"))?;
        Ok((api, raw))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn parse_default_host() {
        let uri = "github://ghp_token@github.com/my-org/my-repo?branch=dev&prefix=data";
        let cfg = GithubObjStoreConfig::from_uri(uri).unwrap();
        assert_eq!(cfg.host, "github.com");
        assert_eq!(cfg.owner, "my-org");
        assert_eq!(cfg.repo, "my-repo");
        assert_eq!(cfg.branch.as_deref(), Some("dev"));
        assert_eq!(cfg.path_prefix.as_deref(), Some("data"));
        assert_eq!(cfg.api_base.as_str(), "https://api.github.com/");
        assert_eq!(cfg.raw_base.as_str(), "https://raw.githubusercontent.com/");
        assert_eq!(cfg.token, "ghp_token");

        let built = cfg.build_uri().unwrap();
        assert_eq!(GithubObjStoreConfig::from_uri(&built).unwrap(), cfg);
        let safe = cfg.build_safe_uri().unwrap();
        assert_eq!(
            safe.as_str(),
            "github://github.com/my-org/my-repo?branch=dev&prefix=data"
        );
    }

    #[test]
    fn parse_enterprise_host() {
        let uri = "github://token@github.example.com/team/app?branch=main";
        let cfg = GithubObjStoreConfig::from_uri(uri).unwrap();
        assert_eq!(cfg.host, "github.example.com");
        assert_eq!(cfg.api_base.as_str(), "https://github.example.com/api/v3/");
        assert_eq!(cfg.raw_base.as_str(), "https://github.example.com/raw/");
    }

    #[test]
    fn parse_with_overrides() {
        let uri = "github://token@custom.host/org/repo?api_base=https%3A%2F%2Fapi.custom.host%2F&raw_base=https%3A%2F%2Fraw.custom.host%2F";
        let cfg = GithubObjStoreConfig::from_uri(uri).unwrap();
        assert_eq!(cfg.api_base.as_str(), "https://api.custom.host/");
        assert_eq!(cfg.raw_base.as_str(), "https://raw.custom.host/");
    }
}
