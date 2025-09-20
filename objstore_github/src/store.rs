use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use anyhow::{Context as _, bail};
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::{Bytes, BytesMut};
use futures::{TryStreamExt, stream};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC};
use reqwest::{
    Client, Response, StatusCode,
    header::{
        ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, HeaderMap, HeaderValue, PRAGMA,
        USER_AGENT,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{
    OffsetDateTime,
    format_description::well_known::{Rfc2822, Rfc3339},
};
use tokio::sync::{Mutex, OnceCell};
use url::Url;

use objstore::{
    Copy, DataSource, DownloadUrlArgs, KeyPage, ListArgs, ObjStore, ObjectMeta, ObjectMetaPage,
    Put, ValueStream,
};
use serde_json::Value;

use crate::GithubObjStoreConfig;

const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// [`ObjStore`] implementation backed by GitHub repository contents.
#[derive(Clone)]
pub struct GithubObjStore {
    state: Arc<State>,
}

impl std::fmt::Debug for GithubObjStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubObjStore")
            .field("owner", &self.state.owner)
            .field("repo", &self.state.repo)
            .field("branch", &self.state.branch)
            .finish()
    }
}

#[derive(Debug)]
struct State {
    client: Client,
    owner: String,
    repo: String,
    endpoints: Endpoints,
    safe_uri: Url,
    branch: BranchState,
    path_prefix: Option<String>,
    list_overlay: Mutex<HashMap<String, ListOverlayEntry>>,
}

#[derive(Debug, Clone)]
struct Endpoints {
    api: Url,
    raw: Url,
}

#[derive(Debug)]
struct BranchState {
    configured: Option<String>,
    resolved: OnceCell<String>,
}

impl BranchState {
    fn new(branch: Option<String>) -> Self {
        Self {
            configured: branch,
            resolved: OnceCell::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct ContentInfo {
    repo_path: String,
    sha: String,
    meta: ObjectMeta,
    inline_content: Option<Bytes>,
}

#[derive(Clone, Debug)]
struct TreeEntryObject {
    key: String,
    sha: String,
    size: Option<u64>,
}

#[derive(Debug, Clone)]
enum ListOverlayEntry {
    Present { sha: String, size: Option<u64> },
    Deleted,
}

#[derive(Deserialize)]
struct GithubContentFile {
    path: String,
    sha: String,
    size: Option<u64>,
    #[serde(rename = "type")]
    kind: String,
    content: Option<String>,
    encoding: Option<String>,
    #[serde(default)]
    truncated: bool,
}

#[derive(Deserialize)]
struct GithubPutResponse {
    content: Option<GithubContentFile>,
    commit: Option<GithubCommitInfo>,
}

#[derive(Deserialize)]
struct GithubCommitInfo {
    sha: String,
    committer: Option<GithubCommitActor>,
    author: Option<GithubCommitActor>,
}

#[derive(Deserialize)]
struct GithubCommitActor {
    date: Option<String>,
}

#[derive(Deserialize)]
struct GithubTreeResponse {
    tree: Vec<GithubTreeEntry>,
}

#[derive(Deserialize)]
struct GithubTreeEntry {
    path: String,
    sha: String,
    #[serde(rename = "type")]
    kind: String,
    size: Option<u64>,
}

#[derive(Deserialize)]
struct GithubBranchResponse {
    commit: GithubBranchCommit,
}

#[derive(Deserialize)]
struct GithubBranchCommit {
    commit: GithubBranchCommitInfo,
}

#[derive(Deserialize)]
struct GithubBranchCommitInfo {
    tree: GithubBranchTree,
}

#[derive(Deserialize)]
struct GithubBranchTree {
    sha: String,
}

#[derive(Deserialize)]
struct GithubRepoResponse {
    default_branch: Option<String>,
}

#[derive(Deserialize)]
struct GithubErrorResponse {
    message: Option<String>,
    errors: Option<Vec<GithubErrorDetail>>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum GithubErrorDetail {
    Message {
        message: Option<String>,
    },
    Field {
        resource: Option<String>,
        field: Option<String>,
        code: Option<String>,
        message: Option<String>,
    },
}

impl GithubObjStore {
    /// Provider identifier (see [`ObjStore::kind`]).
    pub const KIND: &'static str = "objstore.github";

    const USER_AGENT: &'static str = "objstore-github/0.1";

    pub fn new(config: GithubObjStoreConfig) -> Result<Self, anyhow::Error> {
        let client = Self::build_default_client(&config)?;
        Self::new_with_client(config, client)
    }

    fn build_default_client(config: &GithubObjStoreConfig) -> Result<Client, anyhow::Error> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static(Self::USER_AGENT));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("token {}", config.token))
                .context("invalid token for Authorization header")?,
        );

        Client::builder()
            .default_headers(headers)
            .build()
            .context("failed to build reqwest client")
    }

    pub fn new_with_client(
        config: GithubObjStoreConfig,
        client: Client,
    ) -> Result<Self, anyhow::Error> {
        config.validate()?;

        let safe_uri = config.build_safe_uri()?;
        let branch_state = BranchState::new(config.branch.clone());
        let path_prefix = sanitize_prefix(config.path_prefix.as_deref());

        let state = State {
            client,
            owner: config.owner,
            repo: config.repo,
            endpoints: Endpoints {
                api: config.api_base,
                raw: config.raw_base,
            },
            safe_uri,
            branch: branch_state,
            path_prefix,
            list_overlay: Mutex::new(HashMap::new()),
        };

        Ok(Self {
            state: Arc::new(state),
        })
    }

    fn build_repo_path(&self, key: &str) -> Result<String, anyhow::Error> {
        validate_key(key)?;
        let key = key.trim_start_matches('/');
        let repo_path = if let Some(prefix) = &self.state.path_prefix {
            format!("{}/{}", prefix, key)
        } else {
            key.to_string()
        };
        Ok(repo_path)
    }

    fn prune_repo_path(&self, path: &str) -> Option<String> {
        let path = path.trim_start_matches('/');
        if let Some(prefix) = &self.state.path_prefix {
            let prefix_with_sep = format!("{prefix}/");
            path.strip_prefix(&prefix_with_sep)
                .map(|stripped| stripped.to_string())
        } else {
            Some(path.to_string())
        }
    }

    async fn branch(&self) -> Result<String, anyhow::Error> {
        if let Some(branch) = &self.state.branch.configured {
            return Ok(branch.clone());
        }
        if let Some(branch) = self.state.branch.resolved.get() {
            return Ok(branch.clone());
        }
        let branch = fetch_default_branch(&self.state).await?;
        let _ = self.state.branch.resolved.set(branch.clone());
        Ok(branch)
    }

    fn contents_url(&self, repo_path: &str) -> Result<Url, anyhow::Error> {
        let encoded_path = encode_repo_path(repo_path);
        let path = format!(
            "repos/{}/{}/contents/{}",
            encode_segment(&self.state.owner),
            encode_segment(&self.state.repo),
            encoded_path
        );
        self.state
            .endpoints
            .api
            .join(&path)
            .with_context(|| format!("failed to build contents url for path '{}'", repo_path))
    }

    fn tree_url(&self, reference: &str) -> Result<Url, anyhow::Error> {
        let path = format!(
            "repos/{}/{}/git/trees/{}",
            encode_segment(&self.state.owner),
            encode_segment(&self.state.repo),
            encode_segment(reference)
        );
        self.state
            .endpoints
            .api
            .join(&path)
            .with_context(|| format!("failed to build tree url for ref '{}'", reference))
    }

    fn branch_url(&self, reference: &str) -> Result<Url, anyhow::Error> {
        let path = format!(
            "repos/{}/{}/branches/{}",
            encode_segment(&self.state.owner),
            encode_segment(&self.state.repo),
            encode_segment(reference)
        );
        self.state
            .endpoints
            .api
            .join(&path)
            .with_context(|| format!("failed to build branch url for ref '{}'", reference))
    }

    async fn fetch_branch_metadata(
        &self,
        reference: &str,
    ) -> Result<Option<GithubBranchResponse>, anyhow::Error> {
        let url = self.branch_url(reference)?;
        let res = self
            .state
            .client
            .get(url)
            .header(CACHE_CONTROL, "no-cache")
            .header(PRAGMA, "no-cache")
            .send()
            .await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let res = error_for_status(res).await?;
        let branch: GithubBranchResponse = res
            .json()
            .await
            .context("failed to decode github branch response")?;
        Ok(Some(branch))
    }

    fn raw_url(&self, branch: &str, repo_path: &str) -> Result<Url, anyhow::Error> {
        let mut url = self.state.endpoints.raw.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("raw base url does not support path segments"))?;
            segments.extend([self.state.owner.as_str(), self.state.repo.as_str(), branch]);
            for segment in repo_path.split('/') {
                if !segment.is_empty() {
                    segments.push(segment);
                }
            }
        }
        Ok(url)
    }

    async fn get_content_info(&self, key: &str) -> Result<Option<ContentInfo>, anyhow::Error> {
        if self.is_deleted_locally(key).await {
            return Ok(None);
        }
        let repo_path = self.build_repo_path(key)?;
        let branch = self.branch().await?;

        let mut url = self.contents_url(&repo_path)?;
        url.query_pairs_mut().append_pair("ref", &branch);

        let res = self.state.client.get(url).send().await?;
        if res.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let headers = res.headers().clone();
        let res = error_for_status(res).await?;
        let file: GithubContentFile = res
            .json()
            .await
            .context("failed to decode github contents response")?;

        if file.kind != "file" {
            bail!("github path '{}' is not a file", key);
        }

        let mut meta = ObjectMeta::new(key.to_string());
        meta.etag = Some(file.sha.clone());
        meta.size = file.size;
        meta.mime_type = headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|s| s.to_string());
        if let Some(updated) = headers.get("Last-Modified").and_then(parse_http_date) {
            meta.updated_at = Some(updated);
        }
        meta.extra
            .insert("sha".to_string(), Value::String(file.sha.clone()));

        let mut inline_content = None;
        if !file.truncated
            && let (Some(content), Some(encoding)) =
                (file.content.as_deref(), file.encoding.as_deref())
            && encoding.eq_ignore_ascii_case("base64")
        {
            let cleaned: String = content.chars().filter(|c| !c.is_whitespace()).collect();
            let decoded = BASE64_STANDARD
                .decode(cleaned.as_bytes())
                .context("failed to decode base64 content from GitHub API")?;
            let bytes = Bytes::from(decoded);
            if meta.size.is_none() {
                meta.size = Some(bytes.len() as u64);
            }
            inline_content = Some(bytes);
        }

        Ok(Some(ContentInfo {
            repo_path,
            sha: file.sha,
            meta,
            inline_content,
        }))
    }

    async fn is_deleted_locally(&self, key: &str) -> bool {
        let overlay = self.state.list_overlay.lock().await;
        matches!(overlay.get(key), Some(ListOverlayEntry::Deleted))
    }

    async fn download_raw_response(
        &self,
        branch: &str,
        repo_path: &str,
    ) -> Result<Response, anyhow::Error> {
        let url = self.raw_url(branch, repo_path)?;
        let res = self
            .state
            .client
            .get(url)
            .header(ACCEPT, "application/octet-stream")
            .send()
            .await?;
        if res.status() == StatusCode::NOT_FOUND {
            bail!("github object not found for path '{}'", repo_path);
        }
        error_for_status(res).await
    }

    async fn fetch_tree(&self, reference: &str) -> Result<Vec<TreeEntryObject>, anyhow::Error> {
        let branch = match self.fetch_branch_metadata(reference).await? {
            Some(branch) => branch,
            None => return Ok(Vec::new()),
        };
        let tree_sha = branch.commit.commit.tree.sha.clone();
        let url = self.tree_url(&tree_sha)?;
        let res = self
            .state
            .client
            .get(url)
            .query(&[("recursive", "1")])
            .header(CACHE_CONTROL, "no-cache")
            .header(PRAGMA, "no-cache")
            .send()
            .await?;
        if res.status() == StatusCode::NOT_FOUND {
            // A fine-grained token might lack `contents:read` for empty
            // repositories, causing the tree API to 404 even though the
            // branch exists. Treat this as an empty tree so higher-level
            // operations can proceed.
            return Ok(Vec::new());
        }
        let res = error_for_status(res).await?;
        let tree: GithubTreeResponse = res
            .json()
            .await
            .context("failed to decode github tree response")?;

        let mut out = Vec::new();
        for entry in tree.tree.into_iter() {
            if entry.kind != "blob" {
                continue;
            }
            if let Some(key) = self.prune_repo_path(&entry.path) {
                out.push(TreeEntryObject {
                    key,
                    sha: entry.sha,
                    size: entry.size,
                });
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        self.apply_list_overlay(out).await
    }

    async fn apply_list_overlay(
        &self,
        entries: Vec<TreeEntryObject>,
    ) -> Result<Vec<TreeEntryObject>, anyhow::Error> {
        let mut map: HashMap<String, TreeEntryObject> = entries
            .into_iter()
            .map(|entry| (entry.key.clone(), entry))
            .collect();

        let mut overlay = self.state.list_overlay.lock().await;
        let mut remove_keys = Vec::new();

        for (key, overlay_entry) in overlay.iter() {
            match overlay_entry {
                ListOverlayEntry::Present { sha, size } => {
                    if let Some(existing) = map.get(key)
                        && &existing.sha == sha
                    {
                        remove_keys.push(key.clone());
                        continue;
                    }
                    map.insert(
                        key.clone(),
                        TreeEntryObject {
                            key: key.clone(),
                            sha: sha.clone(),
                            size: *size,
                        },
                    );
                }
                ListOverlayEntry::Deleted => {
                    if map.remove(key).is_none() {
                        remove_keys.push(key.clone());
                    }
                }
            }
        }

        for key in remove_keys {
            overlay.remove(&key);
        }

        drop(overlay);

        let mut entries: Vec<_> = map.into_values().collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(entries)
    }

    async fn enforce_put_conditions(
        &self,
        key: &str,
        conditions: &objstore::Conditions,
        existing: Option<&ContentInfo>,
    ) -> Result<(), anyhow::Error> {
        if let Some(if_match) = &conditions.if_match {
            match if_match {
                objstore::MatchValue::Any => {
                    if existing.is_none() {
                        bail!("put condition failed for '{key}': expected existing object");
                    }
                }
                objstore::MatchValue::Tags(tags) => {
                    let Some(current) = existing else {
                        bail!("put condition failed for '{key}': object missing");
                    };
                    if !tags.iter().any(|tag| tag == &current.sha) {
                        bail!("put condition failed for '{key}': ETag mismatch");
                    }
                }
            }
        }

        if let Some(if_none_match) = &conditions.if_none_match {
            match if_none_match {
                objstore::MatchValue::Any => {
                    if existing.is_some() {
                        bail!("put condition failed for '{key}': object already exists");
                    }
                }
                objstore::MatchValue::Tags(tags) => {
                    if let Some(current) = existing
                        && tags.iter().any(|tag| tag == &current.sha)
                    {
                        bail!(
                            "put condition failed for '{key}': existing object matches forbidden tag"
                        );
                    }
                }
            }
        }

        if conditions.if_modified_since.is_some() {
            bail!("put condition 'if_modified_since' is not supported by the GitHub backend");
        }

        if let Some(limit) = conditions.if_unmodified_since
            && let Some(existing) = existing
            && let Some(updated) = existing.meta.updated_at
            && updated > limit
        {
            bail!("put condition failed for '{key}': object modified after required timestamp");
        }

        Ok(())
    }

    async fn list_objects(
        &self,
        args: &ListArgs,
    ) -> Result<(Vec<TreeEntryObject>, Option<Vec<String>>, Option<String>), anyhow::Error> {
        let branch = self.branch().await?;
        let all = self.fetch_tree(&branch).await?;
        let cursor = args.cursor().map(ToOwned::to_owned);
        let prefix = args.prefix().map(ToOwned::to_owned).unwrap_or_default();
        let delimiter = args.delimiter().map(ToOwned::to_owned);
        let limit = args.limit().unwrap_or(1_000) as usize;

        let mut directories = BTreeSet::new();
        let mut filtered = Vec::new();

        for entry in all.into_iter() {
            if let Some(cursor) = &cursor
                && entry.key <= *cursor
            {
                continue;
            }
            if !prefix.is_empty() && !entry.key.starts_with(&prefix) {
                continue;
            }

            if let Some(delim) = &delimiter
                && let Some(remainder) = entry.key.strip_prefix(&prefix)
                && let Some(idx) = remainder.find(delim)
            {
                let dir = &remainder[..idx];
                if !dir.is_empty() {
                    directories.insert(format!("{}{}{}", prefix, dir, delim));
                }
                continue;
            }

            filtered.push(entry);
        }

        let has_more = filtered.len() > limit;
        if has_more {
            filtered.truncate(limit);
        }

        let prefixes = if directories.is_empty() {
            None
        } else {
            Some(directories.into_iter().collect::<Vec<_>>())
        };

        let next_cursor = if has_more {
            filtered.last().map(|entry| entry.key.clone())
        } else {
            None
        };

        Ok((filtered, prefixes, next_cursor))
    }

    async fn delete_simple(&self, key: &str) -> Result<(), anyhow::Error> {
        let info = self
            .get_content_info(key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("key '{}' not found", key))?;
        self.perform_delete(key, info).await
    }

    async fn perform_delete(&self, key: &str, info: ContentInfo) -> Result<(), anyhow::Error> {
        let branch = self.branch().await?;
        let url = self.contents_url(&info.repo_path)?;
        let payload = DeleteRequest::new(&branch, &info.sha, key);

        let res = self.state.client.delete(url).json(&payload).send().await?;
        let _ = error_for_status(res).await?;
        {
            let mut overlay = self.state.list_overlay.lock().await;
            overlay.insert(key.to_string(), ListOverlayEntry::Deleted);
        }
        Ok(())
    }

    async fn put_object(&self, mut put: Put) -> Result<ObjectMeta, anyhow::Error> {
        let key = put.key.clone();
        let data = match put.data {
            DataSource::Data(bytes) => bytes,
            DataSource::Stream(stream) => stream
                .try_fold(BytesMut::new(), |mut acc, chunk| async move {
                    acc.extend_from_slice(&chunk);
                    Ok(acc)
                })
                .await?
                .freeze(),
        };
        let info = self.get_content_info(&key).await?;
        self.enforce_put_conditions(&key, &put.conditions, info.as_ref())
            .await?;

        let branch = self.branch().await?;
        let repo_path = self.build_repo_path(&key)?;
        let url = self.contents_url(&repo_path)?;
        let encoded = BASE64_STANDARD.encode(data.as_ref());
        let sha_ref = info.as_ref().map(|existing| existing.sha.as_str());
        let payload = PutRequest::new(&branch, &key, encoded, sha_ref);

        let res = self.state.client.put(url).json(&payload).send().await?;
        let res = error_for_status(res).await?;
        let resp: GithubPutResponse = res
            .json()
            .await
            .context("failed to decode github put response")?;

        let mut meta = ObjectMeta::new(key);
        if let Some(content) = resp.content {
            meta.key = self
                .prune_repo_path(&content.path)
                .unwrap_or(meta.key.clone());
            meta.etag = Some(content.sha.clone());
            meta.size = content.size;
            meta.extra
                .insert("sha".to_string(), Value::String(content.sha));
        }
        if let Some(commit) = resp.commit {
            if let Some(date) = commit
                .committer
                .and_then(|c| c.date)
                .or_else(|| commit.author.and_then(|c| c.date))
                && let Ok(parsed) = OffsetDateTime::parse(&date, &Rfc3339)
            {
                meta.updated_at = Some(parsed);
                meta.created_at = Some(parsed);
            }
            meta.extra
                .insert("commit_sha".to_string(), Value::String(commit.sha));
        }
        meta.mime_type = put.mime_type.take();
        meta.hash_sha256 = Some(Sha256::digest(&data).into());

        if let Some(sha) = meta
            .extra
            .get("sha")
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned)
        {
            let mut overlay = self.state.list_overlay.lock().await;
            overlay.insert(
                meta.key.clone(),
                ListOverlayEntry::Present {
                    sha,
                    size: meta.size,
                },
            );
        }

        Ok(meta)
    }

    async fn perform_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        let (data, source_meta) = self
            .get_with_meta(&copy.source_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source key '{}' not found", copy.source_key))?;

        let mut put = Put::new(copy.target_key.clone(), DataSource::Data(data));
        put.conditions = copy.conditions;
        put.mime_type = source_meta.mime_type.clone();
        self.put_object(put).await
    }
}

#[async_trait]
impl ObjStore for GithubObjStore {
    fn kind(&self) -> &str {
        Self::KIND
    }

    fn safe_uri(&self) -> &Url {
        &self.state.safe_uri
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        // Discover the default branch only when required. Some fine-grained
        // access tokens lack the permissions needed to read repository
        // metadata, which would make an unconditional metadata request fail
        // even though all other store operations succeed.
        if self.state.branch.configured.is_none() && self.state.branch.resolved.get().is_none() {
            let default_branch = fetch_default_branch(&self.state).await?;
            let _ = self.state.branch.resolved.set(default_branch);
        }

        // Verify branch access explicitly.
        let branch = self.branch().await?;
        let _ = self
            .fetch_branch_metadata(&branch)
            .await?
            .ok_or_else(|| anyhow::anyhow!(format!("github branch '{}' not found", branch)))?;
        Ok(())
    }

    async fn meta(&self, key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        Ok(self.get_content_info(key).await?.map(|info| info.meta))
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, anyhow::Error> {
        Ok(self.get_with_meta(key).await?.map(|(bytes, _)| bytes))
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        Ok(self
            .get_stream_with_meta(key)
            .await?
            .map(|(_, stream)| stream))
    }

    async fn get_with_meta(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>, anyhow::Error> {
        let info = match self.get_content_info(key).await? {
            Some(info) => info,
            None => return Ok(None),
        };
        let mut meta = info.meta;

        let bytes = if let Some(bytes) = info.inline_content.clone() {
            if meta.size.is_none() {
                meta.size = Some(bytes.len() as u64);
            }
            bytes
        } else {
            let branch = self.branch().await?;
            let response = self.download_raw_response(&branch, &info.repo_path).await?;
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(|s| s.to_string());
            let bytes = response.bytes().await?;
            if meta.mime_type.is_none() {
                meta.mime_type = content_type;
            }
            if meta.size.is_none() {
                meta.size = Some(bytes.len() as u64);
            }
            bytes
        };

        meta.hash_sha256 = Some(Sha256::digest(&bytes).into());

        Ok(Some((bytes, meta)))
    }

    async fn get_stream_with_meta(
        &self,
        key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        let info = match self.get_content_info(key).await? {
            Some(info) => info,
            None => return Ok(None),
        };
        let mut meta = info.meta;
        if let Some(bytes) = info.inline_content {
            if meta.size.is_none() {
                meta.size = Some(bytes.len() as u64);
            }
            meta.hash_sha256 = Some(Sha256::digest(&bytes).into());
            let stream = stream::once(async move { Ok::<Bytes, anyhow::Error>(bytes) });
            return Ok(Some((meta, Box::pin(stream))));
        }

        let branch = self.branch().await?;
        let response = self.download_raw_response(&branch, &info.repo_path).await?;
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|s| s.to_string());
        let stream = response.bytes_stream().map_err(anyhow::Error::from);
        if meta.mime_type.is_none() {
            meta.mime_type = content_type;
        }

        Ok(Some((meta, Box::pin(stream))))
    }

    async fn generate_download_url(
        &self,
        _args: DownloadUrlArgs,
    ) -> Result<Option<Url>, anyhow::Error> {
        Ok(None)
    }

    async fn send_put(&self, put: Put) -> Result<ObjectMeta, anyhow::Error> {
        self.put_object(put).await
    }

    async fn send_copy(&self, copy: Copy) -> Result<ObjectMeta, anyhow::Error> {
        self.perform_copy(copy).await
    }

    async fn delete(&self, key: &str) -> Result<(), anyhow::Error> {
        self.delete_simple(key).await
    }

    async fn delete_prefix(&self, prefix: &str) -> Result<(), anyhow::Error> {
        loop {
            let mut args = ListArgs::new().with_limit(256);
            if !prefix.is_empty() {
                args = args.with_prefix(prefix.to_string());
            }
            let page = self.list_keys(args).await?;
            if page.items.is_empty() {
                break;
            }
            for key in page.items {
                self.delete(&key).await?;
            }
        }
        Ok(())
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error> {
        let (objects, prefixes, next_cursor) = self.list_objects(&args).await?;
        let mut items = Vec::new();
        for entry in objects.iter() {
            let mut meta = ObjectMeta::new(entry.key.clone());
            meta.size = entry.size;
            meta.etag = Some(entry.sha.clone());
            meta.extra
                .insert("sha".to_string(), Value::String(entry.sha.clone()));
            items.push(meta);
        }
        Ok(ObjectMetaPage {
            items,
            next_cursor,
            prefixes,
        })
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        let (objects, _, next_cursor) = self.list_objects(&args).await?;
        let mut items = Vec::with_capacity(objects.len());
        for entry in objects.iter() {
            items.push(entry.key.clone());
        }
        Ok(KeyPage { items, next_cursor })
    }
}

fn encode_segment(segment: &str) -> String {
    percent_encoding::utf8_percent_encode(segment, PATH_SEGMENT_ENCODE_SET).to_string()
}

fn encode_repo_path(path: &str) -> String {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .map(encode_segment)
        .collect::<Vec<_>>()
        .join("/")
}

fn sanitize_prefix(prefix: Option<&str>) -> Option<String> {
    let prefix = prefix?.trim_matches('/');
    if prefix.is_empty() {
        None
    } else {
        Some(prefix.to_string())
    }
}

fn validate_key(key: &str) -> Result<(), anyhow::Error> {
    if key.trim().is_empty() {
        bail!("key must not be empty");
    }
    if key.contains('\\') {
        bail!("key must not contain backslashes");
    }
    for segment in key.split('/') {
        if segment.is_empty() {
            bail!("key must not contain empty path segments");
        }
        if matches!(segment, "." | "..") {
            bail!("key must not contain '.' or '..' segments");
        }
    }
    Ok(())
}

fn parse_http_date(value: &HeaderValue) -> Option<OffsetDateTime> {
    value
        .to_str()
        .ok()
        .and_then(|s| OffsetDateTime::parse(s, &Rfc2822).ok())
}

async fn fetch_default_branch(state: &State) -> Result<String, anyhow::Error> {
    let url = state
        .endpoints
        .api
        .join(&format!(
            "repos/{}/{}",
            encode_segment(&state.owner),
            encode_segment(&state.repo)
        ))
        .context("failed to build repository url")?;
    let res = state.client.get(url).send().await?;
    let res = error_for_status(res).await?;
    let repo: GithubRepoResponse = res
        .json()
        .await
        .context("failed to decode repository metadata")?;
    repo.default_branch
        .ok_or_else(|| anyhow::anyhow!("repository does not expose a default branch"))
}

#[derive(Serialize)]
struct PutRequest<'a> {
    message: String,
    branch: String,
    content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sha: Option<&'a str>,
}

impl<'a> PutRequest<'a> {
    fn new(branch: &str, key: &str, content: String, sha: Option<&'a str>) -> Self {
        Self {
            message: format!("objstore: put {}", key),
            branch: branch.to_string(),
            content,
            sha,
        }
    }
}

#[derive(Serialize)]
struct DeleteRequest<'a> {
    message: String,
    branch: String,
    sha: &'a str,
}

impl<'a> DeleteRequest<'a> {
    fn new(branch: &str, sha: &'a str, key: &str) -> Self {
        Self {
            message: format!("objstore: delete {}", key),
            branch: branch.to_string(),
            sha,
        }
    }
}

async fn error_for_status(res: Response) -> Result<Response, anyhow::Error> {
    if res.status().is_success() {
        return Ok(res);
    }
    let status = res.status();
    let text = res.text().await.unwrap_or_default();
    let message = parse_error_message(&text);
    Err(anyhow::anyhow!(
        "GitHub API request failed ({}): {}",
        status,
        message
    ))
}

fn parse_error_message(raw: &str) -> String {
    if let Ok(err) = serde_json::from_str::<GithubErrorResponse>(raw) {
        let mut parts = Vec::new();
        if let Some(message) = err.message {
            parts.push(message);
        }
        if let Some(details) = err.errors {
            for detail in details {
                match detail {
                    GithubErrorDetail::Message { message } => {
                        if let Some(msg) = message {
                            parts.push(msg);
                        }
                    }
                    GithubErrorDetail::Field {
                        resource,
                        field,
                        code,
                        message,
                    } => {
                        let part = format!(
                            "{} {} {} {}",
                            resource.unwrap_or_default(),
                            field.unwrap_or_default(),
                            code.unwrap_or_default(),
                            message.unwrap_or_default()
                        )
                        .trim()
                        .to_string();
                        if !part.is_empty() {
                            parts.push(part);
                        }
                    }
                }
            }
        }
        if !parts.is_empty() {
            return parts.join("; ");
        }
    }
    raw.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use objstore::wrapper::trace::TracedObjStore;
    use objstore_test::test_objstore;

    fn test_strict() -> bool {
        std::env::var("TEST_STRICT").is_ok()
    }

    fn load_test_config() -> Result<Option<GithubObjStoreConfig>, anyhow::Error> {
        const ENV_VAR: &str = "GITHUB_TEST_URI";
        let Ok(uri) = std::env::var(ENV_VAR) else {
            if test_strict() {
                anyhow::bail!("missing required environment variable: {ENV_VAR}");
            }
            eprintln!("skipping github backend test - set {ENV_VAR} to enable");
            return Ok(None);
        };
        let config = GithubObjStoreConfig::from_uri(&uri)?;
        Ok(Some(config))
    }

    #[tokio::test]
    async fn github_backend_flow() {
        let Some(config) = load_test_config().unwrap() else {
            return;
        };
        let store = GithubObjStore::new(config).expect("failed to build github objstore");
        let traced = TracedObjStore::new("github", store);
        test_objstore(&traced).await;
    }
}
