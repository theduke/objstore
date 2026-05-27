use objstore::wrapper::prefix::PrefixObjStore;
use objstore::{
    DownloadUrlArgs, KeyPage, ListArgs, ObjStore, ObjStoreExt as _, ObjectMeta, ObjectMetaPage,
    Put, UploadUrlArgs, ValueStream,
};
use objstore_memory::MemoryObjStore;
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct RecordingListStore {
    args: Mutex<Vec<ListArgs>>,
    list_page: Mutex<Option<ObjectMetaPage>>,
    key_page: Mutex<Option<KeyPage>>,
}

impl RecordingListStore {
    fn with_list_page(list_page: ObjectMetaPage) -> Self {
        Self {
            args: Mutex::new(Vec::new()),
            list_page: Mutex::new(Some(list_page)),
            key_page: Mutex::new(None),
        }
    }

    fn recorded_args(&self) -> ListArgs {
        self.args
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("list args should have been recorded")
    }
}

#[async_trait::async_trait]
impl ObjStore for RecordingListStore {
    fn kind(&self) -> &str {
        "recording"
    }

    fn safe_uri(&self) -> &url::Url {
        static SAFE_URI: std::sync::LazyLock<url::Url> =
            std::sync::LazyLock::new(|| url::Url::parse("memory://recording").unwrap());
        &SAFE_URI
    }

    async fn healthcheck(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn meta(&self, _key: &str) -> Result<Option<ObjectMeta>, anyhow::Error> {
        Ok(None)
    }

    async fn get(&self, _key: &str) -> Result<Option<bytes::Bytes>, anyhow::Error> {
        Ok(None)
    }

    async fn get_stream(&self, _key: &str) -> Result<Option<ValueStream>, anyhow::Error> {
        Ok(None)
    }

    async fn get_with_meta(
        &self,
        _key: &str,
    ) -> Result<Option<(bytes::Bytes, ObjectMeta)>, anyhow::Error> {
        Ok(None)
    }

    async fn get_stream_with_meta(
        &self,
        _key: &str,
    ) -> Result<Option<(ObjectMeta, ValueStream)>, anyhow::Error> {
        Ok(None)
    }

    async fn generate_download_url(
        &self,
        _args: DownloadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        Ok(None)
    }

    async fn generate_upload_url(
        &self,
        _args: UploadUrlArgs,
    ) -> Result<Option<url::Url>, anyhow::Error> {
        Ok(None)
    }

    async fn send_put(&self, _put: Put) -> Result<ObjectMeta, anyhow::Error> {
        unreachable!("send_put is not used in these tests")
    }

    async fn send_copy(&self, _copy: objstore::Copy) -> Result<ObjectMeta, anyhow::Error> {
        unreachable!("send_copy is not used in these tests")
    }

    async fn delete(&self, _key: &str) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn delete_prefix(&self, _prefix: &str) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn list(&self, args: ListArgs) -> Result<ObjectMetaPage, anyhow::Error> {
        self.args.lock().unwrap().push(args);
        self.list_page
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("list page not configured"))
    }

    async fn list_keys(&self, args: ListArgs) -> Result<KeyPage, anyhow::Error> {
        self.args.lock().unwrap().push(args);
        self.key_page
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("key page not configured"))
    }
}

#[tokio::test]
async fn test_prefix_store_matches_objstore_contract() {
    let store = PrefixObjStore::new("tests/prefix", MemoryObjStore::new());
    objstore_test::test_objstore(&store).await;
}

#[tokio::test]
async fn test_prefix_store_normalizes_constructor_prefix() {
    let inner = MemoryObjStore::new();
    let store = PrefixObjStore::new("/tenant-a/", inner.clone());

    store.put("/inside.txt").bytes("hello").await.unwrap();

    assert_eq!(
        inner.get("tenant-a/inside.txt").await.unwrap().unwrap(),
        "hello"
    );
    assert_eq!(store.list_all_keys("").await.unwrap(), vec!["inside.txt"]);
}

#[tokio::test]
async fn test_prefix_store_isolates_namespace() {
    let inner = MemoryObjStore::new();
    let store = PrefixObjStore::new("tenant-a", inner.clone());

    store.put("inside.txt").bytes("hello").await.unwrap();
    inner.put("outside.txt").bytes("world").await.unwrap();
    inner
        .put("tenant-a/shared.txt")
        .bytes("shared")
        .await
        .unwrap();

    assert_eq!(
        store.list_all_keys("").await.unwrap(),
        vec!["inside.txt".to_string(), "shared.txt".to_string()]
    );
    assert_eq!(store.get("inside.txt").await.unwrap().unwrap(), "hello");
    assert_eq!(store.get("shared.txt").await.unwrap().unwrap(), "shared");
    assert!(store.get("outside.txt").await.unwrap().is_none());

    store.delete_prefix("").await.unwrap();

    assert!(inner.get("tenant-a/inside.txt").await.unwrap().is_none());
    assert!(inner.get("tenant-a/shared.txt").await.unwrap().is_none());
    assert_eq!(inner.get("outside.txt").await.unwrap().unwrap(), "world");
}

#[tokio::test]
async fn test_prefix_store_translates_list_cursors() {
    let inner = MemoryObjStore::new();
    let store = PrefixObjStore::new("tenant-a", inner.clone());

    inner.put("tenant-a/a.txt").bytes("a").await.unwrap();
    inner.put("tenant-a/b.txt").bytes("b").await.unwrap();
    inner.put("tenant-a/c.txt").bytes("c").await.unwrap();
    inner.put("tenant-b/ignored.txt").bytes("x").await.unwrap();

    let first_page = store
        .list_keys(ListArgs::new().with_limit(2))
        .await
        .unwrap();
    assert_eq!(first_page.items, vec!["a.txt", "b.txt"]);
    assert_eq!(first_page.next_cursor.as_deref(), Some("b.txt"));

    let second_page = store
        .list_keys(ListArgs::new().with_limit(2).with_cursor("b.txt"))
        .await
        .unwrap();
    assert_eq!(second_page.items, vec!["c.txt"]);
    assert_eq!(second_page.next_cursor.as_deref(), Some("c.txt"));
}

#[tokio::test]
async fn test_prefix_store_translates_list_prefixes_and_preserves_delimiter() {
    let inner = Arc::new(RecordingListStore::with_list_page(ObjectMetaPage {
        items: vec![ObjectMeta::new("tenant-a/nested/file.txt".to_string())],
        next_cursor: Some("tenant-a/nested/file.txt".to_string()),
        prefixes: Some(vec!["tenant-a/nested/subdir".to_string()]),
    }));
    let store = PrefixObjStore::new("tenant-a", inner.clone());

    let page = store
        .list(
            ListArgs::new()
                .with_prefix("/nested")
                .with_delimiter("/")
                .with_cursor("/nested/file.txt"),
        )
        .await
        .unwrap();

    assert_eq!(
        page.items
            .into_iter()
            .map(|item| item.key)
            .collect::<Vec<_>>(),
        vec!["nested/file.txt"]
    );
    assert_eq!(page.next_cursor.as_deref(), Some("nested/file.txt"));
    assert_eq!(page.prefixes, Some(vec!["nested/subdir".to_string()]));

    let args = inner.recorded_args();
    assert_eq!(args.prefix(), Some("tenant-a/nested"));
    assert_eq!(args.delimiter(), Some("/"));
    assert_eq!(args.cursor(), Some("tenant-a/nested/file.txt"));
}

#[tokio::test]
async fn test_prefix_store_rejects_keys_outside_configured_namespace() {
    let inner = RecordingListStore::with_list_page(ObjectMetaPage {
        items: vec![ObjectMeta::new("other-tenant/file.txt".to_string())],
        next_cursor: Some("other-tenant/file.txt".to_string()),
        prefixes: Some(vec!["other-tenant/subdir".to_string()]),
    });
    let store = PrefixObjStore::new("tenant-a", inner);

    let err = store.list(ListArgs::new()).await.unwrap_err();
    assert!(
        err.to_string().contains("outside prefix"),
        "unexpected error: {err:#}"
    );
}

#[tokio::test]
async fn test_prefix_store_strips_leading_slashes_when_joining_paths() {
    let inner = MemoryObjStore::new();
    let store = PrefixObjStore::new("tenant-a", inner.clone());

    store.put("/nested/file.txt").bytes("hello").await.unwrap();
    assert_eq!(
        inner
            .get("tenant-a/nested/file.txt")
            .await
            .unwrap()
            .unwrap(),
        "hello"
    );
    assert!(
        inner
            .get("tenant-a//nested/file.txt")
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        store.list_all_keys("/nested").await.unwrap(),
        vec!["nested/file.txt".to_string()]
    );

    store.delete_prefix("/nested").await.unwrap();
    assert!(
        inner
            .get("tenant-a/nested/file.txt")
            .await
            .unwrap()
            .is_none()
    );
}
