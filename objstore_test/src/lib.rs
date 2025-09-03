//! Test helpers for testing stores.
//!
//! Allows for unified testing to make sure all implementations conform to the
//! same behavior.

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use objstore::{ListArgs, ObjStore, ObjStoreExt, ObjectMeta, ValueStream};
use pretty_assertions::assert_eq;
use sha2::Digest as _;
use time::OffsetDateTime;
use uuid::Uuid;

fn new_keymeta(key: &str, value: &Bytes) -> ObjectMeta {
    let hash = sha2::Sha256::digest(value);
    let now = OffsetDateTime::now_utc();

    let mut meta = ObjectMeta::new(key.to_string());
    meta.size = Some(value.len() as u64);
    meta.created_at = Some(now);
    meta.updated_at = Some(now);
    meta.hash_sha256 = Some(hash.into());

    meta
}

/// Compare metadata, accounting for backends that don't support all features,
/// like hashes or custom attributes.
fn expect_meta(expected: &ObjectMeta, actual: &ObjectMeta) {
    let expected = expected.clone().with_rounded_timestamps_minute();
    let expected_size = expected.size.expect("expected size must set");
    let exected_created_at = expected.created_at.expect("expected created_at must set");
    let expected_updated_at = expected.updated_at.expect("expected updated_at must set");
    let hash_sha256 = expected.hash_sha256.expect("expected hash_sha256 must set");

    let actual = actual.clone().with_rounded_timestamps_minute();

    assert_eq!(
        expected.key,
        actual.key,
        "key should match: {}",
        String::from_utf8_lossy(expected.key.as_ref())
    );

    if let Some(size) = actual.size {
        assert_eq!(
            expected_size,
            size,
            "size should match: {}",
            String::from_utf8_lossy(expected.key.as_ref())
        );
    }

    let now = OffsetDateTime::now_utc();
    if let Some(created_at) = actual.created_at {
        let diff1 = now - created_at;
        let diff2 = now - exected_created_at;
        let diff = if diff1 > diff2 {
            diff1 - diff2
        } else {
            diff2 - diff1
        };
        assert!(
            diff.whole_seconds() < 10,
            "expected {created_at:?} to roughly match {exected_created_at:?}"
        );
    }
    if let Some(updated_at) = actual.updated_at {
        let diff1 = now - updated_at;
        let diff2 = now - expected_updated_at;
        let diff = if diff1 > diff2 {
            diff1 - diff2
        } else {
            diff2 - diff1
        };
        assert!(
            diff.whole_seconds() < 10,
            "expected {updated_at:?} to roughly match {expected_updated_at:?}"
        );
    }
    if let Some(hash) = actual.hash_sha256 {
        assert_eq!(hash_sha256, hash);
    }
}

/// Ensures that a key exists with the given value and metadata.
///
/// Exercies all the diffrent ways to retrieve the key and metadata.
async fn expect_key(store: &impl ObjStore, key: &str, value: &Bytes, meta: ObjectMeta) {
    eprintln!("Expecting key: {key} with meta {meta:?} and value {value:?}");

    let m0 = store
        .meta(key)
        .await
        .expect("meta should be retrievable")
        .expect("meta should exist")
        .with_rounded_timestamps_minute();
    expect_meta(&meta, &m0);

    let v0 = store
        .get(key)
        .await
        .expect("get should be retrievable")
        .expect("value should exist");
    assert_eq!(v0, value, "value should match");

    let (v1, m1) = store
        .get_with_meta(key)
        .await
        .expect("get_with_meta should be retrievable")
        .expect("value and meta should exist");
    assert_eq!(v1, *value, "value should match in get_with_meta");
    expect_meta(&meta, &m1);

    // Stream retrieval.

    let v2 = store
        .get_stream(key)
        .await
        .expect("get_stream should be retrievable")
        .expect("value should exist in stream")
        .try_collect::<BytesMut>()
        .await
        .expect("stream should collect successfully")
        .freeze();
    assert_eq!(v2, *value, "value should match in get_stream");

    let (m2, stream) = store
        .get_stream_with_meta(key)
        .await
        .expect("get_stream_with_meta should be retrievable")
        .expect("value and meta should exist in stream");
    expect_meta(&meta, &m2);
    let v3 = stream
        .try_collect::<BytesMut>()
        .await
        .expect("stream should collect successfully")
        .freeze();
    assert_eq!(v3, *value, "value should match in get_stream_with_meta");
}

async fn test_single_key_flow<D>(store: &D, base_prefix: &str)
where
    D: ObjStore + Sized,
{
    let nested_prefix = Uuid::new_v4().to_string();
    let prefix = format!("{base_prefix}/{nested_prefix}");
    let key_name = Uuid::new_v4().to_string();
    let key = format!("{prefix}/{key_name}");

    // Key does not exist.
    {
        // List with prefix should be empty.
        let keys = store.list_all_keys(&prefix).await.unwrap();
        assert!(keys.is_empty(), "list with prefix should be empty");

        let v0 = store.get(&key).await.unwrap();
        assert!(v0.is_none(), "key should not exist before put");

        let m0 = store.meta(&key).await.unwrap();
        assert!(m0.is_none(), "meta should not exist before put");
    }

    // List with prefix should be empty.
    {
        let keys = store.list_all_keys(&prefix).await.unwrap();
        assert!(keys.is_empty(), "list with prefix should be empty");
    }

    // Put a value and retrieve it.
    {
        let value: Bytes = Uuid::new_v4().to_string().into();
        let expected_meta = new_keymeta(&key, &value);

        store.put(&key).bytes(value.clone()).await.unwrap();

        // List with prefix should contain the key.
        let keys = store.list_all_keys(&prefix).await.unwrap();
        assert_eq!(
            keys,
            vec![key.clone()],
            "list with prefix should contain just the expected key"
        );

        expect_key(store, &key, &value, expected_meta.clone()).await;
    }

    // Copy the key and verify the copy exists.
    // {
    //     let dest = format!("{prefix}/{key_name}_copy");
    //     let copy_meta = store.copy(&key, &dest).send().await.unwrap();
    //     let value_copy = store.get(&dest).await.unwrap().unwrap();
    //     expect_key(store, &dest, &value_copy, copy_meta.clone()).await;
    // }

    // Delete the key and check it no longer exists.
    {
        // Remove both original and copied keys.
        store.delete(&key).await.unwrap();
        // let dest = format!("{prefix}/{key_name}_copy");
        // store.delete(&dest).await.unwrap();

        let keys = store.list_all_keys(&prefix).await.unwrap();
        assert_eq!(
            keys,
            Vec::<String>::new(),
            "list with prefix should be empty after delete"
        );

        let v2 = store.get(&key).await.unwrap();
        assert_eq!(v2, None, "key should not exist after delete");

        let m2 = store.meta(&key).await.unwrap();
        assert!(m2.is_none(), "meta should not exist after delete");

        let v3 = store.get_with_meta(&key).await.unwrap();
        assert_eq!(v3, None, "get_with_meta should return None after delete");
    }

    // Do the same again with stream put/get.
    {
        let value: Bytes = format!("{}_sync", Uuid::new_v4()).into();
        let stream: ValueStream =
            futures::stream::once(std::future::ready(Ok(value.clone()))).boxed();
        store.put(&key).stream(stream).await.unwrap();

        expect_key(store, &key, &value, new_keymeta(&key, &value)).await;

        // Delete again.
        store.delete(&key).await.unwrap();
    }
}

/// Test an ObjStore implementation.
///
/// NOTE: the store must be empty before running this test!
/// A simple way to ensure that is to use a nested path store.
pub async fn test_objstore(store: &impl ObjStore) {
    store.healthcheck().await.expect("health check");

    let prefix = Uuid::new_v4().to_string();

    store.delete_prefix(&prefix).await.unwrap();

    test_single_key_flow(store, &prefix).await;

    let keys = store.list_all_keys(&prefix).await.unwrap();
    assert!(keys.is_empty());

    let page = store
        .list(ListArgs::new().with_prefix(&prefix))
        .await
        .unwrap();
    let items = page.items;
    assert_eq!(items, vec![]);

    let key1_name = Uuid::new_v4().to_string();
    let key1 = format!("{prefix}/{key1_name}");
    let value1 = "hello";

    let v = store.get(&key1).await.unwrap();
    assert!(v.is_none());
    let v = store.get_with_meta(&key1).await.unwrap();
    assert!(v.is_none());
    let v = store.meta(&key1).await.unwrap();
    assert!(v.is_none());

    store.put(&key1).bytes(value1).await.unwrap();
    let key1_created_at = OffsetDateTime::now_utc();
    let key1_meta = {
        let mut m = ObjectMeta::new(key1.clone());
        m.size = Some(value1.len() as u64);
        m.created_at = Some(key1_created_at);
        m.updated_at = Some(key1_created_at);
        m.hash_md5 = Some(md5::compute(value1.as_bytes()).0);
        m.hash_sha256 = Some(sha2::Sha256::digest(value1).into());
        m
    };

    let v = store.get(&key1).await.unwrap().unwrap();
    assert_eq!(v.as_ref(), b"hello");

    let (v, meta1) = store.get_with_meta(&key1).await.unwrap().unwrap();
    assert_eq!(v.as_ref(), b"hello");
    approximate_meta_match(&key1_meta, &meta1, "get_with_meta");

    let meta2 = store.meta(&key1).await.unwrap().unwrap();
    approximate_meta_match(&key1_meta, &meta2, "meta");

    // with prefix
    let nested_prefix = format!("{}/{}", prefix, &key1_name[0..5]);
    let mut items = store
        .list(ListArgs::new().with_prefix(&nested_prefix))
        .await
        .unwrap()
        .items;
    assert_eq!(items.len(), 1);
    items.iter_mut().for_each(|m| m.round_timestamps_second());
    approximate_meta_match(&key1_meta, &items[0], "list with prefix");

    store.delete(&key1).await.unwrap();
    let v = store.get(&key1).await.unwrap();
    assert!(v.is_none());
    let v = store.get_with_meta(&key1).await.unwrap();
    assert!(v.is_none());
    let v = store.meta(&key1).await.unwrap();
    assert!(v.is_none());

    let items = store
        .list(ListArgs::new().with_prefix(&prefix))
        .await
        .unwrap()
        .items;
    assert_eq!(items.len(), 0);

    // MULTI-KEY
    let prefix = Uuid::new_v4().to_string();
    let key1 = format!("{prefix}/key1");
    let value1 = "val1";
    let key2 = format!("{prefix}/key2");
    let value2 = "val2";
    let key3 = format!("{prefix}/key3");
    let value3 = "val3";

    let created = OffsetDateTime::now_utc();

    let key1meta = {
        let mut m = ObjectMeta::new(key1.clone());
        m.size = Some(value1.len() as u64);
        m.created_at = Some(created);
        m.updated_at = Some(created);
        m.hash_md5 = Some(md5::compute(value1.as_bytes()).0);
        m.hash_sha256 = Some(sha2::Sha256::digest(value1).into());
        m
    };
    let key2meta = {
        let mut m = ObjectMeta::new(key2.clone());
        m.size = Some(value2.len() as u64);
        m.created_at = Some(created);
        m.updated_at = Some(created);
        m.hash_md5 = Some(md5::compute(value2.as_bytes()).0);
        m.hash_sha256 = Some(sha2::Sha256::digest(value2).into());
        m
    };
    let key3meta = {
        let mut m = ObjectMeta::new(key3.clone());
        m.size = Some(value3.len() as u64);
        m.created_at = Some(created);
        m.updated_at = Some(created);
        m.hash_md5 = Some(md5::compute(value3.as_bytes()).0);
        m.hash_sha256 = Some(sha2::Sha256::digest(value3).into());
        m
    };

    store.put(&key1).bytes("val1").await.unwrap();
    store.put(&key2).bytes("val2").await.unwrap();
    store.put(&key3).bytes("val3").await.unwrap();

    {
        let meta1 = store.meta(&key1).await.unwrap().unwrap();
        let meta2 = store.meta(&key2).await.unwrap().unwrap();
        let meta3 = store.meta(&key3).await.unwrap().unwrap();
        approximate_meta_match(&meta1, &key1meta, "multi-key meta1");
        approximate_meta_match(&meta2, &key2meta, "multi-key meta2");
        approximate_meta_match(&meta3, &key3meta, "multi-key meta3");
    }

    {
        let mut list = store
            .list(ListArgs::new().with_prefix(&prefix))
            .await
            .unwrap()
            .items;
        assert_eq!(list.len(), 3);
        list.sort_by(|a, b| a.key().cmp(b.key()));
        list.iter_mut().for_each(|m| m.round_timestamps_second());

        approximate_meta_match(&list[0], &key1meta, "list meta1");
        approximate_meta_match(&list[1], &key2meta, "list meta2");
        approximate_meta_match(&list[2], &key3meta, "list meta3");
    }

    // Delete all.
    store.delete_prefix("").await.unwrap();
    let items = store.list(ListArgs::new()).await.unwrap().items;
    assert_eq!(items.len(), 0);
}

fn approximate_datetime_match(a: OffsetDateTime, b: OffsetDateTime, msg: &str) {
    let diff = if a > b { a - b } else { b - a };
    assert!(
        diff.whole_seconds() < 5,
        "inexact datetime match: {msg} | {a:?} vs {b:?}"
    );
}

fn approximate_meta_match(a: &ObjectMeta, b: &ObjectMeta, msg: &str) {
    assert_eq!(a.key, b.key, "key should match: {}", msg);
    if let (Some(a_size), Some(b_size)) = (a.size, b.size) {
        assert_eq!(a_size, b_size, "size should match: {}", msg);
    }
    if let (Some(a_created_at), Some(b_created_at)) = (a.created_at, b.created_at) {
        approximate_datetime_match(
            a_created_at,
            b_created_at,
            &format!("created_at should match: {msg}"),
        );
    }
    if let (Some(a_updated_at), Some(b_updated_at)) = (a.updated_at, b.updated_at) {
        approximate_datetime_match(
            a_updated_at,
            b_updated_at,
            &format!("updated_at should match: {msg}"),
        );
    }
    if let (Some(a_hash), Some(b_hash)) = (a.hash_md5, b.hash_md5) {
        assert_eq!(a_hash, b_hash, "hash_md5 should match: {}", msg);
    }
    if let (Some(a_hash), Some(b_hash)) = (a.hash_sha256, b.hash_sha256) {
        assert_eq!(a_hash, b_hash, "hash_sha256 should match: {}", msg);
    }
    if let (Some(a_etag), Some(b_etag)) = (&a.etag, &b.etag) {
        assert_eq!(a_etag, b_etag, "etag should match: {}", msg);
    }
    if let (Some(a_mime), Some(b_mime)) = (&a.mime_type, &b.mime_type) {
        assert_eq!(a_mime, b_mime, "mime_type should match: {}", msg);
    }

    // todo: extra handling?
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use futures::TryStreamExt;
    use objstore::{ListArgs, ObjStore, ObjStoreExt, DownloadUrlArgs, DataSource, Put, Copy};
    use objstore_memory::MemoryObjStore;
    use std::time::Duration;
    use uuid::Uuid;
    use crate::test_objstore;

    /// Basic integration test using MemoryObjStore
    #[tokio::test]
    async fn test_memory_objstore_basic() {
        let store = MemoryObjStore::new();
        test_objstore(&store).await;
    }

    /// Test ObjStore basic info methods
    #[tokio::test]
    async fn test_objstore_info_methods() {
        let store = MemoryObjStore::new();
        
        // Test kind method
        assert_eq!(store.kind(), "objstore.memory");
        
        // Test safe_uri method
        let uri = store.safe_uri();
        assert_eq!(uri.scheme(), "memory");
    }

    /// Test generate_download_url functionality
    #[tokio::test] 
    async fn test_generate_download_url() {
        let store = MemoryObjStore::new();
        let key = "test-download-url";
        
        // Test on non-existent key - memory store should return None since it doesn't support download URLs
        let args = DownloadUrlArgs::new(key, Duration::from_secs(3600));
        let result = store.generate_download_url(args).await.unwrap();
        assert_eq!(result, None, "Memory store should not support download URLs");
    }

    /// Test get_json functionality
    #[tokio::test]
    async fn test_get_json() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct TestData {
            name: String,
            value: i32,
        }
        
        let store = MemoryObjStore::new();
        let key = "test-json";
        let test_data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        
        // Test get_json on non-existent key
        let result: Option<TestData> = store.get_json(key).await.unwrap();
        assert_eq!(result, None);
        
        // Put JSON data using put builder
        store.put(key).json(&test_data).await.unwrap();
        
        // Get JSON data back
        let retrieved: TestData = store.get_json(key).await.unwrap().unwrap();
        assert_eq!(retrieved, test_data);
    }

    /// Test ObjStoreExt put builder with different data sources
    #[tokio::test]
    async fn test_put_builder_variations() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        
        // Test text put
        let key1 = format!("{}/text", prefix);
        let text_data = "Hello, World!";
        store.put(&key1).text(text_data).await.unwrap();
        let retrieved = store.get(&key1).await.unwrap().unwrap();
        assert_eq!(retrieved, Bytes::from(text_data));
        
        // Test bytes put  
        let key2 = format!("{}/bytes", prefix);
        let bytes_data = Bytes::from("binary data");
        store.put(&key2).bytes(bytes_data.clone()).await.unwrap();
        let retrieved = store.get(&key2).await.unwrap().unwrap();
        assert_eq!(retrieved, bytes_data);
        
        // Test stream put
        let key3 = format!("{}/stream", prefix);
        let stream_data = "streamed data";
        let stream = futures::stream::iter(vec![Ok::<Bytes, anyhow::Error>(Bytes::from(stream_data))]);
        store.put(&key3).stream(stream).await.unwrap();
        let retrieved = store.get(&key3).await.unwrap().unwrap();
        assert_eq!(retrieved, Bytes::from(stream_data));
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test send_put and send_copy direct methods
    #[tokio::test]
    async fn test_direct_send_methods() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        let src_key = format!("{}/source", prefix);
        let dest_key = format!("{}/destination", prefix);
        let data = Bytes::from("test data for copy");
        
        // Test send_put directly
        let put = Put::new(&src_key, DataSource::Data(data.clone()));
        let meta = store.send_put(put).await.unwrap();
        assert_eq!(meta.key, src_key);
        assert_eq!(meta.size, Some(data.len() as u64));
        
        // Verify data was stored
        let retrieved = store.get(&src_key).await.unwrap().unwrap();
        assert_eq!(retrieved, data);
        
        // Test send_copy directly
        let copy = Copy::new(&src_key, &dest_key);
        let copy_meta = store.send_copy(copy).await.unwrap();
        assert_eq!(copy_meta.key, dest_key);
        
        // Verify copy was successful
        let copied_data = store.get(&dest_key).await.unwrap().unwrap();
        assert_eq!(copied_data, data);
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test copy builder functionality
    #[tokio::test]
    async fn test_copy_builder() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        let src_key = format!("{}/source", prefix);
        let dest_key = format!("{}/dest", prefix);
        let data = "data to copy";
        
        // Put source data
        store.put(&src_key).text(data).await.unwrap();
        
        // Test copy using builder
        let copy_meta = store.copy(&src_key, &dest_key).send().await.unwrap();
        assert_eq!(copy_meta.key, dest_key);
        
        // Verify copy
        let copied = store.get(&dest_key).await.unwrap().unwrap();
        assert_eq!(copied, Bytes::from(data));
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test list_keys_stream functionality
    #[tokio::test]
    async fn test_list_keys_stream() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        
        // Create multiple keys
        let keys = vec![
            format!("{}/key1", prefix),
            format!("{}/key2", prefix), 
            format!("{}/key3", prefix),
        ];
        
        for key in &keys {
            store.put(key).text("data").await.unwrap();
        }
        
        // Test list_keys_stream
        let args = ListArgs::new().with_prefix(&prefix);
        let mut collected_keys = Vec::new();
        
        let mut stream = store.list_keys_stream(args);
        while let Some(page) = stream.try_next().await.unwrap() {
            collected_keys.extend(page.items);
        }
        
        collected_keys.sort();
        let mut expected_keys = keys.clone();
        expected_keys.sort();
        assert_eq!(collected_keys, expected_keys);
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test list_stream functionality
    #[tokio::test]  
    async fn test_list_stream() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        
        // Create keys with data
        let test_data = vec![
            (format!("{}/item1", prefix), "data1"),
            (format!("{}/item2", prefix), "data2"),
        ];
        
        for (key, data) in &test_data {
            store.put(key).text(*data).await.unwrap();
        }
        
        // Test list_stream 
        let args = ListArgs::new().with_prefix(&prefix);
        let mut collected_items = Vec::new();
        
        let mut stream = store.list_stream(args);
        while let Some(page) = stream.try_next().await.unwrap() {
            collected_items.extend(page.items);
        }
        
        assert_eq!(collected_items.len(), 2);
        collected_items.sort_by(|a, b| a.key.cmp(&b.key));
        
        assert_eq!(collected_items[0].key, test_data[0].0);
        assert_eq!(collected_items[1].key, test_data[1].0);
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test edge cases with keys
    #[tokio::test]
    async fn test_edge_case_keys() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        
        // Test with special characters in keys
        let special_keys = vec![
            format!("{}/key with spaces", prefix),
            format!("{}/key-with-dashes", prefix),
            format!("{}/key_with_underscores", prefix),
            format!("{}/key.with.dots", prefix),
            format!("{}/key/with/slashes", prefix),
        ];
        
        for key in &special_keys {
            store.put(key).text("test data").await.unwrap();
            let retrieved = store.get(key).await.unwrap().unwrap();
            assert_eq!(retrieved, Bytes::from("test data"));
        }
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test edge cases with values
    #[tokio::test]
    async fn test_edge_case_values() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        
        // Test empty value
        let empty_key = format!("{}/empty", prefix);
        store.put(&empty_key).bytes(Bytes::new()).await.unwrap();
        let retrieved = store.get(&empty_key).await.unwrap().unwrap();
        assert_eq!(retrieved, Bytes::new());
        
        // Test large value (1MB)
        let large_key = format!("{}/large", prefix);
        let large_data = vec![b'x'; 1024 * 1024];
        let large_bytes = Bytes::from(large_data);
        store.put(&large_key).bytes(large_bytes.clone()).await.unwrap();
        let retrieved = store.get(&large_key).await.unwrap().unwrap();
        assert_eq!(retrieved, large_bytes);
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test pagination edge cases
    #[tokio::test]
    async fn test_pagination_edge_cases() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        
        // Test pagination with limit
        let keys = (0..10).map(|i| format!("{}/key{:02}", prefix, i)).collect::<Vec<_>>();
        for key in &keys {
            store.put(key).text("data").await.unwrap();
        }
        
        // Test list with small limit
        let args = ListArgs::new().with_prefix(&prefix).with_limit(3);
        let first_page = store.list(args).await.unwrap();
        assert!(first_page.items.len() <= 3);
        
        // Test list_keys with limit
        let args = ListArgs::new().with_prefix(&prefix).with_limit(5);  
        let key_page = store.list_keys(args).await.unwrap();
        assert!(key_page.items.len() <= 5);
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test purge_all functionality
    #[tokio::test]
    async fn test_purge_all() {
        let store = MemoryObjStore::new();
        
        // Add some test data
        store.put("test1").text("data1").await.unwrap();
        store.put("test2").text("data2").await.unwrap();
        
        // Verify data exists
        assert!(store.get("test1").await.unwrap().is_some());
        assert!(store.get("test2").await.unwrap().is_some());
        
        // Purge all
        store.purge_all().await.unwrap();
        
        // Verify all data is gone
        assert!(store.get("test1").await.unwrap().is_none());
        assert!(store.get("test2").await.unwrap().is_none());
        
        let all_items = store.list(ListArgs::new()).await.unwrap();
        assert_eq!(all_items.items.len(), 0);
    }

    /// Test error handling for non-existent operations
    #[tokio::test]
    async fn test_error_conditions() {
        let store = MemoryObjStore::new();
        let non_existent_key = "does_not_exist";
        
        // These should return None, not error
        assert_eq!(store.get(non_existent_key).await.unwrap(), None);
        assert_eq!(store.meta(non_existent_key).await.unwrap(), None);
        assert_eq!(store.get_with_meta(non_existent_key).await.unwrap(), None);
        
        // For stream operations, we need to test differently since they return streams  
        assert!(store.get_stream(non_existent_key).await.unwrap().is_none());
        assert!(store.get_stream_with_meta(non_existent_key).await.unwrap().is_none());
        
        // Delete of non-existent key should succeed (idempotent)
        store.delete(non_existent_key).await.unwrap();
        
        // get_json should return None for non-existent key
        let result: Option<serde_json::Value> = store.get_json(non_existent_key).await.unwrap();
        assert_eq!(result, None);
    }

    /// Test stream operations  
    #[tokio::test]
    async fn test_stream_operations() {
        let store = MemoryObjStore::new();
        let key = "stream_test";
        let data = "stream test data";
        
        // Put data
        store.put(key).text(data).await.unwrap();
        
        // Test get_stream
        let stream = store.get_stream(key).await.unwrap().unwrap();
        let collected: Bytes = stream.try_collect::<BytesMut>().await.unwrap().freeze();
        assert_eq!(collected, Bytes::from(data));
        
        // Test get_stream_with_meta
        let (meta, stream) = store.get_stream_with_meta(key).await.unwrap().unwrap();
        assert_eq!(meta.key, key);
        let collected: Bytes = stream.try_collect::<BytesMut>().await.unwrap().freeze();
        assert_eq!(collected, Bytes::from(data));
        
        // Clean up
        store.delete(key).await.unwrap();
    }

    /// Test list_all_keys functionality
    #[tokio::test]
    async fn test_list_all_keys() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        
        // Create multiple keys
        let keys = vec![
            format!("{}/key1", prefix),
            format!("{}/key2", prefix),
            format!("{}/key3", prefix),
        ];
        
        for key in &keys {
            store.put(key).text("data").await.unwrap();
        }
        
        // Test list_all_keys
        let all_keys = store.list_all_keys(&prefix).await.unwrap();
        let mut sorted_keys = all_keys.clone();
        sorted_keys.sort();
        let mut expected_keys = keys.clone();
        expected_keys.sort();
        assert_eq!(sorted_keys, expected_keys);
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test ListArgs with delimiter
    #[tokio::test]
    async fn test_list_args_with_delimiter() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        
        // Create keys with hierarchical structure
        let keys = vec![
            format!("{}/dir1/file1", prefix),
            format!("{}/dir1/file2", prefix),
            format!("{}/dir2/file3", prefix),
            format!("{}/file4", prefix),
        ];
        
        for key in &keys {
            store.put(key).text("data").await.unwrap();
        }
        
        // Test list with delimiter
        let args = ListArgs::new().with_prefix(&prefix).with_delimiter("/");
        let page = store.list(args).await.unwrap();
        
        // Should return items at the prefix level plus common prefixes for subdirectories
        assert!(!page.items.is_empty() || page.prefixes.is_some());
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test MIME type functionality
    #[tokio::test]
    async fn test_mime_type_support() {
        let store = MemoryObjStore::new();
        let key = "test-mime";
        let json_data = r#"{"test": "value"}"#;
        
        // Put JSON with explicit mime type (using send directly)
        let mut put = Put::new(key, DataSource::Data(Bytes::from(json_data)));
        put.mime_type = Some("application/json".to_string());
        let meta = store.send_put(put).await.unwrap();
        
        // Check if mime type was preserved (note: MemoryObjStore may not preserve mime types)
        if meta.mime_type.is_some() {
            assert_eq!(meta.mime_type.as_ref().unwrap(), "application/json");
        }
        
        // Clean up
        store.delete(key).await.unwrap();
    }

    /// Test concurrent operations
    #[tokio::test]
    async fn test_concurrent_operations() {
        let store = MemoryObjStore::new();
        let prefix = Uuid::new_v4().to_string();
        
        // Create tasks that run concurrently
        let mut handles = Vec::new();
        
        for i in 0..10 {
            let store_clone = store.clone();
            let key = format!("{}/concurrent_{}", prefix, i);
            let data = format!("data_{}", i);
            
            let handle = tokio::spawn(async move {
                store_clone.put(&key).text(&data).await.unwrap();
                let retrieved = store_clone.get(&key).await.unwrap().unwrap();
                assert_eq!(retrieved, Bytes::from(data));
            });
            handles.push(handle);
        }
        
        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        // Verify all keys exist
        let all_keys = store.list_all_keys(&prefix).await.unwrap();
        assert_eq!(all_keys.len(), 10);
        
        // Clean up
        store.delete_prefix(&prefix).await.unwrap();
    }

    /// Test invalid JSON deserialization
    #[tokio::test]
    async fn test_invalid_json() {
        let store = MemoryObjStore::new();
        let key = "invalid-json";
        
        // Put invalid JSON data
        store.put(key).text("{ invalid json }").await.unwrap();
        
        // Attempt to deserialize as JSON should fail
        let result: Result<Option<serde_json::Value>, _> = store.get_json(key).await;
        assert!(result.is_err(), "Invalid JSON should cause deserialization error");
        
        // Clean up
        store.delete(key).await.unwrap();
    }

    /// Test empty prefix operations
    #[tokio::test]
    async fn test_empty_prefix_operations() {
        let store = MemoryObjStore::new();
        
        // Ensure store is clean
        store.purge_all().await.unwrap();
        
        // Add some data
        store.put("test1").text("data1").await.unwrap();
        store.put("test2").text("data2").await.unwrap();
        
        // List with empty prefix should return all items
        let all_items = store.list_all_keys("").await.unwrap();
        assert!(all_items.len() >= 2);
        assert!(all_items.contains(&"test1".to_string()));
        assert!(all_items.contains(&"test2".to_string()));
        
        // Clean up
        store.delete_prefix("").await.unwrap();
    }
}
