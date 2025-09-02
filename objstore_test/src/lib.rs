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

    let v = store.get(&key1).await.unwrap();
    assert!(v.is_none());
    let v = store.get_with_meta(&key1).await.unwrap();
    assert!(v.is_none());
    let v = store.meta(&key1).await.unwrap();
    assert!(v.is_none());

    store.put(&key1).bytes("hello").await.unwrap();
    let v = store.get(&key1).await.unwrap().unwrap();
    assert_eq!(v.as_ref(), b"hello");

    let (v, mut meta1) = store.get_with_meta(&key1).await.unwrap().unwrap();
    assert_eq!(v.as_ref(), b"hello");
    assert_eq!(meta1.key(), key1);
    if let Some(size) = meta1.size {
        assert_eq!(size, 5);
    }
    meta1.round_timestamps_second();

    let mut meta2 = store.meta(&key1).await.unwrap().unwrap();
    meta2.round_timestamps_second();
    assert_eq!(meta1, meta2);

    let mut items = store
        .list(ListArgs::new().with_prefix(&prefix))
        .await
        .unwrap()
        .items;
    assert_eq!(items.len(), 1);
    items.iter_mut().for_each(|m| m.round_timestamps_second());
    assert_eq!(items[0], meta1);

    // with prefix
    let nested_prefix = format!("{}/{}", prefix, &key1_name[0..5]);
    let mut items = store
        .list(ListArgs::new().with_prefix(&nested_prefix))
        .await
        .unwrap()
        .items;
    assert_eq!(items.len(), 1);
    items.iter_mut().for_each(|m| m.round_timestamps_second());
    assert_eq!(items[0], meta1);

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
    let key2 = format!("{prefix}/key2");
    let key3 = format!("{prefix}/key3");

    store.put(&key1).bytes("val1").await.unwrap();
    store.put(&key2).bytes("val2").await.unwrap();
    store.put(&key3).bytes("val3").await.unwrap();

    let mut meta1 = store.meta(&key1).await.unwrap().unwrap();
    let mut meta2 = store.meta(&key2).await.unwrap().unwrap();
    let mut meta3 = store.meta(&key3).await.unwrap().unwrap();
    meta1.round_timestamps_second();
    meta2.round_timestamps_second();
    meta3.round_timestamps_second();

    let mut list = store
        .list(ListArgs::new().with_prefix(&prefix))
        .await
        .unwrap()
        .items;
    assert_eq!(list.len(), 3);
    list.sort_by(|a, b| a.key().cmp(b.key()));
    list.iter_mut().for_each(|m| m.round_timestamps_second());

    assert_eq!(list[0], meta1);
    assert_eq!(list[1], meta2);
    assert_eq!(list[2], meta3);

    // Delete all.
    store.delete_prefix("").await.unwrap();
    let items = store.list(ListArgs::new()).await.unwrap().items;
    assert_eq!(items.len(), 0);
}
