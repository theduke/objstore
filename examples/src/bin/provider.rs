use objstore::{ObjStoreBuilder, ObjStoreExt};

#[tokio::main]
async fn main() {
    let builder = ObjStoreBuilder::new()
        .with_provider(Box::new(objstore_memory::MemoryProvider))
        .with_provider(Box::new(objstore_fs::FsProvider))
        .with_provider(Box::new(objstore_s3_light::S3LightProvider));

    // let uri = "memory://";
    // let uri = "fs:///tmp/my_store";
    let uri = "s3://ACCESS_KEY:SECRET_KEY@domain.com/bucket-name?style=path";

    let store = builder
        .build(uri)
        .expect("Failed to create object store from URI");

    store.put("hello.txt").text("hello world").await.unwrap();

    let content = store
        .get("hello.txt")
        .await
        .expect("failed to get object")
        .expect("object not found");
    assert_eq!(content.as_ref(), b"hello world");

    store.delete("hello.txt").await.unwrap();
}
