# objstore

Generic object store (S3, Google Cloud Storage, ...) abstraction library for Rust

Provides an `ObjStore` trait, and multiple different implementations,
allowing to easily write flexible code that does not depend on a specific
object store, and allows for easy testing with an in-memory store.

## Backends

Each backend is available as a separate crate.

- [x] `objstore_memory`
   In-memory store, useful for testing and small applications.
- [x] `objstore_fs`
  Filesystem-backed store.
  Stores metadata such as hashes as a separate file.
- [x] `objstore_s3_light`
  Lightweight S3 backend based on `rusty-s3` and `reqwest`.
  Not as full-featured as `objstore_s3`, which uses the official AWS SDK,
  but has way fewer dependencies.
- [ ] `objstore_s3`
  Full-featured S3 backend based on the official AWS SDK.
  Supports more functionality efficiently, but has more dependencies.

  NOTE: not implemented yet, but planned.
  
## Usage

```rust
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
```

## Development

### Testing

The `objstore_test` crate provides a common test helper `objstore_test::test_objstore`,
which ensures all backends conform to the same behaviour.

## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
