# edge-kvstore

Abstractions for interacting with key-value stores.

Implementations:
- [ ] Memory
- [ ] S3
- [ ] File system

## Ranged reads

`ObjStore::build_stream(key)` creates a streaming-read builder. Use
`with_start(offset)` and `with_end(offset)` to select an optional end-exclusive
byte range, then call `send()` to open the stream or `send_with_meta()` to also
retrieve the full object's metadata. With neither bound, the builder streams
the whole object through the backend's ordinary full-read path; adding either
bound selects its ranged-read path.

The lower-level `ObjStore::get_range_stream` accepts a [`ByteRange`] without
first reading the object prefix. `ByteRange::bounded(start, end)` uses an
end-exclusive bound; `ByteRange::from_offset(start)` reads through EOF. Bounded ends
are clamped to EOF, starting exactly at EOF produces an empty stream, and
reversed ranges or starts beyond EOF return `ObjStoreError::InvalidRequest`.
Missing objects return `Ok(None)`. Use `ObjStore::get_range` when collecting the
selected bytes in memory is preferable to streaming.
