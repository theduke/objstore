# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0-alpha.3](https://github.com/theduke/objstore/compare/objstore-v0.1.0-alpha.2...objstore-v0.1.0-alpha.3) - 2026-06-29

### Other

- fix alpha.2 publish dependencies

## [0.1.0-alpha.2](https://github.com/theduke/objstore/compare/objstore-v0.1.0-alpha.1...objstore-v0.1.0-alpha.2) - 2026-06-29

### Added

- Add a PrefixObjStore
- Add ObjStore::as_any
- Add ObjStore::generate_upload_url
- Add S3ObjStore::bucket_create()
- *(s3-light)* Parse md5 and sha256 hashes for object metadata
- Allow specifying the mime_type for objects
- Add mime_type to ObjectMeta
- Add objstore_config crate
- Implement ListArgs::delimiter support
- Add ObjStore::generate_download_url()

### Fixed

- refine typed object store errors
- *(objstore)* correct if-none-match conditions
- Make size semi-mandatory for stream uploads
- fixup! feat: Add ObjStore::generate_download_url()

### Other

- Fix clippy large error lint
- Fix prefix stream errors and remove debug logs
- Use well-known typed ObjStoreError instead of anyhow
- add description to all crates
- add description to all crates
- Inherit crate version from workspace + harmonize deps
- Fix some clippy lints
- Fix docs build issues
- cargo fmt
- Rename KeyMeta* to ObjectMeta*
- Add objstore_gcs to README
- Delete old unused backend directory
- Fix doctests
- Give birth to the objstore
