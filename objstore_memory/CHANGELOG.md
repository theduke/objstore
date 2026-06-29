# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0-alpha.2](https://github.com/theduke/objstore/compare/objstore_memory-v0.1.0-alpha.1...objstore_memory-v0.1.0-alpha.2) - 2026-06-29

### Added

- Add ObjStore::generate_upload_url
- Add objstore_config crate
- Implement ListArgs::delimiter support
- Add ObjStore::generate_download_url()

### Fixed

- Make size semi-mandatory for stream uploads

### Other

- Use well-known typed ObjStoreError instead of anyhow
- Unify Rust edition usage across crates
- add description to all crates
- Inherit crate version from workspace + harmonize deps
- cargo fmt
- Rename KeyMeta* to ObjectMeta*
- Add objstore_gcs to README
- Give birth to the objstore
