# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0-alpha.2](https://github.com/theduke/objstore/compare/objstore_s3_light-v0.1.0-alpha.1...objstore_s3_light-v0.1.0-alpha.2) - 2026-06-29

### Added

- Add ObjStore::generate_upload_url

### Fixed

- refine typed object store errors
- *(s3-light)* improve S3 compatibility
- Make size semi-mandatory for stream uploads

### Other

- Fix clippy large error lint
- Use well-known typed ObjStoreError instead of anyhow
- Add fetch_metadata_after_put setting
- Upgrade rusty-s3 to 0.9
- Upgrade reqwest to 0.13
