Objstore is a Rust library that abstracts over various object storage backends,
providing a unified interface.

It supports different providers, each implemented in their own crate.

## Develop

Use `cargo check -q --message-format=short` to check code.
Only use normal `cargo check` when the short format does not contain enough information.

Run `cargo fmt --all` after making changes to ensure code is properly formatted.
