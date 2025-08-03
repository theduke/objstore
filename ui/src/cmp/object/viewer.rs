use dioxus::prelude::*;
use objstore::ObjectMeta;
use std::sync::Arc;

use crate::cmp::object::helpers::{object_created, object_modified, object_size};

/// Component for displaying metadata of an object.
#[component]
pub fn ObjectViewer(meta: Arc<ObjectMeta>) -> Element {
    let now = time::OffsetDateTime::now_utc();

    rsx! {
        div {
            class: "box",

            table {
                class: "table is-fullwidth",
                tbody {
                    tr {
                        td { "Key" }
                        td { "{meta.key}" }
                    }
                    tr {
                        td { "Size" }
                        td { "{object_size(&meta)}" }
                    }
                    if let Some(updated) = &meta.updated_at {
                        tr {
                            td { "Updated" }
                            td {
                                "{updated.format(&time::format_description::well_known::Iso8601::DEFAULT).unwrap_or_default()}"
                            }
                        }
                    }
                    if let Some(etag) = &meta.etag {
                        tr {
                            td { "ETag" }
                            td { "{etag}" }
                        }
                    }
                    if let Some(sha256) = &meta.hash_sha256 {
                        tr {
                            td { "SHA256" }
                            td { "{hex::encode(&sha256)}" }
                        }
                    }
                    if let Some(md5) = &meta.hash_md5 {
                        tr {
                            td { "MD5" }
                            td { "{hex::encode(&md5)}" }
                        }
                    }
                    // if let Some(version_id) = &meta.version_id {
                    //     tr {
                    //         td { "Version ID" }
                    //         td { "{version_id}" }
                    //     }
                    // }
                    if let Some(mime_type) = &meta.mime_type {
                        tr {
                            td { "Mime-Type" }
                            td { "{mime_type}" }
                        }
                    }
                }
            }
        }
    }
}
