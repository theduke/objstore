use dioxus::prelude::*;
use objstore::ObjectMeta;
use std::sync::Arc;
use time::OffsetDateTime;

use crate::cmp::object::helpers::{object_modified, object_size};

#[component]
pub fn ObjectsTable(
    page: Signal<super::Page>,
    now: OffsetDateTime,
    on_download: EventHandler<Arc<ObjectMeta>>,
    on_delete: EventHandler<Arc<ObjectMeta>>,
) -> Element {
    let page = page.read();

    let content = rsx! {
        table {
            class: "table is-fullwidth",

            thead {
                tr {
                    th {}
                    th { "Name" }
                    th { "Modified" }
                    th { "Size" }
                    th { "Actions" }
                }
            }

            tbody {
                for item in page.objects.iter().cloned() {
                    tr {
                        td {
                            // Selector
                        }

                        {
                            let name = item.key.trim_end_matches('/');
                            let display_name = if let Some((_, name)) = name.rsplit_once('/') {
                                name
                            } else {
                                name
                            };
                            rsx! {
                                td { "{display_name}" }
                            }
                        }

                        td { {object_modified(&item, now)} }

                        td { "{object_size(&item)}" }

                        td {
                            div {
                                class: "buttons",
                                button {
                                    class: "button is-small",
                                    onclick: {
                                        let item = item.clone();
                                        move |_| {
                                            on_download.call(item.clone());
                                        }
                                    },
                                    "Download"
                                }
                                button {
                                    class: "button is-small",
                                    onclick: {
                                        let item = item.clone();
                                        move |_| {
                                            on_delete.call(item.clone());
                                        }
                                    },
                                    dioxus_free_icons::Icon {
                                        fill: "black",
                                        width: 15,
                                        height: 15,
                                        icon: dioxus_free_icons::icons::fa_solid_icons::FaTrash,
                                    },
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    content
}
