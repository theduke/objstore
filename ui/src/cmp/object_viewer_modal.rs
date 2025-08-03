use dioxus::prelude::*;
use dioxus_bulma::Modal;
use objstore::ObjectMeta;
use std::sync::Arc;

use crate::cmp::object::viewer::ObjectViewer;

/// Modal for displaying object metadata.
#[component]
pub fn ObjectViewerModal(
    object_meta: ReadOnlySignal<Arc<ObjectMeta>>,
    on_cancel: EventHandler<()>,
) -> Element {
    rsx! {
        Modal {
            children: rsx! {
                ObjectViewer { object_meta: object_meta.read_unchecked().clone() }
            },
            on_close: move |_| { on_cancel.call(()); }
        }
    }
}
