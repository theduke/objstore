use dioxus::prelude::*;

use crate::{cmp::ConnectionManager, context::use_config_store};

#[component]
pub fn Home() -> Element {
    let store = use_config_store();

    rsx! {
        ConnectionManager {
            store: store,
        }
    }
}
