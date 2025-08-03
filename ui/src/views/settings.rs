use dioxus::prelude::*;

#[component]
pub fn Settings() -> Element {
    rsx! {
        div {
            h1 { class: "title", "Settings" }

            p { "Settings content goes here." }
        }
    }
}
