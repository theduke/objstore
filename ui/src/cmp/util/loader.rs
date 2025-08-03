use dioxus::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadStatus {
    Idle,
    Loading,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LoadState<T> {
    Idle,
    Loading,
    Loaded(Result<T, String>),
}

#[component]
pub fn Spinner() -> Element {
    rsx! {
        button {
            class: "button is-loading",
            "Loading..."
        }
    }
}
