use std::sync::Arc;

use dioxus::prelude::*;
use dioxus_bulma::{Modal, Notification};
use futures::{io::Cursor, StreamExt as _};
use objstore::{DynObjStore, ObjStoreExt};

use crate::cmp::util::loader::LoadState;

#[component]
pub fn ObjectCreator(
    store: ReadOnlySignal<DynObjStore>,
    base_path: ReadOnlySignal<String>,
    on_complete: EventHandler<Arc<objstore::ObjectMeta>>,
    on_cancel: EventHandler<()>,
) -> Element {
    let mut state = use_signal::<LoadState<()>>(|| LoadState::Idle);
    let mut key_input = use_signal(|| String::new());
    let mut content_input = use_signal(|| String::new());

    let tx = use_coroutine::<(), _, _>(move |mut rx| {
        let store = store.clone();
        let base_path = base_path.clone();
        let mut state = state.clone();
        let key_input = key_input.clone();
        let content_input = content_input.clone();
        let on_complete = on_complete.clone();
        async move {
            while let Some(_) = rx.next().await {
                state.set(LoadState::Loading);
                let key = key_input.read_unchecked();
                if key.trim().is_empty() {
                    state.set(LoadState::Loaded(Err(
                        "Path must be specified and cannot be empty".to_string(),
                    )));
                    continue;
                }
                let full_key = format!("{}{}", base_path.read_unchecked(), key);
                let data = content_input.read_unchecked().as_bytes().to_vec();
                match store.read_unchecked().put(&full_key).bytes(data).await {
                    Ok(meta) => {
                        on_complete.call(Arc::new(meta));
                        state.set(LoadState::Loaded(Ok(())));
                    }
                    Err(err) => {
                        state.set(LoadState::Loaded(Err(err.to_string())));
                    }
                }
            }
        }
    });

    rsx! {
        Modal {
            children: rsx! {
                div { class: "box",
                    div { class: "field",
                        label { class: "label", "Key Path" }
                        div { class: "control",
                            input {
                                class: "input",
                                r#type: "text",
                                placeholder: "relative/path/to/file.txt",
                                value: "{key_input}",
                                onchange: move |e| key_input.set(e.value()),
                            }
                        }
                    }

                    div { class: "field",
                        label { class: "label", "Contents" }
                        div { class: "control",
                            textarea {
                                class: "textarea",
                                placeholder: "file contents",
                                value: "{content_input}",
                                onchange: move |e| content_input.set(e.value()),
                            }
                        }
                    }

                    match &*state.read() {
                        LoadState::Idle => rsx! {},
                        LoadState::Loading => rsx! {},
                        LoadState::Loaded(Ok(_)) => rsx! {},
                        LoadState::Loaded(Err(err)) => rsx! {
                            Notification {
                                color: dioxus_bulma::Color::Danger,
                                "{err}"
                            }
                        },
                    }

                    div { class: "buttons",
                        button {
                            class: "button is-primary",
                            class: if let LoadState::Loading = &*state.read() { "is-loading" } else { "" },
                            onclick: move |_| { tx.send(()); },
                            "Create"
                        }
                        button {
                            class: "button",
                            onclick: move |_| { on_cancel.call(()); },
                            "Cancel"
                        }
                    }
                }
            },
            on_close: move |_| { on_cancel.call(()); }
        }
    }
}
