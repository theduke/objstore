use dioxus::prelude::*;
use dioxus_bulma::{Modal, Notification};
use futures::StreamExt as _;
use objstore::DynObjStore;
use std::sync::Arc;

use crate::cmp::util::loader::LoadState;
use crate::store::{default_download_dir, download_object};

/// Modal for downloading objects with output path selection and progress bar.
#[component]
pub fn DownloadModal(
    store: ReadOnlySignal<DynObjStore>,
    object_meta: ReadOnlySignal<Arc<objstore::ObjectMeta>>,
    on_complete: EventHandler<()>,
    on_cancel: EventHandler<()>,
) -> Element {
    let mut state = use_signal::<LoadState<()>>(|| LoadState::Idle);
    let mut progress = use_signal(|| 0u64);
    let size = { object_meta.read_unchecked().size };
    let mut local_path = use_signal(|| {
        let meta = object_meta.read_unchecked();
        let filename = meta
            .key
            .trim_end_matches('/')
            .split('/')
            .last()
            .unwrap_or(&meta.key)
            .replace('/', "_");
        default_download_dir()
            .map(|dir| dir.join(filename).display().to_string())
            .unwrap_or_default()
    });

    let on_progress = use_callback(move |value: u64| {
        progress.set(value);
    });

    let tx = use_coroutine::<(), _, _>(move |mut rx| {
        let store = store.clone();
        let object_meta = object_meta.clone();
        let local_path = local_path.clone();
        let on_complete = on_complete.clone();
        async move {
            while let Some(_) = rx.next().await {
                state.set(LoadState::Loading);
                let meta = object_meta.read_unchecked();
                let path = local_path.read_unchecked();

                match download_object(&store.read_unchecked(), &meta, Some(&path), on_progress)
                    .await
                {
                    Ok(()) => {
                        on_complete.call(());
                        state.set(LoadState::Loaded(Ok(())));
                    }
                    Err(e) => {
                        state.set(LoadState::Loaded(Err(e.to_string())));
                    }
                }
            }
        }
    });

    rsx! {
        Modal {
            children: rsx! {
                div {
                    class: "box",

                    div {
                        class: "field",
                        label { class: "label", "Output Path" }
                        div {
                            class: "control",
                            input {
                                class: "input",
                                r#type: "text",
                                value: "{local_path}",
                                onchange: move |e| local_path.set(e.value())
                            }
                        }
                    }

                    match &*state.read() {
                        LoadState::Idle => rsx! {},
                        LoadState::Loading => rsx! {
                            div {
                                class: "field",
                                div {
                                    class: "control",
                                    progress {
                                        class: "progress is-primary",
                                        max:  if let Some(size) = size {
                                            size.to_string()
                                        },
                                        value: "{progress()}"
                                    }
                                }
                            }
                        },
                        LoadState::Loaded(Ok(())) => rsx! {},
                        LoadState::Loaded(Err(err)) => rsx! {
                            Notification {
                                color: dioxus_bulma::Color::Danger,
                                "{err}"
                            }
                        },
                    }

                    div {
                        class: "buttons",
                        button {
                            class: "button is-primary",
                            class: if let LoadState::Loading = &*state.read() { "is-loading" } else { "" },
                            onclick: move |_| { tx.send(()); },
                            "Download"
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
