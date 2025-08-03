use dioxus::prelude::*;
use dioxus_bulma::{Modal, Notification};
use futures::StreamExt as _;
use objstore::DynObjStore;

use crate::cmp::util::loader::LoadState;

#[component]
pub fn ObjectDeleteModal(
    store: ReadOnlySignal<DynObjStore>,
    object_key: ReadOnlySignal<String>,
    on_complete: EventHandler<()>,
    on_cancel: EventHandler<()>,
) -> Element {
    let mut state = use_signal::<LoadState<()>>(|| LoadState::Idle);

    let tx = use_coroutine::<(), _, _>(move |mut rx| async move {
        while let Some(_) = rx.next().await {
            state.set(LoadState::Loading);
            let new_state = match store.read_unchecked().delete(&object_key()).await {
                Ok(()) => {
                    on_complete.call(());
                    LoadState::Loaded(Ok(()))
                }
                Err(e) => LoadState::Loaded(Err(e.to_string())),
            };

            state.set(new_state);
        }
    });

    rsx! {
        Modal {
            children: rsx!{
                div {
                    class: "box",

                    Notification {
                        color: dioxus_bulma::Color::Warning,
                        span {
                            "Really delete ",
                            strong { "{object_key}" },
                            "?",
                        }
                    }

                    div {
                        class: "buttons",

                        button {
                            class: "button is-danger",
                            class: if let LoadState::Loading = &*state.read() {
                                "is-loading"
                            } else {
                                ""
                            },
                            onclick: move |_| {
                                tx.send(());
                            },
                            "Delete"
                        }

                        button {
                            class: "button",
                            onclick: move |_| {
                                on_cancel.call(());
                            },
                            "Cancel"
                        }
                    }

                }
            },
            on_close: move |_| {
                on_cancel.call(());
            },
        }
    }
}
