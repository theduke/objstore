use dioxus::prelude::*;
use dioxus_bulma::{Color, Notification};

use crate::{cmp::util::loader::Spinner, context::UiConfigStore, router::Route};

#[component]
pub fn ConnectionManager(store: UiConfigStore) -> Element {
    let mut connections = use_resource(move || {
        let store = store.clone();
        async move {
            tracing::info!("Loading connections from config store");
            store.get().load_connections().await
        }
    });

    let connections_loading = match connections.state()() {
        UseResourceState::Pending => true,
        _ => false,
    };

    rsx! {
        div {
            h1 {
                class: "title",
                "Connections"
            }

            div {
                class: "buttons",

                Link {
                    to: Route::NewConnection{},
                    class: "button is-primary",
                    "New Connection"
                }

                button {
                    class: "button",
                    class: if connections_loading {
                        "is-loading"
                    } else {
                        ""
                    },

                    onclick: move |_| {
                        connections.restart();
                    },

                    span {
                        class: "pr-2",
                        "Reload configs"
                    }

                    dioxus_free_icons::Icon {
                        fill: "black",
                        width: 15,
                        height: 15,
                        icon: dioxus_free_icons::icons::fa_solid_icons::FaArrowsRotate,
                    }
                }
            }

            hr {}

            div {
                match &*connections.read() {
                    None => {
                        rsx! {
                            Spinner {}
                        }
                    }
                    Some(Ok(cons)) => {
                        rsx!{

                            div {
                                class: "buttons",

                            }


                            if cons.connections.is_empty() {
                                Notification {
                                    // color: Color::Info,
                                    "No connections found. Please create a new connection."
                                }
                            } else {
                                for conn in cons.connections.iter() {
                                    div {
                                        Link {
                                            to: Route::Browser { store: conn.config.name.clone() },
                                            class: "button is-link",
                                            "{conn.config.name}"
                                        }
                                    }
                                }
                            }

                            if !cons.failed.is_empty() {
                                Notification {
                                    color: Color::Danger,

                                    "Failed to connection(s):"


                                    ul {
                                        class: "content",
                                        for failed in cons.failed.iter() {
                                            li {
                                                "{failed.source:?}: {failed.error}"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    Some(Err(err)) => rsx! {
                        Notification {
                            color: Color::Danger,

                            "{err}"
                        }
                    }
                }
            }
        }
    }
}
