use anyhow::{bail, Context};
use dioxus::prelude::*;
use dioxus_bulma::{Color, Notification};
use objstore_config::ConnectionConfig;
use objstore_s3_light::UrlStyle;

use crate::cmp::{s3::ConnectionPersistence, util::form::FormSubmit};

#[component]
pub fn S3Form(
    on_submit: EventHandler<(ConnectionConfig, ConnectionPersistence)>,
    on_cancel: EventHandler<()>,
    status: ReadOnlySignal<FormSubmit>,
) -> Element {
    let mut errors = use_signal::<Option<Vec<String>>>(|| None);

    let mut value_name = use_signal(|| String::new());
    let mut value_url = use_signal(|| String::new());
    let mut value_bucket = use_signal(|| String::new());
    let mut value_access_key_id = use_signal(|| String::new());
    let mut value_secret_access_key = use_signal(|| String::new());

    let submit = Callback::<ConnectionPersistence>::new(move |persist: ConnectionPersistence| {
        if status.read().is_loading() {
            return;
        }

        let build_values = move || -> Result<ConnectionConfig, anyhow::Error> {
            let name = value_name().trim().to_owned();
            if name.is_empty() {
                bail!("Name must not be empty");
            }

            let url_raw = value_url();
            let url_raw = url_raw.trim();
            if url_raw.is_empty() {
                bail!("URL must not be empty");
            }
            let url = url_raw
                .parse()
                .with_context(|| format!("invalid url '{}'", url_raw))?;

            let s = objstore_s3_light::S3ObjStoreConfig {
                url,
                bucket: value_bucket(),
                region: "auto".to_string(),
                path_style: UrlStyle::Path,
                key: value_access_key_id(),
                secret: value_secret_access_key(),
                token: None,
                path_prefix: None,
            };
            s.validate()?;

            let config = ConnectionConfig {
                name: value_name(),
                uri: s.build_uri()?,
                description: None,
            };

            Ok(config)
        };

        match build_values() {
            Ok(config) => {
                errors.set(None);
                on_submit.call((config, persist));
            }
            Err(e) => {
                errors.set(Some(vec![e.to_string()]));
            }
        }
    });

    let (is_loading, submit_error) = match &*status.read() {
        FormSubmit::Idle => (false, None),
        FormSubmit::Loading => (true, None),
        FormSubmit::Error(err) => (false, Some(err.clone())),
    };

    rsx! {
        form {
            onsubmit: move |e| {
                e.prevent_default();
                submit.call(ConnectionPersistence::Persistent);
            },
            div {
                class: "field",

                label {
                    class: "label",
                    "Name"
                }

                div {
                    class: "control",
                    input {
                        class: "input",
                        required: true,
                        r#type: "text",
                        placeholder: "Enter a name for the connection",
                        value: "{value_name}",
                        onchange: move |e| {
                            value_name.set(e.value());
                        }
                    }

                    span {
                        class: "help",
                        "This name will be used to identify the connection. Optional."
                    }
                }
            }

            div {
                class: "field",

                label {
                    class: "label",
                    "URL"
                }

                div {
                    class: "control",
                    input {
                        class: "input",
                        required: true,
                        r#type: "url",
                        placeholder: "Enter URL",
                        value: "{value_url}",
                        onchange: move |e| {
                            value_url.set(e.value());
                        },
                    }
                }
            }

            div {
                class: "field",

                label {
                    class: "label",
                    "Bucket"
                }

                div {
                    class: "control",
                    input {
                        class: "input",
                        required: true,
                        r#type: "text",
                        placeholder: "Enter bucket name",
                        value: "{value_bucket}",
                        onchange: move |e| {
                            value_bucket.set(e.value());
                        },
                    }
                }
            }

            div {
                class: "field",

                label {
                    class: "label",
                    "Access Key ID"
                }

                div {
                    class: "control",
                    input {
                        class: "input",
                        r#type: "text",
                        required: true,
                        placeholder: "Enter access key ID",
                        value: "{value_access_key_id}",
                        onchange: move |e| {
                            value_access_key_id.set(e.value());
                        },
                    }
                }
            }

            div {
                class: "field",

                label {
                    class: "label",
                    "Secret Access Key"
                }

                div {
                    class: "control",
                    input {
                        class: "input",
                        r#type: "password",
                        required: true,
                        placeholder: "Enter secret access key",
                        value: "{value_secret_access_key}",
                        onchange: move |e| {
                            value_secret_access_key.set(e.value());
                        },
                    }
                }
            }

            if let Some(errors) = errors() {
                Notification {
                    color: Color::Danger,

                    ul {
                        class: "content",

                        for error in errors.iter() {
                            li {
                                "{error}"
                            }
                        }
                    }
                }
            }
            if let Some(err) = &submit_error {
                Notification {
                    color: Color::Danger,
                    "{err:#?}"
                }
            }

            div {
                class: "buttons is-large",

                button {
                    class: "button is-primary",
                    class: if is_loading { "is-loading" } else { "" },
                    r#type: "submit",
                    onclick: move |_| {
                        submit.call(ConnectionPersistence::Persistent);
                    },
                    "Save and connect"
                }

                button {
                    class: "button",
                    class: if is_loading { "is-loading" } else { "" },
                    r#type: "submit",
                    onclick: move |_| {
                        submit.call(ConnectionPersistence::Temporary);
                    },
                    "Open without saving"
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
    }
}
