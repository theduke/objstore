use anyhow::Context;
use dioxus::prelude::*;
use futures::StreamExt as _;
use objstore::DynObjStore;
use objstore_config::{ConnectionConfig, DynConfigStore, LoadedConnection};

use crate::{
    cmp::{
        s3::{ConnectionPersistence, S3Form},
        util::form::FormSubmit,
    },
    context::{use_config_store, use_providers, use_stores},
    router::Route,
};

enum Msg {
    Submit {
        config: ConnectionConfig,
        persist: ConnectionPersistence,
    },
    Cancel,
}

#[component]
pub fn NewConnection() -> Element {
    let mut status = use_signal(|| FormSubmit::Idle);

    let coro = use_coroutine::<Msg, _, _>(move |mut rx| async move {
        let mut task: Option<dioxus_core::Task> = None;

        while let Some(msg) = rx.next().await {
            match msg {
                Msg::Cancel => {
                    if let Some(handle) = task.take() {
                        handle.cancel();
                    }
                }
                Msg::Submit { config, persist } => {
                    status.set(FormSubmit::Loading);
                    let handle = spawn(async move {
                        let builder = use_providers();
                        let store = use_config_store().get().clone();
                        let stores = use_stores();

                        tracing::info!("creating new connection: {:?}", config);
                        match create_connection(&store, &builder, config.clone(), persist).await {
                            Ok((config, store)) => {
                                tracing::info!("Connection created successfully: {:?}", config);
                                stores.register(config.config.name.clone(), config, store);
                            }
                            Err(e) => {
                                tracing::error!("Failed to create connection: {:#?}", e);
                                status.set(FormSubmit::Error(e.to_string()));
                            }
                        }
                    });
                    task = Some(handle);
                }
            }
        }
    });

    rsx! {
        S3Form {
            status,
            on_submit: move |(config, persist)| {
                coro.send(Msg::Submit{
                    config,
                    persist,
                });
            },
            on_cancel: move |_| {
                use_navigator().push(Route::Home {  });
            },
            initial_value: None,
        }
    }
}

async fn create_connection(
    config_store: &DynConfigStore,
    builder: &objstore::ObjStoreBuilder,
    config: ConnectionConfig,
    save: ConnectionPersistence,
) -> Result<(LoadedConnection, DynObjStore), anyhow::Error> {
    let store = builder
        .build(&config.uri)
        .with_context(|| format!("Failed to build store for URI: '{}'", config.uri))?;
    store.healthcheck().await?;

    let con = match save {
        ConnectionPersistence::Persistent => {
            config_store.save_connection(config, true, None).await?
        }
        ConnectionPersistence::Temporary => LoadedConnection {
            source: None,
            config: config.clone(),
        },
    };

    Ok((con, store))
}
