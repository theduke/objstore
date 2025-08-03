use anyhow::bail;
use dioxus::prelude::*;
use dioxus_bulma::{Color, Notification};

use crate::{
    cmp::{util::loader::Spinner, Browser},
    context::{use_config_store, use_stores, ActiveStore},
};

#[component]
pub fn BrowserPage(store: ReadOnlySignal<String>) -> Element {
    let active_store = use_resource::<Result<ActiveStore, anyhow::Error>, _>(move || async move {
        let stores = use_stores();

        let name = store();

        if let Some(store) = stores.get(&name) {
            return Ok(store.clone());
        }

        let config_store = use_config_store();
        let configs = config_store.get().load_connections().await?;

        let Some(config) = configs.get(&name) else {
            bail!("Connection '{name}' not found in config store");
        };

        let builder = crate::context::use_providers();
        let store = builder.build(&config.config.uri)?;

        stores.register(config.config.name.clone(), config.clone(), store.clone());

        Ok::<_, anyhow::Error>(ActiveStore {
            config: config.clone(),
            store,
        })
    });

    let out = match &*active_store.read() {
        None => {
            rsx! {
                Spinner {}
            }
        }
        Some(Err(err)) => {
            rsx! {
                Notification {
                    color: Color::Danger,
                    "{err}"
                }
            }
        }
        Some(Ok(store)) => {
            rsx! {
                Browser {
                    store: store.clone(),
                }
            }
        }
    };

    out
}
