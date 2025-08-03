#![allow(non_snake_case)]

mod cmp;
mod context;
mod router;
mod views;

mod store;

use std::sync::Arc;

use context::provide_stores;
use dioxus::prelude::*;
use objstore_config::DynConfigStore;

const FAVICON: Asset = asset!("/assets/favicon.ico");
const MAIN_CSS: Asset = asset!("/assets/styles/main.css");

fn main() -> Result<(), anyhow::Error> {
    let config_store: DynConfigStore = {
        #[cfg(feature = "desktop")]
        {
            Arc::new(objstore_config::FsConfigStore::new_default()?)
        }

        #[cfg(not(feature = "desktop"))]
        {
            todo!("Config store not implemented for web builds");
        }
    };

    let mut builder = objstore::ObjStoreBuilder::new();

    #[cfg(feature = "desktop")]
    {
        builder = builder.with_provider(Arc::new(objstore_s3_light::S3LightProvider::new()));
    }

    #[cfg(feature = "desktop")]
    {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let guard = rt.enter();
        std::mem::forget(guard);
        std::mem::forget(rt);
    }

    dioxus::LaunchBuilder::new()
        .with_context_provider(move || Box::new(context::UiConfigStore::new(config_store.clone())))
        .with_context_provider(move || Box::new(context::UiStoreBuilder::new(builder.clone())))
        .launch(App);

    Ok(())
}

#[component]
fn App() -> Element {
    provide_stores();

    rsx! {
        document::Link { rel: "icon", href: FAVICON }
        document::Link { rel: "stylesheet", href: MAIN_CSS }

        dioxus_bulma::embed::StylesheetBulma {}


        Router::<router::Route> {}
    }
}
