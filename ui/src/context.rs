use std::{collections::HashMap, sync::Arc};

use dioxus::{
    hooks::use_context_provider,
    signals::{Readable, Signal, Writable as _},
};
use objstore::{DynObjStore, ObjStoreBuilder};
use objstore_config::{DynConfigStore, LoadedConnection};

#[derive(Clone)]
pub struct UiConfigStore(DynConfigStore);

impl PartialEq for UiConfigStore {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for UiConfigStore {}

impl UiConfigStore {
    pub fn new(config_store: DynConfigStore) -> Self {
        Self(config_store)
    }

    pub fn get(&self) -> &DynConfigStore {
        &self.0
    }
}

impl AsRef<DynConfigStore> for UiConfigStore {
    fn as_ref(&self) -> &DynConfigStore {
        &self.0
    }
}

#[derive(Clone, Debug, Default)]
pub struct UiStoreBuilder(Arc<ObjStoreBuilder>);

impl UiStoreBuilder {
    pub fn new(builder: ObjStoreBuilder) -> Self {
        Self(Arc::new(builder))
    }

    pub fn get(&self) -> &ObjStoreBuilder {
        &self.0
    }
}

impl AsRef<ObjStoreBuilder> for UiStoreBuilder {
    fn as_ref(&self) -> &ObjStoreBuilder {
        &self.0
    }
}

impl PartialEq for UiStoreBuilder {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for UiStoreBuilder {}

// pub fn use_connections() -> Signal<LoadedConnections> {
//     dioxus::hooks::use_context::<Signal<LoadedConnections>>()
// }

pub fn use_config_store() -> UiConfigStore {
    dioxus::hooks::use_context::<UiConfigStore>()
}

pub fn use_providers() -> ObjStoreBuilder {
    dioxus::hooks::use_context::<UiStoreBuilder>().get().clone()
}

#[derive(Clone, Debug)]
pub struct ActiveStore {
    pub config: LoadedConnection,
    pub store: DynObjStore,
}

impl PartialEq for ActiveStore {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.store, &other.store) && self.config == other.config
    }
}

impl Eq for ActiveStore {}

#[derive(Clone, Copy, Debug)]
pub struct Stores {
    pub stores: Signal<HashMap<String, ActiveStore>>,
}

impl Stores {
    pub fn register(&self, name: String, config: LoadedConnection, store: DynObjStore) {
        let active = ActiveStore { config, store };
        self.stores.write_unchecked().insert(name, active);
    }

    pub fn get(&self, name: &str) -> Option<ActiveStore> {
        self.stores.read_unchecked().get(name).cloned()
    }
}

pub fn provide_stores() {
    let stores = Stores {
        stores: Signal::new(HashMap::new()),
    };
    use_context_provider(move || stores);
}

pub fn use_stores() -> Stores {
    dioxus::hooks::use_context::<Stores>()
}
