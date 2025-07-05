//! Key-Value storage abstractions.
//!
//! See the [`ObjStore`] trait.

mod builder;
mod provider;
mod store;
mod types;
pub mod wrapper;

pub use self::{
    builder::ObjStoreBuilder,
    provider::ObjStoreProvider,
    store::{DynObjStore, ObjStore, ObjStoreExt},
    types::*,
};
