//! Key-Value storage abstractions.
//!
//! See the [`ObjStore`] trait.

mod builder;
mod error;
mod provider;
mod store;
mod types;
pub mod wrapper;

pub use self::{
    builder::ObjStoreBuilder,
    error::{BoxError, ObjStoreError, Operation, Resource, Result},
    provider::ObjStoreProvider,
    store::{DynObjStore, ObjStore, ObjStoreExt},
    types::*,
};
