use crate::store::DynObjStore;

/// A provider/builder for an object store backend.
///
/// Can construct an object store from a generic URI.
/// See [`crate::ObjStoreBuilder`] for usage.
pub trait ObjStoreProvider {
    /// Get a descriptive name for backend implementation.
    ///
    /// eg: "memory", "s3", ...
    ///
    /// Equates to [`crate::ObjStore::kind`].
    ///
    /// The returned value must also be the protocol used by `Self::parse_uri`.
    fn kind(&self) -> &str;

    /// Build a new [`ObjStore`] from a generic URI.
    ///
    /// Used by the [`crate::ObjStoreBuilder`] to allow for dynamic construction.
    ///
    /// [`Self::kind`] must match the scheme of the provided URL.
    ///
    /// Use query parameters to pass additional configuration options.
    ///
    /// eg:
    /// * `memory://`
    /// * `s3://<access_key>:<secret_key>@<url>/bucket`
    fn build(&self, url: &url::Url) -> Result<DynObjStore, anyhow::Error>;
}
