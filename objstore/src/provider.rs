use crate::store::DynObjStore;

/// A provider/builder for an object store backend.
///
/// Can construct an object store from a generic URI.
/// See [`crate::ObjStoreBuilder`] for usage.
pub trait ObjStoreProvider: Send + Sync + std::fmt::Debug {
    type Config: serde::de::DeserializeOwned + serde::Serialize
    where
        Self: Sized;

    /// Get a unique identifier for this provider.
    ///
    /// eg: "objstore.memory", "objstore.s3", ...
    ///
    /// Equates to [`crate::ObjStore::kind`].
    fn kind(&self) -> &'static str;

    /// Get the url scheme for this provider.
    ///
    /// Used to identify the provider in URIs.
    ///
    /// eg:
    ///   * uri: `memory://` => scheme: `memory`
    ///   * uri: `fs://<path>` => scheme: `fs`
    ///
    /// Equates to [`crate::ObjStore::kind`].
    ///
    /// The returned value must also be the protocol used by `Self::parse_uri`.
    fn url_scheme(&self) -> &str;

    /// Build a new [`crate::ObjStore`] from a generic URI.
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
