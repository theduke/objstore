use std::sync::Arc;

use crate::KvStore;

#[cfg(feature = "memory")]
pub mod memory;

#[cfg(feature = "s3")]
pub mod s3;

#[cfg(feature = "fs")]
pub mod fs;

#[allow(
    clippy::large_enum_variant,
    reason = "Large size does not matter for a config enum"
)]
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KvConfig {
    Memory,
    #[cfg(feature = "fs")]
    Fs(fs::FsKvConfig),
    #[cfg(feature = "s3")]
    S3(s3::S3KvConfig),
}

impl KvConfig {
    pub fn build_dyn(self) -> Result<Arc<dyn KvStore>, anyhow::Error> {
        match self {
            KvConfig::Memory => Ok(Arc::new(memory::MemoryKvStore::new()) as Arc<dyn KvStore>),
            #[cfg(feature = "fs")]
            KvConfig::Fs(config) => {
                let store = fs::FsKvStore::new(config)?;
                Ok(Arc::new(store) as Arc<dyn KvStore>)
            }
            #[cfg(feature = "s3")]
            KvConfig::S3(config) => {
                let store = s3::S3KvStore::new(config)?;
                Ok(Arc::new(store) as Arc<dyn KvStore>)
            }
        }
    }
}
