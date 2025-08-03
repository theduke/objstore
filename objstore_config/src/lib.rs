use std::{path::PathBuf, sync::Arc};

mod fs;

pub use self::fs::FsConfigStore;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ConnectionConfig {
    pub uri: String,
    pub name: String,
    pub description: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ConfigSource {
    File(PathBuf),
}

impl From<PathBuf> for ConfigSource {
    fn from(path: PathBuf) -> Self {
        ConfigSource::File(path)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LoadedConnection {
    pub source: Option<ConfigSource>,
    pub config: ConnectionConfig,
}

#[derive(Debug, Clone, Default)]
pub struct LoadedConnections {
    pub connections: Vec<LoadedConnection>,
    pub failed: Vec<ConnectionLoadError>,
}

impl LoadedConnections {
    pub fn get(&self, name: &str) -> Option<&LoadedConnection> {
        self.connections.iter().find(|c| c.config.name == name)
    }

    pub fn new_one_loaded(source: impl Into<ConfigSource>, config: ConnectionConfig) -> Self {
        Self {
            connections: vec![LoadedConnection {
                source: Some(source.into()),
                config,
            }],
            failed: Vec::new(),
        }
    }

    pub fn new_one_failed(
        source: impl Into<ConfigSource>,
        error: String,
        index: Option<usize>,
    ) -> Self {
        Self {
            connections: Vec::new(),
            failed: vec![ConnectionLoadError {
                source: source.into(),
                error,
                index,
            }],
        }
    }

    fn extend(&mut self, other: Self) {
        self.connections.extend(other.connections);
        self.failed.extend(other.failed);
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionLoadError {
    pub source: ConfigSource,
    pub error: String,
    pub index: Option<usize>,
}

#[async_trait::async_trait]
pub trait ConfigStore {
    async fn load_connections(&self) -> Result<LoadedConnections, anyhow::Error>;

    async fn save_connection(
        &self,
        connection: ConnectionConfig,
        is_new: bool,
        source: Option<ConfigSource>,
    ) -> Result<LoadedConnection, anyhow::Error>;
}

pub type DynConfigStore = Arc<dyn ConfigStore + Send + Sync>;
