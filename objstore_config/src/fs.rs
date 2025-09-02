use std::path::{Path, PathBuf};

use anyhow::{Context as _, bail};

use crate::{
    ConfigSource, ConnectionConfig, ConnectionLoadError, LoadedConnection, LoadedConnections,
};

const CONFIG_DIR_NAME: &str = "objstore";
const CONNECTIONS_DIR_NAME: &str = "connections";

#[derive(Debug, Clone)]
pub struct FsConfigStore {
    path: PathBuf,
}

impl FsConfigStore {
    fn default_config_dir() -> Result<PathBuf, anyhow::Error> {
        let home = std::env::home_dir().context("Could not determine home directory")?;

        let dir = home.join(".config").join(CONFIG_DIR_NAME);

        Ok(dir)
    }

    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn new_default() -> Result<Self, anyhow::Error> {
        let path = Self::default_config_dir()?;
        Ok(Self { path })
    }

    fn connections_dir(&self) -> PathBuf {
        self.path.join(CONNECTIONS_DIR_NAME)
    }

    pub fn connections(&self) -> Result<LoadedConnections, anyhow::Error> {
        let connections_dir = self.connections_dir();

        let reader = match std::fs::read_dir(connections_dir) {
            Ok(reader) => reader,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    return Ok(LoadedConnections {
                        connections: Vec::new(),
                        failed: Vec::new(),
                    });
                }
                bail!("Failed to read connections directory: {}", err);
            }
        };

        let mut cons = LoadedConnections::default();
        for res in reader {
            let entry = res?;
            if entry.file_type()?.is_file() {
                let path = entry.path();
                let contents = std::fs::read_to_string(&path).with_context(|| {
                    format!("Failed to read connection file: '{}'", path.display())
                })?;
                match Self::parse_connection_config(&path, contents) {
                    Ok(new_cons) => {
                        cons.extend(new_cons);
                    }
                    Err(err) => {
                        cons.failed.push(ConnectionLoadError {
                            source: path.into(),
                            error: err.to_string(),
                            index: None,
                        });
                    }
                }
            }
        }

        Ok(cons)
    }

    fn parse_connection_config(
        path: &Path,
        contents: String,
    ) -> Result<LoadedConnections, anyhow::Error> {
        let ext = path
            .extension()
            .context("config file does not have an extension")?
            .to_str()
            .context("config file extension is not valid UTF-8")?;
        let value = match ext {
            "json" => serde_json::from_str::<serde_json::Value>(&contents)
                .context("Failed to parse JSON")?,
            "yaml" | "yml" => serde_yaml::from_str::<serde_json::Value>(&contents)
                .context("Failed to parse YAML connection config")?,
            _ => bail!("Unsupported file extension: '{}'", ext),
        };

        let cons = match value {
            obj @ serde_json::Value::Object(_) => {
                match serde_json::from_value::<ConnectionConfig>(obj) {
                    Ok(config) => LoadedConnections::new_one_loaded(path.to_owned(), config),
                    Err(err) => LoadedConnections::new_one_failed(
                        path.to_owned(),
                        format!("Failed to parse connection config: {err}"),
                        Some(0),
                    ),
                }
            }
            serde_json::Value::Array(arr) => {
                let mut cons = LoadedConnections::default();

                for (index, item) in arr.into_iter().enumerate() {
                    match serde_json::from_value::<ConnectionConfig>(item) {
                        Ok(config) => {
                            cons.connections.push(LoadedConnection {
                                source: Some(path.to_owned().into()),
                                config,
                            });
                        }
                        Err(err) => {
                            cons.failed.push(ConnectionLoadError {
                                source: path.to_owned().into(),
                                error: format!("Failed to parse connection config: {err}"),
                                index: Some(index),
                            });
                        }
                    }
                }

                cons
            }
            other => {
                bail!(
                    "A config file must contain either a list of connection configs or a single connection - got {:?}",
                    other
                );
            }
        };
        Ok(cons)
    }

    fn save_connection(
        &self,
        config: &ConnectionConfig,
        is_new: bool,
        source: Option<ConfigSource>,
    ) -> Result<LoadedConnection, anyhow::Error> {
        // FIXME: handle is_new and source properly
        let _ = (is_new, source);

        let connections_dir = self.connections_dir();
        std::fs::create_dir_all(&connections_dir).with_context(|| {
            format!(
                "Failed to create connections directory '{}'",
                connections_dir.display()
            )
        })?;

        let file_name = format!("{}.yaml", config.name);
        let file_path = connections_dir.join(file_name);

        let contents = serde_yaml::to_string(config)
            .context("Failed to serialize connection config to YAML")?;

        std::fs::write(&file_path, contents).with_context(|| {
            format!(
                "Failed to write connection config to '{}'",
                file_path.display()
            )
        })?;

        Ok(LoadedConnection {
            source: Some(file_path.into()),
            config: config.clone(),
        })
    }
}

#[async_trait::async_trait]
impl crate::ConfigStore for FsConfigStore {
    async fn load_connections(&self) -> Result<LoadedConnections, anyhow::Error> {
        #[cfg(feature = "tokio")]
        {
            let s = self.clone();
            tokio::task::spawn_blocking(move || s.connections())
                .await
                .context("Failed to load connections")?
        }

        #[cfg(not(feature = "tokio"))]
        {
            self.connections()
        }
    }

    async fn save_connection(
        &self,
        config: ConnectionConfig,
        is_new: bool,
        source: Option<ConfigSource>,
    ) -> Result<LoadedConnection, anyhow::Error> {
        #[cfg(feature = "tokio")]
        {
            let s = self.clone();
            tokio::task::spawn_blocking(move || s.save_connection(&config, is_new, source))
                .await
                .context("Failed to save connection")?
        }

        #[cfg(not(feature = "tokio"))]
        {
            self.save_connection(&config, is_new, source)
        }
    }
}
