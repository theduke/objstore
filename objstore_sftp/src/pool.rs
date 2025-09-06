use std::{net::SocketAddr, sync::Arc};

use anyhow::anyhow;
use futures::future::BoxFuture;
use russh::client::{self, Handle};
use russh_sftp::client::{SftpSession, error::Error as SftpError};
use tokio::{
    net::lookup_host,
    sync::{Mutex, Semaphore},
};

/// Internal connection pool managing a single SSH connection with multiple
/// SFTP sessions.
pub(crate) struct SftpPool {
    host: String,
    port: u16,
    username: String,
    password: String,
    handle: Mutex<Option<Handle<ClientHandler>>>,
    sessions: Mutex<Vec<SftpSession>>, // idle sessions
    semaphore: Semaphore,
    reconnect_lock: Mutex<()>,
}

impl SftpPool {
    pub(crate) fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        max_size: usize,
    ) -> Self {
        Self {
            host,
            port,
            username,
            password,
            handle: Mutex::new(None),
            sessions: Mutex::new(Vec::new()),
            semaphore: Semaphore::new(max_size),
            reconnect_lock: Mutex::new(()),
        }
    }

    /// Borrow an SFTP session from the pool and run the provided operation.
    /// If the operation fails due to a connection error the connection is
    /// re-established and the operation is retried once.
    pub(crate) async fn with_sftp<T, F>(&self, f: F) -> Result<T, anyhow::Error>
    where
        F: for<'a> Fn(&'a SftpSession) -> BoxFuture<'a, Result<T, SftpError>> + Send + Sync,
        T: Send,
    {
        let mut last_err = None;
        for _ in 0..2 {
            // limit concurrent sessions
            let permit = self
                .semaphore
                .acquire()
                .await
                .map_err(|_| anyhow!("semaphore closed"))?;
            let session = match self.get_session().await {
                Ok(s) => s,
                Err(e) => {
                    drop(permit);
                    last_err = Some(e);
                    continue;
                }
            };
            let res = f(&session).await;
            match res {
                Ok(val) => {
                    self.sessions.lock().await.push(session);
                    drop(permit);
                    return Ok(val);
                }
                Err(err) => {
                    drop(permit);
                    if Self::is_reconnect_error(&err) {
                        self.invalidate().await;
                        last_err = Some(err.into());
                        continue;
                    } else {
                        self.sessions.lock().await.push(session);
                        return Err(err.into());
                    }
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("operation failed")))
    }

    /// Get a free session or establish a new one.
    async fn get_session(&self) -> Result<SftpSession, anyhow::Error> {
        if let Some(sess) = self.sessions.lock().await.pop() {
            return Ok(sess);
        }

        {
            let mut handle_guard = self.handle.lock().await;
            if handle_guard.is_none() {
                drop(handle_guard);
                self.reconnect().await?;
                handle_guard = self.handle.lock().await;
            }
            let handle = handle_guard
                .as_mut()
                .ok_or_else(|| anyhow!("connection not available"))?;
            let res = Self::open_sftp(handle).await;
            drop(handle_guard);
            match res {
                Ok(sftp) => return Ok(sftp),
                Err(e) => {
                    self.invalidate().await;
                    return Err(e);
                }
            }
        }
    }

    /// Establish a new SSH connection.
    async fn reconnect(&self) -> Result<(), anyhow::Error> {
        let _guard = self.reconnect_lock.lock().await;
        let mut handle_guard = self.handle.lock().await;
        if handle_guard.is_some() {
            return Ok(());
        }
        let config = russh::client::Config::default();
        let config = Arc::new(config);
        let addr_str = format!("{}:{}", self.host, self.port);
        let mut addrs = lookup_host(addr_str).await?;
        let addr: SocketAddr = addrs
            .next()
            .ok_or_else(|| anyhow!("could not resolve address"))?;
        let mut session = client::connect(config, addr, ClientHandler).await?;
        session
            .authenticate_password(self.username.clone(), self.password.clone())
            .await?;
        *handle_guard = Some(session);
        Ok(())
    }

    async fn open_sftp(handle: &mut Handle<ClientHandler>) -> Result<SftpSession, anyhow::Error> {
        let channel = handle.channel_open_session().await?;
        channel.request_subsystem(true, "sftp").await?;
        let sftp = SftpSession::new(channel.into_stream()).await?;
        Ok(sftp)
    }

    async fn invalidate(&self) {
        self.sessions.lock().await.clear();
        let mut h = self.handle.lock().await;
        *h = None;
    }

    fn is_reconnect_error(err: &SftpError) -> bool {
        // Protocol status responses don't require reconnecting
        !matches!(err, SftpError::Status(_))
    }
}

#[derive(Clone)]
pub(crate) struct ClientHandler;

impl russh::client::Handler for ClientHandler {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}
