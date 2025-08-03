use objstore::{DynObjStore, ObjectMeta};

pub type DownloadProgressCallback = Box<dyn Fn(u64) + Send + Sync>;

pub async fn download_object(
    store: &DynObjStore,
    object: &ObjectMeta,
    local_path: Option<&str>,
    on_progress: DownloadProgressCallback,
) -> Result<(), anyhow::Error> {
    #[cfg(feature = "desktop")]
    {
        return download_object_desktop(store, object, local_path, on_progress).await;
    }

    #[cfg(not(feature = "desktop"))]
    {
        bail!("Download support not implemented for this platform");
    }
}

#[cfg(any(feature = "desktop"))]
async fn download_object_desktop(
    store: &DynObjStore,
    object: &ObjectMeta,
    local_path: Option<&str>,
    on_progress: DownloadProgressCallback,
) -> Result<(), anyhow::Error> {
    use anyhow::Context;
    use futures::TryStreamExt;
    use tokio::io::AsyncWriteExt as _;

    let home_dir = std::env::home_dir().context("Failed to determine home directory")?;
    let dir = home_dir.join("Downloads");

    let filename = object
        .key
        .trim_end_matches('/')
        .split('/')
        .last()
        .unwrap_or(&object.key)
        .replace('/', "_");

    let output_path = dir.join(&filename);
    let tmp_path = format!("{filename}.tmp");

    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create directory: {}", dir.display()))?;

    let file = tokio::fs::File::create(&tmp_path)
        .await
        .with_context(|| format!("Failed to create file: {}", tmp_path))?;

    let mut writer = tokio::io::BufWriter::new(file);

    let mut stream = store
        .get_stream(&object.key)
        .await?
        .context("object not found")?;

    let mut progress = 0u64;
    while let Some(chunk) = stream.try_next().await? {
        progress += chunk.len() as u64;
        writer.write_all(&chunk).await?;
        on_progress(progress);
    }
    {
        writer.flush().await?;
        let file = writer.into_inner();
        file.sync_all().await?;
        let _ = file;
    }

    tokio::fs::rename(&tmp_path, &output_path)
        .await
        .with_context(|| format!("Failed to rename temp file to '{}'", output_path.display()))?;

    Ok(())
}
