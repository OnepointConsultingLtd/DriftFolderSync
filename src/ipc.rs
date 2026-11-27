//! Control socket: one JSON request per line, one JSON response per line.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use crate::paths;
use crate::status::PairStatus;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "kebab-case")]
pub enum Request {
    Status,
    Pause { pair: Option<String> },
    Resume { pair: Option<String> },
    SyncNow { pair: Option<String> },
    ConfirmDeletes { pair: String },
    SetInterval { pair: String, interval: String },
    Reload,
    Quit,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Response {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<Vec<PairStatus>>,
}

impl Response {
    pub fn ok() -> Self {
        Self { ok: true, ..Default::default() }
    }
    pub fn err(e: impl ToString) -> Self {
        Self { ok: false, error: Some(e.to_string()), ..Default::default() }
    }
}

pub async fn request(req: &Request) -> Result<Response> {
    let path = paths::socket_path();
    let stream = tokio::time::timeout(Duration::from_secs(3), UnixStream::connect(&path))
        .await
        .context("timed out connecting to daemon")?
        .with_context(|| "the DriftFolderSync daemon is not running (start it with: driftfoldersync daemon)")?;
    let (r, mut w) = stream.into_split();
    let mut line = serde_json::to_string(req)?;
    line.push('\n');
    w.write_all(line.as_bytes()).await?;
    let mut reader = BufReader::new(r);
    let mut resp = String::new();
    tokio::time::timeout(Duration::from_secs(30), reader.read_line(&mut resp))
        .await
        .context("timed out waiting for daemon")??;
    let resp: Response = serde_json::from_str(&resp)?;
    if !resp.ok {
        bail!("{}", resp.error.clone().unwrap_or_else(|| "request failed".into()));
    }
    Ok(resp)
}

pub async fn daemon_running() -> bool {
    UnixStream::connect(paths::socket_path()).await.is_ok()
}

pub fn bind() -> Result<UnixListener> {
    let path = paths::socket_path();
    std::fs::create_dir_all(path.parent().unwrap())?;
    let _ = std::fs::remove_file(&path);
    let l = UnixListener::bind(&path)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    Ok(l)
}

pub async fn serve<F, Fut>(listener: UnixListener, handler: F)
where
    F: Fn(Request) -> Fut + Clone + Send + Sync + 'static,
    Fut: std::future::Future<Output = Response> + Send,
{
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        let h = handler.clone();
        tokio::spawn(async move {
            let (r, mut w) = stream.into_split();
            let mut reader = BufReader::new(r);
            let mut line = String::new();
            if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                return;
            }
            let resp = match serde_json::from_str::<Request>(&line) {
                Ok(req) => h(req).await,
                Err(e) => Response::err(format!("bad request: {e}")),
            };
            let mut out = serde_json::to_string(&resp).unwrap_or_default();
            out.push('\n');
            let _ = w.write_all(out.as_bytes()).await;
        });
    }
}
