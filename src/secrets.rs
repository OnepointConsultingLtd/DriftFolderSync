//! Refresh-token storage.
//!
//! Uses the freedesktop Secret Service (GNOME Keyring / KWallet) when run
//! natively, and the Secret portal backed encrypted file keyring when run in
//! Flatpak (both via `oo7`). If neither is available, falls back to a file
//! readable only by the user, with a warning.

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;

use anyhow::{Context, Result};
use tracing::warn;

use crate::paths;

fn attrs(account: &str) -> HashMap<&str, &str> {
    let mut m = HashMap::new();
    m.insert("application", "driftfoldersync");
    m.insert("account", account);
    m
}

fn file_only() -> bool {
    std::env::var("DRIFTFOLDERSYNC_TOKEN_STORE").is_ok_and(|v| v == "file")
}

async fn keyring() -> Result<oo7::Keyring> {
    if file_only() {
        anyhow::bail!("keyring disabled by DRIFTFOLDERSYNC_TOKEN_STORE=file");
    }
    let k = oo7::Keyring::new().await.context("opening keyring")?;
    let _ = k.unlock().await;
    Ok(k)
}

fn fallback_path(account: &str) -> std::path::PathBuf {
    paths::data_dir().join("tokens").join(format!("{account}.token"))
}

pub async fn store(account: &str, refresh_token: &str) -> Result<()> {
    match keyring().await {
        Ok(k) => {
            k.create_item(
                &format!("DriftFolderSync refresh token ({account})"),
                &attrs(account),
                refresh_token,
                true,
            )
            .await
            .context("storing token in keyring")?;
            let _ = std::fs::remove_file(fallback_path(account));
            Ok(())
        }
        Err(e) => {
            if !file_only() {
                warn!("keyring unavailable ({e:#}); storing token in a private file instead");
            }
            let p = fallback_path(account);
            std::fs::create_dir_all(p.parent().unwrap())?;
            std::fs::set_permissions(p.parent().unwrap(), std::fs::Permissions::from_mode(0o700))?;
            std::fs::write(&p, refresh_token)?;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600))?;
            Ok(())
        }
    }
}

pub async fn load(account: &str) -> Result<Option<String>> {
    if let Ok(k) = keyring().await {
        if let Ok(items) = k.search_items(&attrs(account)).await {
            if let Some(item) = items.first() {
                let _ = item.unlock().await;
                let s = item.secret().await?;
                return Ok(Some(String::from_utf8_lossy(&s).into_owned()));
            }
        }
    }
    match std::fs::read_to_string(fallback_path(account)) {
        Ok(s) => Ok(Some(s.trim().to_string())),
        Err(_) => Ok(None),
    }
}

pub async fn delete(account: &str) -> Result<()> {
    if let Ok(k) = keyring().await {
        let _ = k.delete(&attrs(account)).await;
    }
    let _ = std::fs::remove_file(fallback_path(account));
    Ok(())
}
