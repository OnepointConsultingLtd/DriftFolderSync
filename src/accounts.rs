//! Account and sync-folder management shared by the CLI and the tray.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};

use crate::auth::{self, TokenProvider};
use crate::config::{self, Account, Config, SyncPair};
use crate::drive::DriveClient;
use crate::limiter::{Bandwidth, TokenBucket};
use crate::secrets;

/// Browser sign-in for an existing `[[account]]`. Verifies the e-mail if
/// one is configured and records it otherwise. Returns the e-mail.
pub async fn login(name: &str) -> Result<String> {
    let cfg = Config::load()?;
    let acct = cfg
        .account(name)
        .with_context(|| format!("no account named '{name}' (add it first with `driftfoldersync account add`)"))?
        .clone();
    let http = crate::daemon::http_client(&cfg)?;
    let refresh = auth::browser_login(&http, &acct).await?;
    secrets::store(&acct.name, &refresh).await?;

    let client = client_for(&acct, &http);
    let about = client.about().await?;
    let email = about.user.email_address;
    if !acct.email.is_empty() && !acct.email.eq_ignore_ascii_case(&email) {
        secrets::delete(&acct.name).await?;
        let _ = auth::revoke(&http, &refresh).await;
        bail!(
            "you signed in as {email}, but account '{}' expects {}. Please sign in again with the right Google account.",
            acct.name,
            acct.email
        );
    }
    if acct.email.is_empty() {
        config::set_account_key(&acct.name, "email", email.as_str().into())?;
    }
    Ok(email)
}

pub async fn logout(name: &str) -> Result<()> {
    let cfg = Config::load()?;
    if let Some(tok) = secrets::load(name).await? {
        let http = crate::daemon::http_client(&cfg)?;
        let _ = auth::revoke(&http, &tok).await;
    }
    secrets::delete(name).await
}

pub fn client_for(acct: &Account, http: &reqwest::Client) -> DriveClient {
    let tokens = Arc::new(TokenProvider::new(acct.clone(), http.clone()));
    DriveClient::new(
        http.clone(),
        tokens,
        TokenBucket::new(acct.requests_per_second, acct.request_burst as f64),
        Bandwidth::new(0, 0),
    )
}

pub fn new_pair(name: &str, account: &str, local: &Path, remote: &str) -> Result<SyncPair> {
    let mut p: SyncPair = toml::from_str(&format!(
        "name = {}\naccount = {}\nlocal = {}\n",
        toml_edit::Value::from(name),
        toml_edit::Value::from(account),
        toml_edit::Value::from(local.to_string_lossy().as_ref())
    ))?;
    p.remote = remote.to_string();
    Ok(p)
}

pub fn unique_pair_name(cfg: &Config, base: &str) -> String {
    let base: String = base
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
        .collect();
    let base = if base.is_empty() { "folder".to_string() } else { base };
    let mut name = base.clone();
    let mut i = 2;
    while cfg.pair(&name).is_some() {
        name = format!("{base}-{i}");
        i += 1;
    }
    name
}

/// Interactive "choose a folder" via the FileChooser portal, then add it as
/// a new sync folder mirrored to "My Drive/<folder name>".
pub async fn add_folder_with_picker(account: &str) -> Result<String> {
    use ashpd::desktop::file_chooser::SelectedFiles;
    let files = SelectedFiles::open_file()
        .title("Choose a folder to keep in sync")
        .accept_label("Sync this folder")
        .directory(true)
        .modal(false)
        .send()
        .await?
        .response()?;
    let uri = files.uris().first().context("no folder chosen")?;
    let path: PathBuf = url::Url::parse(uri.as_str())
        .ok()
        .and_then(|u| u.to_file_path().ok())
        .context("chosen folder is not a local path")?;
    if path.starts_with("/run/user") || path.starts_with("/run/flatpak") {
        bail!(
            "the folder is only reachable through the document portal; grant DriftFolderSync access with \
             `flatpak override --user --filesystem=<path> {}`",
            crate::paths::APP_ID
        );
    }
    let cfg = Config::load()?;
    let base = path.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    let name = unique_pair_name(&cfg, &base);
    let pair = new_pair(&name, account, &path, &format!("/{base}"))?;
    config::append_table("sync", &pair)?;
    Ok(name)
}
