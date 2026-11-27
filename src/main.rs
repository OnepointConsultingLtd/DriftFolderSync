// DriftFolderSync — real-time two-way folder synchronisation for Linux.
// Copyright (C) 2026 DriftFolderSync contributors
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option)
// any later version.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
// more details.
//
// You should have received a copy of the GNU General Public License along
// with this program. If not, see <https://www.gnu.org/licenses/>.

mod accounts;
mod auth;
mod autostart;
mod config;
mod daemon;
mod db;
mod drive;
mod engine;
mod ipc;
mod limiter;
mod localfs;
mod notify;
mod paths;
mod reconcile;
mod secrets;
mod status;
mod tray;
mod tree;
mod util;
mod watcher;

#[cfg(test)]
mod testsupport;
#[cfg(test)]
mod e2e_tests;

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

use crate::config::{Account, Config, SyncMode};
use crate::ipc::Request;

#[derive(Parser)]
#[command(name = "driftfoldersync", version, about = "Real-time two-way sync between Linux folders and Google Drive accounts", long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the sync service (normally started at login).
    Daemon {
        /// Do not show a system tray icon.
        #[arg(long)]
        no_tray: bool,
    },
    /// Show the state of every sync folder.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Pause syncing (all folders, or one).
    Pause { folder: Option<String> },
    /// Resume syncing.
    Resume { folder: Option<String> },
    /// Check for changes immediately.
    SyncNow { folder: Option<String> },
    /// Allow a large batch of deletions that was held back for safety.
    ConfirmDeletes { folder: String },
    /// Set how often a folder syncs: realtime, 1m, 5m, 30m, 1h, …
    Interval { folder: String, interval: String },
    /// Manage accounts.
    #[command(subcommand)]
    Account(AccountCmd),
    /// Manage sync folders.
    #[command(subcommand)]
    Folder(FolderCmd),
    /// Open a .gdoc/.gsheet/… link file in the browser.
    Open { file: PathBuf },
    /// Enable or disable starting at login.
    Autostart {
        #[arg(value_parser = ["on", "off"])]
        state: String,
    },
    /// Tell the running daemon to re-read its settings.
    Reload,
    /// Stop the running daemon.
    Quit,
    /// Print the settings file path.
    ConfigPath,
}

#[derive(Subcommand)]
enum AccountCmd {
    /// Add an account (uses your own OAuth client — see README).
    Add {
        name: String,
        #[arg(long)]
        client_id: String,
        #[arg(long, default_value = "")]
        client_secret: String,
        /// Expected Google account e-mail (checked at sign-in).
        #[arg(long, default_value = "")]
        email: String,
    },
    /// Sign in with the browser.
    Login { name: String },
    /// Sign out and revoke access.
    Logout { name: String },
    /// List accounts and whether they are signed in.
    List,
}

#[derive(Subcommand)]
enum FolderCmd {
    /// Add a folder to sync.
    Add {
        /// Name for this sync folder.
        name: String,
        #[arg(long)]
        account: String,
        /// Local directory.
        #[arg(long)]
        local: PathBuf,
        /// Folder in My Drive (or the shared drive), e.g. "/Documents". "/" = whole drive.
        #[arg(long, default_value = "/")]
        remote: String,
        /// Shared drive name or ID.
        #[arg(long, default_value = "")]
        shared_drive: String,
        /// two-way, upload-only or download-only.
        #[arg(long, default_value = "two-way")]
        mode: String,
        /// realtime, 1m, 5m, 30m, …
        #[arg(long, default_value = "realtime")]
        interval: String,
    },
    /// Stop syncing a folder (files are left untouched on both sides).
    Remove { name: String },
    /// List sync folders.
    List,
    /// Forget sync state so the folder is re-compared from scratch.
    Reset { name: String },
}

fn init_logging(daemon: bool) {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    let filter = EnvFilter::try_from_env("DRIFTFOLDERSYNC_LOG").unwrap_or_else(|_| {
        EnvFilter::new(if daemon { "info,zbus=warn,ksni=warn" } else { "warn" })
    });
    let stderr = fmt::layer().with_writer(std::io::stderr).with_target(false);
    let file = if daemon {
        let dir = paths::data_dir();
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("driftfoldersync.log");
        if std::fs::metadata(&path).map(|m| m.len() > 10 * 1024 * 1024).unwrap_or(false) {
            let _ = std::fs::rename(&path, dir.join("driftfoldersync.log.1"));
        }
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .ok()
            .map(|f| fmt::layer().with_writer(std::sync::Mutex::new(f)).with_ansi(false).with_target(false))
    } else {
        None
    };
    tracing_subscriber::registry().with(filter).with(stderr).with(file).init();
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_logging(matches!(cli.cmd, Cmd::Daemon { .. }));
    let _ = rustls::crypto::ring::default_provider().install_default();
    if let Err(e) = run(cli).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::Daemon { no_tray } => daemon::run(no_tray).await,
        Cmd::Status { json } => {
            let r = ipc::request(&Request::Status).await?;
            let st = r.status.unwrap_or_default();
            if json {
                println!("{}", serde_json::to_string_pretty(&st)?);
                return Ok(());
            }
            if st.is_empty() {
                println!("No sync folders configured.");
            }
            for p in st {
                println!("{} [{}] — {}", p.name, p.email, p.state.label());
                println!("  {}  ⇄  {}", p.local, p.remote_url);
                println!("  frequency: {}   last checked: {}", p.sync_interval, p.last_sync.as_deref().unwrap_or("never"));
                if !p.message.is_empty() {
                    println!("  {}", p.message);
                }
                if p.blocked_deletes > 0 {
                    println!("  ⚠ {} deletions held back: driftfoldersync confirm-deletes '{}'", p.blocked_deletes, p.name);
                }
                for e in p.errors.iter().take(3) {
                    println!("  ! {e}");
                }
                for a in p.recent.iter().take(5) {
                    println!("  · {a}");
                }
            }
            Ok(())
        }
        Cmd::Pause { folder } => ipc::request(&Request::Pause { pair: folder }).await.map(|_| ()),
        Cmd::Resume { folder } => ipc::request(&Request::Resume { pair: folder }).await.map(|_| ()),
        Cmd::SyncNow { folder } => ipc::request(&Request::SyncNow { pair: folder }).await.map(|_| ()),
        Cmd::ConfirmDeletes { folder } => ipc::request(&Request::ConfirmDeletes { pair: folder }).await.map(|_| ()),
        Cmd::Interval { folder, interval } => {
            config::parse_interval(&interval)?;
            if ipc::daemon_running().await {
                ipc::request(&Request::SetInterval { pair: folder, interval }).await?;
            } else {
                config::set_pair_key(&folder, "sync_interval", interval.as_str().into())?;
            }
            Ok(())
        }
        Cmd::Reload => ipc::request(&Request::Reload).await.map(|_| ()),
        Cmd::Quit => ipc::request(&Request::Quit).await.map(|_| ()),
        Cmd::ConfigPath => {
            println!("{}", Config::path().display());
            Ok(())
        }
        Cmd::Open { file } => {
            let url = localfs::read_link_file_url(&file)?;
            if !url.starts_with("https://") {
                bail!("refusing to open non-https link");
            }
            if !auth::open_url(&url).await {
                println!("{url}");
            }
            Ok(())
        }
        Cmd::Autostart { state } => {
            let on = state == "on";
            autostart::set(on).await?;
            if Config::path().exists() {
                let text = std::fs::read_to_string(Config::path())?;
                let mut doc: toml_edit::DocumentMut = text.parse()?;
                if doc.get("general").is_none() {
                    doc["general"] = toml_edit::table();
                }
                doc["general"]["start_at_login"] = toml_edit::value(on);
                config::write_atomic(&Config::path(), doc.to_string().as_bytes())?;
            }
            println!("Start at login {}.", if on { "enabled" } else { "disabled" });
            Ok(())
        }
        Cmd::Account(a) => account_cmd(a).await,
        Cmd::Folder(f) => folder_cmd(f).await,
    }
}

async fn account_cmd(a: AccountCmd) -> Result<()> {
    match a {
        AccountCmd::Add { name, client_id, client_secret, email } => {
            let cfg = Config::load()?;
            if cfg.account(&name).is_some() {
                bail!("account '{name}' already exists");
            }
            let acct = Account { name: name.clone(), email, client_id, client_secret, requests_per_second: 5.0, request_burst: 10 };
            config::append_table("account", &acct)?;
            println!("Account '{name}' added. Now signing in…");
            let email = accounts::login(&name).await?;
            println!("Signed in as {email}.");
            Ok(())
        }
        AccountCmd::Login { name } => {
            let email = accounts::login(&name).await?;
            println!("Signed in as {email}.");
            if ipc::daemon_running().await {
                ipc::request(&Request::Reload).await?;
            }
            Ok(())
        }
        AccountCmd::Logout { name } => {
            accounts::logout(&name).await?;
            println!("Signed out of '{name}'.");
            Ok(())
        }
        AccountCmd::List => {
            let cfg = Config::load()?;
            for a in &cfg.accounts {
                let signed_in = secrets::load(&a.name).await?.is_some();
                println!(
                    "{}\t{}\t{}",
                    a.name,
                    if a.email.is_empty() { "-" } else { &a.email },
                    if signed_in { "signed in" } else { "not signed in" }
                );
            }
            Ok(())
        }
    }
}

async fn folder_cmd(f: FolderCmd) -> Result<()> {
    match f {
        FolderCmd::Add { name, account, local, remote, shared_drive, mode, interval } => {
            let cfg = Config::load()?;
            if cfg.account(&account).is_none() {
                bail!("no account named '{account}'");
            }
            let local = if local.is_absolute() { local } else { std::env::current_dir()?.join(local) };
            let mut p = accounts::new_pair(&name, &account, &local, &remote)?;
            p.shared_drive = shared_drive;
            p.mode = match mode.as_str() {
                "two-way" => SyncMode::TwoWay,
                "upload-only" => SyncMode::UploadOnly,
                "download-only" => SyncMode::DownloadOnly,
                m => bail!("unknown mode '{m}'"),
            };
            config::parse_interval(&interval)?;
            p.sync_interval = interval;
            config::append_table("sync", &p)?;
            println!("Added '{name}': {} ⇄ {}{}", local.display(), if p.shared_drive.is_empty() { "My Drive" } else { &p.shared_drive }, p.remote);
            if ipc::daemon_running().await {
                ipc::request(&Request::Reload).await?;
            }
            Ok(())
        }
        FolderCmd::Remove { name } => {
            let path = Config::path();
            let text = std::fs::read_to_string(&path).context("no settings file")?;
            let mut doc: toml_edit::DocumentMut = text.parse()?;
            let arr = doc.get_mut("sync").and_then(|i| i.as_array_of_tables_mut()).context("no sync folders")?;
            let before = arr.len();
            arr.retain(|t| t.get("name").and_then(|n| n.as_str()) != Some(name.as_str()));
            if arr.len() == before {
                bail!("no sync folder named '{name}'");
            }
            config::write_atomic(&path, doc.to_string().as_bytes())?;
            db::Db::open(&paths::data_dir().join("state.db"))?.clear_pair(&name)?;
            println!("Removed '{name}'. No files were deleted.");
            if ipc::daemon_running().await {
                ipc::request(&Request::Reload).await?;
            }
            Ok(())
        }
        FolderCmd::List => {
            let cfg = Config::load()?;
            for p in &cfg.pairs {
                println!(
                    "{}\t{}\t{}{}\t{:?}\t{}{}",
                    p.name,
                    p.local.display(),
                    if p.shared_drive.is_empty() { String::new() } else { format!("[{}]", p.shared_drive) },
                    p.remote,
                    p.mode,
                    p.sync_interval,
                    if p.paused { "\tpaused" } else { "" }
                );
            }
            Ok(())
        }
        FolderCmd::Reset { name } => {
            if ipc::daemon_running().await {
                bail!("stop the daemon first (driftfoldersync quit)");
            }
            db::Db::open(&paths::data_dir().join("state.db"))?.clear_pair(&name)?;
            println!("Sync state for '{name}' cleared; it will be re-compared on next start.");
            Ok(())
        }
    }
}
