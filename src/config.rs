//! Configuration file handling (`config.toml`).

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::paths;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub general: General,
    #[serde(default, rename = "account")]
    pub accounts: Vec<Account>,
    #[serde(default, rename = "sync")]
    pub pairs: Vec<SyncPair>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct General {
    /// Show desktop notifications for conflicts, errors and blocked deletions.
    pub notifications: bool,
    /// Show a system tray (StatusNotifierItem) icon while the daemon runs.
    pub tray: bool,
    /// Explicit proxy URL (http://, https:// or socks5://). Empty = use
    /// the standard *_proxy environment variables, if any.
    pub proxy: String,
    /// Global upload bandwidth limit in KiB/s (0 = unlimited).
    pub max_upload_kibps: u64,
    /// Global download bandwidth limit in KiB/s (0 = unlimited).
    pub max_download_kibps: u64,
    /// Maximum number of simultaneous file transfers per sync folder.
    pub max_concurrent_transfers: usize,
    /// CPU niceness applied to the daemon (0-19). Also lowers I/O priority.
    pub nice: i32,
    /// Start automatically at login.
    pub start_at_login: bool,
}

impl Default for General {
    fn default() -> Self {
        Self {
            notifications: true,
            tray: true,
            proxy: String::new(),
            max_upload_kibps: 0,
            max_download_kibps: 0,
            max_concurrent_transfers: 4,
            nice: 10,
            start_at_login: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Account {
    /// Local name used to refer to the account in `[[sync]]` sections.
    pub name: String,
    /// Expected e-mail address (used as a login hint and to verify that the
    /// correct Google account was chosen in the browser). Optional.
    #[serde(default)]
    pub email: String,
    /// OAuth "Desktop app" client ID from your own Google Cloud project.
    pub client_id: String,
    /// OAuth client secret that Google issues for Desktop app clients.
    #[serde(default)]
    pub client_secret: String,
    /// Sustained API requests per second for this account.
    #[serde(default = "default_rps")]
    pub requests_per_second: f64,
    /// Short burst allowance on top of `requests_per_second`.
    #[serde(default = "default_burst")]
    pub request_burst: u32,
}

fn default_rps() -> f64 {
    5.0
}
fn default_burst() -> u32 {
    10
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SyncMode {
    /// Changes flow both ways (Drive for desktop "mirror" behaviour).
    #[default]
    TwoWay,
    /// Local → cloud only (Drive for desktop "Back up to Google Drive").
    UploadOnly,
    /// Cloud → local only.
    DownloadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ConflictPolicy {
    /// Keep both: the local copy is renamed "name (conflicted copy <host> <date>).ext"
    /// and uploaded; the cloud version is downloaded under the original name.
    #[default]
    KeepBoth,
    PreferLocal,
    PreferRemote,
    /// Whichever has the newer modification time wins.
    PreferNewest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum GoogleDocsMode {
    /// Create small `.gdoc`/`.gsheet`/… link files that open in the browser.
    #[default]
    Link,
    /// Download an Office/OpenDocument export (read-only mirror; local edits
    /// to exported files are not uploaded).
    Export,
    /// Do not represent native documents locally at all.
    Ignore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SymlinkMode {
    /// Ignore symbolic links.
    #[default]
    Skip,
    /// Store symlinks as `<name>.rclonelink` files containing the target
    /// (compatible with `rclone --links`).
    Rclonelink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum DeleteMode {
    /// Deleted items are moved to the cloud bin / local Trash.
    #[default]
    Trash,
    /// Deletions are not propagated at all.
    Never,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum InitialMatch {
    /// Files already present on both sides are considered identical if size
    /// matches and modification times are within 2 seconds; otherwise an MD5
    /// is computed. Fast for migrating from rclone.
    #[default]
    SizeMtime,
    /// Always compare MD5 checksums.
    Checksum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncPair {
    /// Unique name for this sync folder.
    pub name: String,
    /// Name of the `[[account]]` to use.
    pub account: String,
    /// Local directory.
    pub local: PathBuf,
    /// Folder path within My Drive (or the shared drive) — "/" for the root.
    #[serde(default = "default_remote")]
    pub remote: String,
    /// Name or ID of a shared drive. Empty = My Drive.
    #[serde(default)]
    pub shared_drive: String,
    #[serde(default)]
    pub mode: SyncMode,
    /// How often changes are pushed/pulled: "realtime", or a duration such
    /// as "1m", "5m", "30m". Batching avoids floods of API calls for files
    /// under heavy modification.
    #[serde(default = "default_interval")]
    pub sync_interval: String,
    /// In real-time mode, how often the cloud is polled for changes.
    #[serde(default = "default_poll")]
    pub remote_poll_interval: String,
    /// In real-time mode, how long a file must be quiet before it is uploaded.
    #[serde(default = "default_settle")]
    pub settle_time: String,
    /// Full local rescan interval (safety net in addition to inotify).
    #[serde(default = "default_rescan")]
    pub full_rescan_interval: String,
    #[serde(default)]
    pub conflict_policy: ConflictPolicy,
    #[serde(default)]
    pub google_docs: GoogleDocsMode,
    #[serde(default)]
    pub symlinks: SymlinkMode,
    #[serde(default)]
    pub delete: DeleteMode,
    #[serde(default)]
    pub initial_match: InitialMatch,
    /// Glob patterns (gitignore-like, matched against the relative path and
    /// the file name) that are never synced.
    #[serde(default = "default_excludes")]
    pub exclude: Vec<String>,
    /// Selective sync: relative folder paths that are NOT synced.
    #[serde(default)]
    pub exclude_folders: Vec<String>,
    /// Selective sync: if non-empty, ONLY these relative folder paths are synced.
    #[serde(default)]
    pub include_folders: Vec<String>,
    /// Skip files larger than this many MiB (0 = no limit).
    #[serde(default)]
    pub max_file_size_mib: u64,
    /// Download files that Google has flagged as malware/spam (you accept the risk).
    #[serde(default)]
    pub acknowledge_abuse: bool,
    /// Refuse to propagate more than this many deletions in a single cycle
    /// without confirmation (`driftfoldersync confirm-deletes <name>`). 0 = no guard.
    #[serde(default = "default_delete_guard")]
    pub delete_guard: usize,
    /// Pause this folder.
    #[serde(default)]
    pub paused: bool,
}

fn default_remote() -> String {
    "/".into()
}
fn default_interval() -> String {
    "realtime".into()
}
fn default_poll() -> String {
    "30s".into()
}
fn default_settle() -> String {
    "3s".into()
}
fn default_rescan() -> String {
    "6h".into()
}
fn default_delete_guard() -> usize {
    250
}
pub fn default_excludes() -> Vec<String> {
    [
        "*.tmp",
        "*.swp",
        "*~",
        "~$*",
        ".~lock.*#",
        ".directory",
        ".DS_Store",
        "Thumbs.db",
        "desktop.ini",
        ".Trash-*",
        ".driftfoldersync-*",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// `None` means real-time.
pub fn parse_interval(s: &str) -> Result<Option<Duration>> {
    let t = s.trim().to_ascii_lowercase();
    if t.is_empty() || t == "realtime" || t == "real-time" || t == "0" {
        return Ok(None);
    }
    let d = humantime::parse_duration(&t).with_context(|| format!("invalid duration '{s}'"))?;
    Ok(Some(d))
}

pub fn parse_duration(s: &str) -> Result<Duration> {
    humantime::parse_duration(s.trim()).with_context(|| format!("invalid duration '{s}'"))
}

impl SyncPair {
    pub fn interval(&self) -> Option<Duration> {
        parse_interval(&self.sync_interval).ok().flatten()
    }
    pub fn poll_interval(&self) -> Duration {
        parse_duration(&self.remote_poll_interval)
            .unwrap_or(Duration::from_secs(30))
            .max(Duration::from_secs(10))
    }
    pub fn settle(&self) -> Duration {
        parse_duration(&self.settle_time).unwrap_or(Duration::from_secs(3))
    }
    pub fn rescan(&self) -> Duration {
        parse_duration(&self.full_rescan_interval)
            .unwrap_or(Duration::from_secs(6 * 3600))
            .max(Duration::from_secs(300))
    }
}

impl Config {
    pub fn path() -> PathBuf {
        paths::config_dir().join("config.toml")
    }

    pub fn load() -> Result<Self> {
        Self::load_from(&Self::path())
    }

    pub fn load_from(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        let mut names = std::collections::HashSet::new();
        for a in &self.accounts {
            if !names.insert(&a.name) {
                bail!("duplicate account name '{}'", a.name);
            }
            if a.client_id.trim().is_empty() {
                bail!("account '{}' has no client_id (see README: OAuth set-up)", a.name);
            }
        }
        let mut pnames = std::collections::HashSet::new();
        for (i, p) in self.pairs.iter().enumerate() {
            if !pnames.insert(&p.name) {
                bail!("duplicate sync name '{}'", p.name);
            }
            if !self.accounts.iter().any(|a| a.name == p.account) {
                bail!("sync '{}' refers to unknown account '{}'", p.name, p.account);
            }
            if !p.local.is_absolute() {
                bail!("sync '{}': local path must be absolute", p.name);
            }
            parse_interval(&p.sync_interval)?;
            parse_duration(&p.remote_poll_interval)?;
            parse_duration(&p.settle_time)?;
            parse_duration(&p.full_rescan_interval)?;
            for q in &self.pairs[..i] {
                if p.local.starts_with(&q.local) || q.local.starts_with(&p.local) {
                    bail!("sync folders '{}' and '{}' overlap", p.name, q.name);
                }
            }
        }
        Ok(())
    }

    pub fn account(&self, name: &str) -> Option<&Account> {
        self.accounts.iter().find(|a| a.name == name)
    }

    pub fn pair(&self, name: &str) -> Option<&SyncPair> {
        self.pairs.iter().find(|p| p.name == name)
    }
}

/// Edit a single key of a `[[sync]]` table in place, preserving comments.
pub fn set_pair_key(pair: &str, key: &str, value: toml_edit::Value) -> Result<()> {
    edit_table("sync", pair, key, value)
}

pub fn set_account_key(account: &str, key: &str, value: toml_edit::Value) -> Result<()> {
    edit_table("account", account, key, value)
}

fn edit_table(array: &str, name: &str, key: &str, value: toml_edit::Value) -> Result<()> {
    let path = Config::path();
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    let mut doc: toml_edit::DocumentMut = text.parse()?;
    let arr = doc
        .get_mut(array)
        .and_then(|i| i.as_array_of_tables_mut())
        .with_context(|| format!("no [[{array}]] sections in config"))?;
    let table = arr
        .iter_mut()
        .find(|t| t.get("name").and_then(|n| n.as_str()) == Some(name))
        .with_context(|| format!("no [[{array}]] named '{name}'"))?;
    table[key] = toml_edit::Item::Value(value);
    let out = doc.to_string();
    // Re-validate before writing.
    let cfg: Config = toml::from_str(&out)?;
    cfg.validate()?;
    write_atomic(&path, out.as_bytes())
}

/// Replace every key of the `[[array]]` table named `name` with the
/// serialised `value`, keeping comments and the order of other tables.
pub fn update_table<T: Serialize>(array: &str, name: &str, value: &T) -> Result<()> {
    let path = Config::path();
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    let mut doc: toml_edit::DocumentMut = text.parse()?;
    let fresh: toml_edit::DocumentMut = toml::to_string(value)?.parse()?;
    let arr = doc
        .get_mut(array)
        .and_then(|i| i.as_array_of_tables_mut())
        .with_context(|| format!("no [[{array}]] sections in config"))?;
    let table = arr
        .iter_mut()
        .find(|t| t.get("name").and_then(|n| n.as_str()) == Some(name))
        .with_context(|| format!("no [[{array}]] named '{name}'"))?;
    copy_keys(fresh.as_table(), table);
    save_doc(&path, &doc)
}

/// Remove the `[[array]]` table named `name`.
pub fn remove_table(array: &str, name: &str) -> Result<()> {
    let path = Config::path();
    let text = std::fs::read_to_string(&path).context("no settings file")?;
    let mut doc: toml_edit::DocumentMut = text.parse()?;
    let arr = doc
        .get_mut(array)
        .and_then(|i| i.as_array_of_tables_mut())
        .with_context(|| format!("no [[{array}]] sections in config"))?;
    let before = arr.len();
    arr.retain(|t| t.get("name").and_then(|n| n.as_str()) != Some(name));
    if arr.len() == before {
        bail!("no [[{array}]] named '{name}'");
    }
    save_doc(&path, &doc)
}

/// Replace the `[general]` table's keys, keeping comments.
pub fn update_general(g: &General) -> Result<()> {
    let path = Config::path();
    std::fs::create_dir_all(path.parent().unwrap())?;
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    let mut doc: toml_edit::DocumentMut = text.parse()?;
    let fresh: toml_edit::DocumentMut = toml::to_string(g)?.parse()?;
    if doc.get("general").is_none() {
        doc["general"] = toml_edit::table();
    }
    let t = doc["general"].as_table_mut().context("[general] is not a table")?;
    copy_keys(fresh.as_table(), t);
    save_doc(&path, &doc)
}

fn copy_keys(from: &toml_edit::Table, to: &mut toml_edit::Table) {
    for (k, v) in from.iter() {
        if let Some(existing) = to.get_mut(k) {
            if let (Some(old), Some(new)) = (existing.as_value_mut(), v.as_value()) {
                // Keep any trailing comment on the line.
                let decor = old.decor().clone();
                *old = new.clone();
                *old.decor_mut() = decor;
                continue;
            }
        }
        to[k] = v.clone();
    }
}

fn save_doc(path: &Path, doc: &toml_edit::DocumentMut) -> Result<()> {
    let out = doc.to_string();
    let cfg: Config = toml::from_str(&out)?;
    cfg.validate()?;
    write_atomic(path, out.as_bytes())
}

/// Append a new table to the config file, preserving existing content.
pub fn append_table<T: Serialize>(array: &str, value: &T) -> Result<()> {
    let path = Config::path();
    std::fs::create_dir_all(path.parent().unwrap())?;
    let mut text = std::fs::read_to_string(&path).unwrap_or_default();
    #[derive(Serialize)]
    struct Wrap<'a, T: Serialize> {
        #[serde(flatten)]
        inner: std::collections::BTreeMap<&'a str, Vec<&'a T>>,
    }
    let mut m = std::collections::BTreeMap::new();
    m.insert(array, vec![value]);
    let snippet = toml::to_string_pretty(&Wrap { inner: m })?;
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push('\n');
    text.push_str(&snippet);
    let cfg: Config = toml::from_str(&text)?;
    cfg.validate()?;
    write_atomic(&path, text.as_bytes())
}

pub fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intervals() {
        assert_eq!(parse_interval("realtime").unwrap(), None);
        assert_eq!(parse_interval("5m").unwrap(), Some(Duration::from_secs(300)));
        assert_eq!(parse_interval("30m").unwrap(), Some(Duration::from_secs(1800)));
        assert!(parse_interval("banana").is_err());
    }

    #[test]
    fn gui_edits_keep_comments() {
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("DRIFTFOLDERSYNC_CONFIG_DIR", dir.path());
        std::fs::write(Config::path(), include_str!("../config.example.toml")).unwrap();
        let cfg = Config::load().unwrap();
        let mut p = cfg.pair("bkconnect").unwrap().clone();
        p.sync_interval = "30m".into();
        p.exclude_folders = vec!["Old".into()];
        update_table("sync", "bkconnect", &p).unwrap();
        let mut g = cfg.general.clone();
        g.max_upload_kibps = 512;
        update_general(&g).unwrap();
        let text = std::fs::read_to_string(Config::path()).unwrap();
        assert!(text.contains("# global bandwidth limits"), "comments must survive");
        let again = Config::load().unwrap();
        assert_eq!(again.pair("bkconnect").unwrap().sync_interval, "30m");
        assert_eq!(again.pair("bkconnect").unwrap().exclude_folders, vec!["Old".to_string()]);
        assert_eq!(again.general.max_upload_kibps, 512);
        assert_eq!(again.pair("onepoint").unwrap().sync_interval, "realtime");
        remove_table("sync", "bkconnect").unwrap();
        assert!(Config::load().unwrap().pair("bkconnect").is_none());
        std::env::remove_var("DRIFTFOLDERSYNC_CONFIG_DIR");
    }

    #[test]
    fn parse_example() {
        let text = include_str!("../config.example.toml");
        let cfg: Config = toml::from_str(text).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.pairs.len(), 2);
    }
}
