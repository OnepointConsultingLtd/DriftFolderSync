//! Standard locations. Inside Flatpak the XDG_* variables already point at
//! the per-application directories under ~/.var/app/<id>/.

use std::path::PathBuf;

pub const APP_ID: &str = "io.github.OnepointConsultingLtd.DriftFolderSync";
pub const APP_NAME: &str = "DriftFolderSync";

fn xdg(var: &str, fallback: &str) -> PathBuf {
    match std::env::var_os(var) {
        Some(v) if !v.is_empty() => PathBuf::from(v),
        _ => home().join(fallback),
    }
}

pub fn home() -> PathBuf {
    std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/"))
}

pub fn config_dir() -> PathBuf {
    if let Some(p) = std::env::var_os("DRIFTFOLDERSYNC_CONFIG_DIR") {
        return PathBuf::from(p);
    }
    xdg("XDG_CONFIG_HOME", ".config").join("driftfoldersync")
}

pub fn data_dir() -> PathBuf {
    if let Some(p) = std::env::var_os("DRIFTFOLDERSYNC_DATA_DIR") {
        return PathBuf::from(p);
    }
    xdg("XDG_DATA_HOME", ".local/share").join("driftfoldersync")
}

pub fn runtime_dir() -> PathBuf {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    if in_flatpak() {
        // Shared between all running instances of this Flatpak app.
        return base.join("app").join(APP_ID).join("driftfoldersync");
    }
    base.join("driftfoldersync")
}

pub fn socket_path() -> PathBuf {
    runtime_dir().join("control.sock")
}

pub fn in_flatpak() -> bool {
    std::path::Path::new("/.flatpak-info").exists()
}
