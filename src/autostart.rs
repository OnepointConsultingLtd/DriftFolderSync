//! Start at login: the Background portal inside Flatpak, an XDG autostart
//! entry otherwise.

use anyhow::{Context, Result};

use crate::paths;

fn desktop_file() -> std::path::PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| paths::home().join(".config"));
    base.join("autostart").join(format!("{}.desktop", paths::APP_ID))
}

pub async fn set(enabled: bool) -> Result<()> {
    if paths::in_flatpak() {
        use ashpd::desktop::background::Background;
        let resp = Background::request()
            .reason("Keep your folders in sync after you log in")
            .auto_start(enabled)
            .command(&["driftfoldersync", "daemon"])
            .dbus_activatable(false)
            .send()
            .await?
            .response()
            .context("the request to run in the background was refused")?;
        if enabled && !resp.auto_start() {
            anyhow::bail!("start at login was not permitted by the desktop");
        }
        return Ok(());
    }
    let path = desktop_file();
    if enabled {
        let exe = std::env::current_exe()?;
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(
            &path,
            format!(
                "[Desktop Entry]\nType=Application\nName={}\nComment=Keep folders in sync with the cloud\n\
                 Exec=\"{}\" daemon\nIcon={}\nX-GNOME-Autostart-enabled=true\nNoDisplay=true\n",
                paths::APP_NAME,
                exe.display(),
                paths::APP_ID
            ),
        )?;
    } else {
        let _ = std::fs::remove_file(&path);
    }
    Ok(())
}
