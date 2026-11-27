//! System tray icon (StatusNotifierItem / AppIndicator) with status and controls.

use std::time::Duration;

use anyhow::Result;
use ksni::menu::{RadioGroup, RadioItem, StandardItem, SubMenu};
use ksni::{MenuItem, TrayMethods};

use crate::config::Config;
use crate::daemon::Controller;
use crate::notify::Notifier;
use crate::paths;
use crate::status::{PairStatus, State};

const INTERVALS: [(&str, &str); 5] = [
    ("realtime", "Real time"),
    ("1m", "Every minute"),
    ("5m", "Every 5 minutes"),
    ("30m", "Every 30 minutes"),
    ("1h", "Every hour"),
];

pub struct DriftTray {
    ctl: Controller,
    pairs: Vec<PairStatus>,
    accounts: Vec<String>,
}

fn esc(s: &str) -> String {
    s.replace('_', "__")
}

fn overall(pairs: &[PairStatus]) -> State {
    let has = |s: State| pairs.iter().any(|p| p.state == s);
    if has(State::AuthRequired) {
        State::AuthRequired
    } else if has(State::Error) {
        State::Error
    } else if has(State::Syncing) || has(State::Starting) {
        State::Syncing
    } else if has(State::Offline) {
        State::Offline
    } else if !pairs.is_empty() && pairs.iter().all(|p| p.state == State::Paused) {
        State::Paused
    } else {
        State::Idle
    }
}

fn open(ctl: &Controller, target: String) {
    ctl.rt.spawn(async move {
        crate::auth::open_url(&target).await;
    });
}

fn item<F: Fn(&mut DriftTray) + Send + 'static>(label: impl Into<String>, f: F) -> MenuItem<DriftTray> {
    StandardItem { label: label.into(), activate: Box::new(f), ..Default::default() }.into()
}

fn disabled(label: impl Into<String>) -> MenuItem<DriftTray> {
    StandardItem { label: label.into(), enabled: false, ..Default::default() }.into()
}

impl ksni::Tray for DriftTray {
    fn id(&self) -> String {
        "driftfoldersync".into()
    }

    fn title(&self) -> String {
        paths::APP_NAME.into()
    }

    fn icon_name(&self) -> String {
        match overall(&self.pairs) {
            State::Idle => paths::APP_ID.into(),
            State::Syncing | State::Starting => "emblem-synchronizing".into(),
            State::Paused => "media-playback-pause".into(),
            State::Offline => "network-offline".into(),
            State::Error | State::AuthRequired => "dialog-warning".into(),
        }
    }

    fn tool_tip(&self) -> ksni::ToolTip {
        let lines: Vec<String> = self
            .pairs
            .iter()
            .map(|p| {
                let mut l = format!("{}: {}", p.name, p.state.label());
                if !p.message.is_empty() && p.state != State::Idle {
                    l.push_str(&format!(" — {}", p.message));
                }
                l
            })
            .collect();
        ksni::ToolTip {
            title: format!("{} — {}", paths::APP_NAME, overall(&self.pairs).label()),
            description: lines.join("\n"),
            ..Default::default()
        }
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        if let Some(p) = self.pairs.first() {
            open(&self.ctl, format!("file://{}", p.local));
        }
    }

    fn menu(&self) -> Vec<MenuItem<Self>> {
        let mut m: Vec<MenuItem<Self>> = Vec::new();
        m.push(disabled(format!("{} — {}", paths::APP_NAME, overall(&self.pairs).label())));
        m.push(MenuItem::Separator);

        for p in &self.pairs {
            let name = p.name.clone();
            let mut sub: Vec<MenuItem<Self>> = Vec::new();
            let mut st = p.state.label().to_string();
            if p.pending > 0 && p.state == State::Syncing {
                st.push_str(&format!(" ({} pending)", p.pending));
            }
            sub.push(disabled(esc(&st)));
            if !p.email.is_empty() {
                sub.push(disabled(esc(&format!("Account: {}", p.email))));
            }
            if !p.storage.is_empty() {
                sub.push(disabled(format!("Storage: {}", p.storage)));
            }
            if let Some(t) = &p.last_sync {
                sub.push(disabled(format!("Last checked: {t}")));
            }
            sub.push(disabled(format!("Session: {} up, {} down", p.uploaded, p.downloaded)));
            sub.push(MenuItem::Separator);
            let local = p.local.clone();
            sub.push(item("Open folder", move |t| open(&t.ctl, format!("file://{local}"))));
            if !p.remote_url.is_empty() {
                let url = p.remote_url.clone();
                sub.push(item("Open in browser", move |t| open(&t.ctl, url.clone())));
            }
            let n = name.clone();
            sub.push(item("Sync now", move |t| {
                let _ = t.ctl.sync_now(Some(&n));
            }));
            let n = name.clone();
            if p.state == State::Paused {
                sub.push(item("Resume", move |t| {
                    let _ = t.ctl.pause(Some(&n), false);
                }));
            } else {
                sub.push(item("Pause", move |t| {
                    let _ = t.ctl.pause(Some(&n), true);
                }));
            }
            if p.blocked_deletes > 0 {
                let n = name.clone();
                sub.push(item(format!("Allow {} held-back deletions", p.blocked_deletes), move |t| {
                    let _ = t.ctl.confirm_deletes(&n);
                }));
            }
            if p.state == State::AuthRequired {
                let acct = p.account.clone();
                sub.push(item("Sign in again…", move |t| {
                    let acct = acct.clone();
                    let ctl = t.ctl.clone();
                    t.ctl.rt.spawn(async move {
                        match crate::accounts::login(&acct).await {
                            Ok(_) => ctl.reload.notify_one(),
                            Err(e) => Notifier::new(true).send("DriftFolderSync: sign-in failed", &format!("{e:#}")),
                        }
                    });
                }));
            }
            let selected = INTERVALS.iter().position(|(v, _)| *v == p.sync_interval).unwrap_or(0);
            let n = name.clone();
            sub.push(
                SubMenu {
                    label: "Sync frequency".into(),
                    submenu: vec![RadioGroup {
                        selected,
                        select: Box::new(move |t: &mut Self, i| {
                            if let Some((v, _)) = INTERVALS.get(i) {
                                if let Err(e) = t.ctl.set_interval(&n, v) {
                                    tracing::warn!("{e:#}");
                                }
                            }
                        }),
                        options: INTERVALS
                            .iter()
                            .map(|(_, l)| RadioItem { label: l.to_string(), ..Default::default() })
                            .collect(),
                    }
                    .into()],
                    ..Default::default()
                }
                .into(),
            );
            if !p.recent.is_empty() {
                sub.push(
                    SubMenu {
                        label: "Recent activity".into(),
                        submenu: p.recent.iter().take(15).map(|r| disabled(esc(r))).collect(),
                        ..Default::default()
                    }
                    .into(),
                );
            }
            if !p.errors.is_empty() {
                sub.push(
                    SubMenu {
                        label: "Recent problems".into(),
                        submenu: p.errors.iter().take(15).map(|r| disabled(esc(r))).collect(),
                        ..Default::default()
                    }
                    .into(),
                );
            }
            m.push(
                SubMenu {
                    label: esc(&format!("{} — {}", p.name, p.state.label())),
                    submenu: sub,
                    ..Default::default()
                }
                .into(),
            );
        }
        if !self.pairs.is_empty() {
            m.push(MenuItem::Separator);
        }
        m.push(item("Sync all now", |t| {
            let _ = t.ctl.sync_now(None);
        }));
        if self.pairs.iter().all(|p| p.state == State::Paused) && !self.pairs.is_empty() {
            m.push(item("Resume all", |t| {
                let _ = t.ctl.pause(None, false);
            }));
        } else {
            m.push(item("Pause all", |t| {
                let _ = t.ctl.pause(None, true);
            }));
        }
        if !self.accounts.is_empty() {
            m.push(
                SubMenu {
                    label: "Add a folder to sync".into(),
                    submenu: self
                        .accounts
                        .iter()
                        .map(|a| {
                            let a2 = a.clone();
                            item(esc(&format!("Using account '{a}'…")), move |t| {
                                let acct = a2.clone();
                                let ctl = t.ctl.clone();
                                t.ctl.rt.spawn(async move {
                                    match crate::accounts::add_folder_with_picker(&acct).await {
                                        Ok(_) => ctl.reload.notify_one(),
                                        Err(e) => Notifier::new(true)
                                            .send("DriftFolderSync: could not add folder", &format!("{e:#}")),
                                    }
                                });
                            })
                        })
                        .collect(),
                    ..Default::default()
                }
                .into(),
            );
        }
        let cfgpath = Config::path();
        m.push(item("Edit settings file", move |t| open(&t.ctl, format!("file://{}", cfgpath.display()))));
        m.push(item("Reload settings", |t| t.ctl.reload.notify_one()));
        m.push(MenuItem::Separator);
        m.push(item("Quit DriftFolderSync", |t| t.ctl.quit.notify_one()));
        m
    }
}

pub async fn spawn(ctl: Controller) -> Result<ksni::Handle<DriftTray>> {
    let accounts = Config::load().map(|c| c.accounts.iter().map(|a| a.name.clone()).collect()).unwrap_or_default();
    let tray = DriftTray { ctl: ctl.clone(), pairs: ctl.status.snapshot(), accounts };
    let handle = tray
        .disable_dbus_name(paths::in_flatpak())
        .assume_sni_available(true)
        .spawn()
        .await?;
    let h2 = handle.clone();
    let mut rx = ctl.status.subscribe();
    tokio::spawn(async move {
        loop {
            if rx.changed().await.is_err() {
                break;
            }
            // Throttle tray refreshes to at most once per second.
            tokio::time::sleep(Duration::from_secs(1)).await;
            rx.borrow_and_update();
            let snap = ctl.status.snapshot();
            let accounts: Vec<String> =
                Config::load().map(|c| c.accounts.iter().map(|a| a.name.clone()).collect()).unwrap_or_default();
            if h2.update(|t| {
                t.pairs = snap;
                t.accounts = accounts;
            })
            .await
            .is_none()
            {
                break;
            }
        }
    });
    Ok(handle)
}
