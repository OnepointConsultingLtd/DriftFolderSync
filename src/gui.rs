//! Desktop GUI (Slint). Runs as its own process and talks to the sync
//! service over the control socket; closing the window never stops syncing.

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use slint::{ComponentHandle, Model, ModelRc, SharedString, VecModel};
use tokio::runtime::Handle;

use crate::config::{
    self, ConflictPolicy, Config, DeleteMode, GoogleDocsMode, InitialMatch, SymlinkMode, SyncMode, SyncPair,
};
use crate::ipc::{self, Request};
use crate::status::{PairStatus, State};
use crate::{accounts, autostart, paths, secrets};

slint::include_modules!();

const INTERVALS: [&str; 5] = ["realtime", "1m", "5m", "30m", "1h"];

fn ss(s: impl AsRef<str>) -> SharedString {
    SharedString::from(s.as_ref())
}

fn model<T: Clone + 'static>(v: Vec<T>) -> ModelRc<T> {
    ModelRc::new(VecModel::from(v))
}

fn lines(v: &[String]) -> SharedString {
    ss(v.join("\n"))
}

fn unlines(s: &str) -> Vec<String> {
    s.lines().map(|l| l.trim().to_string()).filter(|l| !l.is_empty()).collect()
}

// ---------------------------------------------------------------- conversions

fn status_row(p: &PairStatus) -> FolderStatus {
    let kind = match p.state {
        State::Idle => 0,
        State::Syncing => 1,
        State::Paused => 2,
        State::Offline => 3,
        State::Error => 4,
        State::AuthRequired => 5,
        State::Starting => 6,
    };
    FolderStatus {
        name: ss(&p.name),
        account: ss(&p.account),
        email: ss(&p.email),
        local: ss(&p.local),
        remote_url: ss(&p.remote_url),
        state: ss(p.state.label()),
        state_kind: kind,
        message: ss(&p.message),
        interval: ss(interval_text(&p.sync_interval)),
        last_sync: ss(p.last_sync.clone().unwrap_or_default()),
        pending: p.pending as i32,
        uploaded: p.uploaded as i32,
        downloaded: p.downloaded as i32,
        blocked_deletes: p.blocked_deletes as i32,
        storage: ss(&p.storage),
        recent: model(p.recent.iter().map(ss).collect()),
        errors: model(p.errors.iter().map(ss).collect()),
    }
}

fn interval_text(i: &str) -> String {
    match i {
        "realtime" | "" => "Real time".into(),
        "1m" => "Every minute".into(),
        "5m" => "Every 5 minutes".into(),
        "30m" => "Every 30 minutes".into(),
        "1h" => "Every hour".into(),
        other => format!("Every {other}"),
    }
}

fn folder_row(p: &SyncPair) -> FolderConfig {
    let interval = INTERVALS.iter().position(|i| *i == p.sync_interval).unwrap_or(5) as i32;
    FolderConfig {
        is_new: false,
        name: ss(&p.name),
        account: ss(&p.account),
        local: ss(p.local.to_string_lossy()),
        remote: ss(&p.remote),
        shared_drive: ss(&p.shared_drive),
        mode: match p.mode {
            SyncMode::TwoWay => 0,
            SyncMode::UploadOnly => 1,
            SyncMode::DownloadOnly => 2,
        },
        interval,
        interval_custom: ss(if interval == 5 { p.sync_interval.as_str() } else { "" }),
        poll: ss(&p.remote_poll_interval),
        settle: ss(&p.settle_time),
        rescan: ss(&p.full_rescan_interval),
        conflict: match p.conflict_policy {
            ConflictPolicy::KeepBoth => 0,
            ConflictPolicy::PreferLocal => 1,
            ConflictPolicy::PreferRemote => 2,
            ConflictPolicy::PreferNewest => 3,
        },
        docs: match p.google_docs {
            GoogleDocsMode::Link => 0,
            GoogleDocsMode::Export => 1,
            GoogleDocsMode::Ignore => 2,
        },
        symlinks: match p.symlinks {
            SymlinkMode::Skip => 0,
            SymlinkMode::Rclonelink => 1,
        },
        delete_mode: match p.delete {
            DeleteMode::Trash => 0,
            DeleteMode::Never => 1,
        },
        initial_match: match p.initial_match {
            InitialMatch::SizeMtime => 0,
            InitialMatch::Checksum => 1,
        },
        delete_guard: p.delete_guard.min(i32::MAX as usize) as i32,
        max_file_size: p.max_file_size_mib.min(i32::MAX as u64) as i32,
        excludes: lines(&p.exclude),
        exclude_folders: lines(&p.exclude_folders),
        include_folders: lines(&p.include_folders),
        acknowledge_abuse: p.acknowledge_abuse,
        paused: p.paused,
    }
}

fn pair_from_row(f: &FolderConfig) -> Result<SyncPair> {
    let name = f.name.trim();
    if name.is_empty() {
        bail!("please give the folder a name");
    }
    if f.account.is_empty() {
        bail!("please choose a Google account (add one under Accounts first)");
    }
    let local = f.local.trim();
    if local.is_empty() || !local.starts_with('/') {
        bail!("please choose the folder on this computer");
    }
    let mut remote = f.remote.trim().to_string();
    if !remote.starts_with('/') {
        remote.insert(0, '/');
    }
    let mut p = accounts::new_pair(name, &f.account, std::path::Path::new(local), &remote)?;
    p.shared_drive = f.shared_drive.trim().to_string();
    p.mode = [SyncMode::TwoWay, SyncMode::UploadOnly, SyncMode::DownloadOnly][f.mode.clamp(0, 2) as usize];
    p.sync_interval = match INTERVALS.get(f.interval as usize) {
        Some(i) => i.to_string(),
        None => f.interval_custom.trim().to_string(),
    };
    config::parse_interval(&p.sync_interval).map_err(|_| anyhow!("“{}” is not a valid sync frequency (try 10m or 2h)", p.sync_interval))?;
    let dur = |s: &SharedString, def: &str, what: &str| -> Result<String> {
        let v = if s.trim().is_empty() { def.to_string() } else { s.trim().to_string() };
        config::parse_duration(&v).map_err(|_| anyhow!("“{v}” is not a valid duration for {what}"))?;
        Ok(v)
    };
    p.remote_poll_interval = dur(&f.poll, "30s", "the cloud check interval")?;
    p.settle_time = dur(&f.settle, "3s", "the quiet time")?;
    p.full_rescan_interval = dur(&f.rescan, "6h", "the safety rescan")?;
    p.conflict_policy = [
        ConflictPolicy::KeepBoth,
        ConflictPolicy::PreferLocal,
        ConflictPolicy::PreferRemote,
        ConflictPolicy::PreferNewest,
    ][f.conflict.clamp(0, 3) as usize];
    p.google_docs = [GoogleDocsMode::Link, GoogleDocsMode::Export, GoogleDocsMode::Ignore][f.docs.clamp(0, 2) as usize];
    p.symlinks = [SymlinkMode::Skip, SymlinkMode::Rclonelink][f.symlinks.clamp(0, 1) as usize];
    p.delete = [DeleteMode::Trash, DeleteMode::Never][f.delete_mode.clamp(0, 1) as usize];
    p.initial_match = [InitialMatch::SizeMtime, InitialMatch::Checksum][f.initial_match.clamp(0, 1) as usize];
    p.delete_guard = f.delete_guard.max(0) as usize;
    p.max_file_size_mib = f.max_file_size.max(0) as u64;
    p.exclude = unlines(&f.excludes);
    p.exclude_folders = unlines(&f.exclude_folders).into_iter().map(|s| s.trim_matches('/').to_string()).collect();
    p.include_folders = unlines(&f.include_folders).into_iter().map(|s| s.trim_matches('/').to_string()).collect();
    p.acknowledge_abuse = f.acknowledge_abuse;
    p.paused = f.paused;
    Ok(p)
}

fn new_folder_row(account: &str) -> FolderConfig {
    let mut row = folder_row(&accounts::new_pair("x", "x", std::path::Path::new("/"), "/").unwrap());
    row.is_new = true;
    row.name = ss("");
    row.account = ss(account);
    row.local = ss("");
    row.remote = ss("/");
    row
}

fn general_row(g: &config::General) -> GeneralCfg {
    GeneralCfg {
        notifications: g.notifications,
        tray: g.tray,
        proxy: ss(&g.proxy),
        up_kibps: g.max_upload_kibps.min(i32::MAX as u64) as i32,
        down_kibps: g.max_download_kibps.min(i32::MAX as u64) as i32,
        transfers: g.max_concurrent_transfers as i32,
        nice: g.nice,
        start_at_login: g.start_at_login,
    }
}

fn file_url(path: &str) -> String {
    url::Url::from_directory_path(path)
        .or_else(|_| url::Url::from_file_path(path))
        .map(|u| u.to_string())
        .unwrap_or_else(|_| format!("file://{path}"))
}

// ---------------------------------------------------------------- app state

struct Tree {
    account: String,
    /// Pick a destination folder, or tick folders for selective sync.
    pick: bool,
    /// Excluded paths (selective mode) that are not visible in loaded rows.
    hidden_excludes: Vec<String>,
}

struct App {
    ui: slint::Weak<MainWindow>,
    rt: Handle,
    tree: RefCell<Option<Tree>>,
    tree_rows: Rc<VecModel<TreeRow>>,
    confirm: RefCell<Option<(String, Instant)>>,
    toast_timer: slint::Timer,
}

impl App {
    fn ui(&self) -> MainWindow {
        self.ui.upgrade().expect("window exists")
    }

    fn toast(&self, msg: impl Into<String>, error: bool) {
        let ui = self.ui();
        ui.set_toast(ss(msg.into()));
        ui.set_toast_error(error);
        let w = self.ui.clone();
        self.toast_timer.start(slint::TimerMode::SingleShot, Duration::from_secs(if error { 8 } else { 4 }), move || {
            if let Some(ui) = w.upgrade() {
                ui.set_toast(ss(""));
            }
        });
    }

    /// Ask for a second click within 5 s before doing something drastic.
    fn confirmed(&self, key: &str, prompt: &str) -> bool {
        let mut c = self.confirm.borrow_mut();
        if let Some((k, t)) = c.as_ref() {
            if k == key && t.elapsed() < Duration::from_secs(5) {
                *c = None;
                return true;
            }
        }
        *c = Some((key.to_string(), Instant::now()));
        drop(c);
        self.toast(prompt, true);
        false
    }

    /// Run `fut` on the tokio runtime and hand its result back on the UI thread.
    fn run<T, F, D>(self: &Rc<Self>, fut: F, done: D)
    where
        T: Send + 'static,
        F: std::future::Future<Output = T> + Send + 'static,
        D: FnOnce(&Rc<App>, T) + Send + 'static,
    {
        let me = SendRc(self.clone());
        self.rt.spawn(async move {
            let out = fut.await;
            let _ = slint::invoke_from_event_loop(move || {
                let me = me;
                done(&me.0, out);
            });
        });
    }

    fn load_config(self: &Rc<Self>) {
        let ui = self.ui();
        let cfg = match Config::load() {
            Ok(c) => c,
            Err(e) => {
                self.toast(format!("Settings file problem: {e:#}"), true);
                return;
            }
        };
        let folders: Vec<FolderConfig> = cfg.pairs.iter().map(folder_row).collect();
        let sel = ui.get_selected_folder();
        let cur_name = ui.get_current_folder().name.to_string();
        ui.set_folders(model(folders.clone()));
        ui.set_account_names(model(cfg.accounts.iter().map(|a| ss(&a.name)).collect()));
        if let Some(i) = folders.iter().position(|f| f.name == cur_name) {
            ui.set_selected_folder(i as i32);
            ui.set_current_folder(folders[i].clone());
        } else if sel >= 0 {
            ui.set_selected_folder(-1);
            ui.set_current_folder(FolderConfig::default());
        }
        ui.set_general(general_row(&cfg.general));

        // Accounts (signed-in state comes from the keyring, asynchronously).
        let rows: Vec<AccountRow> = cfg
            .accounts
            .iter()
            .map(|a| AccountRow {
                name: ss(&a.name),
                email: ss(&a.email),
                client_id: ss(&a.client_id),
                client_secret: ss(&a.client_secret),
                signed_in: false,
                folders: cfg.pairs.iter().filter(|p| p.account == a.name).count() as i32,
            })
            .collect();
        ui.set_accounts(model(rows.clone()));
        let names: Vec<String> = cfg.accounts.iter().map(|a| a.name.clone()).collect();
        self.run(
            async move {
                let mut v = Vec::new();
                for n in names {
                    v.push(secrets::load(&n).await.ok().flatten().is_some());
                }
                v
            },
            move |app, signed| {
                let rows: Vec<AccountRow> = rows
                    .into_iter()
                    .zip(signed)
                    .map(|(mut r, s)| {
                        r.signed_in = s;
                        r
                    })
                    .collect();
                app.ui().set_accounts(model(rows));
            },
        );
    }

    fn reload_daemon(self: &Rc<Self>) {
        self.run(async { if ipc::daemon_running().await { ipc::request(&Request::Reload).await.map(|_| ()) } else { Ok(()) } }, |app, r| {
            if let Err(e) = r {
                app.toast(format!("{e:#}"), true);
            }
        });
    }

    fn ipc(self: &Rc<Self>, req: Request) {
        self.run(async move { ipc::request(&req).await }, |app, r| {
            if let Err(e) = r {
                app.toast(format!("{e:#}"), true);
            }
        });
    }

    fn open(self: &Rc<Self>, url: String) {
        self.run(async move { crate::auth::open_url(&url).await }, |app, ok| {
            if !ok {
                app.toast("Could not open it — no handler is available.", true);
            }
        });
    }

    fn sign_in(self: &Rc<Self>, account: String) {
        self.ui().set_signing_in(true);
        self.run(async move { accounts::login(&account).await }, |app, r| {
            app.ui().set_signing_in(false);
            match r {
                Ok(email) => {
                    app.toast(format!("Signed in as {email}"), false);
                    let ui = app.ui();
                    ui.set_adding_account(false);
                    ui.set_new_name(ss(""));
                    ui.set_new_email(ss(""));
                    ui.set_new_client_id(ss(""));
                    ui.set_new_client_secret(ss(""));
                    app.load_config();
                    app.reload_daemon();
                }
                Err(e) => app.toast(format!("Sign-in failed: {e:#}"), true),
            }
        });
    }

    fn cfg_account(&self, name: &str) -> Result<config::Account> {
        Config::load()?.account(name).cloned().with_context(|| format!("no account named '{name}'"))
    }

    // ------------------------------------------------------------ tree dialog

    fn open_tree(self: &Rc<Self>, title: &str, note: &str, tree: Tree, roots: Vec<TreeRow>) {
        let ui = self.ui();
        *self.tree.borrow_mut() = Some(tree);
        self.tree_rows.set_vec(roots);
        ui.set_tree_title(ss(title));
        ui.set_tree_note(ss(note));
        ui.set_tree_chosen(-1);
        ui.set_tree_pick(self.tree.borrow().as_ref().unwrap().pick);
        ui.set_tree_open(true);
    }

    fn tree_expand(self: &Rc<Self>, i: usize) {
        let Some(row) = self.tree_rows.row_data(i) else { return };
        if row.expanded {
            // Collapse: remove descendants, remembering excluded ones.
            let j = i + 1;
            while j < self.tree_rows.row_count() && self.tree_rows.row_data(j).unwrap().depth > row.depth {
                let r = self.tree_rows.row_data(j).unwrap();
                if !r.pick_placeholder() && !r.checked && r.enabled {
                    if let Some(t) = self.tree.borrow_mut().as_mut() {
                        t.hidden_excludes.push(r.path.to_string());
                    }
                }
                self.tree_rows.remove(j);
            }
            let mut r = row;
            r.expanded = false;
            self.tree_rows.set_row_data(i, r);
            return;
        }
        let Some(account) = self.tree.borrow().as_ref().map(|t| t.account.clone()) else { return };
        let mut loading = row.clone();
        loading.loading = true;
        self.tree_rows.set_row_data(i, loading);
        let id = row.id.to_string();
        let drive = row.drive.to_string();
        let acct = self.cfg_account(&account);
        self.run(
            async move {
                let acct = acct?;
                let http = crate::daemon::http_client(&Config::load()?)?;
                let c = accounts::client_for(&acct, &http);
                let drive_id = if drive.is_empty() { None } else { Some(c.find_shared_drive(&drive).await?) };
                c.list_folders(&id, drive_id.as_deref()).await
            },
            move |app, res| {
                // The row may have moved if the user collapsed something meanwhile.
                let Some(pos) = (0..app.tree_rows.row_count()).find(|&k| app.tree_rows.row_data(k).unwrap().id == row.id) else {
                    return;
                };
                let mut parent = app.tree_rows.row_data(pos).unwrap();
                parent.loading = false;
                match res {
                    Ok(mut kids) => {
                        kids.sort_by_key(|f| f.name.to_lowercase());
                        parent.expanded = true;
                        parent.expandable = !kids.is_empty();
                        let mut hidden = app.tree.borrow().as_ref().map(|t| t.hidden_excludes.clone()).unwrap_or_default();
                        for (k, f) in kids.iter().enumerate() {
                            let path = if parent.path.is_empty() { f.name.clone() } else { format!("{}/{}", parent.path, f.name) };
                            let excluded = hidden.iter().any(|h| *h == path);
                            hidden.retain(|h| *h != path);
                            app.tree_rows.insert(
                                pos + 1 + k,
                                TreeRow {
                                    id: ss(&f.id),
                                    name: ss(&f.name),
                                    path: ss(&path),
                                    drive: parent.drive.clone(),
                                    depth: parent.depth + 1,
                                    expandable: true,
                                    expanded: false,
                                    loading: false,
                                    checked: parent.checked && !excluded,
                                    enabled: parent.checked && parent.enabled,
                                },
                            );
                        }
                        if let Some(t) = app.tree.borrow_mut().as_mut() {
                            t.hidden_excludes = hidden;
                        }
                    }
                    Err(e) => app.toast(format!("Could not list cloud folders: {e:#}"), true),
                }
                app.tree_rows.set_row_data(pos, parent);
            },
        );
    }

    fn tree_toggle(&self, i: usize) {
        let Some(mut row) = self.tree_rows.row_data(i) else { return };
        row.checked = !row.checked;
        let depth = row.depth;
        self.tree_rows.set_row_data(i, row);
        // Recompute "enabled" everywhere: a row is enabled iff all its
        // ancestors are ticked.
        let _ = depth;
        let mut stack: Vec<(i32, bool)> = vec![];
        for j in 0..self.tree_rows.row_count() {
            let mut r = self.tree_rows.row_data(j).unwrap();
            while stack.last().is_some_and(|(d, _)| *d >= r.depth) {
                stack.pop();
            }
            let ancestors_ok = stack.iter().all(|(_, ok)| *ok);
            if r.enabled != ancestors_ok {
                r.enabled = ancestors_ok;
                self.tree_rows.set_row_data(j, r.clone());
            }
            stack.push((r.depth, r.checked && ancestors_ok));
        }
    }

    fn tree_ok(self: &Rc<Self>) {
        let ui = self.ui();
        let Some(tree) = self.tree.borrow_mut().take() else { return };
        ui.set_tree_open(false);
        let mut cur = ui.get_current_folder();
        if tree.pick {
            let i = ui.get_tree_chosen();
            let Some(r) = (i >= 0).then(|| self.tree_rows.row_data(i as usize)).flatten() else { return };
            cur.remote = ss(format!("/{}", r.path));
            cur.shared_drive = r.drive.clone();
        } else {
            let mut excl: Vec<String> = tree.hidden_excludes;
            for r in self.tree_rows.iter() {
                // Only the top-most unticked folder needs listing.
                if !r.checked && r.enabled {
                    excl.push(r.path.to_string());
                }
            }
            excl.sort();
            excl.dedup();
            cur.exclude_folders = lines(&excl);
            self.toast("Selection updated — press “Save changes” to apply it.", false);
        }
        ui.set_current_folder(cur);
    }
}

/// Lets an `Rc<App>` ride inside a closure that is sent to the UI thread and
/// only ever dereferenced there.
struct SendRc(Rc<App>);
unsafe impl Send for SendRc {}

trait Placeholder {
    fn pick_placeholder(&self) -> bool;
}
impl Placeholder for TreeRow {
    fn pick_placeholder(&self) -> bool {
        self.id.is_empty()
    }
}

// ---------------------------------------------------------------- entry point

pub fn run(rt: Handle) -> Result<()> {
    // Single instance: if a window is already open, ask it to show itself.
    let sock = paths::runtime_dir().join("gui.sock");
    if std::os::unix::net::UnixStream::connect(&sock).is_ok() {
        println!("DriftFolderSync is already open.");
        return Ok(());
    }

    let ui = MainWindow::new()?;
    ui.set_version(ss(env!("CARGO_PKG_VERSION")));
    let tree_rows = Rc::new(VecModel::<TreeRow>::default());
    ui.set_tree_rows(ModelRc::from(tree_rows.clone()));
    let app = Rc::new(App {
        ui: ui.as_weak(),
        rt: rt.clone(),
        tree: RefCell::new(None),
        tree_rows,
        confirm: RefCell::new(None),
        toast_timer: slint::Timer::default(),
    });
    app.load_config();

    // Listen for "show yourself" from later launches.
    {
        let w = ui.as_weak();
        let _ = std::fs::create_dir_all(sock.parent().unwrap());
        let _ = std::fs::remove_file(&sock);
        let sock2 = sock.clone();
        rt.spawn(async move {
            let Ok(l) = tokio::net::UnixListener::bind(&sock2) else { return };
            loop {
                if l.accept().await.is_ok() {
                    let w = w.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(ui) = w.upgrade() {
                            let _ = ui.show();
                        }
                    });
                }
            }
        });
    }

    // Live status from the service, once a second.
    if std::env::var_os("DRIFTFOLDERSYNC_SNAPSHOT_DIR").is_none() {
        let w = ui.as_weak();
        rt.spawn(async move {
            loop {
                let st = if ipc::daemon_running().await { ipc::request(&Request::Status).await.ok() } else { None };
                let w = w.clone();
                let alive = slint::invoke_from_event_loop(move || {
                    let Some(ui) = w.upgrade() else { return };
                    ui.set_daemon_running(st.is_some());
                    let rows: Vec<FolderStatus> =
                        st.and_then(|r| r.status).unwrap_or_default().iter().map(status_row).collect();
                    let problems = rows
                        .iter()
                        .filter(|r| r.state_kind == 4 || r.state_kind == 5 || r.blocked_deletes > 0)
                        .count();
                    ui.set_problem_count(problems as i32);
                    ui.set_statuses(model(rows));
                });
                if alive.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    wire(&app, &ui);
    #[cfg(debug_assertions)]
    if let Some(dir) = std::env::var_os("DRIFTFOLDERSYNC_SNAPSHOT_DIR") {
        snapshot::start(&ui, std::path::PathBuf::from(dir));
    }
    ui.run()?;
    let _ = std::fs::remove_file(&sock);
    Ok(())
}

fn status_of(ui: &MainWindow, name: &str) -> Option<FolderStatus> {
    ui.get_statuses().iter().find(|s| s.name == name)
}

fn wire(app: &Rc<App>, ui: &MainWindow) {
    // ---- service control
    {
        let a = app.clone();
        ui.on_start_daemon(move || match start_daemon() {
            Ok(()) => a.toast("Starting the sync service…", false),
            Err(e) => a.toast(format!("Could not start the sync service: {e:#}"), true),
        });
    }
    {
        let a = app.clone();
        ui.on_sync_all(move || a.ipc(Request::SyncNow { pair: None }));
    }
    {
        let a = app.clone();
        ui.on_pause_all(move |p| a.ipc(if p { Request::Pause { pair: None } } else { Request::Resume { pair: None } }));
    }
    {
        let a = app.clone();
        ui.on_sync_now(move |n| a.ipc(Request::SyncNow { pair: Some(n.to_string()) }));
    }
    {
        let a = app.clone();
        ui.on_pause(move |n, p| {
            let pair = Some(n.to_string());
            a.ipc(if p { Request::Pause { pair } } else { Request::Resume { pair } })
        });
    }
    {
        let a = app.clone();
        ui.on_allow_deletes(move |n| {
            if a.confirmed(&format!("allow:{n}"), "Press “Allow deletions” again to confirm.") {
                a.ipc(Request::ConfirmDeletes { pair: n.to_string() });
            }
        });
    }
    {
        let a = app.clone();
        ui.on_open_folder(move |n| {
            let ui = a.ui();
            let path = status_of(&ui, &n).map(|s| s.local.to_string()).or_else(|| {
                Config::load().ok()?.pair(&n).map(|p| p.local.to_string_lossy().into_owned())
            });
            if let Some(p) = path {
                a.open(file_url(&p));
            }
        });
    }
    {
        let a = app.clone();
        ui.on_open_browser(move |n| {
            if let Some(s) = status_of(&a.ui(), &n) {
                a.open(s.remote_url.to_string());
            }
        });
    }
    {
        let a = app.clone();
        ui.on_open_url(move |u| a.open(u.to_string()));
    }

    // ---- accounts
    {
        let a = app.clone();
        ui.on_sign_in(move |n| a.sign_in(n.to_string()));
    }
    {
        let a = app.clone();
        ui.on_sign_out(move |n| {
            if !a.confirmed(&format!("signout:{n}"), "Press “Sign out” again to confirm. Folders using this account will stop syncing.") {
                return;
            }
            let n = n.to_string();
            a.run(async move { accounts::logout(&n).await }, |app, r| match r {
                Ok(()) => {
                    app.toast("Signed out.", false);
                    app.load_config();
                    app.reload_daemon();
                }
                Err(e) => app.toast(format!("{e:#}"), true),
            });
        });
    }
    {
        let a = app.clone();
        ui.on_remove_account(move |n| {
            if !a.confirmed(&format!("rmacct:{n}"), "Press “Remove” again to remove this account.") {
                return;
            }
            let name = n.to_string();
            match config::remove_table("account", &name) {
                Ok(()) => {
                    a.run(async move { accounts::logout(&name).await }, |app, _| {
                        app.toast("Account removed.", false);
                        app.load_config();
                        app.reload_daemon();
                    });
                }
                Err(e) => a.toast(format!("{e:#}"), true),
            }
        });
    }
    {
        let a = app.clone();
        ui.on_add_account(move |name, email, id, secret| {
            let name = name.trim().to_string();
            if !name.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
                a.toast("Account names may only contain letters, digits, - and _.", true);
                return;
            }
            if Config::load().ok().and_then(|c| c.account(&name).cloned()).is_some() {
                a.toast(format!("There is already an account called “{name}”."), true);
                return;
            }
            let acct = config::Account {
                name: name.clone(),
                email: email.trim().to_string(),
                client_id: id.trim().to_string(),
                client_secret: secret.trim().to_string(),
                requests_per_second: 5.0,
                request_burst: 10,
            };
            match config::append_table("account", &acct) {
                Ok(()) => {
                    a.load_config();
                    a.sign_in(name);
                }
                Err(e) => a.toast(format!("{e:#}"), true),
            }
        });
    }

    // ---- folders
    {
        let a = app.clone();
        ui.on_new_folder(move || {
            let ui = a.ui();
            let first = ui.get_account_names().row_data(0).map(|s| s.to_string()).unwrap_or_default();
            if first.is_empty() {
                a.toast("Add a Google account first (Accounts page).", true);
            }
            ui.set_selected_folder(-1);
            ui.set_current_folder(new_folder_row(&first));
            ui.set_page(1);
        });
    }
    {
        let a = app.clone();
        ui.on_save_folder(move |f| {
            let res = pair_from_row(&f).and_then(|p| {
                if f.is_new {
                    if Config::load()?.pair(&p.name).is_some() {
                        bail!("there is already a folder called “{}”", p.name);
                    }
                    config::append_table("sync", &p)
                } else {
                    config::update_table("sync", &p.name, &p)
                }
            });
            match res {
                Ok(()) => {
                    a.toast(if f.is_new { "Folder added — syncing will start shortly." } else { "Saved." }, false);
                    let ui = a.ui();
                    let mut cur = f.clone();
                    cur.is_new = false;
                    ui.set_current_folder(cur);
                    a.load_config();
                    a.reload_daemon();
                }
                Err(e) => a.toast(format!("{e:#}"), true),
            }
        });
    }
    {
        let a = app.clone();
        ui.on_remove_folder(move |n| {
            if !a.confirmed(&format!("rm:{n}"), "Press “Remove folder” again to stop syncing it. No files will be deleted.") {
                return;
            }
            let name = n.to_string();
            let r = config::remove_table("sync", &name)
                .and_then(|_| crate::db::Db::open(&paths::data_dir().join("state.db"))?.clear_pair(&name));
            match r {
                Ok(()) => {
                    a.toast("Folder removed. Its files were left untouched.", false);
                    let ui = a.ui();
                    ui.set_selected_folder(-1);
                    ui.set_current_folder(FolderConfig::default());
                    a.load_config();
                    a.reload_daemon();
                }
                Err(e) => a.toast(format!("{e:#}"), true),
            }
        });
    }
    {
        let a = app.clone();
        ui.on_reset_folder(move |n| {
            if !a.confirmed(
                &format!("reset:{n}"),
                "Press again to re-compare this folder from scratch (nothing is deleted; it may take a while).",
            ) {
                return;
            }
            let name = n.to_string();
            a.run(
                async move {
                    let running = ipc::daemon_running().await;
                    if running {
                        ipc::request(&Request::Quit).await?;
                        for _ in 0..100 {
                            if !ipc::daemon_running().await {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                    crate::db::Db::open(&paths::data_dir().join("state.db"))?.clear_pair(&name)?;
                    Ok::<bool, anyhow::Error>(running)
                },
                |app, r| match r {
                    Ok(was_running) => {
                        if was_running {
                            let _ = start_daemon();
                        }
                        app.toast("Sync state cleared; the folder will be re-compared.", false);
                    }
                    Err(e) => app.toast(format!("{e:#}"), true),
                },
            );
        });
    }
    {
        let a = app.clone();
        ui.on_browse_local(move || {
            a.run(
                async {
                    use ashpd::desktop::file_chooser::SelectedFiles;
                    let files = SelectedFiles::open_file()
                        .title("Choose the folder to keep in sync")
                        .accept_label("Choose")
                        .directory(true)
                        .modal(true)
                        .send()
                        .await?
                        .response()?;
                    let uri = files.uris().first().cloned().context("no folder chosen")?;
                    let p = url::Url::parse(uri.as_str())?.to_file_path().map_err(|_| anyhow!("not a local folder"))?;
                    Ok::<_, anyhow::Error>(p)
                },
                |app, r| match r {
                    Ok(p) => {
                        let ui = app.ui();
                        let mut cur = ui.get_current_folder();
                        let s = p.to_string_lossy().into_owned();
                        if s.starts_with("/run/user/") {
                            app.toast(
                                "That folder is outside what DriftFolderSync may access. Grant access with: flatpak override --user --filesystem=<folder> io.github.OnepointConsultingLtd.DriftFolderSync",
                                true,
                            );
                            return;
                        }
                        if cur.is_new && cur.name.is_empty() {
                            let cfg = Config::load().unwrap_or_default();
                            let base = p.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
                            cur.name = ss(accounts::unique_pair_name(&cfg, &base));
                            if cur.remote == "/" {
                                cur.remote = ss(format!("/{base}"));
                            }
                        }
                        cur.local = ss(s);
                        ui.set_current_folder(cur);
                    }
                    Err(e) => app.toast(format!("{e:#}"), true),
                },
            );
        });
    }
    {
        let a = app.clone();
        ui.on_browse_remote(move || {
            let cur = a.ui().get_current_folder();
            let account = cur.account.to_string();
            let acct = a.cfg_account(&account);
            a.run(
                async move {
                    let acct = acct?;
                    let http = crate::daemon::http_client(&Config::load()?)?;
                    let c = accounts::client_for(&acct, &http);
                    let root = c.get("root").await?;
                    let drives = c.shared_drives().await.unwrap_or_default();
                    Ok::<_, anyhow::Error>((root.id, drives))
                },
                move |app, r| match r {
                    Ok((root, drives)) => {
                        let mut rows = vec![TreeRow {
                            id: ss(root),
                            name: ss("My Drive"),
                            path: ss(""),
                            drive: ss(""),
                            depth: 0,
                            expandable: true,
                            expanded: false,
                            loading: false,
                            checked: true,
                            enabled: true,
                        }];
                        for (id, name) in drives {
                            rows.push(TreeRow {
                                id: ss(&id),
                                name: ss(format!("{name} (shared drive)")),
                                path: ss(""),
                                drive: ss(&name),
                                depth: 0,
                                expandable: true,
                                expanded: false,
                                loading: false,
                                checked: true,
                                enabled: true,
                            });
                        }
                        app.open_tree(
                            "Choose the folder in the cloud",
                            "Click a folder to select it; click ▸ to look inside.",
                            Tree { account: account.clone(), pick: true, hidden_excludes: vec![] },
                            rows,
                        );
                        app.tree_expand(0);
                    }
                    Err(e) => app.toast(format!("Could not reach Google Drive: {e:#}"), true),
                },
            );
        });
    }
    {
        let a = app.clone();
        ui.on_selective(move || {
            let cur = a.ui().get_current_folder();
            let pair = match pair_from_row(&cur) {
                Ok(p) => p,
                Err(e) => {
                    a.toast(format!("{e:#}"), true);
                    return;
                }
            };
            let acct = a.cfg_account(&pair.account);
            let excluded: Vec<String> = pair.exclude_folders.clone();
            if !pair.include_folders.is_empty() {
                a.toast("Note: “Only sync these folders” is set, so only those folders are synced regardless of ticks here.", false);
            }
            a.run(
                async move {
                    let acct = acct?;
                    let http = crate::daemon::http_client(&Config::load()?)?;
                    let c = accounts::client_for(&acct, &http);
                    let (top, drive) = if pair.shared_drive.is_empty() {
                        (c.get("root").await?.id, None)
                    } else {
                        let d = c.find_shared_drive(&pair.shared_drive).await?;
                        (d.clone(), Some(d))
                    };
                    let root = c.resolve_folder(&top, &pair.remote, false).await?;
                    let mut kids = c.list_folders(&root, drive.as_deref()).await?;
                    kids.sort_by_key(|f| f.name.to_lowercase());
                    Ok::<_, anyhow::Error>((kids, pair.shared_drive.clone(), pair.account.clone()))
                },
                move |app, r| match r {
                    Ok((kids, drive, account)) => {
                        let ex: HashSet<&String> = excluded.iter().collect();
                        let rows: Vec<TreeRow> = kids
                            .iter()
                            .map(|f| TreeRow {
                                id: ss(&f.id),
                                name: ss(&f.name),
                                path: ss(&f.name),
                                drive: ss(&drive),
                                depth: 0,
                                expandable: true,
                                expanded: false,
                                loading: false,
                                checked: !ex.contains(&f.name),
                                enabled: true,
                            })
                            .collect();
                        let shown: HashSet<String> = kids.iter().map(|f| f.name.clone()).collect();
                        let hidden: Vec<String> = excluded.iter().filter(|e| !shown.contains(*e)).cloned().collect();
                        app.open_tree(
                            "Choose folders to sync",
                            "Ticked folders are kept on this computer. Unticked folders stop syncing (any copies already on this computer are left where they are).",
                            Tree { account, pick: false, hidden_excludes: hidden },
                            rows,
                        );
                    }
                    Err(e) => app.toast(format!("Could not list cloud folders: {e:#}"), true),
                },
            );
        });
    }
    {
        let a = app.clone();
        ui.on_tree_expand(move |i| a.tree_expand(i as usize));
    }
    {
        let a = app.clone();
        ui.on_tree_toggle(move |i| a.tree_toggle(i as usize));
    }
    {
        let a = app.clone();
        ui.on_tree_ok(move || a.tree_ok());
    }
    {
        let a = app.clone();
        ui.on_tree_cancel(move || {
            *a.tree.borrow_mut() = None;
            a.ui().set_tree_open(false);
        });
    }

    // ---- preferences
    {
        let a = app.clone();
        ui.on_save_general(move |g| {
            let mut cfg = Config::load().map(|c| c.general).unwrap_or_default();
            let autostart_changed = cfg.start_at_login != g.start_at_login;
            cfg.notifications = g.notifications;
            cfg.tray = g.tray;
            cfg.proxy = g.proxy.trim().to_string();
            if !cfg.proxy.is_empty() && reqwest::Proxy::all(&cfg.proxy).is_err() {
                a.toast("That proxy address is not valid.", true);
                return;
            }
            cfg.max_upload_kibps = g.up_kibps.max(0) as u64;
            cfg.max_download_kibps = g.down_kibps.max(0) as u64;
            cfg.max_concurrent_transfers = g.transfers.clamp(1, 16) as usize;
            cfg.nice = g.nice.clamp(0, 19);
            cfg.start_at_login = g.start_at_login;
            if let Err(e) = config::update_general(&cfg) {
                a.toast(format!("{e:#}"), true);
                return;
            }
            let on = g.start_at_login;
            a.run(
                async move { if autostart_changed { autostart::set(on).await } else { Ok(()) } },
                |app, r| match r {
                    Ok(()) => {
                        app.toast("Preferences saved.", false);
                        app.reload_daemon();
                    }
                    Err(e) => app.toast(format!("Saved, but start-at-login could not be changed: {e:#}"), true),
                },
            );
        });
    }

    // ---- activity / log
    {
        let a = app.clone();
        ui.on_refresh_log(move || {
            let path = paths::data_dir().join("driftfoldersync.log");
            let text = std::fs::read_to_string(&path).unwrap_or_else(|_| "No log yet.".into());
            let tail: Vec<&str> = text.lines().rev().take(500).collect();
            let tail: Vec<&str> = tail.into_iter().rev().collect();
            a.ui().set_log_text(ss(strip_ansi(&tail.join("\n"))));
        });
    }
    {
        let a = app.clone();
        ui.on_open_log(move || a.open(file_url(&paths::data_dir().join("driftfoldersync.log").to_string_lossy())));
    }
}

fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for d in chars.by_ref() {
                if d.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Launch the background service as an independent process.
pub fn start_daemon() -> Result<()> {
    use std::os::unix::process::CommandExt;
    let exe = std::env::current_exe()?;
    std::process::Command::new(exe)
        .arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .process_group(0)
        .spawn()?;
    Ok(())
}

/// Open the window from elsewhere (tray): a separate process.
pub fn launch() {
    use std::os::unix::process::CommandExt;
    if let Ok(exe) = std::env::current_exe() {
        let _ = std::process::Command::new(exe)
            .arg("gui")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0)
            .spawn();
    }
}

/// Developer aid (debug builds only): fill the window with sample data and
/// save an image of every page, then quit.
#[cfg(debug_assertions)]
mod snapshot {
    use super::*;

    fn sample_status(name: &str, kind: i32, state: &str) -> FolderStatus {
        FolderStatus {
            name: ss(name),
            account: ss("work"),
            email: ss("me@example.com"),
            local: ss(format!("/home/me/{name}")),
            remote_url: ss("https://drive.google.com/drive/folders/x"),
            state: ss(state),
            state_kind: kind,
            message: ss(if kind == 1 { "Syncing 12/40" } else { "" }),
            interval: ss(interval_text(if kind == 1 { "realtime" } else { "5m" })),
            last_sync: ss("2026-11-27 11:42:07"),
            pending: if kind == 1 { 28 } else { 0 },
            uploaded: 132,
            downloaded: 57,
            blocked_deletes: if kind == 4 { 312 } else { 0 },
            storage: ss("41.2 GiB of 2.0 TiB used"),
            recent: model(vec![ss("11:42 Uploaded: Reports/Q4.xlsx (88.1 KiB)"), ss("11:40 Downloaded: notes.md (2.1 KiB)")]),
            errors: model(if kind == 4 { vec![ss("11:41 Contracts/old.pdf: permission denied")] } else { vec![] }),
        }
    }

    pub fn start(ui: &MainWindow, dir: std::path::PathBuf) {
        let _ = std::fs::create_dir_all(&dir);
        ui.set_statuses(model(vec![
            sample_status("documents", 0, "Up to date"),
            sample_status("projects", 1, "Syncing"),
            sample_status("archive", 4, "Error"),
        ]));
        ui.set_daemon_running(true);
        ui.set_problem_count(1);
        let mut f = new_folder_row("work");
        f.is_new = false;
        f.name = ss("documents");
        f.local = ss("/home/me/Documents");
        f.remote = ss("/Documents");
        f.exclude_folders = ss("Old stuff\nArchive/2019");
        ui.set_folders(model(vec![f.clone(), { let mut g = f.clone(); g.name = ss("projects"); g }]));
        ui.set_selected_folder(0);
        ui.set_current_folder(f);
        ui.set_account_names(model(vec![ss("work"), ss("client")]));
        ui.set_accounts(model(vec![
            AccountRow { name: ss("work"), email: ss("me@example.com"), client_id: ss(""), client_secret: ss(""), signed_in: true, folders: 2 },
            AccountRow { name: ss("client"), email: ss("me@client.example"), client_id: ss(""), client_secret: ss(""), signed_in: false, folders: 0 },
        ]));
        ui.set_log_text(ss("2026-11-27T11:42:07Z  INFO Uploaded: Reports/Q4.xlsx\n2026-11-27T11:42:09Z  INFO 0 actions"));
        let steps: Vec<(&str, Box<dyn Fn(&MainWindow)>)> = vec![
            ("1-overview", Box::new(|ui| ui.set_page(0))),
            ("2-folders", Box::new(|ui| ui.set_page(1))),
            ("3-accounts", Box::new(|ui| ui.set_page(2))),
            ("4-add-account", Box::new(|ui| ui.set_adding_account(true))),
            ("5-preferences", Box::new(|ui| ui.set_page(3))),
            ("6-activity", Box::new(|ui| ui.set_page(4))),
            ("7-about", Box::new(|ui| ui.set_page(5))),
            ("8-tree", Box::new(|ui| {
                ui.set_page(1);
                ui.set_tree_rows(model(vec![
                    TreeRow { id: ss("a"), name: ss("Clients"), path: ss("Clients"), drive: ss(""), depth: 0, expandable: true, expanded: true, loading: false, checked: true, enabled: true },
                    TreeRow { id: ss("b"), name: ss("Acme"), path: ss("Clients/Acme"), drive: ss(""), depth: 1, expandable: true, expanded: false, loading: false, checked: true, enabled: true },
                    TreeRow { id: ss("c"), name: ss("Old stuff"), path: ss("Old stuff"), drive: ss(""), depth: 0, expandable: true, expanded: false, loading: false, checked: false, enabled: true },
                ]));
                ui.set_tree_title(ss("Choose folders to sync"));
                ui.set_tree_note(ss("Ticked folders are kept on this computer."));
                ui.set_tree_pick(false);
                ui.set_tree_open(true);
            })),
        ];
        let w = ui.as_weak();
        let step = std::rc::Rc::new(std::cell::Cell::new(0usize));
        let timer = Box::leak(Box::new(slint::Timer::default()));
        timer.start(slint::TimerMode::Repeated, Duration::from_millis(900), move || {
            let Some(ui) = w.upgrade() else { return };
            let n = step.get();
            if n > 0 {
                let (name, _) = &steps[n - 1];
                match ui.window().take_snapshot() {
                    Ok(buf) => {
                        let mut ppm = format!("P6\n{} {}\n255\n", buf.width(), buf.height()).into_bytes();
                        for p in buf.as_slice() {
                            ppm.extend_from_slice(&[p.r, p.g, p.b]);
                        }
                        let _ = std::fs::write(dir.join(format!("{name}.ppm")), ppm);
                    }
                    Err(e) => eprintln!("snapshot failed: {e}"),
                }
            }
            if n == steps.len() {
                let _ = slint::quit_event_loop();
                return;
            }
            (steps[n].1)(&ui);
            step.set(n + 1);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_round_trip() {
        let mut p = accounts::new_pair("docs", "work", std::path::Path::new("/home/me/Docs"), "/Docs").unwrap();
        p.sync_interval = "10m".into();
        p.exclude_folders = vec!["Old".into(), "Archive/2019".into()];
        p.mode = SyncMode::UploadOnly;
        p.google_docs = GoogleDocsMode::Export;
        let row = folder_row(&p);
        assert_eq!(row.interval, 5);
        let back = pair_from_row(&row).unwrap();
        assert_eq!(toml::to_string(&back).unwrap(), toml::to_string(&p).unwrap());
    }

    #[test]
    fn ansi() {
        assert_eq!(strip_ansi("\u{1b}[2mhi\u{1b}[0m there"), "hi there");
    }
}
