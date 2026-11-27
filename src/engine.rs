//! The sync engine for one local folder ↔ cloud folder pair.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::auth::AuthError;
use crate::config::{ConflictPolicy, InitialMatch, SyncMode, SyncPair};
use crate::db::{Db, Entry, Kind, Op};
use crate::drive::{api_error, DriveClient, DriveFile};
use crate::localfs::{self, Filter, LocalInfo};
use crate::notify::Notifier;
use crate::reconcile::{decide, Action, Ctx, Side};
use crate::status::{State, StatusHub};
use crate::tree::{self, Mapping, RemoteTree, RCLONELINK};
use crate::util;
use crate::watcher::{self, WatchEvent};

#[derive(Debug)]
pub enum Command {
    SyncNow,
    Pause(bool),
    ConfirmDeletes,
    SetInterval(String),
    Shutdown,
}

pub struct Env {
    pub cfg: SyncPair,
    pub email: String,
    pub drive: Arc<DriveClient>,
    pub db: Arc<Db>,
    pub status: StatusHub,
    pub notifier: Notifier,
    pub max_transfers: usize,
}

struct Engine {
    env: Arc<Env>,
    root: PathBuf,
    filter: Filter,
    base: BTreeMap<String, Entry>,
    tree: RemoteTree,
    drive_id: Option<String>,
    local_dirty: BTreeSet<String>,
    local_trees: BTreeSet<String>,
    remote_dirty: BTreeSet<String>,
    remote_trees: BTreeSet<String>,
    full_pending: bool,
    confirm_deletes: bool,
    held_deletes: usize,
    /// Local paths that could not be read in the last cycle.
    unreadable: Vec<String>,
    paused: bool,
    interval: Option<Duration>,
    host: String,
    /// Paths that failed permanently this session (e.g. malware-flagged).
    skip: HashSet<String>,
}

/// Run one sync pair until shutdown.
pub async fn run(
    env: Env,
    mut cmd_rx: mpsc::UnboundedReceiver<Command>,
) {
    let env = Arc::new(env);
    let name = env.cfg.name.clone();
    let status = env.status.clone();
    status.update(&name, |s| {
        s.account = env.cfg.account.clone();
        s.email = env.email.clone();
        s.local = env.cfg.local.display().to_string();
        s.state = State::Starting;
        s.sync_interval = env.cfg.sync_interval.clone();
    });

    let mut attempt = 0u32;
    let mut engine = loop {
        match Engine::init(env.clone()).await {
            Ok(e) => break e,
            Err(e) => {
                let (state, wait) = classify(&e, attempt);
                status.update(&name, |s| {
                    s.state = state;
                    s.message = format!("{e:#}");
                });
                status.error(&name, format!("start-up failed: {e:#}"));
                if state == State::AuthRequired {
                    env.notifier.send("DriftFolderSync: sign-in required", &format!("{e}"));
                }
                attempt += 1;
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    c = cmd_rx.recv() => match c {
                        None | Some(Command::Shutdown) => return,
                        _ => {}
                    }
                }
            }
        }
    };
    engine.main_loop(cmd_rx).await;
}

fn classify(e: &anyhow::Error, attempt: u32) -> (State, Duration) {
    if e.downcast_ref::<AuthError>().is_some() {
        return (State::AuthRequired, Duration::from_secs(300));
    }
    let wait = Duration::from_secs((15u64 << attempt.min(5)).min(600));
    if e.downcast_ref::<reqwest::Error>().is_some()
        || e.chain().any(|c| c.downcast_ref::<reqwest::Error>().is_some())
    {
        return (State::Offline, wait);
    }
    (State::Error, wait)
}

fn parent_of(rel: &str) -> &str {
    rel.rfind('/').map(|i| &rel[..i]).unwrap_or("")
}

fn file_name(rel: &str) -> &str {
    rel.rsplit('/').next().unwrap_or(rel)
}

fn depth(rel: &str) -> usize {
    rel.matches('/').count()
}

fn under(path: &str, dir: &str) -> bool {
    path.len() > dir.len() && path.starts_with(dir) && path.as_bytes()[dir.len()] == b'/'
}

fn range_under<'a, V>(m: &'a BTreeMap<String, V>, prefix: &str) -> impl Iterator<Item = (&'a String, &'a V)> + 'a {
    let start = format!("{prefix}/");
    let end = format!("{prefix}0");
    m.range(start..end)
}

impl Engine {
    async fn init(env: Arc<Env>) -> Result<Self> {
        let cfg = &env.cfg;
        let name = cfg.name.clone();
        if !cfg.local.is_dir() {
            std::fs::create_dir_all(&cfg.local)
                .with_context(|| format!("creating {}", cfg.local.display()))?;
        }
        let drive = &env.drive;
        let about = drive.about().await?;
        let storage = about
            .storage_quota
            .as_ref()
            .map(|q| {
                let used = q.usage.as_deref().and_then(|u| u.parse::<u64>().ok()).unwrap_or(0);
                match q.limit.as_deref().and_then(|l| l.parse::<u64>().ok()) {
                    Some(l) if l > 0 => format!("{} of {} used", util::human_bytes(used), util::human_bytes(l)),
                    _ => format!("{} used", util::human_bytes(used)),
                }
            })
            .unwrap_or_default();
        env.status.update(&name, |s| {
            s.email = about.user.email_address.clone();
            s.storage = storage;
        });
        let drive_id = if cfg.shared_drive.is_empty() {
            None
        } else {
            Some(drive.find_shared_drive(&cfg.shared_drive).await?)
        };
        let top = match &drive_id {
            Some(d) => d.clone(),
            None => drive.get("root").await?.id,
        };
        let mapping = Mapping { docs: cfg.google_docs, symlinks: cfg.symlinks };
        let stored_root = env.db.get_kv(&name, "root_id")?;
        let root_id = match &stored_root {
            // Once synced, the cloud folder is identified by its ID, never
            // re-resolved by name: a same-named folder appearing elsewhere,
            // or the folder being renamed, must not redirect the sync.
            Some(id) => match drive.get(id).await {
                Ok(f) if !f.trashed => id.clone(),
                Ok(_) => bail!(
                    "the cloud folder for '{name}' is in the Drive bin. Restore it, or run \
                     `driftfoldersync folder reset {name}` to start this folder from scratch"
                ),
                Err(e) if api_error(&e).is_some_and(|a| a.is_not_found()) => bail!(
                    "the cloud folder for '{name}' no longer exists. Run `driftfoldersync folder reset {name}` \
                     to start this folder from scratch"
                ),
                Err(e) => return Err(e),
            },
            None => {
                let create = cfg.mode != SyncMode::DownloadOnly;
                drive.resolve_folder(&top, &cfg.remote, create).await?
            }
        };
        let stored_map = env.db.get_kv(&name, "mapping")?;
        let map_sig = format!("{:?}/{:?}", cfg.google_docs, cfg.symlinks);
        if stored_root.is_none() || stored_map.as_deref() != Some(map_sig.as_str()) {
            if stored_root.is_some() {
                warn!(pair = %name, "google_docs/symlinks setting changed; re-comparing this folder");
            }
            env.db.clear_pair(&name)?;
            env.db.set_kv(&name, "root_id", &root_id)?;
            env.db.set_kv(&name, "mapping", &map_sig)?;
        }
        let url = format!("https://drive.google.com/drive/folders/{root_id}");
        env.status.update(&name, |s| s.remote_url = url);

        let mut tree = RemoteTree::new(root_id.clone(), mapping);
        let base: BTreeMap<String, Entry> = env.db.load_entries(&name)?.into_iter().collect();
        let mut full_pending = true;

        if env.db.get_kv(&name, "page_token")?.is_none() {
            // Take the change token *before* listing so nothing is missed.
            let token = drive.start_page_token(drive_id.as_deref()).await?;
            env.status.update(&name, |s| {
                s.state = State::Syncing;
                s.message = "Listing cloud files…".into();
            });
            let status = env.status.clone();
            let n2 = name.clone();
            let files = drive
                .list_tree(&root_id, drive_id.as_deref(), |n| {
                    status.update(&n2, |s| s.message = format!("Listing cloud files… {n}"));
                })
                .await?;
            info!(pair = %name, "listed {} cloud items", files.len());
            for f in files {
                tree.upsert(f);
            }
            let ops = std::mem::take(&mut tree.ops);
            env.db.apply(&name, &ops)?;
            env.db.set_kv(&name, "page_token", &token)?;
        } else {
            tree.load(env.db.load_remote(&name)?);
            full_pending = true;
        }
        let prefer: HashMap<String, String> =
            base.iter().map(|(p, e)| (p.clone(), e.remote_id.clone())).collect();
        tree.reindex(&prefer);

        Ok(Self {
            root: cfg.local.clone(),
            filter: Filter::new(cfg)?,
            base,
            tree,
            drive_id,
            local_dirty: BTreeSet::new(),
            local_trees: BTreeSet::new(),
            remote_dirty: BTreeSet::new(),
            remote_trees: BTreeSet::new(),
            full_pending,
            confirm_deletes: false,
            held_deletes: 0,
            unreadable: Vec::new(),
            paused: cfg.paused,
            interval: cfg.interval(),
            host: util::hostname(),
            skip: HashSet::new(),
            env,
        })
    }

    fn name(&self) -> &str {
        &self.env.cfg.name
    }

    async fn main_loop(&mut self, mut cmd_rx: mpsc::UnboundedReceiver<Command>) {
        let (wtx, mut wrx) = mpsc::unbounded_channel();
        let _watcher = match watcher::spawn(&self.root, self.filter.clone(), wtx) {
            Ok(w) => Some(w),
            Err(e) => {
                self.env.status.error(self.name(), format!("file watching unavailable: {e}; using periodic rescans"));
                None
            }
        };

        let cfg = self.env.cfg.clone();
        let poll = cfg.poll_interval();
        let settle = cfg.settle();
        let rescan = cfg.rescan();
        let mut next_poll = Instant::now();
        let mut next_rescan = Instant::now() + rescan;
        let mut next_batch = Instant::now();
        let mut flush_at: Option<Instant> = None;
        let mut first_event_at: Option<Instant> = None;
        let mut backoff_until: Option<Instant> = None;
        let mut failures = 0u32;
        let mut force = true;

        self.set_idle_state();

        loop {
            let now = Instant::now();
            let wake = if self.paused {
                now + Duration::from_secs(3600)
            } else {
                let mut w = match self.interval {
                    None => {
                        let mut w = next_poll.min(next_rescan);
                        if let Some(f) = flush_at {
                            w = w.min(f);
                        }
                        w
                    }
                    Some(_) => next_batch,
                };
                if let Some(b) = backoff_until {
                    w = w.max(b);
                }
                w
            };

            tokio::select! {
                c = cmd_rx.recv() => match c {
                    None | Some(Command::Shutdown) => break,
                    Some(Command::SyncNow) => { force = true; backoff_until = None; }
                    Some(Command::Pause(p)) => {
                        self.paused = p;
                        self.set_idle_state();
                        if !p { force = true; }
                    }
                    Some(Command::ConfirmDeletes) => { self.confirm_deletes = true; force = true; }
                    Some(Command::SetInterval(s)) => {
                        self.interval = crate::config::parse_interval(&s).ok().flatten();
                        self.env.status.update(self.name(), |st| st.sync_interval = s.clone());
                        next_batch = Instant::now() + self.interval.unwrap_or_default();
                    }
                },
                ev = wrx.recv() => {
                    let Some(ev) = ev else { continue };
                    self.on_watch(ev);
                    // Drain whatever else is queued without waking up repeatedly.
                    while let Ok(ev) = wrx.try_recv() { self.on_watch(ev); }
                    if self.interval.is_none() {
                        let n = Instant::now();
                        let first = *first_event_at.get_or_insert(n);
                        // Debounce, but never delay more than 30 s under constant churn.
                        // At least 1 s, so both halves of a move/rename are seen together.
                        let wait = settle.max(Duration::from_secs(1));
                        flush_at = Some((n + wait).min(first + Duration::from_secs(30)));
                    }
                    self.env.status.update(self.name(), |s| s.pending = self.local_dirty.len() + self.local_trees.len());
                    continue;
                },
                _ = tokio::time::sleep_until(wake) => {}
            }

            if self.paused {
                continue;
            }
            let now = Instant::now();
            if !force && backoff_until.is_some_and(|b| now < b) {
                continue;
            }
            let mut poll_remote = force;
            let run = match self.interval {
                None => {
                    if now >= next_poll {
                        poll_remote = true;
                    }
                    if now >= next_rescan {
                        self.full_pending = true;
                    }
                    force || poll_remote || flush_at.is_some_and(|f| now >= f) || self.full_pending
                }
                Some(iv) => {
                    if now >= next_batch || force {
                        next_batch = now + iv;
                        poll_remote = true;
                        if now >= next_rescan {
                            self.full_pending = true;
                        }
                        true
                    } else {
                        false
                    }
                }
            };
            if !run {
                continue;
            }
            force = false;
            if poll_remote {
                next_poll = now + poll;
            }
            if self.full_pending {
                next_rescan = now + rescan;
            }
            flush_at = None;
            first_event_at = None;

            match self.cycle(poll_remote).await {
                Ok(more) => {
                    failures = 0;
                    backoff_until = None;
                    if more && self.interval.is_none() {
                        // Work deferred (settling files, conflict copies): come back soon.
                        // In batch mode it simply waits for the next tick.
                        flush_at = Some(Instant::now() + settle.max(Duration::from_secs(2)));
                    }
                    self.set_idle_state();
                }
                Err(e) => {
                    let (state, wait) = classify(&e, failures);
                    failures += 1;
                    backoff_until = Some(Instant::now() + wait);
                    let msg = format!("{e:#}");
                    self.env.status.error(self.name(), msg.clone());
                    self.env.status.update(self.name(), |s| {
                        s.state = state;
                        s.message = msg.clone();
                    });
                    if state == State::AuthRequired {
                        self.env.notifier.send("DriftFolderSync: sign-in required", &msg);
                    }
                    // Make sure nothing is lost: rescan after an error.
                    self.full_pending = true;
                }
            }
        }
        info!(pair = %self.name(), "stopped");
    }

    fn set_idle_state(&self) {
        let paused = self.paused;
        self.env.status.update(self.name(), |s| {
            s.state = if paused { State::Paused } else { State::Idle };
            if !paused {
                s.message.clear();
            }
        });
    }

    fn on_watch(&mut self, ev: WatchEvent) {
        match ev {
            WatchEvent::Path(p) => {
                self.local_dirty.insert(p);
            }
            WatchEvent::Tree(p) => {
                self.local_trees.insert(p);
            }
            WatchEvent::Overflow => self.full_pending = true,
            WatchEvent::WatchLimit => {
                self.env.notifier.send(
                    "DriftFolderSync: too many folders to watch",
                    "Raise fs.inotify.max_user_watches; changes in some folders will be picked up by periodic rescans.",
                );
            }
        }
    }

    /// Pull remote changes from the Drive changes feed into the tree.
    async fn poll_remote(&mut self) -> Result<()> {
        let name = self.name().to_string();
        let token = self
            .env
            .db
            .get_kv(&name, "page_token")?
            .ok_or_else(|| anyhow!("missing page token"))?;
        let (changes, new_token) = self.env.drive.changes(&token, self.drive_id.as_deref()).await?;
        if changes.is_empty() {
            if new_token != token {
                self.env.db.set_kv(&name, "page_token", &new_token)?;
            }
            return Ok(());
        }
        debug!(pair = %name, "{} remote changes", changes.len());
        let mut touched: Vec<String> = Vec::new();
        let mut new_folders: Vec<String> = Vec::new();
        let mut pending: Vec<DriveFile> = Vec::new();
        for ch in changes {
            let Some(id) = ch.file_id.clone() else { continue };
            if id == self.tree.root {
                continue;
            }
            if let Some(p) = self.tree.path_of(&id) {
                self.remote_trees.insert(p.clone());
            }
            match ch.file {
                Some(f) if !ch.removed && !f.trashed => {
                    if f.parents.iter().any(|p| self.tree.contains(p)) {
                        let known = self.tree.get(&id).is_some();
                        if f.is_folder() && !known {
                            new_folders.push(id.clone());
                        }
                        self.tree.upsert(f);
                        touched.push(id);
                    } else if self.tree.get(&id).is_some() {
                        self.tree.remove(&id); // moved out of the synced folder
                    } else {
                        pending.push(f);
                    }
                }
                _ => {
                    if self.tree.get(&id).is_some() {
                        self.tree.remove(&id);
                    }
                }
            }
        }
        // Items whose parent arrived later in the same batch.
        loop {
            let before = pending.len();
            let mut rest = Vec::new();
            for f in pending {
                if f.parents.iter().any(|p| self.tree.contains(p)) {
                    if f.is_folder() {
                        new_folders.push(f.id.clone());
                    }
                    touched.push(f.id.clone());
                    self.tree.upsert(f);
                } else {
                    rest.push(f);
                }
            }
            pending = rest;
            if pending.len() == before || pending.is_empty() {
                break;
            }
        }
        // Folders moved in from elsewhere: fetch their contents.
        for chunk in new_folders.chunks(40) {
            for root in chunk {
                let files = self.env.drive.list_tree(root, self.drive_id.as_deref(), |_| {}).await?;
                for f in files {
                    touched.push(f.id.clone());
                    self.tree.upsert(f);
                }
            }
        }
        self.reindex();
        for id in touched {
            if let Some(p) = self.tree.path_of(&id) {
                if self.tree.by_path(p).is_some_and(|(_, k)| k == Kind::Dir) {
                    self.remote_trees.insert(p.clone());
                } else {
                    self.remote_dirty.insert(p.clone());
                }
            }
        }
        let ops = std::mem::take(&mut self.tree.ops);
        self.env.db.apply(&name, &ops)?;
        self.env.db.set_kv(&name, "page_token", &new_token)?;
        Ok(())
    }

    fn reindex(&mut self) {
        if !self.tree.is_stale() {
            return;
        }
        let prefer: HashMap<String, String> =
            self.base.iter().map(|(p, e)| (p.clone(), e.remote_id.clone())).collect();
        self.tree.reindex(&prefer);
    }

    /// One reconciliation pass. Returns true if work was deferred.
    async fn cycle(&mut self, poll_remote: bool) -> Result<bool> {
        let name = self.name().to_string();
        if poll_remote {
            self.poll_remote().await?;
        }
        let full = std::mem::take(&mut self.full_pending);
        if !full
            && self.local_dirty.is_empty()
            && self.local_trees.is_empty()
            && self.remote_dirty.is_empty()
            && self.remote_trees.is_empty()
        {
            self.env.status.update(&name, |s| {
                s.last_sync = Some(chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string());
                s.pending = 0;
                s.cycles += 1;
            });
            return Ok(false);
        }
        self.env.status.update(&name, |s| {
            s.state = State::Syncing;
            s.message = if full { "Scanning…".into() } else { "Syncing…".into() };
        });

        // An unmounted or unreadable sync folder must never look "empty".
        if let Err(e) = std::fs::read_dir(&self.root) {
            self.full_pending = true;
            bail!("sync folder {} is not accessible ({e}); nothing will be changed until it is back", self.root.display());
        }

        // ---- 1. Gather candidate paths and local state ----------------
        let mut local: HashMap<String, Option<LocalInfo>> = HashMap::new();
        let mut unknown: Vec<String> = Vec::new();
        let mut cands: BTreeSet<String> = BTreeSet::new();
        let symlinks = self.env.cfg.symlinks;
        if full {
            let root = self.root.clone();
            let filter = self.filter.clone();
            let (scanned, unk) = tokio::task::spawn_blocking(move || {
                let mut m = HashMap::new();
                let mut u = Vec::new();
                localfs::scan(&root, "", &filter, symlinks, &mut m, &mut u);
                (m, u)
            })
            .await?;
            unknown.extend(unk);
            cands.extend(scanned.keys().cloned());
            for (k, v) in scanned {
                local.insert(k, Some(v));
            }
            cands.extend(self.tree.paths().cloned());
            cands.extend(self.base.keys().cloned());
            self.local_dirty.clear();
            self.local_trees.clear();
            self.remote_dirty.clear();
            self.remote_trees.clear();
        } else {
            let trees: BTreeSet<String> = std::mem::take(&mut self.local_trees)
                .into_iter()
                .chain(std::mem::take(&mut self.remote_trees))
                .collect();
            for t in &trees {
                cands.insert(t.clone());
                self.expand(t, &mut cands, &mut local, &mut unknown).await?;
            }
            cands.extend(std::mem::take(&mut self.local_dirty));
            cands.extend(std::mem::take(&mut self.remote_dirty));
            // A changed path whose parent directory is new must bring the parent along.
            let extra: Vec<String> = cands
                .iter()
                .flat_map(|p| {
                    let mut v = Vec::new();
                    let mut cur = parent_of(p);
                    while !cur.is_empty() && !self.base.contains_key(cur) {
                        v.push(cur.to_string());
                        cur = parent_of(cur);
                    }
                    v
                })
                .collect();
            cands.extend(extra);
        }
        cands.retain(|p| !p.is_empty());
        for p in &cands {
            if !local.contains_key(p) {
                match localfs::stat_checked(&self.root, p, symlinks) {
                    Ok(l) => {
                        local.insert(p.clone(), l);
                    }
                    Err(e) => {
                        warn!("cannot stat {p}: {e}; leaving it untouched");
                        unknown.push(p.clone());
                    }
                }
            }
        }
        // Anything we could not read is left completely alone this cycle,
        // and retried later.
        if !unknown.is_empty() {
            cands.retain(|p| !unknown.iter().any(|u| u.is_empty() || p == u || under(p, u)));
            for u in &unknown {
                if !self.unreadable.contains(u) {
                    self.env.status.error(&name, format!("cannot read '{u}'; it is not being synced until it is readable"));
                }
            }
            self.local_trees.extend(unknown.iter().cloned());
            self.unreadable = unknown.clone();
        } else {
            self.unreadable.clear();
        }
        // Filtering (selective sync, excludes).
        cands.retain(|p| {
            let is_dir = local.get(p).and_then(|l| l.as_ref()).is_some_and(|l| l.kind == Kind::Dir)
                || self.tree.by_path(p).is_some_and(|(_, k)| k == Kind::Dir)
                || self.base.get(p).is_some_and(|b| b.kind == Kind::Dir);
            !self.filter.excluded(p, is_dir) && !self.skip.contains(p)
        });

        // ---- 2. Settle: don't touch files still being written ------------
        let settle_ns = self.env.cfg.settle().as_nanos() as i64;
        let now_ns = util::system_time_to_ns(std::time::SystemTime::now());
        let mut deferred = false;
        cands.retain(|p| {
            if let Some(Some(l)) = local.get(p) {
                if l.kind == Kind::File && now_ns - l.mtime_ns < settle_ns {
                    self.local_dirty.insert(p.clone());
                    deferred = true;
                    return false;
                }
            }
            true
        });

        // ---- 3. Renames / moves (cheap metadata operations) --------------
        self.detect_renames(&mut cands, &mut local).await?;

        // ---- 4. Hash where size/mtime alone cannot decide ----------------
        let mut need_hash: Vec<String> = Vec::new();
        for p in &cands {
            let Some(Some(l)) = local.get(p) else { continue };
            if l.kind != Kind::File {
                continue;
            }
            let r = self.tree.by_path(p);
            match self.base.get(p) {
                Some(b) if b.kind == Kind::File => {
                    if b.size == l.size && b.mtime_ns != l.mtime_ns {
                        need_hash.push(p.clone());
                    }
                }
                None => {
                    if let Some((rf, Kind::File)) = r {
                        if rf.size_u64() == l.size
                            && (self.env.cfg.initial_match == InitialMatch::Checksum
                                || (rf.mtime_ns() - l.mtime_ns).abs() > 2_000_000_000)
                        {
                            need_hash.push(p.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        let hashes = self.hash_files(need_hash).await;

        // ---- 5. Decide ----------------------------------------------------
        let ctx = Ctx { mode: self.env.cfg.mode, delete: self.env.cfg.delete };
        let mut plan: BTreeMap<String, Action> = BTreeMap::new();
        let mut touch: Vec<Op> = Vec::new();
        for p in &cands {
            let l = local.get(p).cloned().flatten();
            let r = self.tree.by_path(p).map(|(f, k)| (f.clone(), k));
            let b = self.base.get(p).cloned();

            if b.is_none() && r.is_none() && self.tree.is_shadow(p) {
                debug!("not uploading '{p}': it looks like an export of a cloud document");
                continue;
            }

            let mut lkind = l.as_ref().map(|l| l.kind);
            // A regular file standing for a link/export keeps that kind.
            if let (Some(Kind::File), Some(bk)) = (lkind, b.as_ref().map(|b| b.kind)) {
                if matches!(bk, Kind::Link | Kind::Export) {
                    lkind = Some(bk);
                }
            }
            if let (Some(Kind::File), None, Some((_, rk))) = (lkind, &b, &r) {
                if matches!(rk, Kind::Link | Kind::Export) {
                    lkind = Some(*rk);
                }
            }

            let l_changed = match (&l, &b, lkind) {
                (Some(l), Some(b), Some(k)) => {
                    if k != b.kind {
                        true
                    } else {
                        match k {
                            Kind::Dir | Kind::Link => false,
                            // Edits to an exported Office file are noticed so
                            // that they are never silently overwritten.
                            Kind::Export => b.size != l.size || b.mtime_ns != l.mtime_ns,
                            Kind::Symlink => {
                                b.md5.as_deref()
                                    != l.link_target.as_deref().map(|t| util::md5_bytes(t.as_bytes())).as_deref()
                            }
                            Kind::File => {
                                if b.size != l.size {
                                    true
                                } else if b.mtime_ns == l.mtime_ns {
                                    false
                                } else {
                                    match hashes.get(p) {
                                        Some(h) if Some(h) == b.md5.as_ref() => {
                                            // Content identical; refresh stat in base.
                                            let mut nb = b.clone();
                                            nb.mtime_ns = l.mtime_ns;
                                            nb.inode = l.inode;
                                            touch.push(Op::PutEntry(p.clone(), nb.clone()));
                                            self.base.insert(p.clone(), nb);
                                            false
                                        }
                                        _ => true,
                                    }
                                }
                            }
                        }
                    }
                }
                _ => false,
            };
            let r_changed = match (&r, &b) {
                (Some((f, k)), Some(b)) => {
                    f.id != b.remote_id || *k != b.kind || tree::remote_fp(f, *k) != b.remote_fp
                }
                _ => false,
            };
            let ls = lkind.map(|k| Side { kind: k, changed: l_changed });
            let rs = r.as_ref().map(|(_, k)| Side { kind: *k, changed: r_changed });
            let same = || self.same_content(p, l.as_ref(), r.as_ref().map(|(f, _)| f), &hashes);
            let act = decide(ctx, ls, rs, b.is_some(), same);
            if act != Action::Nothing {
                plan.insert(p.clone(), act);
            }
        }
        self.env.db.apply(&name, &touch)?;

        // ---- 6. Directory deletions: whole-folder safety -----------------
        self.resolve_folder_deletes(&mut plan);

        // ---- 7. Mass-deletion guard ---------------------------------------
        let deletes: usize = plan
            .iter()
            .filter(|(_, a)| matches!(a, Action::DeleteLocal | Action::DeleteRemote))
            .map(|(p, _)| 1 + range_under(&self.base, p).count())
            .sum();
        let guard = self.env.cfg.delete_guard;
        if guard > 0 && deletes > guard && !self.confirm_deletes {
            let held: Vec<(String, Action)> = plan
                .iter()
                .filter(|(_, a)| matches!(a, Action::DeleteLocal | Action::DeleteRemote))
                .map(|(p, a)| (p.clone(), *a))
                .collect();
            plan.retain(|_, a| !matches!(a, Action::DeleteLocal | Action::DeleteRemote));
            // Keep them pending so they are re-evaluated (cheaply) next time.
            for (p, a) in held {
                let is_dir = self.base.get(&p).is_some_and(|b| b.kind == Kind::Dir);
                match (a, is_dir) {
                    (Action::DeleteRemote, true) => self.local_trees.insert(p),
                    (Action::DeleteRemote, false) => self.local_dirty.insert(p),
                    (_, true) => self.remote_trees.insert(p),
                    (_, false) => self.remote_dirty.insert(p),
                };
            }
            if self.held_deletes != deletes {
                self.held_deletes = deletes;
                let msg = format!(
                    "{deletes} deletions held back (limit {guard}). Check the folder, then run: driftfoldersync confirm-deletes '{name}' (or use the tray menu)"
                );
                self.env.status.error(&name, msg.clone());
                self.env.notifier.send("DriftFolderSync: large deletion held back", &msg);
            }
            self.env.status.update(&name, |s| s.blocked_deletes = deletes);
        } else {
            self.confirm_deletes = false;
            self.held_deletes = 0;
            self.env.status.update(&name, |s| s.blocked_deletes = 0);
        }

        // ---- 8. Execute -----------------------------------------------------
        let total = plan.len();
        self.env.status.update(&name, |s| s.pending = total);
        if total > 0 {
            info!(pair = %name, "{total} actions");
        }
        let more = self.execute(plan, &local, &hashes).await?;

        let ops = std::mem::take(&mut self.tree.ops);
        self.env.db.apply(&name, &ops)?;
        self.env.status.update(&name, |s| {
            s.last_sync = Some(chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string());
            s.pending = 0;
            s.cycles += 1;
        });
        Ok(deferred || more)
    }

    async fn expand(
        &mut self,
        t: &str,
        cands: &mut BTreeSet<String>,
        local: &mut HashMap<String, Option<LocalInfo>>,
        unknown: &mut Vec<String>,
    ) -> Result<()> {
        cands.extend(range_under(&self.base, t).map(|(k, _)| k.clone()));
        cands.extend(self.tree.paths_under(t).cloned());
        let root = self.root.clone();
        let filter = self.filter.clone();
        let symlinks = self.env.cfg.symlinks;
        let start = t.to_string();
        let (scanned, unk) = tokio::task::spawn_blocking(move || {
            let mut m = HashMap::new();
            let mut u = Vec::new();
            match localfs::stat_checked(&root, &start, symlinks) {
                Ok(Some(l)) if l.kind == Kind::Dir => localfs::scan(&root, &start, &filter, symlinks, &mut m, &mut u),
                Ok(_) => {}
                Err(_) => u.push(start.clone()),
            }
            (m, u)
        })
        .await?;
        unknown.extend(unk);
        for (k, v) in scanned {
            cands.insert(k.clone());
            local.insert(k, Some(v));
        }
        Ok(())
    }

    fn same_content(
        &self,
        p: &str,
        l: Option<&LocalInfo>,
        r: Option<&DriveFile>,
        hashes: &HashMap<String, String>,
    ) -> bool {
        let (Some(l), Some(r)) = (l, r) else { return false };
        match l.kind {
            Kind::Symlink => l
                .link_target
                .as_deref()
                .is_some_and(|t| r.md5_checksum.as_deref() == Some(util::md5_bytes(t.as_bytes()).as_str())),
            Kind::File => {
                if l.size != r.size_u64() {
                    return false;
                }
                if let Some(h) = hashes.get(p) {
                    return r.md5_checksum.as_deref() == Some(h.as_str());
                }
                // Not hashed: sizes equal and mtimes within 2 s (size-mtime mode).
                self.env.cfg.initial_match == InitialMatch::SizeMtime
                    && (r.mtime_ns() - l.mtime_ns).abs() <= 2_000_000_000
            }
            _ => false,
        }
    }

    async fn hash_files(&self, paths: Vec<String>) -> HashMap<String, String> {
        if paths.is_empty() {
            return HashMap::new();
        }
        if paths.len() > 20 {
            let n = paths.len();
            self.env.status.update(self.name(), |s| s.message = format!("Checking {n} files…"));
        }
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let mut out = HashMap::new();
            for p in paths {
                match util::md5_file(&root.join(&p)) {
                    Ok(h) => {
                        out.insert(p, h);
                    }
                    Err(e) => debug!("hash {p}: {e}"),
                }
            }
            out
        })
        .await
        .unwrap_or_default()
    }

    /// Detect local and remote renames/moves and apply them as cheap
    /// metadata operations instead of delete + re-transfer.
    async fn detect_renames(
        &mut self,
        cands: &mut BTreeSet<String>,
        local: &mut HashMap<String, Option<LocalInfo>>,
    ) -> Result<()> {
        if self.env.cfg.mode != SyncMode::TwoWay {
            return Ok(());
        }
        // Local renames: a new local path whose inode belongs to a base entry
        // whose old path no longer exists locally.
        let new_local: Vec<(String, LocalInfo)> = cands
            .iter()
            .filter(|p| !self.base.contains_key(*p) && self.tree.by_path(p).is_none())
            .filter_map(|p| local.get(p).cloned().flatten().map(|l| (p.clone(), l)))
            .collect();
        let mut by_inode: Option<HashMap<u64, String>> = None;
        let mut renames: Vec<(String, String, bool)> = Vec::new(); // (old, new, remote_side)
        if !new_local.is_empty() {
            let map = by_inode.get_or_insert_with(|| {
                self.base.iter().map(|(p, e)| (e.inode, p.clone())).collect()
            });
            for (p, l) in &new_local {
                let Some(old) = map.get(&l.inode) else { continue };
                let Some(b) = self.base.get(old) else { continue };
                if b.kind != l.kind || old == p || under(p, old) {
                    continue;
                }
                if l.kind == Kind::File && (b.size != l.size || b.mtime_ns != l.mtime_ns) {
                    continue;
                }
                // Inode numbers can be reused (and repeat across mounts), so a
                // directory must also still contain something we synced in it.
                if l.kind == Kind::Dir {
                    let kids: Vec<&str> = range_under(&self.base, old)
                        .filter(|(k, _)| !k[old.len() + 1..].contains('/'))
                        .map(|(k, _)| &k[old.len() + 1..])
                        .collect();
                    let new_dir = self.root.join(p);
                    if !kids.is_empty() && !kids.iter().any(|k| new_dir.join(k).symlink_metadata().is_ok()) {
                        continue;
                    }
                }
                // Old path must be gone locally.
                let old_now = local
                    .get(old)
                    .cloned()
                    .unwrap_or_else(|| localfs::stat(&self.root, old, self.env.cfg.symlinks));
                if old_now.is_some_and(|o| o.inode == l.inode) {
                    continue;
                }
                // Remote must still be what we last synced.
                match self.tree.by_path(old) {
                    Some((f, k)) if f.id == b.remote_id && tree::remote_fp(f, k) == b.remote_fp => {}
                    _ => continue,
                }
                renames.push((old.clone(), p.clone(), true));
            }
        }
        // Remote renames: a remote id now at a new path, previously synced elsewhere.
        let mut by_id: Option<HashMap<String, String>> = None;
        for p in cands.iter() {
            if self.base.contains_key(p) {
                continue;
            }
            let Some((f, _)) = self.tree.by_path(p) else { continue };
            if local.get(p).is_some_and(|l| l.is_some()) {
                continue;
            }
            let map = by_id.get_or_insert_with(|| {
                self.base.iter().map(|(p, e)| (e.remote_id.clone(), p.clone())).collect()
            });
            let Some(old) = map.get(&f.id) else { continue };
            if old == p || self.tree.by_path(old).is_some_and(|(g, _)| g.id == f.id) || under(p, old) {
                continue;
            }
            let b = &self.base[old];
            let cur = localfs::stat(&self.root, old, self.env.cfg.symlinks);
            match cur {
                Some(c) if c.inode == b.inode && (b.kind == Kind::Dir || (c.size == b.size && c.mtime_ns == b.mtime_ns)) => {
                    renames.push((old.clone(), p.clone(), false));
                }
                _ => {}
            }
        }
        if renames.is_empty() {
            return Ok(());
        }
        // Shallowest first, so parent moves happen before child moves.
        renames.sort_by_key(|(_, new, _)| depth(new));
        let mut done: Vec<(String, String)> = Vec::new();
        for (old, new, remote_side) in renames {
            // Skip renames already covered by an ancestor rename.
            if done.iter().any(|(o, n)| under(&old, o) && under(&new, n)) {
                continue;
            }
            let res = if remote_side {
                self.rename_remote(&old, &new).await
            } else {
                self.rename_local(&old, &new)
            };
            match res {
                Ok(()) => {
                    let what = if remote_side { "Renamed in cloud" } else { "Renamed locally" };
                    self.env.status.activity(self.name(), format!("{what}: {old} → {new}"));
                    self.rebase(&old, &new);
                    for path in [&old, &new] {
                        if let Some(l) = local.get_mut(path) {
                            *l = localfs::stat(&self.root, path, self.env.cfg.symlinks);
                        }
                    }
                    // Children paths of a moved directory need fresh local info.
                    let kids: Vec<String> = range_under(&self.base, &new).map(|(k, _)| k.clone()).collect();
                    for k in kids {
                        local.insert(k.clone(), localfs::stat(&self.root, &k, self.env.cfg.symlinks));
                        cands.insert(k);
                    }
                    done.push((old, new));
                }
                Err(e) => {
                    self.env.status.error(self.name(), format!("rename {old} → {new} failed: {e:#}"));
                }
            }
        }
        Ok(())
    }

    async fn rename_remote(&mut self, old: &str, new: &str) -> Result<()> {
        let b = self.base.get(old).ok_or_else(|| anyhow!("no base"))?;
        let f = self.tree.get(&b.remote_id).ok_or_else(|| anyhow!("remote item vanished"))?.clone();
        let new_parent = self
            .tree
            .folder_id(parent_of(new))
            .ok_or_else(|| anyhow!("destination folder not in cloud yet"))?;
        let old_parent = f.parents.first().cloned().unwrap_or_default();
        let mut new_name = util::local_to_remote_name(file_name(new));
        if b.kind == Kind::Symlink {
            new_name.push_str(RCLONELINK);
        }
        if let Some(t) = crate::drive::native_type(&f.mime_type) {
            // "Plan.gdoc" / "Plan.docx" locally is "Plan" in the cloud.
            let ext = if b.kind == Kind::Export { t.export_ext } else { t.link_ext };
            if !ext.is_empty() && !f.name.to_ascii_lowercase().ends_with(ext) {
                if let Some(s) = new_name.strip_suffix(ext) {
                    new_name = s.to_string();
                }
            }
        }
        let name_arg = (new_name != f.name).then_some(new_name.as_str());
        let (from, to) = if new_parent != old_parent {
            (Some(old_parent.as_str()), Some(new_parent.as_str()))
        } else {
            (None, None)
        };
        let updated = self.env.drive.update_metadata(&f.id, name_arg, from, to).await?;
        self.tree.upsert(updated);
        self.reindex();
        Ok(())
    }

    fn rename_local(&mut self, old: &str, new: &str) -> Result<()> {
        let src = self.root.join(old);
        let dst = self.root.join(new);
        if dst.symlink_metadata().is_ok() {
            bail!("destination exists");
        }
        if let Some(p) = dst.parent() {
            std::fs::create_dir_all(p)?;
        }
        std::fs::rename(&src, &dst)?;
        Ok(())
    }

    /// Move base entries from `old` (and below) to `new`.
    fn rebase(&mut self, old: &str, new: &str) {
        let mut ops = Vec::new();
        let mut moved: Vec<(String, Entry)> = Vec::new();
        if let Some(e) = self.base.remove(old) {
            moved.push((new.to_string(), e));
            ops.push(Op::DelEntry(old.to_string()));
        }
        let kids: Vec<String> = range_under(&self.base, old).map(|(k, _)| k.clone()).collect();
        for k in kids {
            let e = self.base.remove(&k).unwrap();
            ops.push(Op::DelEntry(k.clone()));
            moved.push((format!("{new}{}", &k[old.len()..]), e));
        }
        for (p, mut e) in moved {
            if let Some(l) = localfs::stat(&self.root, &p, self.env.cfg.symlinks) {
                e.inode = l.inode;
                if e.kind == Kind::Dir {
                    e.mtime_ns = l.mtime_ns;
                }
            }
            if let Some((f, k)) = self.tree.by_path(&p) {
                e.remote_fp = tree::remote_fp(f, k);
            }
            ops.push(Op::PutEntry(p.clone(), e.clone()));
            self.base.insert(p, e);
        }
        if let Err(e) = self.env.db.apply(self.name(), &ops) {
            warn!("db: {e}");
        }
    }

    /// For folder deletions, trash the whole folder in one call only if
    /// nothing below it would be lost; otherwise keep the folder and let
    /// children be handled individually.
    fn resolve_folder_deletes(&self, plan: &mut BTreeMap<String, Action>) {
        let dir_deletes: Vec<(String, Action)> = plan
            .iter()
            .filter(|(p, a)| {
                matches!(a, Action::DeleteLocal | Action::DeleteRemote)
                    && self.base.get(*p).is_some_and(|b| b.kind == Kind::Dir)
            })
            .map(|(p, a)| (p.clone(), *a))
            .collect();
        // Process shallowest first so that ancestors absorb descendants.
        let mut dir_deletes = dir_deletes;
        dir_deletes.sort_by_key(|(p, _)| depth(p));
        for (p, a) in dir_deletes {
            if !plan.contains_key(&p) {
                continue; // absorbed by an ancestor
            }
            let kids: Vec<(String, Action)> = range_under(plan, &p).map(|(k, v)| (k.clone(), *v)).collect();
            let safe = kids.iter().all(|(_, ka)| *ka == a || *ka == Action::Forget)
                && match a {
                    Action::DeleteRemote => self.remote_subtree_unchanged(&p),
                    _ => self.local_subtree_unchanged(&p),
                };
            if safe {
                for (k, _) in kids {
                    plan.remove(&k);
                }
            } else {
                // Keep the folder; children are handled one by one.
                let replacement = if a == Action::DeleteRemote { Action::MkdirLocal } else { Action::MkdirRemote };
                plan.insert(p, replacement);
            }
        }
    }

    async fn execute(
        &mut self,
        plan: BTreeMap<String, Action>,
        local: &HashMap<String, Option<LocalInfo>>,
        hashes: &HashMap<String, String>,
    ) -> Result<bool> {
        let name = self.name().to_string();
        let mut more = false;
        let mut ops: Vec<Op> = Vec::new();

        // Records / forgets.
        for (p, a) in &plan {
            match a {
                Action::Forget => {
                    self.base.remove(p);
                    ops.push(Op::DelEntry(p.clone()));
                }
                Action::Record => {
                    if let (Some(Some(l)), Some((f, k))) = (local.get(p), self.tree.by_path(p)) {
                        let md5 = match l.kind {
                            Kind::File => hashes.get(p).cloned().or_else(|| f.md5_checksum.clone()),
                            Kind::Symlink => l.link_target.as_deref().map(|t| util::md5_bytes(t.as_bytes())),
                            _ => None,
                        };
                        let e = Entry {
                            kind: k,
                            remote_id: f.id.clone(),
                            size: l.size,
                            mtime_ns: l.mtime_ns,
                            inode: l.inode,
                            md5,
                            remote_fp: tree::remote_fp(f, k),
                        };
                        ops.push(Op::PutEntry(p.clone(), e.clone()));
                        self.base.insert(p.clone(), e);
                    }
                }
                _ => {}
            }
        }
        self.env.db.apply(&name, &std::mem::take(&mut ops))?;

        // Folder creation, parents first.
        let mut mkdirs: Vec<(&String, &Action)> = plan
            .iter()
            .filter(|(_, a)| matches!(a, Action::MkdirLocal | Action::MkdirRemote))
            .collect();
        mkdirs.sort_by_key(|(p, _)| depth(p));
        for (p, a) in mkdirs {
            let r = match a {
                Action::MkdirLocal => self.mkdir_local(p),
                _ => self.mkdir_remote(p).await,
            };
            if let Err(e) = r {
                if is_fatal(&e) {
                    return Err(e);
                }
                self.env.status.error(&name, format!("{p}: {e:#}"));
            }
        }

        // Conflicts (sequential, they rename local files).
        for (p, a) in &plan {
            if *a != Action::Conflict {
                continue;
            }
            match self.conflict(p, local.get(p).cloned().flatten()).await {
                Ok(m) => more |= m,
                Err(e) => {
                    if is_fatal(&e) {
                        return Err(e);
                    }
                    self.env.status.error(&name, format!("{p}: {e:#}"));
                }
            }
        }

        // Transfers, concurrently.
        let mut jobs: Vec<Job> = Vec::new();
        for (p, a) in &plan {
            match a {
                Action::Upload => {
                    let Some(Some(l)) = local.get(p).cloned() else { continue };
                    if l.kind == Kind::File && self.filter.too_big(l.size) {
                        continue;
                    }
                    let Some(parent) = self.tree.folder_id(parent_of(p)) else {
                        // Parent folder not created (yet); try again later.
                        self.local_dirty.insert(p.clone());
                        more = true;
                        continue;
                    };
                    let existing = self.tree.by_path(p).map(|(f, k)| (f.clone(), k));
                    jobs.push(Job::Upload { path: p.clone(), local: l, parent, existing, md5: hashes.get(p).cloned() });
                }
                Action::Download => {
                    let Some((f, k)) = self.tree.by_path(p).map(|(f, k)| (f.clone(), k)) else { continue };
                    if k == Kind::File && self.filter.too_big(f.size_u64()) {
                        continue;
                    }
                    jobs.push(Job::Download { path: p.clone(), file: f, kind: k, expect_local: local.get(p).cloned().flatten() });
                }
                _ => {}
            }
        }
        if !jobs.is_empty() {
            let env = self.env.clone();
            let root = self.root.clone();
            let host = self.host.clone();
            let total = jobs.len();
            let mut done = 0usize;
            let mut stream = futures::stream::iter(jobs.into_iter().map(|j| {
                let env = env.clone();
                let root = root.clone();
                let host = host.clone();
                async move { run_job(env, root, host, j).await }
            }))
            .buffer_unordered(self.env.max_transfers.max(1));
            while let Some(out) = stream.next().await {
                done += 1;
                self.env.status.update(&name, |s| {
                    s.pending = total - done;
                    s.message = format!("Syncing {done}/{total}");
                });
                match out.result {
                    Ok(o) => {
                        if let Some(f) = o.remote {
                            let kind = o.entry.as_ref().map(|e| e.kind).unwrap_or(Kind::File);
                            self.tree.upsert_at(&out.path, f, kind);
                        }
                        if let Some(e) = o.entry {
                            ops.push(Op::PutEntry(out.path.clone(), e.clone()));
                            self.base.insert(out.path.clone(), e);
                        }
                        if o.redo {
                            self.local_dirty.insert(out.path.clone());
                            more = true;
                        }
                        if let Some(m) = o.msg {
                            self.env.status.activity(&name, m);
                        }
                        self.env.status.update(&name, |s| {
                            s.uploaded += o.up;
                            s.downloaded += o.down;
                        });
                    }
                    Err(e) => {
                        if is_fatal(&e) {
                            drop(stream);
                            self.env.db.apply(&name, &ops)?;
                            return Err(e);
                        }
                        if api_error(&e).is_some_and(|a| a.is_abuse()) {
                            self.skip.insert(out.path.clone());
                            self.env.status.error(&name, format!(
                                "{}: flagged by Google as malware/spam; not downloaded (set acknowledge_abuse = true to override)",
                                out.path
                            ));
                        } else if api_error(&e).is_some_and(|a| a.is_quota()) {
                            self.env.notifier.send("DriftFolderSync: cloud storage full", &format!("{e}"));
                            self.env.status.error(&name, format!("{}: {e:#}", out.path));
                        } else {
                            self.env.status.error(&name, format!("{}: {e:#}", out.path));
                            // Try again next cycle.
                            self.local_dirty.insert(out.path.clone());
                        }
                    }
                }
                if ops.len() >= 500 {
                    self.env.db.apply(&name, &std::mem::take(&mut ops))?;
                    let tops = std::mem::take(&mut self.tree.ops);
                    self.env.db.apply(&name, &tops)?;
                }
            }
            self.reindex();
        }
        self.env.db.apply(&name, &std::mem::take(&mut ops))?;

        // Deletions, deepest first.
        let mut dels: Vec<(&String, &Action)> = plan
            .iter()
            .filter(|(_, a)| matches!(a, Action::DeleteLocal | Action::DeleteRemote))
            .collect();
        dels.sort_by_key(|(p, _)| std::cmp::Reverse(depth(p)));
        for (p, a) in dels {
            let r = match a {
                Action::DeleteRemote => self.delete_remote(p).await,
                _ => self.delete_local(p),
            };
            if let Err(e) = r {
                if is_fatal(&e) {
                    return Err(e);
                }
                self.env.status.error(&name, format!("{p}: {e:#}"));
            }
        }
        self.reindex();
        Ok(more)
    }

    fn mkdir_local(&mut self, p: &str) -> Result<()> {
        let full = self.root.join(p);
        std::fs::create_dir_all(&full)?;
        let l = localfs::stat(&self.root, p, self.env.cfg.symlinks).ok_or_else(|| anyhow!("mkdir failed"))?;
        if let Some((f, _)) = self.tree.by_path(p) {
            let e = Entry { kind: Kind::Dir, remote_id: f.id.clone(), size: 0, mtime_ns: l.mtime_ns, inode: l.inode, md5: None, remote_fp: String::new() };
            self.env.db.apply(self.name(), &[Op::PutEntry(p.to_string(), e.clone())])?;
            self.base.insert(p.to_string(), e);
        }
        Ok(())
    }

    async fn mkdir_remote(&mut self, p: &str) -> Result<()> {
        let parent = self
            .tree
            .folder_id(parent_of(p))
            .ok_or_else(|| anyhow!("parent folder missing in cloud"))?;
        let f = match self.tree.by_path(p) {
            Some((f, Kind::Dir)) => f.clone(),
            _ => {
                let f = self.env.drive.create_folder(&util::local_to_remote_name(file_name(p)), &parent).await?;
                self.env.status.activity(self.name(), format!("Created folder in cloud: {p}"));
                f
            }
        };
        let l = localfs::stat(&self.root, p, self.env.cfg.symlinks);
        let e = Entry {
            kind: Kind::Dir,
            remote_id: f.id.clone(),
            size: 0,
            mtime_ns: l.as_ref().map(|l| l.mtime_ns).unwrap_or(0),
            inode: l.as_ref().map(|l| l.inode).unwrap_or(0),
            md5: None,
            remote_fp: String::new(),
        };
        self.tree.upsert_at(p, f, Kind::Dir);
        self.env.db.apply(self.name(), &[Op::PutEntry(p.to_string(), e.clone())])?;
        self.base.insert(p.to_string(), e);
        Ok(())
    }

    /// True if every cloud item below the synced folder `p` is one we
    /// synced and is unchanged since — i.e. trashing the folder loses nothing.
    fn remote_subtree_unchanged(&self, p: &str) -> bool {
        let Some(b) = self.base.get(p) else { return false };
        let synced: HashMap<&str, &Entry> =
            range_under(&self.base, p).map(|(_, e)| (e.remote_id.as_str(), e)).collect();
        self.tree.descendants(&b.remote_id).iter().all(|f| {
            synced.get(f.id.as_str()).is_some_and(|e| tree::remote_fp(f, e.kind) == e.remote_fp)
        })
    }

    /// True if every local item below `p` (including excluded, unreadable or
    /// still-settling files) was synced and is unchanged since.
    fn local_subtree_unchanged(&self, p: &str) -> bool {
        let Ok(all) = localfs::walk_all(&self.root, p) else { return false };
        all.iter().all(|(rel, l)| self.local_unchanged(rel, l))
    }

    fn local_unchanged(&self, rel: &str, l: &LocalInfo) -> bool {
        let Some(b) = self.base.get(rel) else { return false };
        match (b.kind, l.kind) {
            (Kind::Dir, Kind::Dir) => true,
            (Kind::Dir, _) | (_, Kind::Dir) => false,
            (Kind::Symlink, Kind::Symlink) => b.mtime_ns == l.mtime_ns,
            (Kind::Symlink, _) | (_, Kind::Symlink) => false,
            _ => b.size == l.size && b.mtime_ns == l.mtime_ns,
        }
    }

    async fn delete_remote(&mut self, p: &str) -> Result<()> {
        let Some(b) = self.base.get(p).cloned() else { return Ok(()) };
        // Re-check just before acting: transfers may have taken a long time.
        match localfs::stat_checked(&self.root, p, self.env.cfg.symlinks) {
            Ok(None) => {}
            _ => {
                self.local_dirty.insert(p.to_string());
                return Ok(());
            }
        }
        if b.kind == Kind::Dir && !self.remote_subtree_unchanged(p) {
            self.remote_trees.insert(p.to_string());
            return Ok(());
        }
        if let Some((f, _)) = self.tree.by_path(p) {
            if f.id == b.remote_id {
                self.env.drive.trash(&f.id).await?;
                let id = f.id.clone();
                self.tree.remove(&id);
                self.env.status.activity(self.name(), format!("Moved to cloud bin: {p}"));
            }
        }
        self.forget_tree(p)
    }

    fn delete_local(&mut self, p: &str) -> Result<()> {
        // Re-check just before acting: never trash anything that is new or
        // was edited since the plan was made.
        let safe = match localfs::stat_checked(&self.root, p, self.env.cfg.symlinks) {
            Ok(None) => return self.forget_tree(p),
            Ok(Some(l)) if l.kind == Kind::Dir => self.local_unchanged(p, &l) && self.local_subtree_unchanged(p),
            Ok(Some(l)) => self.local_unchanged(p, &l),
            Err(_) => false,
        };
        if !safe {
            debug!("not trashing '{p}': it changed locally");
            self.local_trees.insert(p.to_string());
            return Ok(());
        }
        localfs::trash(&self.root, p)?;
        self.env.status.activity(self.name(), format!("Moved to Trash: {p}"));
        self.forget_tree(p)
    }

    fn forget_tree(&mut self, p: &str) -> Result<()> {
        let mut ops = vec![Op::DelEntry(p.to_string())];
        self.base.remove(p);
        let kids: Vec<String> = range_under(&self.base, p).map(|(k, _)| k.clone()).collect();
        for k in kids {
            self.base.remove(&k);
            ops.push(Op::DelEntry(k));
        }
        self.env.db.apply(self.name(), &ops)
    }

    /// Returns true if follow-up work was queued.
    async fn conflict(&mut self, p: &str, l: Option<LocalInfo>) -> Result<bool> {
        let Some(l) = l else { return Ok(false) };
        let Some((f, rk)) = self.tree.by_path(p).map(|(f, k)| (f.clone(), k)) else { return Ok(false) };
        let mut policy = self.env.cfg.conflict_policy;
        if l.kind == Kind::Dir || rk == Kind::Dir || l.kind != rk || rk == Kind::Export {
            policy = ConflictPolicy::KeepBoth;
        }
        if policy == ConflictPolicy::PreferNewest {
            policy = if l.mtime_ns >= f.mtime_ns() { ConflictPolicy::PreferLocal } else { ConflictPolicy::PreferRemote };
        }
        let name = self.name().to_string();
        match policy {
            ConflictPolicy::PreferLocal => {
                let parent = self.tree.folder_id(parent_of(p)).ok_or_else(|| anyhow!("no parent"))?;
                let out = run_job(
                    self.env.clone(),
                    self.root.clone(),
                    self.host.clone(),
                    Job::Upload { path: p.to_string(), local: l, parent, existing: Some((f, rk)), md5: None },
                )
                .await;
                self.absorb(out)?;
                self.env.status.activity(&name, format!("Conflict on {p}: kept local version"));
                Ok(false)
            }
            ConflictPolicy::PreferRemote => {
                localfs::trash(&self.root, p)?;
                let out = run_job(
                    self.env.clone(),
                    self.root.clone(),
                    self.host.clone(),
                    Job::Download { path: p.to_string(), file: f, kind: rk, expect_local: None },
                )
                .await;
                self.absorb(out)?;
                self.env.status.activity(&name, format!("Conflict on {p}: kept cloud version (local copy moved to Trash)"));
                Ok(false)
            }
            _ => {
                // Keep both: rename the local copy, then bring down the cloud version.
                let cname = util::conflict_name(file_name(p), &self.host);
                let dir = parent_of(p);
                let newp = if dir.is_empty() { cname.clone() } else { format!("{dir}/{cname}") };
                std::fs::rename(self.root.join(p), self.root.join(&newp))?;
                // Forget the old base; the renamed copy is a new local item.
                self.forget_tree(p)?;
                self.local_dirty.insert(newp.clone());
                if l.kind == Kind::Dir {
                    self.local_trees.insert(newp.clone());
                }
                if rk == Kind::Dir {
                    self.mkdir_local(p)?;
                    self.remote_trees.insert(p.to_string());
                } else {
                    let out = run_job(
                        self.env.clone(),
                        self.root.clone(),
                        self.host.clone(),
                        Job::Download { path: p.to_string(), file: f, kind: rk, expect_local: None },
                    )
                    .await;
                    self.absorb(out)?;
                }
                let msg = format!("Conflict on '{p}': both versions kept; your local copy is '{cname}'");
                self.env.status.activity(&name, msg.clone());
                self.env.notifier.send("DriftFolderSync: sync conflict", &msg);
                Ok(true)
            }
        }
    }

    fn absorb(&mut self, out: JobOut) -> Result<()> {
        let o = out.result?;
        if let Some(f) = o.remote {
            self.tree.upsert(f);
            self.reindex();
        }
        if let Some(e) = o.entry {
            self.env.db.apply(self.name(), &[Op::PutEntry(out.path.clone(), e.clone())])?;
            self.base.insert(out.path, e);
        }
        Ok(())
    }
}

fn is_fatal(e: &anyhow::Error) -> bool {
    e.downcast_ref::<AuthError>().is_some()
}

enum Job {
    Upload {
        path: String,
        local: LocalInfo,
        parent: String,
        existing: Option<(DriveFile, Kind)>,
        md5: Option<String>,
    },
    Download {
        path: String,
        file: DriveFile,
        kind: Kind,
        expect_local: Option<LocalInfo>,
    },
}

struct JobOut {
    path: String,
    result: Result<Done>,
}

#[derive(Default)]
struct Done {
    remote: Option<DriveFile>,
    entry: Option<Entry>,
    redo: bool,
    msg: Option<String>,
    up: u64,
    down: u64,
}

async fn run_job(env: Arc<Env>, root: PathBuf, host: String, job: Job) -> JobOut {
    let path = match &job {
        Job::Upload { path, .. } | Job::Download { path, .. } => path.clone(),
    };
    let _ = host;
    let result = match job {
        Job::Upload { path, local, parent, existing, md5 } => upload(&env, &root, &path, local, &parent, existing, md5).await,
        Job::Download { path, file, kind, expect_local } => download(&env, &root, &path, file, kind, expect_local).await,
    };
    JobOut { path, result }
}

async fn upload(
    env: &Env,
    root: &Path,
    p: &str,
    l: LocalInfo,
    parent: &str,
    existing: Option<(DriveFile, Kind)>,
    md5: Option<String>,
) -> Result<Done> {
    let full = root.join(p);
    // Re-check that nothing changed since we decided.
    let now = localfs::stat(root, p, env.cfg.symlinks);
    if now.as_ref().map(|n| (n.size, n.mtime_ns, n.kind)) != Some((l.size, l.mtime_ns, l.kind)) {
        return Ok(Done { redo: true, ..Default::default() });
    }
    let mut remote_name = util::local_to_remote_name(file_name(p));
    let mut existing_id = None;
    if let Some((f, k)) = &existing {
        if *k == l.kind {
            existing_id = Some(f.id.clone());
        } else {
            // Type changed (e.g. file replaced by symlink): bin the old one.
            env.drive.trash(&f.id).await?;
        }
    }

    let (src, cleanup, md5) = match l.kind {
        Kind::Symlink => {
            remote_name.push_str(RCLONELINK);
            let target = l.link_target.clone().unwrap_or_default();
            let tmp = crate::paths::runtime_dir().join(format!("link-{}", util::random_token(9)));
            std::fs::create_dir_all(tmp.parent().unwrap())?;
            std::fs::write(&tmp, target.as_bytes())?;
            (tmp, true, util::md5_bytes(target.as_bytes()))
        }
        _ => {
            let md5 = match md5 {
                Some(m) => m,
                None => {
                    let f2 = full.clone();
                    tokio::task::spawn_blocking(move || util::md5_file(&f2)).await??
                }
            };
            (full.clone(), false, md5)
        }
    };
    if existing_id.is_some() && existing.as_ref().and_then(|(f, _)| f.md5_checksum.as_deref()) == Some(md5.as_str()) {
        // Same content already in the cloud (e.g. touched only).
        let (f, k) = existing.unwrap();
        if cleanup {
            let _ = std::fs::remove_file(&src);
        }
        return Ok(Done {
            entry: Some(Entry { kind: k, remote_id: f.id.clone(), size: l.size, mtime_ns: l.mtime_ns, inode: l.inode, md5: Some(md5), remote_fp: tree::remote_fp(&f, k) }),
            remote: Some(f),
            ..Default::default()
        });
    }
    let res = env
        .drive
        .upload(&src, &remote_name, Some(parent), existing_id.as_deref(), l.mtime_ns, &|_| {})
        .await;
    if cleanup {
        let _ = std::fs::remove_file(&src);
    }
    let f = res?;
    let after = localfs::stat(root, p, env.cfg.symlinks);
    let corrupt = f.md5_checksum.is_some() && f.md5_checksum.as_deref() != Some(md5.as_str());
    let redo = corrupt || after.as_ref().map(|n| (n.size, n.mtime_ns)) != Some((l.size, l.mtime_ns));
    let kind = l.kind;
    let entry = Entry {
        kind,
        remote_id: f.id.clone(),
        size: l.size,
        // If the cloud copy does not match what we sent (the file changed
        // mid-upload, or the transfer was damaged), record a state that is
        // guaranteed to look "locally changed" so it is uploaded again.
        mtime_ns: if corrupt { i64::MIN } else { l.mtime_ns },
        inode: l.inode,
        md5: if corrupt { None } else { Some(md5) },
        remote_fp: tree::remote_fp(&f, kind),
    };
    if corrupt {
        warn!("cloud checksum of '{p}' does not match the local file; will upload again");
    }
    let verb = if existing_id.is_some() { "Updated in cloud" } else { "Uploaded" };
    Ok(Done {
        msg: Some(format!("{verb}: {p} ({})", util::human_bytes(l.size))),
        up: 1,
        remote: Some(f),
        entry: Some(entry),
        redo,
        ..Default::default()
    })
}

async fn download(
    env: &Env,
    root: &Path,
    p: &str,
    f: DriveFile,
    kind: Kind,
    expect_local: Option<LocalInfo>,
) -> Result<Done> {
    let full = root.join(p);
    let dir = full.parent().ok_or_else(|| anyhow!("bad path"))?.to_path_buf();
    std::fs::create_dir_all(&dir)?;
    if kind == Kind::File {
        if let Some(free) = util::free_space(&dir) {
            let need = f.size_u64() + 256 * 1024 * 1024;
            if free < need {
                bail!("not enough disk space ({} free, {} needed)", util::human_bytes(free), util::human_bytes(need));
            }
        }
    }
    let tmp = localfs::tmp_path(&dir);
    let result: Result<()> = async {
        match kind {
            Kind::File => env.drive.download(&f, &tmp, env.cfg.acknowledge_abuse, &|_| {}).await,
            Kind::Export => {
                let t = crate::drive::native_type(&f.mime_type).and_then(|t| t.export_mime).ok_or_else(|| anyhow!("not exportable"))?;
                env.drive.export(&f.id, t, &tmp, &|_| {}).await
            }
            Kind::Link => {
                let url = f.web_view_link.clone().unwrap_or_else(|| format!("https://drive.google.com/open?id={}", f.id));
                std::fs::write(&tmp, localfs::link_file_contents(&url, &f.id, &env.email))?;
                Ok(())
            }
            Kind::Symlink => {
                env.drive.download(&f, &tmp, env.cfg.acknowledge_abuse, &|_| {}).await?;
                let target = std::fs::read_to_string(&tmp)?;
                std::fs::remove_file(&tmp)?;
                std::os::unix::fs::symlink(target.trim_end_matches('\n'), &tmp)?;
                Ok(())
            }
            Kind::Dir => bail!("cannot download a folder"),
        }
    }
    .await;
    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if kind != Kind::Symlink {
        localfs::set_mtime(&tmp, f.mtime_ns())?;
    }
    // Make sure the local file didn't change while we were downloading.
    let now = localfs::stat(root, p, env.cfg.symlinks);
    let unchanged = match (&now, &expect_local) {
        (None, _) => true,
        (Some(n), Some(e)) => n.size == e.size && n.mtime_ns == e.mtime_ns && n.kind == e.kind,
        (Some(_), None) => false,
    };
    if !unchanged {
        let _ = std::fs::remove_file(&tmp);
        return Ok(Done { redo: true, ..Default::default() });
    }
    if now.as_ref().is_some_and(|n| n.kind == Kind::Dir) {
        let _ = std::fs::remove_file(&tmp);
        bail!("a local folder is in the way");
    }
    std::fs::rename(&tmp, &full)?;
    let l = localfs::stat(root, p, env.cfg.symlinks).ok_or_else(|| anyhow!("file vanished after download"))?;
    let md5 = match kind {
        Kind::File => f.md5_checksum.clone(),
        Kind::Symlink => l.link_target.as_deref().map(|t| util::md5_bytes(t.as_bytes())),
        _ => None,
    };
    let entry = Entry {
        kind,
        remote_id: f.id.clone(),
        size: l.size,
        mtime_ns: l.mtime_ns,
        inode: l.inode,
        md5,
        remote_fp: tree::remote_fp(&f, kind),
    };
    let msg = match kind {
        Kind::Link => None,
        _ => Some(format!("Downloaded: {p} ({})", util::human_bytes(l.size))),
    };
    Ok(Done { remote: Some(f), entry: Some(entry), msg, down: 1, ..Default::default() })
}
