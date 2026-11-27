//! Low-overhead local change detection with inotify.
//!
//! * One watch per *directory* (never per file), so ~21k watches cover a
//!   tree of 120k files.
//! * Only "end of write" events are requested (`CLOSE_WRITE`, not `MODIFY`),
//!   so a file being written continuously produces one event per save, not
//!   one per `write()`.
//! * Events are coalesced into a set of dirty paths by the engine; nothing
//!   is hashed or uploaded until the batch interval / settle time expires.
//! * Kernel queue overflow or hitting the watch limit degrade gracefully
//!   to a (stat-only) rescan.

use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, warn};

use crate::localfs::Filter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatchEvent {
    /// A single path (file or directory entry) may have changed.
    Path(String),
    /// A directory subtree may have changed (created, moved in/out, deleted).
    Tree(String),
    /// Events were lost; a full rescan is required.
    Overflow,
    /// The inotify watch limit was reached; parts of the tree are unwatched.
    WatchLimit,
}

pub struct Watcher {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

const MASK: WatchMask = WatchMask::CREATE
    .union(WatchMask::DELETE)
    .union(WatchMask::CLOSE_WRITE)
    .union(WatchMask::MOVED_FROM)
    .union(WatchMask::MOVED_TO)
    .union(WatchMask::DELETE_SELF)
    .union(WatchMask::ONLYDIR)
    .union(WatchMask::DONT_FOLLOW)
    .union(WatchMask::EXCL_UNLINK);

struct State {
    ino: Inotify,
    root: PathBuf,
    filter: Filter,
    wds: HashMap<WatchDescriptor, String>,
    by_path: HashMap<String, WatchDescriptor>,
    limit_hit: bool,
    tx: UnboundedSender<WatchEvent>,
}

impl State {
    /// Watch `rel` and every directory beneath it.
    fn add_tree(&mut self, rel: &str) {
        let mut stack = vec![rel.to_string()];
        while let Some(dir) = stack.pop() {
            let full = if dir.is_empty() { self.root.clone() } else { self.root.join(&dir) };
            match self.ino.watches().add(&full, MASK) {
                Ok(wd) => {
                    self.wds.insert(wd.clone(), dir.clone());
                    self.by_path.insert(dir.clone(), wd);
                }
                Err(e) if e.raw_os_error() == Some(libc::ENOSPC) => {
                    if !self.limit_hit {
                        self.limit_hit = true;
                        warn!(
                            "inotify watch limit reached at {} watches; raise fs.inotify.max_user_watches. \
                             Unwatched folders are still covered by periodic rescans.",
                            self.wds.len()
                        );
                        let _ = self.tx.send(WatchEvent::WatchLimit);
                    }
                    return;
                }
                Err(e) => {
                    debug!("cannot watch {}: {e}", full.display());
                    continue;
                }
            }
            let Ok(rd) = std::fs::read_dir(&full) else { continue };
            for ent in rd.flatten() {
                if !ent.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                let Ok(name) = ent.file_name().into_string() else { continue };
                let child = if dir.is_empty() { name } else { format!("{dir}/{name}") };
                if !self.filter.excluded(&child, true) {
                    stack.push(child);
                }
            }
        }
    }

    /// Stop watching `rel` and everything under it.
    fn remove_tree(&mut self, rel: &str) {
        let prefix = format!("{rel}/");
        let victims: Vec<String> = self
            .by_path
            .keys()
            .filter(|p| p.as_str() == rel || p.starts_with(&prefix))
            .cloned()
            .collect();
        for p in victims {
            if let Some(wd) = self.by_path.remove(&p) {
                self.wds.remove(&wd);
                let _ = self.ino.watches().remove(wd);
            }
        }
    }
}

pub fn spawn(root: &Path, filter: Filter, tx: UnboundedSender<WatchEvent>) -> Result<Watcher> {
    let ino = Inotify::init()?;
    let mut st = State {
        ino,
        root: root.to_path_buf(),
        filter,
        wds: HashMap::new(),
        by_path: HashMap::new(),
        limit_hit: false,
        tx,
    };
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = stop.clone();
    let thread = std::thread::Builder::new()
        .name("inotify".into())
        .spawn(move || {
            st.add_tree("");
            debug!("watching {} directories under {}", st.wds.len(), st.root.display());
            run(st, stop2)
        })?;
    Ok(Watcher { stop, thread: Some(thread) })
}

fn run(mut st: State, stop: Arc<AtomicBool>) {
    let mut buf = vec![0u8; 64 * 1024];
    let fd = st.ino.as_raw_fd();
    while !stop.load(Ordering::SeqCst) {
        let mut pfd = libc::pollfd { fd, events: libc::POLLIN, revents: 0 };
        let r = unsafe { libc::poll(&mut pfd, 1, 1000) };
        if r <= 0 {
            continue;
        }
        let events: Vec<(WatchDescriptor, EventMask, Option<String>)> = match st.ino.read_events(&mut buf) {
            Ok(evs) => evs
                .map(|e| (e.wd, e.mask, e.name.map(|n| n.to_string_lossy().into_owned())))
                .collect(),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => {
                error!("inotify read failed: {e}");
                return;
            }
        };
        for (wd, mask, name) in events {
            if mask.contains(EventMask::Q_OVERFLOW) {
                warn!("inotify queue overflow; scheduling rescan");
                let _ = st.tx.send(WatchEvent::Overflow);
                continue;
            }
            if mask.contains(EventMask::IGNORED) {
                if let Some(p) = st.wds.remove(&wd) {
                    st.by_path.remove(&p);
                }
                continue;
            }
            let Some(dir) = st.wds.get(&wd).cloned() else { continue };
            let Some(name) = name else {
                // Event about the watched directory itself (DELETE_SELF).
                if mask.contains(EventMask::DELETE_SELF) && dir.is_empty() {
                    error!("sync root {} was deleted or moved", st.root.display());
                    let _ = st.tx.send(WatchEvent::Overflow);
                }
                continue;
            };
            let rel = if dir.is_empty() { name } else { format!("{dir}/{name}") };
            let is_dir = mask.contains(EventMask::ISDIR);
            if st.filter.excluded(&rel, is_dir) {
                continue;
            }
            // Report first, then (re)build watches, so that the two halves of
            // a rename reach the engine as close together as possible.
            let ev = if is_dir { WatchEvent::Tree(rel.clone()) } else { WatchEvent::Path(rel.clone()) };
            if st.tx.send(ev).is_err() {
                return;
            }
            if is_dir {
                if mask.intersects(EventMask::CREATE | EventMask::MOVED_TO) {
                    st.remove_tree(&rel);
                    st.add_tree(&rel);
                } else if mask.intersects(EventMask::MOVED_FROM | EventMask::DELETE) {
                    st.remove_tree(&rel);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn sees_changes() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir(d.path().join("sub")).unwrap();
        let pair: crate::config::SyncPair =
            toml::from_str("name='t'\naccount='a'\nlocal='/tmp/x'").unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let _w = spawn(d.path(), Filter::new(&pair).unwrap(), tx).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(d.path().join("sub/a.txt"), "x").unwrap();
        std::fs::write(d.path().join("sub/b.tmp"), "x").unwrap();
        std::fs::create_dir(d.path().join("new")).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        std::fs::write(d.path().join("new/c.txt"), "x").unwrap();
        let mut seen = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while let Ok(Some(ev)) = tokio::time::timeout_at(deadline, rx.recv()).await {
            seen.push(ev);
            if seen.contains(&WatchEvent::Path("new/c.txt".into())) {
                break;
            }
        }
        assert!(seen.contains(&WatchEvent::Path("sub/a.txt".into())), "{seen:?}");
        assert!(seen.contains(&WatchEvent::Tree("new".into())), "{seen:?}");
        assert!(seen.contains(&WatchEvent::Path("new/c.txt".into())), "{seen:?}");
        assert!(!seen.iter().any(|e| matches!(e, WatchEvent::Path(p) if p.ends_with(".tmp"))));
    }
}
