//! Local file system access: filtering, scanning, stat, atomic writes, trash.

use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use tracing::warn;

use crate::config::{SymlinkMode, SyncPair};
use crate::db::Kind;
use crate::paths;

pub const TMP_PREFIX: &str = ".driftfoldersync-tmp-";
pub const LOCAL_TRASH: &str = ".driftfoldersync-trash";

#[derive(Debug, Clone, PartialEq)]
pub struct LocalInfo {
    pub kind: Kind,
    pub size: u64,
    pub mtime_ns: i64,
    pub inode: u64,
    pub link_target: Option<String>,
}

/// Decides which relative paths take part in syncing.
#[derive(Debug, Clone)]
pub struct Filter {
    name_globs: GlobSet,
    path_globs: GlobSet,
    include: Vec<String>,
    exclude_dirs: Vec<String>,
    max_size: u64,
}

fn norm(p: &str) -> String {
    p.trim_matches('/').to_string()
}

fn under(path: &str, dir: &str) -> bool {
    path == dir || (path.starts_with(dir) && path.as_bytes().get(dir.len()) == Some(&b'/'))
}

impl Filter {
    pub fn new(pair: &SyncPair) -> Result<Self> {
        let mut names = GlobSetBuilder::new();
        let mut paths = GlobSetBuilder::new();
        let mut patterns = pair.exclude.clone();
        // Always exclude our own temporary files and local trash.
        patterns.push(format!("{TMP_PREFIX}*"));
        patterns.push(LOCAL_TRASH.to_string());
        for p in &patterns {
            let p = p.trim();
            if p.is_empty() {
                continue;
            }
            if p.contains('/') {
                let g = p.trim_start_matches('/');
                paths.add(Glob::new(g).with_context(|| format!("bad exclude pattern '{p}'"))?);
                paths.add(Glob::new(&format!("{g}/**"))?);
            } else {
                names.add(Glob::new(p).with_context(|| format!("bad exclude pattern '{p}'"))?);
            }
        }
        Ok(Self {
            name_globs: names.build()?,
            path_globs: paths.build()?,
            include: pair.include_folders.iter().map(|s| norm(s)).filter(|s| !s.is_empty()).collect(),
            exclude_dirs: pair.exclude_folders.iter().map(|s| norm(s)).filter(|s| !s.is_empty()).collect(),
            max_size: pair.max_file_size_mib * 1024 * 1024,
        })
    }

    /// Is `rel` (a relative, '/'-separated path) excluded?
    pub fn excluded(&self, rel: &str, is_dir: bool) -> bool {
        if rel.is_empty() {
            return false;
        }
        let name = rel.rsplit('/').next().unwrap_or(rel);
        if self.name_globs.is_match(name) || self.path_globs.is_match(rel) {
            return true;
        }
        if self.exclude_dirs.iter().any(|d| under(rel, d)) {
            return true;
        }
        if !self.include.is_empty() {
            let inside = self.include.iter().any(|d| under(rel, d));
            let ancestor = is_dir && self.include.iter().any(|d| under(d, rel));
            if !inside && !ancestor {
                return true;
            }
        }
        false
    }

    pub fn too_big(&self, size: u64) -> bool {
        self.max_size > 0 && size > self.max_size
    }
}

/// Lenient stat: any error counts as "absent". Only for places where a
/// wrong answer cannot cause a deletion.
pub fn stat(root: &Path, rel: &str, symlinks: SymlinkMode) -> Option<LocalInfo> {
    stat_checked(root, rel, symlinks).ok().flatten()
}

fn is_absent(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::NotFound || e.raw_os_error() == Some(libc::ENOTDIR)
}

/// Strict stat: `Ok(None)` only if the path really does not exist (or is a
/// type we never sync); any other failure (EACCES, EIO, stale mount, …) is
/// an error, so that it can never be mistaken for a deletion.
pub fn stat_checked(root: &Path, rel: &str, symlinks: SymlinkMode) -> std::io::Result<Option<LocalInfo>> {
    let full = if rel.is_empty() { root.to_path_buf() } else { root.join(rel) };
    let md = match std::fs::symlink_metadata(&full) {
        Ok(m) => m,
        Err(e) if is_absent(&e) => return Ok(None),
        Err(e) => return Err(e),
    };
    let ft = md.file_type();
    let mtime_ns = md.mtime() * 1_000_000_000 + md.mtime_nsec();
    if ft.is_symlink() {
        if symlinks != SymlinkMode::Rclonelink {
            return Ok(None);
        }
        let target = std::fs::read_link(&full)?;
        let t = target.to_string_lossy().into_owned();
        return Ok(Some(LocalInfo {
            kind: Kind::Symlink,
            size: t.len() as u64,
            mtime_ns,
            inode: md.ino(),
            link_target: Some(t),
        }));
    }
    let kind = if ft.is_dir() {
        Kind::Dir
    } else if ft.is_file() {
        Kind::File
    } else {
        return Ok(None); // sockets, fifos, devices
    };
    Ok(Some(LocalInfo {
        kind,
        size: if kind == Kind::Dir { 0 } else { md.len() },
        mtime_ns,
        inode: md.ino(),
        link_target: None,
    }))
}

/// Recursively scan `root/start`. Returns relative path → info. Paths that
/// could not be read are added to `unknown` (their subtrees must be left
/// alone, never treated as deleted).
pub fn scan(
    root: &Path,
    start: &str,
    filter: &Filter,
    symlinks: SymlinkMode,
    out: &mut HashMap<String, LocalInfo>,
    unknown: &mut Vec<String>,
) {
    let mut stack = vec![start.to_string()];
    while let Some(dir) = stack.pop() {
        let full = if dir.is_empty() { root.to_path_buf() } else { root.join(&dir) };
        let rd = match std::fs::read_dir(&full) {
            Ok(r) => r,
            Err(e) if is_absent(&e) => continue,
            Err(e) => {
                warn!("cannot read {}: {e}; leaving it untouched", full.display());
                unknown.push(dir.clone());
                continue;
            }
        };
        for ent in rd {
            let ent = match ent {
                Ok(e) => e,
                Err(e) => {
                    warn!("error listing {}: {e}; leaving it untouched", full.display());
                    unknown.push(dir.clone());
                    break;
                }
            };
            let name = match ent.file_name().into_string() {
                Ok(n) => n,
                Err(n) => {
                    warn!("skipping non-UTF-8 file name {:?} in {}", n, full.display());
                    continue;
                }
            };
            let rel = if dir.is_empty() { name } else { format!("{dir}/{name}") };
            let is_dir = ent.file_type().map(|t| t.is_dir()).unwrap_or(false);
            if filter.excluded(&rel, is_dir) {
                continue;
            }
            match stat_checked(root, &rel, symlinks) {
                Ok(Some(info)) => {
                    if info.kind == Kind::Dir {
                        stack.push(rel.clone());
                    }
                    out.insert(rel, info);
                }
                Ok(None) => {}
                Err(e) => {
                    warn!("cannot stat {}: {e}; leaving it untouched", root.join(&rel).display());
                    unknown.push(rel);
                }
            }
        }
    }
}

/// Every entry below `root/start` *without* filtering (excluded and
/// settling files included). Used to prove a folder is safe to delete as a
/// whole; any read error is returned rather than ignored.
pub fn walk_all(root: &Path, start: &str) -> std::io::Result<Vec<(String, LocalInfo)>> {
    let mut out = Vec::new();
    let mut stack = vec![start.to_string()];
    while let Some(dir) = stack.pop() {
        for ent in std::fs::read_dir(root.join(&dir))? {
            let ent = ent?;
            let name = ent.file_name().to_string_lossy().into_owned();
            let rel = format!("{dir}/{name}");
            let md = std::fs::symlink_metadata(ent.path())?;
            let info = LocalInfo {
                kind: if md.is_dir() {
                    Kind::Dir
                } else if md.file_type().is_symlink() {
                    Kind::Symlink
                } else {
                    Kind::File
                },
                size: if md.is_dir() { 0 } else { md.len() },
                mtime_ns: md.mtime() * 1_000_000_000 + md.mtime_nsec(),
                inode: md.ino(),
                link_target: None,
            };
            if info.kind == Kind::Dir {
                stack.push(rel.clone());
            }
            out.push((rel, info));
        }
    }
    Ok(out)
}

pub fn tmp_path(dir: &Path) -> PathBuf {
    dir.join(format!("{TMP_PREFIX}{}", crate::util::random_token(9)))
}

pub fn set_mtime(path: &Path, ns: i64) -> Result<()> {
    let f = std::fs::File::options().write(true).open(path)?;
    f.set_modified(crate::util::ns_to_system_time(ns))?;
    Ok(())
}

/// The user's real home Trash directory (also correct inside Flatpak,
/// where XDG_DATA_HOME points into the sandbox).
fn home_trash() -> PathBuf {
    let base = if paths::in_flatpak() {
        std::env::var_os("HOST_XDG_DATA_HOME").map(PathBuf::from)
    } else {
        std::env::var_os("XDG_DATA_HOME").map(PathBuf::from)
    }
    .filter(|p| p.is_absolute())
    .unwrap_or_else(|| paths::home().join(".local/share"));
    base.join("Trash")
}

fn trash_url_encode(p: &Path) -> String {
    use std::os::unix::ffi::OsStrExt;
    let mut out = String::new();
    for &b in p.as_os_str().as_bytes() {
        if b.is_ascii_alphanumeric() || b"/-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Move into the freedesktop.org home Trash (same file system only).
fn trash_freedesktop(full: &Path) -> std::io::Result<()> {
    let trash = home_trash();
    let files = trash.join("files");
    let info = trash.join("info");
    std::fs::create_dir_all(&files)?;
    std::fs::create_dir_all(&info)?;
    let name = full.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| "item".into());
    let (stem, ext) = crate::util::split_ext(&name);
    let date = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S");
    for i in 1..1000 {
        let candidate = if i == 1 { name.clone() } else { format!("{stem}.{i}{ext}") };
        let info_path = info.join(format!("{candidate}.trashinfo"));
        // Reserve the name atomically.
        let f = std::fs::OpenOptions::new().write(true).create_new(true).open(&info_path);
        let mut f = match f {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        };
        use std::io::Write;
        write!(f, "[Trash Info]\nPath={}\nDeletionDate={date}\n", trash_url_encode(full))?;
        drop(f);
        return match std::fs::rename(full, files.join(&candidate)) {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = std::fs::remove_file(&info_path);
                Err(e)
            }
        };
    }
    Err(std::io::Error::other("could not find a free name in Trash"))
}

/// Move a local item to the desktop Trash so deletions are recoverable.
/// Falls back to a hidden per-folder trash when that is impossible (e.g. the
/// sync folder is on a different file system from the home Trash).
pub fn trash(root: &Path, rel: &str) -> Result<()> {
    let full = root.join(rel);
    if std::fs::symlink_metadata(&full).is_err() {
        return Ok(());
    }
    let res = trash_freedesktop(&full).or_else(|e| {
        if paths::in_flatpak() {
            Err(anyhow::anyhow!(e))
        } else {
            trash::delete(&full).map_err(|e| anyhow::anyhow!("{e}"))
        }
    });
    if let Err(e) = res {
        warn!("desktop Trash unavailable for {} ({e}); using {LOCAL_TRASH}", full.display());
        let dir = root.join(LOCAL_TRASH);
        std::fs::create_dir_all(&dir)?;
        let name = rel.replace('/', "_");
        let stamp = chrono::Local::now().format("%Y%m%d-%H%M%S");
        std::fs::rename(&full, dir.join(format!("{stamp} {name}")))?;
    }
    Ok(())
}

/// Contents of a `.gdoc`-style link file.
pub fn link_file_contents(url: &str, id: &str, email: &str) -> String {
    serde_json::json!({
        "": "This is a link to a cloud document. Open it with 'driftfoldersync open <file>' or double-click. Do not edit.",
        "url": url,
        "doc_id": id,
        "email": email,
    })
    .to_string()
}

pub fn read_link_file_url(path: &Path) -> Result<String> {
    let text = std::fs::read_to_string(path)?;
    let v: serde_json::Value = serde_json::from_str(&text).context("not a link file")?;
    v.get("url")
        .and_then(|u| u.as_str())
        .map(String::from)
        .context("link file has no url")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(exclude: &[&str], inc: &[&str], exc: &[&str]) -> SyncPair {
        let mut p: SyncPair = toml::from_str(
            r#"name = "t"
               account = "a"
               local = "/tmp/x""#,
        )
        .unwrap();
        p.exclude = exclude.iter().map(|s| s.to_string()).collect();
        p.include_folders = inc.iter().map(|s| s.to_string()).collect();
        p.exclude_folders = exc.iter().map(|s| s.to_string()).collect();
        p
    }

    #[test]
    fn globs() {
        let f = Filter::new(&pair(&["*.tmp", "build/out"], &[], &[])).unwrap();
        assert!(f.excluded("a/b.tmp", false));
        assert!(f.excluded("build/out", true));
        assert!(f.excluded("build/out/x.o", false));
        assert!(!f.excluded("build/other", false));
        assert!(f.excluded(".driftfoldersync-tmp-abc", false));
    }

    #[test]
    fn selective() {
        let f = Filter::new(&pair(&[], &["Projects/Live"], &["Projects/Live/Old"])).unwrap();
        assert!(!f.excluded("Projects", true));
        assert!(f.excluded("Projects/readme.txt", false));
        assert!(!f.excluded("Projects/Live/a.txt", false));
        assert!(f.excluded("Projects/Live/Old/b.txt", false));
        assert!(f.excluded("Other", true));
        assert!(f.excluded("Projects/LiveX", true));
    }

    #[test]
    fn trash_to_freedesktop() {
        let _ = crate::testsupport::fake(); // sets XDG_DATA_HOME to a temp dir
        let d = tempfile::tempdir_in(std::env::var("XDG_DATA_HOME").map(|x| PathBuf::from(x).parent().unwrap().to_path_buf()).unwrap()).unwrap();
        std::fs::write(d.path().join("gone.txt"), "x").unwrap();
        std::fs::write(d.path().join("gone 2.txt"), "y").unwrap();
        trash(d.path(), "gone.txt").unwrap();
        trash(d.path(), "gone 2.txt").unwrap();
        assert!(!d.path().join("gone.txt").exists());
        let t = home_trash();
        assert!(t.join("files").read_dir().unwrap().count() >= 2);
        let info = std::fs::read_dir(t.join("info")).unwrap().filter_map(|e| std::fs::read_to_string(e.ok()?.path()).ok()).collect::<Vec<_>>().join("");
        assert!(info.contains("gone%202.txt"), "{info}");
    }

    #[test]
    fn scan_and_stat() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("a/b")).unwrap();
        std::fs::write(d.path().join("a/b/f.txt"), "hi").unwrap();
        std::fs::write(d.path().join("a/x.tmp"), "hi").unwrap();
        std::os::unix::fs::symlink("b/f.txt", d.path().join("a/l")).unwrap();
        let f = Filter::new(&pair(&["*.tmp"], &[], &[])).unwrap();
        let mut m = HashMap::new();
        scan(d.path(), "", &f, SymlinkMode::Rclonelink, &mut m, &mut vec![]);
        assert_eq!(m["a/b/f.txt"].size, 2);
        assert_eq!(m["a"].kind, Kind::Dir);
        assert_eq!(m["a/l"].kind, Kind::Symlink);
        assert_eq!(m["a/l"].link_target.as_deref(), Some("b/f.txt"));
        assert!(!m.contains_key("a/x.tmp"));
        let mut m2 = HashMap::new();
        scan(d.path(), "", &f, SymlinkMode::Skip, &mut m2, &mut vec![]);
        assert!(!m2.contains_key("a/l"));
    }
}
