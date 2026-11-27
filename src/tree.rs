//! In-memory model of the remote folder tree for one sync pair, with a
//! mapping from local relative paths to Drive file IDs.

use std::collections::{BTreeMap, HashMap, HashSet};

use tracing::warn;

use crate::config::{GoogleDocsMode, SymlinkMode};
use crate::db::{Kind, Op};
use crate::drive::{native_type, DriveFile, SHORTCUT_MIME};
use crate::util::remote_to_local_name;

pub const RCLONELINK: &str = ".rclonelink";

#[derive(Debug, Clone, Copy)]
pub struct Mapping {
    pub docs: GoogleDocsMode,
    pub symlinks: SymlinkMode,
}

/// Local name and kind for a remote item; `None` if it is not represented.
pub fn local_name(f: &DriveFile, m: Mapping) -> Option<(String, Kind)> {
    let name = remote_to_local_name(&f.name);
    if name.is_empty() || name == "." || name == ".." {
        return None;
    }
    if f.is_folder() {
        return Some((name, Kind::Dir));
    }
    if f.mime_type == SHORTCUT_MIME {
        return None;
    }
    if f.is_native_doc() {
        let t = native_type(&f.mime_type);
        return match m.docs {
            GoogleDocsMode::Ignore => None,
            GoogleDocsMode::Export => match t {
                Some(t) if t.export_mime.is_some() => Some((add_ext(&name, t.export_ext), Kind::Export)),
                Some(t) => Some((add_ext(&name, t.link_ext), Kind::Link)),
                None => None,
            },
            GoogleDocsMode::Link => t.map(|t| (add_ext(&name, t.link_ext), Kind::Link)),
        };
    }
    if m.symlinks == SymlinkMode::Rclonelink {
        if let Some(stem) = name.strip_suffix(RCLONELINK) {
            if !stem.is_empty() {
                return Some((stem.to_string(), Kind::Symlink));
            }
        }
    }
    Some((name, Kind::File))
}

fn add_ext(name: &str, ext: &str) -> String {
    if name.to_ascii_lowercase().ends_with(ext) {
        name.to_string()
    } else {
        format!("{name}{ext}")
    }
}

/// Fingerprint used to detect remote changes for a given local kind.
pub fn remote_fp(f: &DriveFile, kind: Kind) -> String {
    match kind {
        Kind::Dir | Kind::Link => String::new(),
        Kind::Export => f.modified_time.clone().unwrap_or_default(),
        Kind::File | Kind::Symlink => match &f.md5_checksum {
            Some(m) => m.clone(),
            None => format!("sz:{}:mt:{}", f.size_u64(), f.mtime_ns()),
        },
    }
}

pub struct RemoteTree {
    pub root: String,
    mapping: Mapping,
    items: HashMap<String, DriveFile>,
    children: HashMap<String, HashSet<String>>,
    /// local path → (id, kind)
    index: BTreeMap<String, (String, Kind)>,
    id_path: HashMap<String, String>,
    /// Local paths where a rclone-style Office export of a native document
    /// might exist; new local files there are not uploaded.
    shadows: HashSet<String>,
    pub ops: Vec<Op>,
    stale: bool,
}

impl RemoteTree {
    pub fn new(root: String, mapping: Mapping) -> Self {
        Self {
            root,
            mapping,
            items: HashMap::new(),
            children: HashMap::new(),
            index: BTreeMap::new(),
            id_path: HashMap::new(),
            shadows: HashSet::new(),
            ops: Vec::new(),
            stale: true,
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn contains(&self, id: &str) -> bool {
        id == self.root || self.items.contains_key(id)
    }

    pub fn get(&self, id: &str) -> Option<&DriveFile> {
        self.items.get(id)
    }

    /// Load without generating persistence ops.
    pub fn load(&mut self, files: Vec<DriveFile>) {
        for f in files {
            self.insert(f);
        }
        self.stale = true;
    }

    fn insert(&mut self, f: DriveFile) {
        if let Some(old) = self.items.get(&f.id) {
            for p in &old.parents {
                if let Some(c) = self.children.get_mut(p) {
                    c.remove(&f.id);
                }
            }
        }
        for p in &f.parents {
            self.children.entry(p.clone()).or_default().insert(f.id.clone());
        }
        self.items.insert(f.id.clone(), f);
    }

    pub fn upsert(&mut self, f: DriveFile) {
        if f.id == self.root {
            return;
        }
        if self.items.get(&f.id) == Some(&f) {
            return;
        }
        self.ops.push(Op::PutRemote(f.clone()));
        self.insert(f);
        self.stale = true;
    }

    /// Insert/update an item whose local path is already known, keeping the
    /// index current without a full rebuild (used after our own uploads and
    /// folder creations, which can number in the thousands per cycle).
    pub fn upsert_at(&mut self, rel: &str, f: DriveFile, kind: Kind) {
        let was_stale = self.stale;
        let id = f.id.clone();
        let moved = self.id_path.get(&id).is_some_and(|p| p != rel);
        self.upsert(f);
        if was_stale || moved {
            return; // a full reindex is pending anyway / needed
        }
        self.index.insert(rel.to_string(), (id.clone(), kind));
        self.id_path.insert(id, rel.to_string());
        self.stale = false;
    }

    /// Every item below folder `id` (not just the ones mapped to local
    /// paths: also ignored native docs, shortcuts, duplicate-name losers…).
    pub fn descendants(&self, id: &str) -> Vec<&DriveFile> {
        let mut out = vec![];
        let mut stack = vec![id.to_string()];
        while let Some(i) = stack.pop() {
            if let Some(k) = self.children.get(&i) {
                for c in k {
                    if let Some(f) = self.items.get(c) {
                        out.push(f);
                        stack.push(c.clone());
                    }
                }
            }
        }
        out
    }

    pub fn is_stale(&self) -> bool {
        self.stale
    }

    /// Remove an item and all its descendants.
    pub fn remove(&mut self, id: &str) {
        let mut stack = vec![id.to_string()];
        while let Some(i) = stack.pop() {
            if let Some(kids) = self.children.remove(&i) {
                stack.extend(kids);
            }
            if let Some(f) = self.items.remove(&i) {
                for p in &f.parents {
                    if let Some(c) = self.children.get_mut(p) {
                        c.remove(&i);
                    }
                }
                self.ops.push(Op::DelRemote(i));
            }
        }
        self.stale = true;
    }

    /// Recompute the path index if anything changed.
    pub fn reindex(&mut self, prefer: &HashMap<String, String>) {
        if !self.stale {
            return;
        }
        self.index.clear();
        self.id_path.clear();
        self.shadows.clear();
        let mut stack: Vec<(String, String)> = vec![(self.root.clone(), String::new())];
        while let Some((id, path)) = stack.pop() {
            let Some(kids) = self.children.get(&id) else { continue };
            let mut kids: Vec<&DriveFile> = kids.iter().filter_map(|k| self.items.get(k)).collect();
            // Deterministic winner for duplicate names: the one already
            // synced at that path, else the lowest ID.
            kids.sort_by(|a, b| a.id.cmp(&b.id));
            // Pick one winner per local name first; only winners are
            // indexed and descended into, so two same-named cloud folders
            // are never merged into one local folder.
            let mut taken: HashMap<String, (&DriveFile, Kind)> = HashMap::new();
            for f in kids {
                if f.trashed {
                    continue;
                }
                let Some((name, kind)) = local_name(f, self.mapping) else { continue };
                let rel = if path.is_empty() { name.clone() } else { format!("{path}/{name}") };
                if f.is_native_doc() {
                    if let Some(t) = native_type(&f.mime_type) {
                        let base = remote_to_local_name(&f.name);
                        let dir = if path.is_empty() { String::new() } else { format!("{path}/") };
                        if !t.export_ext.is_empty() {
                            self.shadows.insert(format!("{dir}{}", add_ext(&base, t.export_ext)));
                            self.shadows.insert(format!("{dir}{base}{}", t.export_ext));
                        }
                    }
                }
                match taken.get(&rel) {
                    None => {
                        taken.insert(rel, (f, kind));
                    }
                    Some((prev, _)) => {
                        warn!(
                            "duplicate name in cloud folder: '{}' (ids {} and {}); only one is synced",
                            rel, prev.id, f.id
                        );
                        // Kids are sorted by ID, so the earlier one wins unless
                        // the later one is the one we already sync.
                        if prefer.get(&rel) == Some(&f.id) {
                            taken.insert(rel, (f, kind));
                        }
                    }
                }
            }
            for (rel, (f, kind)) in taken {
                self.index.insert(rel.clone(), (f.id.clone(), kind));
                self.id_path.insert(f.id.clone(), rel.clone());
                if kind == Kind::Dir {
                    stack.push((f.id.clone(), rel));
                }
            }
        }
        self.stale = false;
    }

    pub fn by_path(&self, rel: &str) -> Option<(&DriveFile, Kind)> {
        let (id, kind) = self.index.get(rel)?;
        Some((self.items.get(id)?, *kind))
    }

    pub fn path_of(&self, id: &str) -> Option<&String> {
        self.id_path.get(id)
    }

    pub fn is_shadow(&self, rel: &str) -> bool {
        self.shadows.contains(rel)
    }

    /// The Drive folder ID for a local directory path ("" = root).
    pub fn folder_id(&self, rel: &str) -> Option<String> {
        if rel.is_empty() {
            return Some(self.root.clone());
        }
        match self.index.get(rel) {
            Some((id, Kind::Dir)) => Some(id.clone()),
            _ => None,
        }
    }

    pub fn paths(&self) -> impl Iterator<Item = &String> {
        self.index.keys()
    }

    pub fn paths_under<'a>(&'a self, prefix: &str) -> impl Iterator<Item = &'a String> + 'a {
        let start = format!("{prefix}/");
        let end = format!("{prefix}0"); // '0' sorts right after '/'
        self.index.range(start..end).map(|(k, _)| k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(id: &str, name: &str, parent: &str, mime: &str) -> DriveFile {
        DriveFile {
            id: id.into(),
            name: name.into(),
            mime_type: mime.into(),
            parents: vec![parent.into()],
            md5_checksum: Some("m".into()),
            size: Some("1".into()),
            modified_time: Some("2024-01-01T00:00:00.000Z".into()),
            trashed: false,
            drive_id: None,
            web_view_link: None,
        }
    }

    const M: Mapping = Mapping { docs: GoogleDocsMode::Link, symlinks: SymlinkMode::Rclonelink };

    #[test]
    fn index_and_mapping() {
        let mut t = RemoteTree::new("root".into(), M);
        t.upsert(file("d", "Docs", "root", crate::drive::FOLDER_MIME));
        t.upsert(file("a", "a.txt", "d", "text/plain"));
        t.upsert(file("g", "Plan", "d", "application/vnd.google-apps.document"));
        t.upsert(file("l", "link.rclonelink", "d", "application/octet-stream"));
        t.upsert(file("s", "a/b", "root", "text/plain"));
        t.reindex(&HashMap::new());
        assert_eq!(t.by_path("Docs").unwrap().1, Kind::Dir);
        assert_eq!(t.by_path("Docs/a.txt").unwrap().0.id, "a");
        assert_eq!(t.by_path("Docs/Plan.gdoc").unwrap().1, Kind::Link);
        assert_eq!(t.by_path("Docs/link").unwrap().1, Kind::Symlink);
        assert_eq!(t.by_path("a／b").unwrap().0.id, "s");
        assert!(t.is_shadow("Docs/Plan.docx"));
        let under: Vec<_> = t.paths_under("Docs").cloned().collect();
        assert_eq!(under.len(), 3);
        t.remove("d");
        t.reindex(&HashMap::new());
        assert!(t.by_path("Docs/a.txt").is_none());
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn duplicate_folders_are_not_merged() {
        let mut t = RemoteTree::new("root".into(), M);
        t.upsert(file("a1", "A", "root", crate::drive::FOLDER_MIME));
        t.upsert(file("a2", "A", "root", crate::drive::FOLDER_MIME));
        t.upsert(file("x", "x", "a1", "text/plain"));
        t.upsert(file("y", "y", "a2", "text/plain"));
        let mut prefer = HashMap::new();
        prefer.insert("A".to_string(), "a2".to_string());
        t.reindex(&prefer);
        assert_eq!(t.by_path("A").unwrap().0.id, "a2");
        assert!(t.by_path("A/y").is_some());
        assert!(t.by_path("A/x").is_none(), "loser's children must not be merged in");
    }

    #[test]
    fn move_updates_path() {
        let mut t = RemoteTree::new("root".into(), M);
        t.upsert(file("d1", "A", "root", crate::drive::FOLDER_MIME));
        t.upsert(file("d2", "B", "root", crate::drive::FOLDER_MIME));
        t.upsert(file("f", "x", "d1", "text/plain"));
        t.reindex(&HashMap::new());
        assert_eq!(t.path_of("f").unwrap(), "A/x");
        t.upsert(file("f", "y", "d2", "text/plain"));
        t.reindex(&HashMap::new());
        assert_eq!(t.path_of("f").unwrap(), "B/y");
        assert!(t.by_path("A/x").is_none());
    }
}
