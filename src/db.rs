//! Persistent sync state (SQLite, WAL mode).
//!
//! * `entries` — the "base" state of every synced path: what both sides
//!   looked like when they were last known to be identical. This is what
//!   makes three-way reconciliation (and therefore safe deletes and conflict
//!   detection) possible.
//! * `remote` — a cached copy of the remote folder tree, kept up to date
//!   incrementally with the Drive changes feed.
//! * `kv` — misc per-pair values (changes page token, root id, …).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

use crate::drive::DriveFile;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    File,
    Dir,
    /// `.gdoc`-style link file for a native Google document.
    Link,
    /// Office export of a native Google document (download-only).
    Export,
    /// Symbolic link stored remotely as `<name>.rclonelink`.
    Symlink,
}

impl Kind {
    pub fn to_i64(self) -> i64 {
        match self {
            Kind::File => 0,
            Kind::Dir => 1,
            Kind::Link => 2,
            Kind::Export => 3,
            Kind::Symlink => 4,
        }
    }
    pub fn from_i64(v: i64) -> Self {
        match v {
            1 => Kind::Dir,
            2 => Kind::Link,
            3 => Kind::Export,
            4 => Kind::Symlink,
            _ => Kind::File,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub kind: Kind,
    pub remote_id: String,
    pub size: u64,
    pub mtime_ns: i64,
    pub inode: u64,
    /// MD5 of the local content when last synced (files and symlinks).
    pub md5: Option<String>,
    /// Remote fingerprint when last synced (see `engine::remote_fp`).
    pub remote_fp: String,
}

pub struct Db {
    conn: Mutex<Connection>,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path.parent().unwrap())?;
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS entries (
                pair TEXT NOT NULL, path TEXT NOT NULL, kind INTEGER NOT NULL,
                remote_id TEXT NOT NULL, size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,
                inode INTEGER NOT NULL, md5 TEXT, remote_fp TEXT NOT NULL,
                PRIMARY KEY (pair, path));
             CREATE TABLE IF NOT EXISTS remote (
                pair TEXT NOT NULL, id TEXT NOT NULL, json TEXT NOT NULL,
                PRIMARY KEY (pair, id));
             CREATE TABLE IF NOT EXISTS kv (
                pair TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
                PRIMARY KEY (pair, key));",
        )?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn load_entries(&self, pair: &str) -> Result<HashMap<String, Entry>> {
        let c = self.conn.lock().unwrap();
        let mut st = c.prepare(
            "SELECT path, kind, remote_id, size, mtime_ns, inode, md5, remote_fp FROM entries WHERE pair = ?1",
        )?;
        let rows = st.query_map(params![pair], |r| {
            Ok((
                r.get::<_, String>(0)?,
                Entry {
                    kind: Kind::from_i64(r.get(1)?),
                    remote_id: r.get(2)?,
                    size: r.get::<_, i64>(3)? as u64,
                    mtime_ns: r.get(4)?,
                    inode: r.get::<_, i64>(5)? as u64,
                    md5: r.get(6)?,
                    remote_fp: r.get(7)?,
                },
            ))
        })?;
        let mut m = HashMap::new();
        for r in rows {
            let (p, e) = r?;
            m.insert(p, e);
        }
        Ok(m)
    }

    pub fn load_remote(&self, pair: &str) -> Result<Vec<DriveFile>> {
        let c = self.conn.lock().unwrap();
        let mut st = c.prepare("SELECT json FROM remote WHERE pair = ?1")?;
        let rows = st.query_map(params![pair], |r| r.get::<_, String>(0))?;
        let mut v = Vec::new();
        for r in rows {
            if let Ok(f) = serde_json::from_str(&r?) {
                v.push(f);
            }
        }
        Ok(v)
    }

    pub fn get_kv(&self, pair: &str, key: &str) -> Result<Option<String>> {
        let c = self.conn.lock().unwrap();
        Ok(c
            .query_row(
                "SELECT value FROM kv WHERE pair = ?1 AND key = ?2",
                params![pair, key],
                |r| r.get(0),
            )
            .optional()?)
    }

    pub fn set_kv(&self, pair: &str, key: &str, value: &str) -> Result<()> {
        let c = self.conn.lock().unwrap();
        c.execute(
            "INSERT OR REPLACE INTO kv (pair, key, value) VALUES (?1, ?2, ?3)",
            params![pair, key, value],
        )?;
        Ok(())
    }

    /// Apply a batch of changes atomically.
    pub fn apply(&self, pair: &str, ops: &[Op]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut c = self.conn.lock().unwrap();
        let tx = c.transaction()?;
        {
            let mut put = tx.prepare_cached(
                "INSERT OR REPLACE INTO entries (pair, path, kind, remote_id, size, mtime_ns, inode, md5, remote_fp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?;
            let mut del = tx.prepare_cached("DELETE FROM entries WHERE pair = ?1 AND path = ?2")?;
            let mut putr =
                tx.prepare_cached("INSERT OR REPLACE INTO remote (pair, id, json) VALUES (?1, ?2, ?3)")?;
            let mut delr = tx.prepare_cached("DELETE FROM remote WHERE pair = ?1 AND id = ?2")?;
            for op in ops {
                match op {
                    Op::PutEntry(p, e) => {
                        put.execute(params![
                            pair,
                            p,
                            e.kind.to_i64(),
                            e.remote_id,
                            e.size as i64,
                            e.mtime_ns,
                            e.inode as i64,
                            e.md5,
                            e.remote_fp
                        ])?;
                    }
                    Op::DelEntry(p) => {
                        del.execute(params![pair, p])?;
                    }
                    Op::PutRemote(f) => {
                        putr.execute(params![pair, f.id, serde_json::to_string(f)?])?;
                    }
                    Op::DelRemote(id) => {
                        delr.execute(params![pair, id])?;
                    }
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Forget everything about a pair (used when a pair is removed or reset).
    pub fn clear_pair(&self, pair: &str) -> Result<()> {
        let c = self.conn.lock().unwrap();
        c.execute("DELETE FROM entries WHERE pair = ?1", params![pair])?;
        c.execute("DELETE FROM remote WHERE pair = ?1", params![pair])?;
        c.execute("DELETE FROM kv WHERE pair = ?1", params![pair])?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    PutEntry(String, Entry),
    DelEntry(String),
    PutRemote(DriveFile),
    DelRemote(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("s.db")).unwrap();
        let e = Entry {
            kind: Kind::File,
            remote_id: "abc".into(),
            size: 3,
            mtime_ns: 42,
            inode: 7,
            md5: Some("x".into()),
            remote_fp: "x".into(),
        };
        db.apply("p", &[Op::PutEntry("a/b".into(), e.clone())]).unwrap();
        let m = db.load_entries("p").unwrap();
        assert_eq!(m.get("a/b"), Some(&e));
        db.apply("p", &[Op::DelEntry("a/b".into())]).unwrap();
        assert!(db.load_entries("p").unwrap().is_empty());
        db.set_kv("p", "k", "v").unwrap();
        assert_eq!(db.get_kv("p", "k").unwrap().as_deref(), Some("v"));
    }
}
