//! End-to-end tests of the sync engine against the fake Drive server.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use crate::auth::TokenProvider;
use crate::config::{Account, SyncPair};
use crate::db::Db;
use crate::drive::{DriveClient, FOLDER_MIME};
use crate::engine::{self, Command, Env};
use crate::limiter::{Bandwidth, TokenBucket};
use crate::notify::Notifier;
use crate::status::{State, StatusHub};
use crate::testsupport::{fake, ROOT};

struct H {
    dir: tempfile::TempDir,
    name: String,
    folder: String,
    tx: mpsc::UnboundedSender<Command>,
    status: StatusHub,
    task: tokio::task::JoinHandle<()>,
}

static N: AtomicU32 = AtomicU32::new(0);

async fn setup(tweak: impl FnOnce(&mut SyncPair)) -> H {
    setup_with(tweak, |_, _| {}).await
}

/// `pre` may populate the local dir and remote folder before the engine starts.
async fn setup_with(tweak: impl FnOnce(&mut SyncPair), pre: impl FnOnce(&Path, &str)) -> H {
    let f = fake();
    let n = N.fetch_add(1, Ordering::SeqCst);
    let name = format!("t{n}-{}", std::process::id());
    let dir = tempfile::tempdir().unwrap();
    let folder = f.state.lock().unwrap().add(ROOT, &name, FOLDER_MIME, b"", None);
    pre(dir.path(), &folder);

    let acct = Account {
        name: name.clone(),
        email: String::new(),
        client_id: "x".into(),
        client_secret: String::new(),
        requests_per_second: 0.0,
        request_burst: 1,
    };
    crate::secrets::store(&name, "refresh").await.unwrap();
    let mut pair = crate::accounts::new_pair(&name, &name, dir.path(), &format!("/{name}")).unwrap();
    pair.settle_time = "0s".into();
    pair.remote_poll_interval = "1h".into();
    pair.full_rescan_interval = "1h".into();
    pair.symlinks = crate::config::SymlinkMode::Rclonelink;
    tweak(&mut pair);

    let http = reqwest::Client::new();
    let tokens = Arc::new(TokenProvider::new(acct.clone(), http.clone()));
    let drive = Arc::new(DriveClient::new(http, tokens, TokenBucket::new(0.0, 1.0), Bandwidth::new(0, 0)));
    let db_path = crate::paths::data_dir().join(format!("{name}.db"));
    let db = Arc::new(Db::open(&db_path).unwrap());
    let status = StatusHub::new();
    let env = Env {
        cfg: pair,
        email: "test@example.com".into(),
        drive,
        db,
        status: status.clone(),
        notifier: Notifier::new(false),
        max_transfers: 3,
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(engine::run(env, rx));
    let h = H { dir, name, folder, tx, status, task };
    wait_idle(&h, 0).await;
    h
}

fn cycles(h: &H) -> (u64, State) {
    h.status
        .snapshot()
        .into_iter()
        .find(|p| p.name == h.name)
        .map(|p| (p.cycles, p.state))
        .unwrap_or((0, State::Starting))
}

async fn wait_idle(h: &H, after: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (c, s) = cycles(h);
        if c > after && s == State::Idle {
            // Wait for things to quieten down (watcher echoes etc.).
            tokio::time::sleep(Duration::from_millis(400)).await;
            let (c2, s2) = cycles(h);
            if c2 == c && s2 == State::Idle {
                return;
            }
            continue;
        }
        if tokio::time::Instant::now() > deadline {
            let st = h.status.snapshot();
            panic!("timed out waiting for sync: {st:#?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn sync(h: &H) {
    // Let inotify deliver pending events first.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (c, _) = cycles(h);
    h.tx.send(Command::SyncNow).unwrap();
    wait_idle(h, c).await;
}

fn local(h: &H, rel: &str) -> PathBuf {
    h.dir.path().join(rel)
}

fn remote_child(parent: &str, name: &str) -> Option<crate::drive::DriveFile> {
    fake().state.lock().unwrap().child(parent, name).cloned()
}

fn remote_path(h: &H, rel: &str) -> Option<crate::drive::DriveFile> {
    let mut cur = h.folder.clone();
    let mut last = None;
    for c in rel.split('/') {
        let f = remote_child(&cur, c)?;
        cur = f.id.clone();
        last = Some(f);
    }
    last
}

fn uploads(id: &str) -> u32 {
    fake().state.lock().unwrap().uploads.get(id).copied().unwrap_or(0)
}

fn remote_content(id: &str) -> Vec<u8> {
    fake().state.lock().unwrap().content(id).to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_download_and_symlinks() {
    let h = setup(|_| {}).await;
    std::fs::create_dir_all(local(&h, "docs/sub")).unwrap();
    std::fs::write(local(&h, "a.txt"), "alpha").unwrap();
    std::fs::write(local(&h, "docs/sub/b.txt"), "bravo").unwrap();
    std::os::unix::fs::symlink("docs/sub/b.txt", local(&h, "link")).unwrap();
    sync(&h).await;

    let a = remote_path(&h, "a.txt").expect("a.txt uploaded");
    assert_eq!(remote_content(&a.id), b"alpha");
    let b = remote_path(&h, "docs/sub/b.txt").expect("nested upload");
    assert_eq!(remote_content(&b.id), b"bravo");
    let l = remote_path(&h, "link.rclonelink").expect("symlink stored as .rclonelink");
    assert_eq!(remote_content(&l.id), b"docs/sub/b.txt");

    // Remote additions come down, including a native Google document.
    let docs = remote_path(&h, "docs").unwrap();
    {
        let mut s = fake().state.lock().unwrap();
        s.add(&docs.id, "c.txt", "text/plain", b"charlie", None);
        s.add(&docs.id, "Plan", "application/vnd.google-apps.document", b"", None);
        s.add(&h.folder, "rl.rclonelink", "application/octet-stream", b"a.txt", None);
    }
    sync(&h).await;
    assert_eq!(std::fs::read_to_string(local(&h, "docs/c.txt")).unwrap(), "charlie");
    let gdoc = std::fs::read_to_string(local(&h, "docs/Plan.gdoc")).unwrap();
    assert!(gdoc.contains("https://docs.example/"), "{gdoc}");
    assert_eq!(std::fs::read_link(local(&h, "rl")).unwrap(), PathBuf::from("a.txt"));
    h.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn edits_both_ways_and_no_echo() {
    let h = setup(|_| {}).await;
    std::fs::write(local(&h, "f.txt"), "v1").unwrap();
    sync(&h).await;
    let f = remote_path(&h, "f.txt").unwrap();

    std::fs::write(local(&h, "f.txt"), "v2 local").unwrap();
    sync(&h).await;
    assert_eq!(remote_content(&f.id), b"v2 local");
    assert_eq!(remote_path(&h, "f.txt").unwrap().id, f.id, "updated in place");

    fake().state.lock().unwrap().set_content(&f.id, b"v3 remote");
    sync(&h).await;
    assert_eq!(std::fs::read_to_string(local(&h, "f.txt")).unwrap(), "v3 remote");

    // The download must not bounce back as an upload.
    let uploads_before = uploads(&f.id);
    sync(&h).await;
    sync(&h).await;
    assert_eq!(uploads(&f.id), uploads_before);
    h.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renames_and_moves_are_metadata_only() {
    let h = setup(|_| {}).await;
    std::fs::create_dir_all(local(&h, "dir1")).unwrap();
    std::fs::write(local(&h, "dir1/x.txt"), "x").unwrap();
    std::fs::write(local(&h, "y.txt"), "y").unwrap();
    sync(&h).await;
    let x = remote_path(&h, "dir1/x.txt").unwrap();
    let y = remote_path(&h, "y.txt").unwrap();
    let d = remote_path(&h, "dir1").unwrap();
    let before = (uploads(&x.id), uploads(&y.id));

    std::fs::rename(local(&h, "dir1"), local(&h, "dir2")).unwrap();
    std::fs::rename(local(&h, "y.txt"), local(&h, "dir2/y2.txt")).unwrap();
    sync(&h).await;
    assert_eq!(remote_path(&h, "dir2").unwrap().id, d.id, "folder renamed, not recreated");
    assert_eq!(remote_path(&h, "dir2/x.txt").unwrap().id, x.id);
    assert_eq!(remote_path(&h, "dir2/y2.txt").unwrap().id, y.id, "file moved, not re-uploaded");
    assert_eq!((uploads(&x.id), uploads(&y.id)), before, "no content re-uploaded");

    // Remote rename comes down as a local rename.
    fake().state.lock().unwrap().rename(&x.id, "x-renamed.txt", Some(&h.folder));
    sync(&h).await;
    assert!(local(&h, "x-renamed.txt").exists());
    assert!(!local(&h, "dir2/x.txt").exists());
    h.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deletions_go_to_the_bin() {
    let h = setup(|_| {}).await;
    std::fs::create_dir_all(local(&h, "old")).unwrap();
    std::fs::write(local(&h, "old/1.txt"), "1").unwrap();
    std::fs::write(local(&h, "2.txt"), "2").unwrap();
    std::fs::write(local(&h, "3.txt"), "3").unwrap();
    sync(&h).await;
    let old = remote_path(&h, "old").unwrap();
    let two = remote_path(&h, "2.txt").unwrap();
    let three = remote_path(&h, "3.txt").unwrap();

    std::fs::remove_dir_all(local(&h, "old")).unwrap();
    std::fs::remove_file(local(&h, "2.txt")).unwrap();
    sync(&h).await;
    {
        let s = fake().state.lock().unwrap();
        assert!(s.is_trashed(&old.id));
        assert!(s.is_trashed(&two.id));
    }
    fake().state.lock().unwrap().trash(&three.id);
    sync(&h).await;
    assert!(!local(&h, "3.txt").exists());
    h.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicts_keep_both() {
    let h = setup(|_| {}).await;
    std::fs::write(local(&h, "c.txt"), "base").unwrap();
    sync(&h).await;
    let c = remote_path(&h, "c.txt").unwrap();
    // Pause so both sides can change before the next cycle.
    h.tx.send(Command::Pause(true)).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    std::fs::write(local(&h, "c.txt"), "local edit").unwrap();
    fake().state.lock().unwrap().set_content(&c.id, b"remote edit");
    h.tx.send(Command::Pause(false)).unwrap();
    sync(&h).await;
    sync(&h).await;
    assert_eq!(std::fs::read_to_string(local(&h, "c.txt")).unwrap(), "remote edit");
    let copies: Vec<String> = std::fs::read_dir(h.dir.path())
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| n.contains("conflicted copy"))
        .collect();
    assert_eq!(copies.len(), 1, "{copies:?}");
    assert_eq!(std::fs::read_to_string(local(&h, &copies[0])).unwrap(), "local edit");
    assert!(remote_path(&h, &copies[0]).is_some(), "conflicted copy uploaded");
    h.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn migration_from_rclone_transfers_nothing() {
    // Identical trees on both sides with matching mtimes (as rclone leaves
    // them), plus an rclone-exported .docx of a native document.
    let mtime = "2024-03-04T05:06:07.000Z";
    let mtime_ns = crate::util::rfc3339_to_ns(mtime).unwrap();
    let h = setup_with(
        |_| {},
        |dir, folder| {
            let mut s = fake().state.lock().unwrap();
            let sub = s.add(folder, "sub", FOLDER_MIME, b"", None);
            s.add(folder, "one.txt", "text/plain", b"one", Some(mtime));
            s.add(&sub, "two.txt", "text/plain", b"two", Some(mtime));
            s.add(folder, "Report", "application/vnd.google-apps.document", b"", None);
            drop(s);
            std::fs::create_dir(dir.join("sub")).unwrap();
            std::fs::write(dir.join("one.txt"), "one").unwrap();
            std::fs::write(dir.join("sub/two.txt"), "two").unwrap();
            std::fs::write(dir.join("Report.docx"), "exported by rclone").unwrap();
            for f in ["one.txt", "sub/two.txt"] {
                crate::localfs::set_mtime(&dir.join(f), mtime_ns).unwrap();
            }
        },
    )
    .await;
    sync(&h).await;
    for n in ["one.txt", "sub/two.txt"] {
        let f = remote_path(&h, n).unwrap();
        assert_eq!(uploads(&f.id), 0, "{n} should not have been uploaded");
    }
    assert_eq!(fake().state.lock().unwrap().files.values().filter(|(f, _)| f.parents.contains(&h.folder)).count(), 3);
    assert!(local(&h, "Report.gdoc").exists());
    assert!(remote_path(&h, "Report.docx").is_none());
    h.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batching_limits_uploads_of_busy_files() {
    let h = setup(|p| p.sync_interval = "2s".into()).await;
    for i in 0..30 {
        std::fs::write(local(&h, "busy.log"), format!("line {i}\n").repeat(i + 1)).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(4500)).await;
    let f = remote_path(&h, "busy.log").unwrap();
    let n = uploads(&f.id);
    assert!((1..=2).contains(&n), "expected 1-2 uploads in batch mode, got {n}");
    assert_eq!(remote_content(&f.id), "line 29\n".repeat(30).as_bytes());
    h.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mass_deletion_is_held_back_until_confirmed() {
    let h = setup(|p| p.delete_guard = 3).await;
    for i in 0..6 {
        std::fs::write(local(&h, &format!("{i}.txt")), "x").unwrap();
    }
    sync(&h).await;
    let ids: Vec<String> = (0..6).map(|i| remote_path(&h, &format!("{i}.txt")).unwrap().id).collect();
    for i in 0..6 {
        std::fs::remove_file(local(&h, &format!("{i}.txt"))).unwrap();
    }
    sync(&h).await;
    assert!(ids.iter().all(|id| !fake().state.lock().unwrap().is_trashed(id)), "deletions must be held");
    let blocked = h.status.snapshot().into_iter().find(|p| p.name == h.name).unwrap().blocked_deletes;
    assert_eq!(blocked, 6);
    h.tx.send(Command::ConfirmDeletes).unwrap();
    sync(&h).await;
    assert!(ids.iter().all(|id| fake().state.lock().unwrap().is_trashed(id)));
    h.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_file_resumable_upload() {
    let h = setup(|_| {}).await;
    let data: Vec<u8> = (0..(9 * 1024 * 1024 + 123)).map(|i| (i % 251) as u8).collect();
    std::fs::write(local(&h, "big.bin"), &data).unwrap();
    sync(&h).await;
    let f = remote_path(&h, "big.bin").unwrap();
    assert_eq!(remote_content(&f.id), data);
    h.task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selective_sync_and_excludes() {
    let h = setup(|p| {
        p.exclude_folders = vec!["private".into()];
    })
    .await;
    std::fs::create_dir_all(local(&h, "private")).unwrap();
    std::fs::write(local(&h, "private/secret.txt"), "s").unwrap();
    std::fs::write(local(&h, "draft.tmp"), "t").unwrap();
    std::fs::write(local(&h, "ok.txt"), "o").unwrap();
    sync(&h).await;
    assert!(remote_path(&h, "ok.txt").is_some());
    assert!(remote_path(&h, "private").is_none());
    assert!(remote_path(&h, "draft.tmp").is_none());
    h.task.abort();
}

// ---- Regression tests for data-safety defects found in review ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_unreadable_dir_must_not_delete_remote() {
    use std::os::unix::fs::PermissionsExt;
    let h = setup(|_| {}).await;
    std::fs::create_dir_all(local(&h, "locked")).unwrap();
    std::fs::write(local(&h, "locked/a.txt"), "a").unwrap();
    std::fs::write(local(&h, "locked/b.txt"), "b").unwrap();
    sync(&h).await;
    let a = remote_path(&h, "locked/a.txt").unwrap();
    // Any event that marks the folder dirty (here: a touch-like rename round
    // trip) followed by a permission problem / EIO on the folder.
    h.tx.send(Command::Pause(true)).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    std::fs::rename(local(&h, "locked"), local(&h, "locked.x")).unwrap();
    std::fs::rename(local(&h, "locked.x"), local(&h, "locked")).unwrap();
    std::fs::set_permissions(local(&h, "locked"), std::fs::Permissions::from_mode(0o000)).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    h.tx.send(Command::Pause(false)).unwrap();
    sync(&h).await;
    let trashed = fake().state.lock().unwrap().is_trashed(&a.id);
    std::fs::set_permissions(local(&h, "locked"), std::fs::Permissions::from_mode(0o755)).unwrap();
    h.task.abort();
    assert!(!trashed, "a read_dir/stat permission error was treated as a local deletion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_folder_delete_trashes_unsynced_remote_docs() {
    let h = setup(|p| p.google_docs = crate::config::GoogleDocsMode::Ignore).await;
    std::fs::create_dir_all(local(&h, "proj")).unwrap();
    std::fs::write(local(&h, "proj/a.txt"), "a").unwrap();
    sync(&h).await;
    let proj = remote_path(&h, "proj").unwrap();
    let sheet = fake().state.lock().unwrap().add(&proj.id, "Budget", "application/vnd.google-apps.spreadsheet", b"", None);
    h.tx.send(Command::SyncNow).unwrap();
    sync(&h).await;
    std::fs::remove_dir_all(local(&h, "proj")).unwrap();
    sync(&h).await;
    let trashed = fake().state.lock().unwrap().is_trashed(&sheet);
    h.task.abort();
    assert!(!trashed, "a never-synced Google Sheet was trashed along with its folder");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn review_remote_folder_delete_trashes_settling_local_file() {
    let h = setup(|p| p.settle_time = "4s".into()).await;
    std::fs::create_dir_all(local(&h, "d")).unwrap();
    std::fs::write(local(&h, "d/old.txt"), "o").unwrap();
    tokio::time::sleep(Duration::from_millis(4200)).await;
    sync(&h).await;
    let d = remote_path(&h, "d").expect("d uploaded");
    // New local work lands in d just as d is deleted in the cloud.
    std::fs::write(local(&h, "d/new-work.txt"), "important").unwrap();
    fake().state.lock().unwrap().trash(&d.id);
    sync(&h).await;
    let survived = local(&h, "d/new-work.txt").exists() || remote_path(&h, "d/new-work.txt").is_some();
    h.task.abort();
    assert!(survived, "unsynced local file was trashed with its folder");
}
