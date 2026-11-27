//! Test support: an in-process fake of the subset of the Drive v3 REST API
//! that DriftFolderSync uses, so the sync engine can be tested end to end without
//! network access or a Google account.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::drive::{DriveFile, FOLDER_MIME};

pub const ROOT: &str = "ROOT";

#[derive(Default)]
pub struct FakeState {
    pub files: HashMap<String, (DriveFile, Vec<u8>)>,
    changes: Vec<(u64, String)>,
    seq: u64,
    next_id: u64,
    sessions: HashMap<String, (Option<String>, serde_json::Value, Vec<u8>, u64)>,
    /// Count of content uploads per file id (for assertions).
    pub uploads: HashMap<String, u32>,
    pub downloads: u32,
}

impl FakeState {
    fn new() -> Self {
        let mut s = Self::default();
        s.files.insert(
            ROOT.into(),
            (
                DriveFile {
                    id: ROOT.into(),
                    name: "My Drive".into(),
                    mime_type: FOLDER_MIME.into(),
                    ..blank()
                },
                vec![],
            ),
        );
        s
    }

    fn touch(&mut self, id: &str) {
        self.seq += 1;
        self.changes.push((self.seq, id.to_string()));
    }

    fn new_id(&mut self) -> String {
        self.next_id += 1;
        format!("F{:06}", self.next_id)
    }

    pub fn add(&mut self, parent: &str, name: &str, mime: &str, content: &[u8], mtime: Option<&str>) -> String {
        let id = self.new_id();
        let is_folder = mime == FOLDER_MIME;
        let native = mime.starts_with("application/vnd.google-apps.");
        let f = DriveFile {
            id: id.clone(),
            name: name.into(),
            mime_type: mime.into(),
            parents: vec![parent.into()],
            md5_checksum: (!native && !is_folder).then(|| crate::util::md5_bytes(content)),
            size: (!native && !is_folder).then(|| content.len().to_string()),
            modified_time: Some(mtime.map(String::from).unwrap_or_else(now)),
            web_view_link: Some(format!("https://docs.example/{id}")),
            ..blank()
        };
        self.files.insert(id.clone(), (f, content.to_vec()));
        self.touch(&id);
        id
    }

    pub fn set_content(&mut self, id: &str, content: &[u8]) {
        let (f, c) = self.files.get_mut(id).unwrap();
        *c = content.to_vec();
        f.md5_checksum = Some(crate::util::md5_bytes(content));
        f.size = Some(content.len().to_string());
        f.modified_time = Some(now());
        self.touch(id);
    }

    pub fn rename(&mut self, id: &str, name: &str, parent: Option<&str>) {
        let (f, _) = self.files.get_mut(id).unwrap();
        f.name = name.into();
        if let Some(p) = parent {
            f.parents = vec![p.into()];
        }
        self.touch(id);
    }

    pub fn trash(&mut self, id: &str) {
        self.files.get_mut(id).unwrap().0.trashed = true;
        self.touch(id);
    }

    /// Find a non-trashed child by name.
    pub fn child(&self, parent: &str, name: &str) -> Option<&DriveFile> {
        self.files
            .values()
            .map(|(f, _)| f)
            .find(|f| !f.trashed && f.name == name && f.parents.iter().any(|p| p == parent))
    }

    pub fn content(&self, id: &str) -> &[u8] {
        &self.files[id].1
    }

    fn effectively_trashed(&self, id: &str) -> bool {
        let mut cur = id.to_string();
        for _ in 0..100 {
            let Some((f, _)) = self.files.get(&cur) else { return true };
            if f.trashed {
                return true;
            }
            match f.parents.first() {
                Some(p) => cur = p.clone(),
                None => return false,
            }
        }
        false
    }

    pub fn is_trashed(&self, id: &str) -> bool {
        self.effectively_trashed(id)
    }
}

fn blank() -> DriveFile {
    DriveFile {
        id: String::new(),
        name: String::new(),
        mime_type: String::new(),
        parents: vec![],
        md5_checksum: None,
        size: None,
        modified_time: None,
        trashed: false,
        drive_id: None,
        web_view_link: None,
    }
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub struct Fake {
    pub url: String,
    pub state: Arc<Mutex<FakeState>>,
}

/// The shared fake server (one per test binary), plus process-wide test
/// environment set-up (token file store, temporary data/trash dirs).
pub fn fake() -> &'static Fake {
    static F: OnceLock<Fake> = OnceLock::new();
    F.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let tmp = std::env::temp_dir().join(format!("driftfoldersync-test-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        std::env::set_var("DRIFTFOLDERSYNC_TOKEN_STORE", "file");
        std::env::set_var("DRIFTFOLDERSYNC_DATA_DIR", tmp.join("data"));
        std::env::set_var("XDG_DATA_HOME", tmp.join("xdg"));
        let state = Arc::new(Mutex::new(FakeState::new()));
        let st2 = state.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async move {
                let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
                tx.send(l.local_addr().unwrap().port()).unwrap();
                loop {
                    let (sock, _) = l.accept().await.unwrap();
                    let st = st2.clone();
                    tokio::spawn(async move {
                        let _ = serve_conn(sock, st).await;
                    });
                }
            });
        });
        let port = rx.recv().unwrap();
        let url = format!("http://127.0.0.1:{port}");
        crate::drive::set_endpoint(url.clone());
        Fake { url, state }
    })
}

struct Req {
    method: String,
    path: String,
    query: HashMap<String, String>,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

async fn serve_conn(sock: tokio::net::TcpStream, st: Arc<Mutex<FakeState>>) -> std::io::Result<()> {
    let (r, mut w) = sock.into_split();
    let mut r = BufReader::new(r);
    loop {
        let mut line = String::new();
        if r.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let target = parts.next().unwrap_or("/").to_string();
        let mut headers = HashMap::new();
        loop {
            let mut h = String::new();
            r.read_line(&mut h).await?;
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some((k, v)) = h.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }
        let len: usize = headers.get("content-length").and_then(|v| v.parse().ok()).unwrap_or(0);
        let mut body = vec![0u8; len];
        r.read_exact(&mut body).await?;
        let u = url::Url::parse(&format!("http://x{target}")).unwrap();
        let req = Req {
            method,
            path: u.path().to_string(),
            query: u.query_pairs().map(|(k, v)| (k.into_owned(), v.into_owned())).collect(),
            headers,
            body,
        };
        let (status, extra, body) = handle(&st, req);
        let reason = match status {
            200 => "OK",
            308 => "Resume Incomplete",
            404 => "Not Found",
            _ => "Error",
        };
        let mut resp = format!("HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\n", body.len());
        for (k, v) in extra {
            resp.push_str(&format!("{k}: {v}\r\n"));
        }
        resp.push_str("Content-Type: application/json\r\n\r\n");
        w.write_all(resp.as_bytes()).await?;
        w.write_all(&body).await?;
    }
}

fn json(v: serde_json::Value) -> (u16, Vec<(String, String)>, Vec<u8>) {
    (200, vec![], serde_json::to_vec(&v).unwrap())
}

fn not_found() -> (u16, Vec<(String, String)>, Vec<u8>) {
    (404, vec![], br#"{"error":{"message":"File not found","errors":[{"reason":"notFound"}]}}"#.to_vec())
}

/// Extract quoted values preceding " in parents", after "name = " and "mimeType = ".
fn parse_q(q: &str) -> (Vec<String>, Option<String>, Option<String>) {
    let mut parents = vec![];
    let mut name = None;
    let mut mime = None;
    let chars: Vec<char> = q.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\'' {
            let start = i;
            let mut s = String::new();
            i += 1;
            while i < chars.len() && chars[i] != '\'' {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                }
                s.push(chars[i]);
                i += 1;
            }
            let before: String = chars[..start].iter().collect();
            let after: String = chars[(i + 1).min(chars.len())..].iter().collect();
            if after.starts_with(" in parents") {
                parents.push(s);
            } else if before.trim_end().ends_with("name =") {
                name = Some(s);
            } else if before.trim_end().ends_with("mimeType =") {
                mime = Some(s);
            }
        }
        i += 1;
    }
    (parents, name, mime)
}

fn multipart(req: &Req) -> (serde_json::Value, Vec<u8>) {
    let ct = req.headers.get("content-type").cloned().unwrap_or_default();
    let boundary = ct.split("boundary=").nth(1).unwrap().to_string();
    let delim = format!("--{boundary}");
    let b = &req.body;
    let find = |hay: &[u8], needle: &[u8], from: usize| -> usize {
        (from..hay.len() - needle.len() + 1).find(|&i| &hay[i..i + needle.len()] == needle).unwrap()
    };
    let p1 = find(b, delim.as_bytes(), 0) + delim.len();
    let h1 = find(b, b"\r\n\r\n", p1) + 4;
    let e1 = find(b, delim.as_bytes(), h1);
    let meta: serde_json::Value = serde_json::from_slice(&b[h1..e1 - 2]).unwrap();
    let h2 = find(b, b"\r\n\r\n", e1) + 4;
    let end = find(b, format!("\r\n{delim}--").as_bytes(), h2);
    (meta, b[h2..end].to_vec())
}

fn apply_upload(s: &mut FakeState, existing: Option<String>, meta: &serde_json::Value, content: Vec<u8>) -> DriveFile {
    let mtime = meta.get("modifiedTime").and_then(|v| v.as_str()).map(String::from);
    let id = match existing {
        Some(id) => {
            s.set_content(&id, &content);
            id
        }
        None => {
            let parent = meta["parents"][0].as_str().unwrap_or(ROOT).to_string();
            let name = meta["name"].as_str().unwrap().to_string();
            s.add(&parent, &name, "application/octet-stream", &content, None)
        }
    };
    let (f, _) = s.files.get_mut(&id).unwrap();
    if let Some(m) = mtime {
        f.modified_time = Some(m);
    }
    let out = f.clone();
    *s.uploads.entry(id).or_default() += 1;
    out
}

fn handle(st: &Arc<Mutex<FakeState>>, req: Req) -> (u16, Vec<(String, String)>, Vec<u8>) {
    let mut s = st.lock().unwrap();
    let p = req.path.as_str();
    let m = req.method.as_str();
    if p == "/token" {
        return json(serde_json::json!({"access_token": "fake", "expires_in": 3600}));
    }
    if p == "/drive/v3/about" {
        return json(serde_json::json!({"user": {"emailAddress": "test@example.com"}, "storageQuota": {"usage": "1024"}}));
    }
    if p == "/drive/v3/changes/startPageToken" {
        return json(serde_json::json!({"startPageToken": s.seq.to_string()}));
    }
    if p == "/drive/v3/changes" {
        let from: u64 = req.query["pageToken"].parse().unwrap();
        let mut seen = std::collections::HashSet::new();
        let mut out = vec![];
        for (seq, id) in &s.changes {
            if *seq > from && seen.insert(id.clone()) {
                let f = &s.files[id].0;
                out.push(serde_json::json!({"fileId": id, "removed": false, "file": f}));
            }
        }
        return json(serde_json::json!({"changes": out, "newStartPageToken": s.seq.to_string()}));
    }
    if p == "/drive/v3/files" && m == "GET" {
        let (parents, name, mime) = parse_q(req.query.get("q").map(|s| s.as_str()).unwrap_or(""));
        let files: Vec<&DriveFile> = s
            .files
            .values()
            .map(|(f, _)| f)
            .filter(|f| !f.trashed && f.parents.iter().any(|x| parents.contains(x)))
            .filter(|f| name.as_ref().is_none_or(|n| &f.name == n))
            .filter(|f| mime.as_ref().is_none_or(|t| &f.mime_type == t))
            .collect();
        return json(serde_json::json!({"files": files}));
    }
    if p == "/drive/v3/files" && m == "POST" {
        let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
        let parent = v["parents"][0].as_str().unwrap().to_string();
        let id = s.add(&parent, v["name"].as_str().unwrap(), v["mimeType"].as_str().unwrap_or(""), b"", None);
        return json(serde_json::to_value(&s.files[&id].0).unwrap());
    }
    if let Some(id) = p.strip_prefix("/drive/v3/files/") {
        let id = if id == "root" { ROOT } else { id };
        if !s.files.contains_key(id) {
            return not_found();
        }
        let id = id.to_string();
        if m == "GET" && req.query.get("alt").map(|a| a.as_str()) == Some("media") {
            s.downloads += 1;
            return (200, vec![], s.files[&id].1.clone());
        }
        if m == "GET" {
            return json(serde_json::to_value(&s.files[&id].0).unwrap());
        }
        if m == "PATCH" {
            let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            let (f, _) = s.files.get_mut(&id).unwrap();
            if let Some(n) = v.get("name").and_then(|n| n.as_str()) {
                f.name = n.into();
            }
            if v.get("trashed").and_then(|t| t.as_bool()) == Some(true) {
                f.trashed = true;
            }
            if let Some(to) = req.query.get("addParents") {
                f.parents = vec![to.clone()];
            }
            s.touch(&id);
            return json(serde_json::to_value(&s.files[&id].0).unwrap());
        }
    }
    if p.starts_with("/upload/drive/v3/files") {
        let existing = p.strip_prefix("/upload/drive/v3/files/").map(String::from);
        match req.query.get("uploadType").map(|s| s.as_str()) {
            Some("multipart") => {
                let (meta, content) = multipart(&req);
                let f = apply_upload(&mut s, existing, &meta, content);
                return json(serde_json::to_value(&f).unwrap());
            }
            Some("resumable") => {
                let meta: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let size: u64 = req.headers["x-upload-content-length"].parse().unwrap();
                let sid = s.new_id();
                s.sessions.insert(sid.clone(), (existing, meta, vec![], size));
                return (200, vec![("Location".into(), format!("{}/session/{sid}", fake().url))], b"{}".to_vec());
            }
            _ => {}
        }
    }
    if let Some(sid) = p.strip_prefix("/session/") {
        let sid = sid.to_string();
        let range = req.headers.get("content-range").cloned().unwrap_or_default();
        let (_, _, buf, size) = s.sessions.get_mut(&sid).unwrap();
        let size = *size;
        if !range.starts_with("bytes */") {
            buf.extend_from_slice(&req.body);
        }
        let have = buf.len() as u64;
        if have < size {
            return (308, vec![("Range".into(), format!("bytes=0-{}", have - 1))], vec![]);
        }
        let (existing, meta, buf, _) = s.sessions.remove(&sid).unwrap();
        let f = apply_upload(&mut s, existing, &meta, buf);
        return json(serde_json::to_value(&f).unwrap());
    }
    (400, vec![], format!("unhandled {m} {p}").into_bytes())
}
