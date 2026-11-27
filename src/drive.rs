//! Minimal Drive API v3 client: only the public, documented REST API is used.
//!
//! Every request goes through a per-account token bucket and is retried with
//! exponential back-off on rate-limit (403 rateLimitExceeded /
//! userRateLimitExceeded, 429) and transient (5xx, network) errors, as
//! recommended by Google's API usage guidelines.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use md5::{Digest, Md5};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tracing::{debug, warn};

use crate::auth::TokenProvider;
use crate::limiter::{backoff, Bandwidth, TokenBucket};

static ENDPOINT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Base URL of the Google APIs. Overridable (DRIFTFOLDERSYNC_API_ENDPOINT) only so
/// that the test-suite can run against a local fake server.
pub fn endpoint() -> &'static str {
    ENDPOINT.get_or_init(|| {
        std::env::var("DRIFTFOLDERSYNC_API_ENDPOINT").unwrap_or_else(|_| "https://www.googleapis.com".into())
    })
}
#[cfg(test)]
pub fn set_endpoint(url: String) {
    let _ = ENDPOINT.set(url);
}
fn api() -> String {
    format!("{}/drive/v3", endpoint())
}
fn upload_base() -> String {
    format!("{}/upload/drive/v3", endpoint())
}
pub const FOLDER_MIME: &str = "application/vnd.google-apps.folder";
pub const SHORTCUT_MIME: &str = "application/vnd.google-apps.shortcut";
const FIELDS: &str =
    "id,name,mimeType,parents,md5Checksum,size,modifiedTime,trashed,driveId,webViewLink";
const MAX_ATTEMPTS: u32 = 8;
const SIMPLE_UPLOAD_LIMIT: u64 = 5 * 1024 * 1024;
/// Must be a multiple of 256 KiB.
const CHUNK: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DriveFile {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub mime_type: String,
    #[serde(default)]
    pub parents: Vec<String>,
    #[serde(default)]
    pub md5_checksum: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub modified_time: Option<String>,
    #[serde(default)]
    pub trashed: bool,
    #[serde(default)]
    pub drive_id: Option<String>,
    #[serde(default)]
    pub web_view_link: Option<String>,
}

impl DriveFile {
    pub fn is_folder(&self) -> bool {
        self.mime_type == FOLDER_MIME
    }
    pub fn is_native_doc(&self) -> bool {
        self.mime_type.starts_with("application/vnd.google-apps.") && !self.is_folder()
    }
    pub fn size_u64(&self) -> u64 {
        self.size.as_deref().and_then(|s| s.parse().ok()).unwrap_or(0)
    }
    pub fn mtime_ns(&self) -> i64 {
        self.modified_time.as_deref().and_then(crate::util::rfc3339_to_ns).unwrap_or(0)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FileList {
    #[serde(default)]
    files: Vec<DriveFile>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Change {
    pub file_id: Option<String>,
    #[serde(default)]
    pub removed: bool,
    pub file: Option<DriveFile>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChangeList {
    #[serde(default)]
    changes: Vec<Change>,
    next_page_token: Option<String>,
    new_start_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct About {
    pub user: AboutUser,
    #[serde(default)]
    pub storage_quota: Option<Quota>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AboutUser {
    #[serde(default)]
    pub email_address: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Quota {
    pub limit: Option<String>,
    pub usage: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SharedDrive {
    id: String,
    name: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Drive API error {status}: {reason} — {message}")]
pub struct ApiError {
    pub status: u16,
    pub reason: String,
    pub message: String,
}

impl ApiError {
    pub fn is_not_found(&self) -> bool {
        self.status == 404
    }
    pub fn is_abuse(&self) -> bool {
        self.reason == "cannotDownloadAbusiveFile"
    }
    pub fn is_quota(&self) -> bool {
        self.reason == "storageQuotaExceeded" || self.reason == "quotaExceeded"
    }
    fn is_rate_limit(&self) -> bool {
        self.status == 429
            || (self.status == 403
                && matches!(
                    self.reason.as_str(),
                    "rateLimitExceeded" | "userRateLimitExceeded" | "sharingRateLimitExceeded"
                ))
    }
    fn is_transient(&self) -> bool {
        self.is_rate_limit() || self.status >= 500 || self.status == 408
    }
}

pub fn api_error(e: &anyhow::Error) -> Option<&ApiError> {
    e.downcast_ref::<ApiError>()
}

async fn parse_error(resp: reqwest::Response) -> ApiError {
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    #[derive(Deserialize)]
    struct Outer {
        error: Inner,
    }
    #[derive(Deserialize)]
    struct Inner {
        #[serde(default)]
        message: String,
        #[serde(default)]
        errors: Vec<Detail>,
    }
    #[derive(Deserialize)]
    struct Detail {
        #[serde(default)]
        reason: String,
    }
    match serde_json::from_str::<Outer>(&text) {
        Ok(o) => ApiError {
            status,
            reason: o.error.errors.first().map(|d| d.reason.clone()).unwrap_or_default(),
            message: o.error.message,
        },
        Err(_) => ApiError { status, reason: String::new(), message: text.chars().take(300).collect() },
    }
}

/// Native Google document types and how they are represented locally.
pub struct NativeType {
    pub mime: &'static str,
    pub link_ext: &'static str,
    pub export_mime: Option<&'static str>,
    pub export_ext: &'static str,
}

pub const NATIVE_TYPES: &[NativeType] = &[
    NativeType { mime: "application/vnd.google-apps.document", link_ext: ".gdoc",
        export_mime: Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"), export_ext: ".docx" },
    NativeType { mime: "application/vnd.google-apps.spreadsheet", link_ext: ".gsheet",
        export_mime: Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"), export_ext: ".xlsx" },
    NativeType { mime: "application/vnd.google-apps.presentation", link_ext: ".gslides",
        export_mime: Some("application/vnd.openxmlformats-officedocument.presentationml.presentation"), export_ext: ".pptx" },
    NativeType { mime: "application/vnd.google-apps.drawing", link_ext: ".gdraw",
        export_mime: Some("image/svg+xml"), export_ext: ".svg" },
    NativeType { mime: "application/vnd.google-apps.form", link_ext: ".gform", export_mime: None, export_ext: "" },
    NativeType { mime: "application/vnd.google-apps.jam", link_ext: ".gjam", export_mime: Some("application/pdf"), export_ext: ".pdf" },
    NativeType { mime: "application/vnd.google-apps.map", link_ext: ".gmap", export_mime: None, export_ext: "" },
    NativeType { mime: "application/vnd.google-apps.site", link_ext: ".gsite", export_mime: None, export_ext: "" },
    NativeType { mime: "application/vnd.google-apps.script", link_ext: ".gscript",
        export_mime: Some("application/vnd.google-apps.script+json"), export_ext: ".json" },
];

pub fn native_type(mime: &str) -> Option<&'static NativeType> {
    NATIVE_TYPES.iter().find(|t| t.mime == mime)
}

pub struct DriveClient {
    http: reqwest::Client,
    tokens: Arc<TokenProvider>,
    bucket: Arc<TokenBucket>,
    bandwidth: Bandwidth,
}

pub type Progress<'a> = &'a (dyn Fn(u64) + Send + Sync);

fn escape_q(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

impl DriveClient {
    pub fn new(
        http: reqwest::Client,
        tokens: Arc<TokenProvider>,
        bucket: Arc<TokenBucket>,
        bandwidth: Bandwidth,
    ) -> Self {
        Self { http, tokens, bucket, bandwidth }
    }

    /// Sends a request built by `mk`, with rate limiting, auth and retries.
    async fn execute<F>(&self, mk: F) -> Result<reqwest::Response>
    where
        F: Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    {
        let mut attempt = 0;
        let mut reauthed = false;
        loop {
            self.bucket.acquire(1.0).await;
            let token = self.tokens.access_token().await?;
            let res = mk(&self.http).bearer_auth(&token).send().await;
            match res {
                Ok(r) if r.status().is_success() || r.status() == StatusCode::PERMANENT_REDIRECT => {
                    return Ok(r)
                }
                Ok(r) if r.status() == StatusCode::UNAUTHORIZED && !reauthed => {
                    reauthed = true;
                    self.tokens.invalidate().await;
                }
                Ok(r) => {
                    let retry_after = r
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(Duration::from_secs);
                    let e = parse_error(r).await;
                    if e.is_transient() && attempt < MAX_ATTEMPTS {
                        let d = retry_after.unwrap_or_else(|| backoff(attempt));
                        if e.is_rate_limit() {
                            warn!("rate limited by Drive ({}); backing off {:?}", e.reason, d);
                            self.bucket.block_for(d).await;
                        } else {
                            debug!("transient error {}; retrying in {:?}", e.status, d);
                            tokio::time::sleep(d).await;
                        }
                        attempt += 1;
                        continue;
                    }
                    return Err(e.into());
                }
                Err(e) if attempt < MAX_ATTEMPTS && !e.is_builder() => {
                    let d = backoff(attempt);
                    debug!("network error {e}; retrying in {:?}", d);
                    tokio::time::sleep(d).await;
                    attempt += 1;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub async fn about(&self) -> Result<About> {
        let r = self
            .execute(|c| c.get(format!("{}/about", api())).query(&[("fields", "user(emailAddress,displayName),storageQuota(limit,usage)")]))
            .await?;
        Ok(r.json().await?)
    }

    pub async fn get(&self, id: &str) -> Result<DriveFile> {
        let r = self
            .execute(|c| {
                c.get(format!("{}/files/{id}", api()))
                    .query(&[("fields", FIELDS), ("supportsAllDrives", "true")])
            })
            .await?;
        Ok(r.json().await?)
    }

    pub async fn find_shared_drive(&self, name_or_id: &str) -> Result<String> {
        let mut page: Option<String> = None;
        loop {
            let p = page.clone();
            let r = self
                .execute(|c| {
                    let mut q = c
                        .get(format!("{}/drives", api()))
                        .query(&[("pageSize", "100"), ("fields", "nextPageToken,drives(id,name)")]);
                    if let Some(p) = &p {
                        q = q.query(&[("pageToken", p)]);
                    }
                    q
                })
                .await?;
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct L {
                #[serde(default)]
                drives: Vec<SharedDrive>,
                next_page_token: Option<String>,
            }
            let l: L = r.json().await?;
            if let Some(d) = l.drives.iter().find(|d| d.id == name_or_id || d.name == name_or_id) {
                return Ok(d.id.clone());
            }
            match l.next_page_token {
                Some(t) => page = Some(t),
                None => bail!("shared drive '{name_or_id}' not found"),
            }
        }
    }

    /// Resolve (and optionally create) a folder path under `root_id`.
    pub async fn resolve_folder(&self, root_id: &str, path: &str, create: bool) -> Result<String> {
        let mut cur = root_id.to_string();
        for comp in path.split('/').filter(|c| !c.is_empty()) {
            let q = format!(
                "name = '{}' and '{}' in parents and mimeType = '{}' and trashed = false",
                escape_q(comp),
                escape_q(&cur),
                FOLDER_MIME
            );
            let r = self
                .execute(|c| {
                    c.get(format!("{}/files", api())).query(&[
                        ("q", q.as_str()),
                        ("fields", "files(id,name)"),
                        ("supportsAllDrives", "true"),
                        ("includeItemsFromAllDrives", "true"),
                    ])
                })
                .await?;
            let l: FileList = r.json().await?;
            if l.files.len() > 1 {
                bail!(
                    "there are {} folders named '{comp}' in the same place in the cloud; \
                     rename or remove the duplicates so the sync target is unambiguous",
                    l.files.len()
                );
            }
            cur = match l.files.first() {
                Some(f) => f.id.clone(),
                None if create => self.create_folder(comp, &cur).await?.id,
                None => bail!("remote folder '{path}' does not exist"),
            };
        }
        Ok(cur)
    }

    /// Sub-folders directly inside `parent` (for the folder browser).
    pub async fn list_folders(&self, parent: &str, drive_id: Option<&str>) -> Result<Vec<DriveFile>> {
        let q = format!("'{}' in parents and mimeType = '{}' and trashed = false", escape_q(parent), FOLDER_MIME);
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let p = page.clone();
            let fields = format!("nextPageToken,files({FIELDS})");
            let r = self
                .execute(|c| {
                    let mut rq = c.get(format!("{}/files", api())).query(&[
                        ("q", q.as_str()),
                        ("fields", fields.as_str()),
                        ("pageSize", "1000"),
                        ("orderBy", "name"),
                        ("supportsAllDrives", "true"),
                        ("includeItemsFromAllDrives", "true"),
                    ]);
                    if let Some(d) = drive_id {
                        rq = rq.query(&[("corpora", "drive"), ("driveId", d)]);
                    }
                    if let Some(p) = &p {
                        rq = rq.query(&[("pageToken", p)]);
                    }
                    rq
                })
                .await?;
            let l: FileList = r.json().await?;
            out.extend(l.files);
            match l.next_page_token {
                Some(t) => page = Some(t),
                None => return Ok(out),
            }
        }
    }

    /// Shared drives the user can see: (id, name).
    pub async fn shared_drives(&self) -> Result<Vec<(String, String)>> {
        let mut out = Vec::new();
        let mut page: Option<String> = None;
        loop {
            let p = page.clone();
            let r = self
                .execute(|c| {
                    let mut q = c
                        .get(format!("{}/drives", api()))
                        .query(&[("pageSize", "100"), ("fields", "nextPageToken,drives(id,name)")]);
                    if let Some(p) = &p {
                        q = q.query(&[("pageToken", p)]);
                    }
                    q
                })
                .await?;
            #[derive(Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct L {
                #[serde(default)]
                drives: Vec<SharedDrive>,
                next_page_token: Option<String>,
            }
            let l: L = r.json().await?;
            out.extend(l.drives.into_iter().map(|d| (d.id, d.name)));
            match l.next_page_token {
                Some(t) => page = Some(t),
                None => return Ok(out),
            }
        }
    }

    /// List all non-trashed descendants of `root` (breadth first, batching
    /// up to 40 parents per query to minimise API calls — like rclone's
    /// --fast-list).
    pub async fn list_tree(
        &self,
        root: &str,
        drive_id: Option<&str>,
        mut on_page: impl FnMut(usize),
    ) -> Result<Vec<DriveFile>> {
        let mut out = Vec::new();
        let mut queue = vec![root.to_string()];
        while !queue.is_empty() {
            let batch: Vec<String> = queue.drain(..queue.len().min(40)).collect();
            let parents = batch
                .iter()
                .map(|id| format!("'{}' in parents", escape_q(id)))
                .collect::<Vec<_>>()
                .join(" or ");
            let q = format!("({parents}) and trashed = false");
            let mut page: Option<String> = None;
            loop {
                let p = page.clone();
                let fields = format!("nextPageToken,files({FIELDS})");
                let r = self
                    .execute(|c| {
                        let mut rq = c.get(format!("{}/files", api())).query(&[
                            ("q", q.as_str()),
                            ("fields", fields.as_str()),
                            ("pageSize", "1000"),
                            ("supportsAllDrives", "true"),
                            ("includeItemsFromAllDrives", "true"),
                        ]);
                        if let Some(d) = drive_id {
                            rq = rq.query(&[("corpora", "drive"), ("driveId", d)]);
                        }
                        if let Some(p) = &p {
                            rq = rq.query(&[("pageToken", p)]);
                        }
                        rq
                    })
                    .await?;
                let l: FileList = r.json().await?;
                for f in &l.files {
                    if f.is_folder() {
                        queue.push(f.id.clone());
                    }
                }
                out.extend(l.files);
                on_page(out.len());
                match l.next_page_token {
                    Some(t) => page = Some(t),
                    None => break,
                }
            }
        }
        Ok(out)
    }

    pub async fn start_page_token(&self, drive_id: Option<&str>) -> Result<String> {
        let r = self
            .execute(|c| {
                let mut q = c
                    .get(format!("{}/changes/startPageToken", api()))
                    .query(&[("supportsAllDrives", "true")]);
                if let Some(d) = drive_id {
                    q = q.query(&[("driveId", d)]);
                }
                q
            })
            .await?;
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct T {
            start_page_token: String,
        }
        Ok(r.json::<T>().await?.start_page_token)
    }

    /// Fetch all changes since `token`. Returns (changes, new token).
    pub async fn changes(&self, token: &str, drive_id: Option<&str>) -> Result<(Vec<Change>, String)> {
        let mut out = Vec::new();
        let mut page = token.to_string();
        let fields = format!("nextPageToken,newStartPageToken,changes(fileId,removed,file({FIELDS}))");
        loop {
            let p = page.clone();
            let r = self
                .execute(|c| {
                    let mut q = c.get(format!("{}/changes", api())).query(&[
                        ("pageToken", p.as_str()),
                        ("pageSize", "1000"),
                        ("fields", fields.as_str()),
                        ("supportsAllDrives", "true"),
                        ("includeItemsFromAllDrives", "true"),
                        ("includeRemoved", "true"),
                    ]);
                    if let Some(d) = drive_id {
                        q = q.query(&[("driveId", d)]);
                    }
                    q
                })
                .await?;
            let l: ChangeList = r.json().await?;
            out.extend(l.changes);
            if let Some(t) = l.new_start_page_token {
                return Ok((out, t));
            }
            page = l.next_page_token.ok_or_else(|| anyhow!("changes: missing page token"))?;
        }
    }

    pub async fn create_folder(&self, name: &str, parent: &str) -> Result<DriveFile> {
        let body = json!({"name": name, "mimeType": FOLDER_MIME, "parents": [parent]});
        let r = self
            .execute(|c| {
                c.post(format!("{}/files", api()))
                    .query(&[("fields", FIELDS), ("supportsAllDrives", "true")])
                    .json(&body)
            })
            .await?;
        Ok(r.json().await?)
    }

    /// Rename and/or move.
    pub async fn update_metadata(
        &self,
        id: &str,
        new_name: Option<&str>,
        move_from: Option<&str>,
        move_to: Option<&str>,
    ) -> Result<DriveFile> {
        let mut body = serde_json::Map::new();
        if let Some(n) = new_name {
            body.insert("name".into(), json!(n));
        }
        let r = self
            .execute(|c| {
                let mut q = c
                    .patch(format!("{}/files/{id}", api()))
                    .query(&[("fields", FIELDS), ("supportsAllDrives", "true")]);
                if let (Some(from), Some(to)) = (move_from, move_to) {
                    q = q.query(&[("addParents", to), ("removeParents", from)]);
                }
                q.json(&body)
            })
            .await?;
        Ok(r.json().await?)
    }

    pub async fn trash(&self, id: &str) -> Result<()> {
        let body = json!({"trashed": true});
        match self
            .execute(|c| {
                c.patch(format!("{}/files/{id}", api()))
                    .query(&[("fields", "id"), ("supportsAllDrives", "true")])
                    .json(&body)
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if api_error(&e).is_some_and(|a| a.is_not_found()) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Download file content to `dest`, verifying the MD5 if Drive supplies one.
    pub async fn download(
        &self,
        f: &DriveFile,
        dest: &Path,
        acknowledge_abuse: bool,
        progress: Progress<'_>,
    ) -> Result<()> {
        let id = f.id.clone();
        let ack = if acknowledge_abuse { "true" } else { "false" };
        let resp = self
            .execute(|c| {
                c.get(format!("{}/files/{id}", api()))
                    .query(&[("alt", "media"), ("supportsAllDrives", "true"), ("acknowledgeAbuse", ack)])
            })
            .await?;
        let hash = self.stream_to_file(resp, dest, progress).await?;
        if let Some(expected) = &f.md5_checksum {
            if &hash != expected {
                let _ = tokio::fs::remove_file(dest).await;
                bail!("checksum mismatch downloading '{}'", f.name);
            }
        }
        Ok(())
    }

    pub async fn export(&self, id: &str, mime: &str, dest: &Path, progress: Progress<'_>) -> Result<()> {
        let resp = self
            .execute(|c| {
                c.get(format!("{}/files/{id}/export", api())).query(&[("mimeType", mime)])
            })
            .await?;
        self.stream_to_file(resp, dest, progress).await?;
        Ok(())
    }

    async fn stream_to_file(&self, resp: reqwest::Response, dest: &Path, progress: Progress<'_>) -> Result<String> {
        let mut file = tokio::fs::File::create(dest)
            .await
            .with_context(|| format!("creating {}", dest.display()))?;
        let mut h = Md5::new();
        let mut stream = resp.bytes_stream();
        let mut total = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            self.bandwidth.down.acquire(chunk.len() as f64).await;
            h.update(&chunk);
            file.write_all(&chunk).await?;
            total += chunk.len() as u64;
            progress(total);
        }
        file.flush().await?;
        file.sync_all().await?;
        Ok(crate::util::hex(&h.finalize()))
    }

    /// Upload a new file (`existing = None`) or new content for an existing
    /// file. `modified_ns` sets Drive's modifiedTime to the local mtime.
    pub async fn upload(
        &self,
        src: &Path,
        name: &str,
        parent: Option<&str>,
        existing: Option<&str>,
        modified_ns: i64,
        progress: Progress<'_>,
    ) -> Result<DriveFile> {
        let size = tokio::fs::metadata(src).await?.len();
        let mut meta = serde_json::Map::new();
        meta.insert("modifiedTime".into(), json!(crate::util::ns_to_rfc3339(modified_ns)));
        if existing.is_none() {
            meta.insert("name".into(), json!(name));
            if let Some(p) = parent {
                meta.insert("parents".into(), json!([p]));
            }
        }
        let meta = serde_json::Value::Object(meta);
        let (method, url) = match existing {
            Some(id) => (Method::PATCH, format!("{}/files/{id}", upload_base())),
            None => (Method::POST, format!("{}/files", upload_base())),
        };

        if size <= SIMPLE_UPLOAD_LIMIT {
            let data = tokio::fs::read(src).await?;
            self.bandwidth.up.acquire(data.len() as f64).await;
            let boundary = format!("driftfoldersync{}", crate::util::random_token(12));
            let mut body = Vec::with_capacity(data.len() + 512);
            body.extend_from_slice(
                format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{meta}\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n")
                    .as_bytes(),
            );
            body.extend_from_slice(&data);
            body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
            let body = bytes::Bytes::from(body);
            let r = self
                .execute(|c| {
                    c.request(method.clone(), &url)
                        .query(&[("uploadType", "multipart"), ("fields", FIELDS), ("supportsAllDrives", "true")])
                        .header("Content-Type", format!("multipart/related; boundary={boundary}"))
                        .body(body.clone())
                })
                .await?;
            progress(size);
            return Ok(r.json().await?);
        }

        // Resumable upload.
        let mut session = self.start_resumable(&method, &url, &meta, size).await?;
        let mut file = tokio::fs::File::open(src).await?;
        let mut offset = 0u64;
        let mut buf = vec![0u8; CHUNK];
        let mut failures = 0u32;
        loop {
            let len = ((size - offset) as usize).min(CHUNK);
            file.seek(std::io::SeekFrom::Start(offset)).await?;
            file.read_exact(&mut buf[..len]).await?;
            self.bandwidth.up.acquire(len as f64).await;
            self.bucket.acquire(1.0).await;
            let token = self.tokens.access_token().await?;
            let end = offset + len as u64;
            let res = self
                .http
                .put(&session)
                .bearer_auth(&token)
                .header("Content-Length", len.to_string())
                .header("Content-Range", format!("bytes {offset}-{}/{size}", end - 1))
                .body(bytes::Bytes::copy_from_slice(&buf[..len]))
                .send()
                .await;
            match res {
                Ok(r) if r.status().is_success() => {
                    progress(size);
                    return Ok(r.json().await?);
                }
                Ok(r) if r.status() == StatusCode::PERMANENT_REDIRECT => {
                    // No Range header means nothing has been persisted yet.
                    offset = parse_range(r.headers()).unwrap_or(0);
                    failures = 0;
                    progress(offset);
                }
                Ok(r) if r.status() == StatusCode::NOT_FOUND || r.status() == StatusCode::GONE => {
                    // Session expired: start again.
                    if failures > 3 {
                        return Err(parse_error(r).await.into());
                    }
                    failures += 1;
                    session = self.start_resumable(&method, &url, &meta, size).await?;
                    offset = 0;
                }
                Ok(r) => {
                    let e = parse_error(r).await;
                    if !e.is_transient() || failures >= MAX_ATTEMPTS {
                        return Err(e.into());
                    }
                    if e.is_rate_limit() {
                        self.bucket.block_for(backoff(failures)).await;
                    } else {
                        tokio::time::sleep(backoff(failures)).await;
                    }
                    failures += 1;
                    offset = self.query_resumable(&session, size).await?.unwrap_or(offset);
                }
                Err(e) => {
                    if failures >= MAX_ATTEMPTS {
                        return Err(e.into());
                    }
                    tokio::time::sleep(backoff(failures)).await;
                    failures += 1;
                    match self.query_resumable(&session, size).await? {
                        Some(o) => offset = o,
                        None => {
                            // Completed, but the response was lost.
                            match existing {
                                Some(id) => return self.get(id).await,
                                None => bail!("upload of '{name}' completed but the response was lost; it will be reconciled on the next cycle"),
                            }
                        }
                    }
                }
            }
        }
    }

    async fn start_resumable(&self, method: &Method, url: &str, meta: &serde_json::Value, size: u64) -> Result<String> {
        let r = self
            .execute(|c| {
                c.request(method.clone(), url)
                    .query(&[("uploadType", "resumable"), ("fields", FIELDS), ("supportsAllDrives", "true")])
                    .header("X-Upload-Content-Type", "application/octet-stream")
                    .header("X-Upload-Content-Length", size.to_string())
                    .json(meta)
            })
            .await?;
        r.headers()
            .get("location")
            .and_then(|v| v.to_str().ok())
            .map(String::from)
            .ok_or_else(|| anyhow!("resumable upload: no session URL"))
    }

    /// Ask how much of a resumable upload the server has. `None` = complete.
    async fn query_resumable(&self, session: &str, size: u64) -> Result<Option<u64>> {
        let token = self.tokens.access_token().await?;
        let r = self
            .http
            .put(session)
            .bearer_auth(&token)
            .header("Content-Length", "0")
            .header("Content-Range", format!("bytes */{size}"))
            .send()
            .await?;
        if r.status().is_success() {
            return Ok(None);
        }
        if r.status() == StatusCode::PERMANENT_REDIRECT {
            return Ok(Some(parse_range(r.headers()).unwrap_or(0)));
        }
        Err(parse_error(r).await.into())
    }
}

fn parse_range(h: &reqwest::header::HeaderMap) -> Option<u64> {
    let v = h.get("range")?.to_str().ok()?; // "bytes=0-12345"
    let end: u64 = v.rsplit('-').next()?.parse().ok()?;
    Some(end + 1)
}
