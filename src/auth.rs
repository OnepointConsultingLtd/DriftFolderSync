//! OAuth 2.0 for installed applications: browser sign-in using the loopback
//! redirect flow with PKCE, plus access-token refresh.
//!
//! Each `[[account]]` has its own refresh token, so different directory
//! trees can be synced with different Google accounts.

use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::config::Account;
use crate::secrets;

const AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
fn token_url() -> String {
    let e = crate::drive::endpoint();
    if e.starts_with("https://www.googleapis.com") {
        "https://oauth2.googleapis.com/token".into()
    } else {
        format!("{e}/token")
    }
}
const REVOKE_URL: &str = "https://oauth2.googleapis.com/revoke";
/// Full Drive access is required to mirror existing files and folders.
pub const SCOPE: &str = "https://www.googleapis.com/auth/drive";

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("account '{0}' is not signed in — run: driftfoldersync account login {0}")]
    NotLoggedIn(String),
    #[error("sign-in for account '{0}' has expired or was revoked — run: driftfoldersync account login {0}")]
    Expired(String),
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    #[serde(default)]
    refresh_token: Option<String>,
}

#[derive(Deserialize)]
struct TokenError {
    error: String,
    #[serde(default)]
    error_description: String,
}

/// Opens a URL in the user's browser (portal first, then xdg-open).
pub async fn open_url(url: &str) -> bool {
    if let Ok(uri) = ashpd::Uri::parse(url) {
        let r = ashpd::desktop::open_uri::OpenFileRequest::default()
            .ask(false)
            .send_uri(&uri)
            .await;
        if r.is_ok() {
            return true;
        }
    }
    tokio::process::Command::new("xdg-open")
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Interactive sign-in. Returns the refresh token.
pub async fn browser_login(http: &reqwest::Client, acct: &Account) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let redirect = format!("http://127.0.0.1:{port}");

    let verifier = crate::util::random_token(48);
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(Sha256::digest(verifier.as_bytes()));
    let state = crate::util::random_token(16);

    let mut url = url::Url::parse(AUTH_URL)?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("client_id", &acct.client_id)
            .append_pair("redirect_uri", &redirect)
            .append_pair("response_type", "code")
            .append_pair("scope", SCOPE)
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("state", &state)
            .append_pair("access_type", "offline")
            .append_pair("prompt", "consent select_account");
        if !acct.email.is_empty() {
            q.append_pair("login_hint", &acct.email);
        }
    }

    println!("Opening your browser to sign in to account '{}'.", acct.name);
    println!("If it does not open, visit this URL:\n\n{url}\n");
    if !open_url(url.as_str()).await {
        warn!("could not launch a browser automatically");
    }

    let code = tokio::time::timeout(Duration::from_secs(600), wait_for_code(&listener, &state))
        .await
        .map_err(|_| anyhow!("timed out waiting for browser sign-in"))??;

    let mut form = vec![
        ("code", code.as_str()),
        ("client_id", acct.client_id.as_str()),
        ("redirect_uri", redirect.as_str()),
        ("grant_type", "authorization_code"),
        ("code_verifier", verifier.as_str()),
    ];
    if !acct.client_secret.is_empty() {
        form.push(("client_secret", acct.client_secret.as_str()));
    }
    let resp = http.post(token_url()).form(&form).send().await?;
    if !resp.status().is_success() {
        let e: TokenError = resp.json().await.unwrap_or(TokenError {
            error: "unknown".into(),
            error_description: String::new(),
        });
        bail!("token exchange failed: {} {}", e.error, e.error_description);
    }
    let tr: TokenResponse = resp.json().await?;
    tr.refresh_token
        .ok_or_else(|| anyhow!("Google did not return a refresh token; remove the app's access at https://myaccount.google.com/permissions and try again"))
}

async fn wait_for_code(listener: &TcpListener, state: &str) -> Result<String> {
    loop {
        let (mut sock, _) = listener.accept().await?;
        let mut buf = vec![0u8; 8192];
        let n = sock.read(&mut buf).await.unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]);
        let path = req
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_string();
        let url = url::Url::parse(&format!("http://localhost{path}"))?;
        let mut code = None;
        let mut got_state = None;
        let mut error = None;
        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "code" => code = Some(v.into_owned()),
                "state" => got_state = Some(v.into_owned()),
                "error" => error = Some(v.into_owned()),
                _ => {}
            }
        }
        if code.is_none() && error.is_none() {
            // favicon or similar; ignore
            let _ = sock
                .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                .await;
            continue;
        }
        let (ok, msg) = match (&code, &error, got_state.as_deref() == Some(state)) {
            (Some(_), _, true) => (true, "Sign-in complete. You can close this tab and return to DriftFolderSync."),
            (_, Some(_), _) => (false, "Sign-in was cancelled or refused."),
            _ => (false, "Sign-in failed: state mismatch."),
        };
        let body = format!(
            "<!doctype html><html><head><meta charset=utf-8><title>DriftFolderSync</title></head>\
             <body style=\"font-family:sans-serif;margin:3em\"><h2>DriftFolderSync</h2><p>{msg}</p></body></html>"
        );
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(resp.as_bytes()).await;
        let _ = sock.shutdown().await;
        if ok {
            return Ok(code.unwrap());
        }
        bail!("{msg} {}", error.unwrap_or_default());
    }
}

pub async fn revoke(http: &reqwest::Client, refresh_token: &str) -> Result<()> {
    http.post(REVOKE_URL)
        .form(&[("token", refresh_token)])
        .send()
        .await?;
    Ok(())
}

/// Supplies valid access tokens for one account, refreshing as needed.
pub struct TokenProvider {
    account: Account,
    http: reqwest::Client,
    state: Mutex<Option<(String, Instant)>>,
}

impl TokenProvider {
    pub fn new(account: Account, http: reqwest::Client) -> Self {
        Self { account, http, state: Mutex::new(None) }
    }

    pub async fn access_token(&self) -> Result<String> {
        let mut st = self.state.lock().await;
        if let Some((tok, exp)) = st.as_ref() {
            if Instant::now() + Duration::from_secs(60) < *exp {
                return Ok(tok.clone());
            }
        }
        let refresh = secrets::load(&self.account.name)
            .await?
            .ok_or_else(|| AuthError::NotLoggedIn(self.account.name.clone()))?;
        let mut form = vec![
            ("refresh_token", refresh.as_str()),
            ("client_id", self.account.client_id.as_str()),
            ("grant_type", "refresh_token"),
        ];
        if !self.account.client_secret.is_empty() {
            form.push(("client_secret", self.account.client_secret.as_str()));
        }
        let resp = self
            .http
            .post(token_url())
            .form(&form)
            .send()
            .await
            .context("contacting Google token endpoint")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let e: TokenError = resp.json().await.unwrap_or(TokenError {
                error: format!("http {status}"),
                error_description: String::new(),
            });
            if e.error == "invalid_grant" || e.error == "unauthorized_client" {
                return Err(AuthError::Expired(self.account.name.clone()).into());
            }
            bail!("token refresh failed: {} {}", e.error, e.error_description);
        }
        let tr: TokenResponse = resp.json().await?;
        if let Some(r) = tr.refresh_token {
            // Rotated refresh token.
            secrets::store(&self.account.name, &r).await?;
        }
        info!(account = %self.account.name, "access token refreshed");
        let exp = Instant::now() + Duration::from_secs(tr.expires_in);
        *st = Some((tr.access_token.clone(), exp));
        Ok(tr.access_token)
    }

    /// Forget the cached access token (e.g. after a 401).
    pub async fn invalidate(&self) {
        *self.state.lock().await = None;
    }
}
