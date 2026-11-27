//! Shared, observable status of every sync folder (used by IPC and tray).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum State {
    #[default]
    Starting,
    Idle,
    Syncing,
    Paused,
    Offline,
    Error,
    AuthRequired,
}

impl State {
    pub fn label(self) -> &'static str {
        match self {
            State::Starting => "Starting",
            State::Idle => "Up to date",
            State::Syncing => "Syncing",
            State::Paused => "Paused",
            State::Offline => "Offline — will retry",
            State::Error => "Error",
            State::AuthRequired => "Sign-in required",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PairStatus {
    pub name: String,
    pub account: String,
    pub email: String,
    pub storage: String,
    pub local: String,
    pub remote_url: String,
    pub state: State,
    pub message: String,
    pub sync_interval: String,
    pub last_sync: Option<String>,
    pub pending: usize,
    pub uploaded: u64,
    pub downloaded: u64,
    pub blocked_deletes: usize,
    /// Completed sync cycles (for tests and diagnostics).
    pub cycles: u64,
    pub recent: VecDeque<String>,
    pub errors: VecDeque<String>,
}

#[derive(Clone)]
pub struct StatusHub {
    inner: Arc<Mutex<BTreeMap<String, PairStatus>>>,
    tx: watch::Sender<u64>,
}

impl Default for StatusHub {
    fn default() -> Self {
        Self::new()
    }
}

impl StatusHub {
    pub fn new() -> Self {
        let (tx, _) = watch::channel(0);
        Self { inner: Arc::new(Mutex::new(BTreeMap::new())), tx }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.tx.subscribe()
    }

    pub fn update(&self, pair: &str, f: impl FnOnce(&mut PairStatus)) {
        {
            let mut m = self.inner.lock().unwrap();
            let st = m.entry(pair.to_string()).or_default();
            st.name = pair.to_string();
            f(st);
        }
        self.tx.send_modify(|v| *v = v.wrapping_add(1));
    }

    pub fn remove(&self, pair: &str) {
        self.inner.lock().unwrap().remove(pair);
        self.tx.send_modify(|v| *v = v.wrapping_add(1));
    }

    pub fn snapshot(&self) -> Vec<PairStatus> {
        self.inner.lock().unwrap().values().cloned().collect()
    }

    pub fn activity(&self, pair: &str, msg: String) {
        tracing::info!(pair, "{msg}");
        self.update(pair, |s| {
            let stamp = chrono::Local::now().format("%H:%M");
            s.recent.push_front(format!("{stamp} {msg}"));
            s.recent.truncate(25);
        });
    }

    pub fn error(&self, pair: &str, msg: String) {
        tracing::warn!(pair, "{msg}");
        self.update(pair, |s| {
            let stamp = chrono::Local::now().format("%H:%M");
            s.errors.push_front(format!("{stamp} {msg}"));
            s.errors.truncate(25);
        });
    }
}
