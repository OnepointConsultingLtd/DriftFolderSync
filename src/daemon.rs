//! The background service: runs every sync pair, the control socket and the tray.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Result};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::auth::TokenProvider;
use crate::config::{self, Config};
use crate::db::Db;
use crate::drive::DriveClient;
use crate::engine::{self, Command, Env};
use crate::ipc::{self, Request, Response};
use crate::limiter::{Bandwidth, TokenBucket};
use crate::notify::Notifier;
use crate::paths;
use crate::status::StatusHub;

pub fn http_client(cfg: &Config) -> Result<reqwest::Client> {
    let mut b = reqwest::Client::builder()
        .user_agent(concat!("DriftFolderSync/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(30))
        .read_timeout(Duration::from_secs(120))
        .pool_idle_timeout(Duration::from_secs(90));
    if !cfg.general.proxy.trim().is_empty() {
        b = b.proxy(reqwest::Proxy::all(cfg.general.proxy.trim())?);
    }
    Ok(b.build()?)
}

struct Running {
    tx: mpsc::UnboundedSender<Command>,
    handle: JoinHandle<()>,
}

/// Shared control surface used by the IPC server and the tray.
#[derive(Clone)]
pub struct Controller {
    pairs: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Command>>>>,
    pub status: StatusHub,
    pub quit: Arc<Notify>,
    pub reload: Arc<Notify>,
    pub rt: tokio::runtime::Handle,
}

impl Controller {
    fn send(&self, pair: Option<&str>, mk: impl Fn() -> Command) -> Result<()> {
        let m = self.pairs.lock().unwrap();
        match pair {
            Some(p) => match m.get(p) {
                Some(tx) => {
                    let _ = tx.send(mk());
                }
                None => bail!("no sync folder named '{p}'"),
            },
            None => {
                for tx in m.values() {
                    let _ = tx.send(mk());
                }
            }
        }
        Ok(())
    }

    pub fn pause(&self, pair: Option<&str>, paused: bool) -> Result<()> {
        self.send(pair, || Command::Pause(paused))
    }

    pub fn sync_now(&self, pair: Option<&str>) -> Result<()> {
        self.send(pair, || Command::SyncNow)
    }

    pub fn confirm_deletes(&self, pair: &str) -> Result<()> {
        self.send(Some(pair), || Command::ConfirmDeletes)
    }

    pub fn set_interval(&self, pair: &str, interval: &str) -> Result<()> {
        config::parse_interval(interval)?;
        config::set_pair_key(pair, "sync_interval", interval.into())?;
        let iv = interval.to_string();
        self.send(Some(pair), || Command::SetInterval(iv.clone()))
    }

    pub fn handle(&self, req: Request) -> Response {
        let r = match req {
            Request::Status => {
                return Response { ok: true, status: Some(self.status.snapshot()), ..Default::default() }
            }
            Request::Pause { pair } => self.pause(pair.as_deref(), true),
            Request::Resume { pair } => self.pause(pair.as_deref(), false),
            Request::SyncNow { pair } => self.sync_now(pair.as_deref()),
            Request::ConfirmDeletes { pair } => self.confirm_deletes(&pair),
            Request::SetInterval { pair, interval } => self.set_interval(&pair, &interval),
            Request::Reload => {
                self.reload.notify_one();
                Ok(())
            }
            Request::Quit => {
                self.quit.notify_one();
                Ok(())
            }
        };
        match r {
            Ok(()) => Response::ok(),
            Err(e) => Response::err(format!("{e:#}")),
        }
    }
}

fn start_pairs(
    cfg: &Config,
    http: &reqwest::Client,
    db: &Arc<Db>,
    status: &StatusHub,
    bandwidth: &Bandwidth,
) -> HashMap<String, Running> {
    let notifier = Notifier::new(cfg.general.notifications);
    let mut clients: HashMap<String, Arc<DriveClient>> = HashMap::new();
    let mut out = HashMap::new();
    for pair in &cfg.pairs {
        let Some(acct) = cfg.account(&pair.account) else { continue };
        let drive = clients
            .entry(acct.name.clone())
            .or_insert_with(|| {
                let tokens = Arc::new(TokenProvider::new(acct.clone(), http.clone()));
                let bucket = TokenBucket::new(acct.requests_per_second, acct.request_burst as f64);
                Arc::new(DriveClient::new(http.clone(), tokens, bucket, bandwidth.clone()))
            })
            .clone();
        let (tx, rx) = mpsc::unbounded_channel();
        let env = Env {
            cfg: pair.clone(),
            email: acct.email.clone(),
            drive,
            db: db.clone(),
            status: status.clone(),
            notifier: notifier.clone(),
            max_transfers: cfg.general.max_concurrent_transfers,
        };
        info!(pair = %pair.name, local = %pair.local.display(), remote = %pair.remote, "starting");
        let handle = tokio::spawn(engine::run(env, rx));
        out.insert(pair.name.clone(), Running { tx, handle });
    }
    out
}

async fn stop_pairs(running: HashMap<String, Running>, status: &StatusHub) {
    for (name, r) in running {
        let _ = r.tx.send(Command::Shutdown);
        match tokio::time::timeout(Duration::from_secs(20), r.handle).await {
            Ok(_) => {}
            Err(_) => warn!(pair = %name, "did not stop in time"),
        }
        status.remove(&name);
    }
}

pub async fn run(no_tray: bool) -> Result<()> {
    if ipc::daemon_running().await {
        // Launched again (e.g. from the app menu): just trigger a sync.
        let _ = ipc::request(&Request::SyncNow { pair: None }).await;
        println!("DriftFolderSync is already running.");
        return Ok(());
    }
    let mut cfg = Config::load()?;
    crate::util::lower_priority(cfg.general.nice);
    if cfg.pairs.is_empty() {
        warn!("no sync folders configured yet — see `driftfoldersync folder add --help`");
    }

    let db = Arc::new(Db::open(&paths::data_dir().join("state.db"))?);
    let status = StatusHub::new();
    let bandwidth = Bandwidth::new(cfg.general.max_upload_kibps, cfg.general.max_download_kibps);
    let mut http = http_client(&cfg)?;
    let mut running = start_pairs(&cfg, &http, &db, &status, &bandwidth);

    let ctl = Controller {
        pairs: Arc::new(Mutex::new(running.iter().map(|(k, v)| (k.clone(), v.tx.clone())).collect())),
        status: status.clone(),
        quit: Arc::new(Notify::new()),
        reload: Arc::new(Notify::new()),
        rt: tokio::runtime::Handle::current(),
    };

    let listener = ipc::bind()?;
    let c2 = ctl.clone();
    tokio::spawn(ipc::serve(listener, move |req| {
        let c = c2.clone();
        async move { c.handle(req) }
    }));

    let _tray = if cfg.general.tray && !no_tray {
        match crate::tray::spawn(ctl.clone()).await {
            Ok(t) => Some(t),
            Err(e) => {
                warn!("system tray unavailable: {e:#}");
                None
            }
        }
    } else {
        None
    };

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    info!("DriftFolderSync {} running with {} sync folder(s)", env!("CARGO_PKG_VERSION"), running.len());
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = sigterm.recv() => break,
            _ = ctl.quit.notified() => break,
            _ = sighup.recv() => ctl.reload.notify_one(),
            _ = ctl.reload.notified() => {
                match Config::load() {
                    Ok(new) => {
                        info!("reloading configuration");
                        stop_pairs(std::mem::take(&mut running), &status).await;
                        cfg = new;
                        bandwidth.update(cfg.general.max_upload_kibps, cfg.general.max_download_kibps).await;
                        http = http_client(&cfg)?;
                        running = start_pairs(&cfg, &http, &db, &status, &bandwidth);
                        *ctl.pairs.lock().unwrap() =
                            running.iter().map(|(k, v)| (k.clone(), v.tx.clone())).collect();
                    }
                    Err(e) => error!("not reloading, configuration is invalid: {e:#}"),
                }
            }
        }
    }
    info!("shutting down");
    stop_pairs(running, &status).await;
    let _ = std::fs::remove_file(paths::socket_path());
    Ok(())
}
