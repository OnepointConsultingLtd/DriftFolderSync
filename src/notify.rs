//! Desktop notifications (org.freedesktop.Notifications).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct Notifier {
    enabled: bool,
    /// Rate-limit identical notifications.
    last: Arc<Mutex<HashMap<String, Instant>>>,
}

impl Notifier {
    pub fn new(enabled: bool) -> Self {
        Self { enabled, last: Arc::default() }
    }

    pub fn send(&self, summary: &str, body: &str) {
        if !self.enabled {
            return;
        }
        let key = format!("{summary}\n{body}");
        {
            let mut m = self.last.lock().unwrap();
            let now = Instant::now();
            if m.get(&key).is_some_and(|t| now.duration_since(*t) < Duration::from_secs(600)) {
                return;
            }
            m.insert(key, now);
        }
        let summary = summary.to_string();
        let body = body.to_string();
        tokio::task::spawn_blocking(move || {
            let r = notify_rust::Notification::new()
                .appname(crate::paths::APP_NAME)
                .summary(&summary)
                .body(&body)
                .icon(crate::paths::APP_ID)
                .show();
            if let Err(e) = r {
                tracing::debug!("notification failed: {e}");
            }
        });
    }
}
