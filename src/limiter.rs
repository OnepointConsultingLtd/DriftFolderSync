//! Rate limiting: API request token buckets and bandwidth throttling.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use tokio::sync::Mutex;

/// A classic token bucket. `acquire(n)` waits until `n` tokens are available.
#[derive(Debug)]
pub struct TokenBucket {
    inner: Mutex<Bucket>,
}

#[derive(Debug)]
struct Bucket {
    rate: f64,     // tokens per second (0 = unlimited)
    capacity: f64, // maximum burst
    tokens: f64,
    last: Instant,
    /// Set after a server-side rate limit error: everyone waits until then.
    blocked_until: Option<Instant>,
}

impl TokenBucket {
    pub fn new(rate: f64, capacity: f64) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Bucket {
                rate,
                capacity: capacity.max(1.0),
                tokens: capacity.max(1.0),
                last: Instant::now(),
                blocked_until: None,
            }),
        })
    }

    pub async fn set_rate(&self, rate: f64, capacity: f64) {
        let mut b = self.inner.lock().await;
        b.rate = rate;
        b.capacity = capacity.max(1.0);
        b.tokens = b.tokens.min(b.capacity);
    }

    pub async fn acquire(&self, n: f64) {
        loop {
            let wait = {
                let mut b = self.inner.lock().await;
                let now = Instant::now();
                if let Some(t) = b.blocked_until {
                    if t > now {
                        Some(t - now)
                    } else {
                        b.blocked_until = None;
                        None
                    }
                } else {
                    None
                }
                .or_else(|| {
                    if b.rate <= 0.0 {
                        return None;
                    }
                    let elapsed = now.duration_since(b.last).as_secs_f64();
                    b.last = now;
                    b.tokens = (b.tokens + elapsed * b.rate).min(b.capacity);
                    // Requests larger than the bucket are allowed to drive it negative.
                    let need = n.min(b.capacity);
                    if b.tokens >= need {
                        b.tokens -= n;
                        None
                    } else {
                        Some(Duration::from_secs_f64((need - b.tokens) / b.rate))
                    }
                })
            };
            match wait {
                None => return,
                Some(d) => tokio::time::sleep(d).await,
            }
        }
    }

    /// Pause every user of this bucket for `d` (server asked us to back off).
    pub async fn block_for(&self, d: Duration) {
        let mut b = self.inner.lock().await;
        let until = Instant::now() + d;
        if b.blocked_until.map_or(true, |t| t < until) {
            b.blocked_until = Some(until);
        }
        b.tokens = 0.0;
    }
}

/// Global bandwidth limiters (bytes per second; 0 = unlimited).
#[derive(Debug, Clone)]
pub struct Bandwidth {
    pub up: Arc<TokenBucket>,
    pub down: Arc<TokenBucket>,
}

impl Bandwidth {
    pub fn new(up_kibps: u64, down_kibps: u64) -> Self {
        let mk = |k: u64| {
            let bps = (k * 1024) as f64;
            // Allow ~1 second of burst.
            TokenBucket::new(bps, bps.max(64.0 * 1024.0))
        };
        Self { up: mk(up_kibps), down: mk(down_kibps) }
    }

    pub async fn update(&self, up_kibps: u64, down_kibps: u64) {
        let up = (up_kibps * 1024) as f64;
        let down = (down_kibps * 1024) as f64;
        self.up.set_rate(up, up.max(64.0 * 1024.0)).await;
        self.down.set_rate(down, down.max(64.0 * 1024.0)).await;
    }
}

/// Exponential back-off with jitter for retryable API errors.
pub fn backoff(attempt: u32) -> Duration {
    let base = 1u64 << attempt.min(6); // 1,2,4,…,64 s
    let jitter_ms = random_u64() % 1000;
    Duration::from_millis(base * 1000 + jitter_ms)
}

pub fn random_u64() -> u64 {
    let mut b = [0u8; 8];
    crate::util::random_bytes(&mut b);
    u64::from_le_bytes(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn bucket_limits_rate() {
        let b = TokenBucket::new(10.0, 10.0);
        let start = tokio::time::Instant::now();
        for _ in 0..30 {
            b.acquire(1.0).await;
        }
        // 10 burst + 20 at 10/s ≈ 2 s
        let el = start.elapsed();
        assert!(el >= Duration::from_millis(1900), "{el:?}");
    }

    #[tokio::test]
    async fn unlimited_is_instant() {
        let b = TokenBucket::new(0.0, 1.0);
        for _ in 0..1000 {
            b.acquire(1e9).await;
        }
    }
}
