use std::sync::Mutex;
use tokio::time::Instant;

/// 令牌桶限速器。rate = None 或 0 表示不限速。
/// 桶容量 = 1 秒配额（允许小幅突发），令牌按速率连续补充。
pub struct TokenBucket {
    inner: Mutex<Inner>,
}

struct Inner {
    /// bytes/s；0 = 不限
    rate: u64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(rate: Option<u64>) -> Self {
        let rate = rate.unwrap_or(0);
        // 初始给满 1 秒配额，起步不被卡
        let tokens = if rate == 0 { f64::INFINITY } else { rate as f64 };
        Self { inner: Mutex::new(Inner { rate, tokens, last: Instant::now() }) }
    }

    pub fn set_rate(&self, rate: Option<u64>) {
        let rate = rate.unwrap_or(0);
        let mut g = self.inner.lock().unwrap();
        g.rate = rate;
        if rate == 0 {
            g.tokens = f64::INFINITY;
        }
    }

    /// 等待取得 `amount` 个令牌（字节数）。不限速时立即返回。
    pub async fn acquire(&self, amount: u64) {
        loop {
            let sleep_for = {
                let mut g = self.inner.lock().unwrap();
                let now = Instant::now();
                let elapsed = now.duration_since(g.last).as_secs_f64();
                g.last = now;
                if g.rate == 0 {
                    return; // 不限速
                }
                g.tokens = (g.tokens + elapsed * g.rate as f64).min(g.rate as f64);
                if g.tokens >= amount as f64 {
                    g.tokens -= amount as f64;
                    return;
                }
                // 需要等待的时长（秒）
                ((amount as f64 - g.tokens) / g.rate as f64).max(0.001)
            };
            tokio::time::sleep(std::time::Duration::from_secs_f64(sleep_for)).await;
        }
    }
}
