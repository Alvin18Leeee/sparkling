//! 可编程 HTTP 测试服务器。行为由 ServerConfig 驱动，
//! 服务于 probe/多线程/偷段/续传/错误处理等所有集成测试。
#![allow(dead_code)]
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use base64::Engine as _;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub enum FailMode {
    None,
    Always5xx,
    /// 前 N 次请求返回 500（计数全局递增）
    FailFirstN(u32),
    /// 对带 Range 的请求返回 416
    Always416,
    /// Content-MD5 头故意给错值
    WrongMd5,
}

#[derive(Clone)]
pub struct ServerConfig {
    pub size: u64,
    pub support_range: bool,
    pub fail_mode: FailMode,
    /// Range 起点在 [0, size/2) 的请求先 sleep 这么久（偷段测试）
    pub slow_first_half: Option<Duration>,
    /// 响应体发送 N 字节后掐断连接
    pub drop_after: Option<u64>,
    /// 覆盖 Content-Disposition
    pub disposition: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            size: 1024 * 1024, // 1 MiB
            support_range: true,
            fail_mode: FailMode::None,
            slow_first_half: None,
            drop_after: None,
            disposition: None,
        }
    }
}

struct ServerState {
    cfg: ServerConfig,
    requests: AtomicU64,
    v2: AtomicBool,
}

pub struct TestServer {
    pub url: String,
    pub data: Vec<u8>,
    state: Arc<ServerState>,
}

/// 内容 v1：字节 = i % 251；v2：字节 = (i * 7 + 3) % 241（保证不同）
fn content(size: u64, v2: bool) -> Vec<u8> {
    (0..size)
        .map(|i| if v2 { ((i * 7 + 3) % 241) as u8 } else { (i % 251) as u8 })
        .collect()
}

async fn handler(State(st): State<Arc<ServerState>>, req_headers: HeaderMap) -> impl IntoResponse {
    let n = st.requests.fetch_add(1, Ordering::SeqCst);
    let cfg = &st.cfg;
    let v2 = st.v2.load(Ordering::SeqCst);
    let data = content(cfg.size, v2);

    let fail = match &cfg.fail_mode {
        FailMode::Always5xx => true,
        FailMode::FailFirstN(k) => n < *k as u64,
        _ => false,
    };
    if fail {
        return (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response();
    }

    // 解析 Range: bytes=a-b
    let range = req_headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_range);

    if range.is_some() && matches!(cfg.fail_mode, FailMode::Always416) {
        return (StatusCode::RANGE_NOT_SATISFIABLE, "bad range").into_response();
    }

    // 空文件：直接 200 + 空 body（size-1 下溢守卫，Task 8 空文件测试依赖）
    if cfg.size == 0 {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ETAG,
            HeaderValue::from_str(if v2 { "\"v2\"" } else { "\"v1\"" }).unwrap(),
        );
        return (StatusCode::OK, headers, Body::empty()).into_response();
    }

    let is_partial = cfg.support_range && range.is_some();
    let (start, end) = match range {
        Some((a, b)) if cfg.support_range => (a, b.min(cfg.size - 1)),
        _ => (0, cfg.size.saturating_sub(1)),
    };

    if let Some(d) = cfg.slow_first_half {
        if start < cfg.size / 2 {
            tokio::time::sleep(d).await;
        }
    }

    let slice = Vec::from(&data[start as usize..=(end as usize)]);
    let mut resp_headers = HeaderMap::new();
    let status = if is_partial {
        resp_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, cfg.size)).unwrap(),
        );
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    resp_headers.insert(
        header::ETAG,
        HeaderValue::from_str(if v2 { "\"v2\"" } else { "\"v1\"" }).unwrap(),
    );
    if let Some(d) = &cfg.disposition {
        resp_headers.insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&format!("attachment; filename=\"{d}\"")).unwrap(),
        );
    }
    // Content-MD5：默认给正确值，WrongMd5 模式给别的文件的哈希
    let md5_of = if matches!(cfg.fail_mode, FailMode::WrongMd5) {
        content(cfg.size, !v2)
    } else {
        slice.clone()
    };
    use md5::{Digest, Md5};
    let digest = base64::engine::general_purpose::STANDARD.encode(Md5::digest(&md5_of));
    resp_headers.insert("content-md5", HeaderValue::from_str(&digest).unwrap());

    // 流式分块（64KiB）：drop_after 模式精确发送 limit 字节后以错误掐断；普通模式全量发送
    let drop_after = cfg.drop_after;
    let chunk_size = 64 * 1024usize;
    let chunks: Vec<Result<Vec<u8>, std::io::Error>> = if let Some(limit) = drop_after {
        let mut bounded: Vec<Result<Vec<u8>, std::io::Error>> = Vec::new();
        let mut sent = 0u64;
        for chunk in slice.chunks(chunk_size) {
            let remaining = limit.saturating_sub(sent);
            if remaining == 0 {
                bounded.push(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe, "dropped",
                )));
                break;
            }
            let take = (remaining as usize).min(chunk.len());
            sent += take as u64;
            bounded.push(Ok(chunk[..take].to_vec()));
        }
        if sent >= limit {
            bounded.push(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe, "dropped",
            )));
        }
        bounded
    } else {
        slice.chunks(chunk_size).map(|c| Ok(c.to_vec())).collect()
    };
    let body = Body::from_stream(futures::stream::iter(chunks));
    (status, resp_headers, body).into_response()
}

fn parse_range(v: &str) -> Option<(u64, u64)> {
    let spec = v.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

pub fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    format!("{:x}", h.finalize())
}

pub async fn start(cfg: ServerConfig) -> TestServer {
    let state = Arc::new(ServerState {
        cfg: cfg.clone(),
        requests: AtomicU64::new(0),
        v2: AtomicBool::new(false),
    });
    let app = Router::new()
        .route("/file.bin", get(handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    TestServer {
        url: format!("http://{addr}/file.bin"),
        data: content(cfg.size, false),
        state,
    }
}

impl TestServer {
    /// 切换服务端内容与 ETag（v1 → v2）
    pub fn set_content_v2(&self) {
        self.state.v2.store(true, Ordering::SeqCst);
    }
    /// 当前内容（受 v2 切换影响）的 sha256
    pub fn current_sha256(&self) -> String {
        let v2 = self.state.v2.load(Ordering::SeqCst);
        sha256_hex(&content(self.state.cfg.size, v2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn server_serves_range_and_full() {
        let server = start(ServerConfig { size: 10_000, ..Default::default() }).await;
        let client = reqwest::Client::new();
        // 全量
        let full = client.get(&server.url).send().await.unwrap();
        assert_eq!(full.status(), 200);
        assert_eq!(full.bytes().await.unwrap().len(), 10_000);
        // Range（reqwest 手动加头）
        let part = client
            .get(&server.url)
            .header("Range", "bytes=100-199")
            .send()
            .await
            .unwrap();
        assert_eq!(part.status(), 206);
        assert_eq!(part.headers()["content-range"], "bytes 100-199/10000");
        assert_eq!(part.bytes().await.unwrap().len(), 100);
        assert_eq!(server.data.len(), 10_000);
    }

    #[tokio::test]
    async fn server_serves_empty_file() {
        // 空文件不 panic：Range 探测也回 200 空 body（Task 8 空文件测试依赖）
        let server = start(ServerConfig { size: 0, ..Default::default() }).await;
        let resp = reqwest::Client::new()
            .get(&server.url)
            .header("Range", "bytes=0-0")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.bytes().await.unwrap().len(), 0);
    }
}
