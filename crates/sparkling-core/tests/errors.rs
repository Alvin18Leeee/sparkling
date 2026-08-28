mod common;

use common::{sha256_hex, start, wait_state, FailMode, ServerConfig};
use sparkling_core::engine::Engine;
use sparkling_core::http_engine::{HttpEngine, RetryPolicy};
use sparkling_core::task::{TaskSpec, TaskState};
use std::time::Duration;

fn spec(url: String, dir: &tempfile::TempDir) -> TaskSpec {
    TaskSpec {
        url,
        save_dir: dir.path().to_path_buf(),
        filename: None,
        segments: 8,
        max_speed: None,
    }
}

fn fast_engine() -> HttpEngine {
    HttpEngine::new_with_policy(None, RetryPolicy::fast())
}

#[test]
fn backoff_math() {
    let rp = RetryPolicy::default();
    assert_eq!(rp.backoff(1), Duration::from_secs(1));
    assert_eq!(rp.backoff(2), Duration::from_secs(2));
    assert_eq!(rp.backoff(3), Duration::from_secs(4));
    assert_eq!(rp.backoff(10), Duration::from_secs(30)); // 封顶
}

#[tokio::test]
async fn probe_retries_transient_5xx() {
    // 前 3 次请求 500，之后正常：探测自身要能重试过去
    let server = start(ServerConfig {
        size: 256 * 1024,
        fail_mode: FailMode::FailFirstN(3),
        ..Default::default()
    }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = fast_engine();
    let handle = engine.submit(spec(server.url.clone(), &dir)).await.unwrap();
    let mut rx = handle.subscribe();
    wait_state(&mut rx, TaskState::Completed, Duration::from_secs(30)).await;
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
}

#[tokio::test]
async fn persistent_5xx_fails_task() {
    let server = start(ServerConfig { fail_mode: FailMode::Always5xx, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = fast_engine();
    let handle = engine.submit(spec(server.url.clone(), &dir)).await.unwrap();
    let mut rx = handle.subscribe();
    let snap = wait_state(&mut rx, TaskState::Failed, Duration::from_secs(30)).await;
    assert!(snap.error.unwrap().contains("500"));
}

#[tokio::test]
async fn connection_drop_resumes_from_offset() {
    // 每个响应体发 512KB 后掐断：worker 每次从新偏移续传。
    // 注意 drop_after 必须显著大于 hyper 的写缓冲（实测 ~400KB）——
    // 太小的限额客户端收到 0 字节，重试零进展，任务会 Failed 而非完成。
    // segments 必须为 2（每段 1MiB > 512KiB 限额）——8×256KiB 段小于限额时
    // 掐断永远不会触发，测试空转（D33）
    let server = start(ServerConfig {
        size: 2 * 1024 * 1024,
        drop_after: Some(512 * 1024),
        ..Default::default()
    }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = fast_engine();
    let mut s = spec(server.url.clone(), &dir);
    s.segments = 2;
    let handle = engine.submit(s).await.unwrap();
    let mut rx = handle.subscribe();
    wait_state(&mut rx, TaskState::Completed, Duration::from_secs(60)).await;
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
}

#[tokio::test]
async fn no_range_drop_restarts_from_zero() {
    // 不支持 Range + 前两次响应掐断（probe 一次 + 首个 body 一次）→
    // 顺序路径中途断连后从头重下并完成。
    // drop_first_n = 2 是关键：只掐一次会被 probe 消耗掉，顺序重置路径不被行使（D33）。
    // drop_after 必须大于 hyper 写缓冲（实测 ~120-160KiB）：100_000 级别的掐断连
    // 响应头都送不到客户端，probe 直接失败、两次掐断都被探测重试吃掉（实测
    // 4 请求、进度直达满值——空转）；512KiB 时 probe 拿得到头，body 请求在
    // ~383KiB 处断掉，实测进度时间线 …392623 → 24444… 清零后重爬到满值。
    let server = start(ServerConfig {
        size: 1024 * 1024,
        support_range: false,
        drop_after: Some(512 * 1024),
        drop_first_n: 2,
        ..Default::default()
    }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = fast_engine();
    let handle = engine.submit(spec(server.url.clone(), &dir)).await.unwrap();
    let mut rx = handle.subscribe();
    wait_state(&mut rx, TaskState::Completed, Duration::from_secs(60)).await;
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
}

#[tokio::test]
async fn server_416_falls_back_to_plain_get() {
    // 对 Range 一律 416 的服务器：探测退回 plain GET → 顺序下载完成
    let server = start(ServerConfig { fail_mode: FailMode::Always416, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = fast_engine();
    let handle = engine.submit(spec(server.url.clone(), &dir)).await.unwrap();
    let mut rx = handle.subscribe();
    wait_state(&mut rx, TaskState::Completed, Duration::from_secs(30)).await;
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
}

#[tokio::test]
async fn wrong_md5_fails_without_producing_file() {
    let server = start(ServerConfig {
        size: 256 * 1024,
        fail_mode: FailMode::WrongMd5,
        ..Default::default()
    }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = fast_engine();
    let handle = engine.submit(spec(server.url.clone(), &dir)).await.unwrap();
    let mut rx = handle.subscribe();
    let snap = wait_state(&mut rx, TaskState::Failed, Duration::from_secs(30)).await;
    assert!(snap.error.unwrap().contains("校验"));
    // 正式文件不产出（红线）；.part 保留供排查
    assert!(!dir.path().join("file.bin").exists());
    assert!(dir.path().join("file.bin.sparkling.part").exists());
}
