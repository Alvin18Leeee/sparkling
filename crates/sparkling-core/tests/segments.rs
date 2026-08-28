mod common;

use common::{sha256_hex, start, wait_state, wait_until, ServerConfig};
use sparkling_core::engine::Engine;
use sparkling_core::http_engine::HttpEngine;
use sparkling_core::task::{TaskSpec, TaskState};
use std::time::Duration;

fn spec(url: String, dir: &tempfile::TempDir, max_speed: Option<u64>) -> TaskSpec {
    TaskSpec {
        url,
        save_dir: dir.path().to_path_buf(),
        filename: None,
        segments: 8,
        max_speed,
    }
}

#[tokio::test]
async fn multithread_download_completes() {
    // 1 MiB / 8 段 = 128KB < 偷段阈值 256KB → 不发生偷段，段数断言稳定
    let server = start(ServerConfig { size: 1024 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = HttpEngine::new(None);
    // 2 MB/s 限速：确保下载期间控制文件确已落盘（周期 + 分片完成点），
    // 使结尾的 !file.bin.sparkling.exists() 真正检验 finalize 的删除
    let handle = engine.submit(spec(server.url.clone(), &dir, Some(2_000_000))).await.unwrap();
    let mut rx = handle.subscribe();
    let snap = wait_state(&mut rx, TaskState::Completed, Duration::from_secs(30)).await;
    assert_eq!(snap.downloaded, 1024 * 1024);
    assert_eq!(snap.segments.len(), 8); // 低于偷段阈值，段数恒为 8
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
    assert!(!dir.path().join("file.bin.sparkling.part").exists());
    assert!(!dir.path().join("file.bin.sparkling").exists());
}

#[tokio::test]
async fn control_file_persisted_while_running() {
    // 限速让任务持续 > 2s，验证控制文件周期落盘；随后 drop 引擎模拟崩溃
    let server = start(ServerConfig { size: 4 * 1024 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = HttpEngine::new(None);
    let handle = engine
        .submit(spec(server.url.clone(), &dir, Some(300_000))) // 300 KB/s → 约 14s
        .await
        .unwrap();
    let mut rx = handle.subscribe();
    wait_until(&mut rx, |s| s.downloaded > 100_000, Duration::from_secs(10)).await;
    tokio::time::sleep(Duration::from_secs(3)).await; // 越过 2s 保存周期
    assert!(dir.path().join("file.bin.sparkling").exists(), "控制文件应周期性落盘");
    drop(handle);
    drop(engine); // abort 全部 worker，模拟进程崩溃
    assert!(dir.path().join("file.bin.sparkling.part").exists(), "崩溃后 .part 应保留");
}

#[tokio::test]
async fn stealing_eliminates_tail() {
    // 前半段起始的请求被延迟 400ms：先完成的 worker 应从慢段偷走剩余部分
    let server = start(ServerConfig {
        size: 2 * 1024 * 1024,
        slow_first_half: Some(Duration::from_millis(400)),
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let engine = HttpEngine::new(None);
    let handle = engine.submit(spec(server.url.clone(), &dir, None)).await.unwrap();
    let mut rx = handle.subscribe();
    let mut max_segments = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        {
            let cur = rx.borrow().clone();
            max_segments = max_segments.max(cur.segments.len());
            match cur.state {
                TaskState::Completed => break,
                TaskState::Failed => panic!("任务失败: {:?}", cur.error),
                _ => {}
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("下载超时");
        }
        if tokio::time::timeout_at(deadline, rx.changed()).await.is_err() {
            panic!("下载超时（通道关闭）");
        }
    }
    assert!(max_segments > 8, "应发生偷段，观察到最大 {max_segments} 段");
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
}
