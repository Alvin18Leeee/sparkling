mod common;

use common::{sha256_hex, start, wait_state, wait_until, ServerConfig};
use sparkling_core::control_file;
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
async fn pause_resume_completes() {
    let server = start(ServerConfig { size: 4 * 1024 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = HttpEngine::new(None);
    let handle = engine
        .submit(spec(server.url.clone(), &dir, Some(400_000)))
        .await
        .unwrap();
    let mut rx = handle.subscribe();
    wait_until(&mut rx, |s| s.downloaded > 200_000, Duration::from_secs(10)).await;

    handle.pause().unwrap();
    let paused = wait_state(&mut rx, TaskState::Paused, Duration::from_secs(10)).await;
    // 暂停后进度冻结
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(rx.borrow().downloaded, paused.downloaded);
    // 控制文件在暂停点落盘
    assert!(dir.path().join("file.bin.sparkling").exists());

    handle.resume().unwrap();
    let done = wait_state(&mut rx, TaskState::Completed, Duration::from_secs(60)).await;
    assert_eq!(done.downloaded, 4 * 1024 * 1024);
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
    assert!(!dir.path().join("file.bin.sparkling").exists());
    assert!(!dir.path().join("file.bin.sparkling.part").exists());
}

#[tokio::test]
async fn crash_recovery_resumes_from_offset() {
    let server = start(ServerConfig { size: 2 * 1024 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = HttpEngine::new(None);
        let handle = engine
            .submit(spec(server.url.clone(), &dir, Some(300_000)))
            .await
            .unwrap();
        let mut rx = handle.subscribe();
        wait_until(&mut rx, |s| s.downloaded > 300_000, Duration::from_secs(15)).await;
        drop(handle);
        drop(engine); // 模拟进程崩溃（Drop abort 全部 worker）
    }
    let ctl = control_file::load(&dir.path().join("file.bin.sparkling")).unwrap();
    let done_before: u64 = ctl.segments.iter().map(|s| s.downloaded).sum();
    assert!(done_before >= 300_000, "控制文件应记录已下载量");

    let engine2 = HttpEngine::new(None);
    let handle2 = engine2.submit(spec(server.url.clone(), &dir, None)).await.unwrap();
    let mut rx2 = handle2.subscribe();
    let done = wait_state(&mut rx2, TaskState::Completed, Duration::from_secs(60)).await;
    // 关键：恢复后 downloaded 计数从断点累计到全量
    assert_eq!(done.downloaded, 2 * 1024 * 1024);
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
}

#[tokio::test]
async fn etag_change_restarts_from_zero() {
    let server = start(ServerConfig { size: 1024 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = HttpEngine::new(None);
    let handle = engine
        .submit(spec(server.url.clone(), &dir, Some(300_000)))
        .await
        .unwrap();
    let mut rx = handle.subscribe();
    wait_until(&mut rx, |s| s.downloaded > 150_000, Duration::from_secs(10)).await;
    handle.pause().unwrap();
    let paused_at = wait_state(&mut rx, TaskState::Paused, Duration::from_secs(10)).await.downloaded;

    server.set_content_v2(); // 远端文件变化（内容 + ETag）
    handle.resume().unwrap();

    // 观察到进度回落到 0（从零重下），且最终完成
    let mut saw_reset = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        {
            let cur = rx.borrow().clone();
            match cur.state {
                TaskState::Running if cur.downloaded < paused_at => saw_reset = true,
                TaskState::Completed => break,
                TaskState::Failed => panic!("任务失败: {:?}", cur.error),
                _ => {}
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("等待完成超时");
        }
        if tokio::time::timeout_at(deadline, rx.changed()).await.is_err() {
            panic!("等待完成超时（通道关闭）");
        }
    }
    assert!(saw_reset, "应观察到从零重下");
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), server.current_sha256());
}

#[tokio::test]
async fn cancel_cleans_up_files() {
    let server = start(ServerConfig { size: 2 * 1024 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let engine = HttpEngine::new(None);
    let handle = engine
        .submit(spec(server.url.clone(), &dir, Some(200_000)))
        .await
        .unwrap();
    let mut rx = handle.subscribe();
    wait_until(&mut rx, |s| s.downloaded > 100_000, Duration::from_secs(10)).await;
    handle.cancel().unwrap();
    wait_state(&mut rx, TaskState::Cancelled, Duration::from_secs(10)).await;
    assert!(!dir.path().join("file.bin.sparkling.part").exists());
    assert!(!dir.path().join("file.bin.sparkling").exists());
    assert!(!dir.path().join("file.bin").exists());
}

#[tokio::test]
async fn corrupt_control_file_restarts_cleanly() {
    let server = start(ServerConfig { size: 512 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    // 预置损坏控制文件与脏 .part
    std::fs::write(dir.path().join("file.bin.sparkling"), b"{ not json !!!").unwrap();
    std::fs::write(dir.path().join("file.bin.sparkling.part"), b"garbage").unwrap();

    let engine = HttpEngine::new(None);
    let handle = engine.submit(spec(server.url.clone(), &dir, None)).await.unwrap();
    let mut rx = handle.subscribe();
    wait_state(&mut rx, TaskState::Completed, Duration::from_secs(30)).await;
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
}
