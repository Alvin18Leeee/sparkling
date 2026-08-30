mod common;

use common::{start, FailMode, ServerConfig};
use sparkling_core::probe::probe;

fn client() -> reqwest::Client {
    reqwest::Client::builder().build().unwrap()
}

#[tokio::test]
async fn probe_range_server() {
    let server = start(ServerConfig {
        size: 5000,
        ..Default::default()
    })
    .await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert_eq!(p.total, 5000);
    assert!(p.supports_range);
    assert_eq!(p.filename, "file.bin");
    assert_eq!(p.etag.as_deref(), Some("\"v1\""));
}

#[tokio::test]
async fn probe_no_range_server() {
    let server = start(ServerConfig {
        size: 5000,
        support_range: false,
        ..Default::default()
    })
    .await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert_eq!(p.total, 5000);
    assert!(!p.supports_range);
}

#[tokio::test]
async fn probe_disposition_overrides_filename() {
    // 注意：这里用 ASCII 文件名。测试服务器的 Content-Disposition 头由
    // HeaderValue::from_str 构造，非 ASCII（如 "报表.zip"）虽能作为 obs-text
    // 字节发出，但 probe 侧 HeaderValue::to_str() 拒绝非可见 ASCII 字节，
    // 解析会回退到 URL 文件名。filename*=UTF-8'' 百分号编码路径可表达非
    // ASCII 文件名，但服务器封装格式（filename="..."）不支持注入该形式。
    let server = start(ServerConfig {
        size: 100,
        disposition: Some("report-q4.zip".into()),
        ..Default::default()
    })
    .await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert_eq!(p.filename, "report-q4.zip");
}

#[tokio::test]
async fn probe_http_error() {
    let server = start(ServerConfig {
        fail_mode: FailMode::Always5xx,
        ..Default::default()
    })
    .await;
    let err = probe(&client(), &server.url).await.unwrap_err();
    assert!(matches!(
        err,
        sparkling_core::SparklingError::HttpStatus { status: 500, .. }
    ));
}

#[tokio::test]
async fn probe_content_md5_present() {
    let server = start(ServerConfig {
        size: 100,
        ..Default::default()
    })
    .await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert!(p.content_md5.is_some());
}

mod engine_tests {
    use crate::common::{sha256_hex, start, wait_state, FailMode, ServerConfig};
    use sparkling_core::engine::Engine;
    use sparkling_core::task::{TaskKind, TaskSpec, TaskState};
    use std::time::Duration;

    async fn engine() -> sparkling_core::http_engine::HttpEngine {
        sparkling_core::http_engine::HttpEngine::new(None)
    }

    fn spec(url: String, dir: &tempfile::TempDir) -> TaskSpec {
        TaskSpec {
            url,
            save_dir: dir.path().to_path_buf(),
            filename: None,
            segments: 8,
            max_speed: None,
            kind: TaskKind::Http,
            video: None,
        }
    }

    #[tokio::test]
    async fn no_range_downloads_sequentially() {
        let server = start(ServerConfig {
            size: 256 * 1024,
            support_range: false,
            ..Default::default()
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let e = engine().await;
        let handle = e.submit(spec(server.url.clone(), &dir)).await.unwrap();
        let mut rx = handle.subscribe();
        let final_snap = wait_state(&mut rx, TaskState::Completed, Duration::from_secs(30)).await;
        assert_eq!(final_snap.total, 256 * 1024);
        let file = std::fs::read(dir.path().join("file.bin")).unwrap();
        assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
        // 无残留临时文件
        assert!(!dir.path().join("file.bin.sparkling.part").exists());
        assert!(!dir.path().join("file.bin.sparkling").exists());
    }

    #[tokio::test]
    async fn empty_file_completes_immediately() {
        let server = start(ServerConfig {
            size: 0,
            ..Default::default()
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let e = engine().await;
        let handle = e.submit(spec(server.url.clone(), &dir)).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Completed, Duration::from_secs(10)).await;
        let meta = std::fs::metadata(dir.path().join("file.bin")).unwrap();
        assert_eq!(meta.len(), 0);
    }

    #[tokio::test]
    async fn disposition_filename_used() {
        // 同 probe_disposition_overrides_filename 的 D21 约束：HeaderValue::to_str
        // 拒绝非可见 ASCII（obs-text）字节，非 ASCII 的 filename="..." 解析会回退到
        // URL 文件名，故此处用 ASCII 名（保留空格）验证"引擎把 probe 的文件名落盘"。
        // support_range: false —— 多线程分支在 Task 9 前是刻意桩（返回"在 Task 9
        // 实现"错误），文件名测试只需任意可完成路径，走单线程顺序分支。
        let server = start(ServerConfig {
            size: 100,
            support_range: false,
            disposition: Some("report q4.zip".into()),
            ..Default::default()
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let e = engine().await;
        let handle = e.submit(spec(server.url.clone(), &dir)).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Completed, Duration::from_secs(10)).await;
        assert!(dir.path().join("report q4.zip").exists());
    }

    #[tokio::test]
    async fn user_filename_overrides() {
        // support_range: false —— 同上，多线程分支在 Task 9 前是刻意桩
        let server = start(ServerConfig {
            size: 100,
            support_range: false,
            ..Default::default()
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let e = engine().await;
        let mut s = spec(server.url.clone(), &dir);
        s.filename = Some("自定义.bin".into());
        let handle = e.submit(s).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Completed, Duration::from_secs(10)).await;
        assert!(dir.path().join("自定义.bin").exists());
    }

    #[tokio::test]
    async fn user_filename_traversal_is_sanitized() {
        // I3 回归：用户提供的文件名携带 ..\..\ 穿越 → 消毒后只余最后分量，
        // 文件必须落在 save_dir 之内（消毒点在 run_download 的文件名汇聚处，
        // 用户覆盖与探测结果过同一关卡；ctl/.part 路径同由消毒名派生）
        let server = start(ServerConfig {
            size: 100,
            support_range: false,
            ..Default::default()
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        let e = engine().await;
        let mut s = spec(server.url.clone(), &dir);
        s.filename = Some("..\\..\\evil.exe".into());
        let handle = e.submit(s).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Completed, Duration::from_secs(10)).await;
        assert!(
            dir.path().join("evil.exe").exists(),
            "应落盘为 save_dir 内的 evil.exe"
        );
        // 穿越目标（save_dir 上两级）不得出现文件
        assert!(!dir.path().join("..").join("..").join("evil.exe").exists());
        // save_dir 内除正式产物外无残留临时文件
        assert!(!dir.path().join("evil.exe.sparkling.part").exists());
        assert!(!dir.path().join("evil.exe.sparkling").exists());
    }

    #[tokio::test]
    async fn probe_error_fails_task() {
        let server = start(ServerConfig {
            fail_mode: FailMode::Always5xx,
            ..Default::default()
        })
        .await;
        let dir = tempfile::tempdir().unwrap();
        // Task 12 起探测复用任务重试策略：默认策略的退避全程 31s，
        // 超出本测试 10s 等待窗口——改用 fast 策略，断言意图不变
        // （持续 5xx → 重试耗尽 → Failed 且消息含 500）
        let e = sparkling_core::http_engine::HttpEngine::new_with_policy(
            None,
            sparkling_core::http_engine::RetryPolicy::fast(),
        );
        let handle = e.submit(spec(server.url.clone(), &dir)).await.unwrap();
        let mut rx = handle.subscribe();
        let snap = wait_state(&mut rx, TaskState::Failed, Duration::from_secs(10)).await;
        assert!(snap.error.unwrap().contains("500"));
    }
}

mod disk_tests {
    use sparkling_core::disk::{check_space, required_space};

    #[test]
    fn required_space_is_102_percent() {
        assert_eq!(required_space(1000), 1020);
        assert_eq!(required_space(0), 0);
    }

    #[test]
    fn insufficient_space_detected() {
        let dir = tempfile::tempdir().unwrap();
        let err = check_space(dir.path(), u64::MAX / 2).unwrap_err();
        assert!(matches!(
            err,
            sparkling_core::SparklingError::InsufficientDisk { .. }
        ));
    }

    #[test]
    fn enough_space_ok() {
        let dir = tempfile::tempdir().unwrap();
        assert!(check_space(dir.path(), 0).is_ok());
    }
}
