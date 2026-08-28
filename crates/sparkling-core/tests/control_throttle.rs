use sparkling_core::control_file::{self, ControlFile};
use sparkling_core::segment::split;

fn sample() -> ControlFile {
    ControlFile {
        url: "http://example.com/a.bin".into(),
        etag: Some("\"v1\"".into()),
        last_modified: None,
        total_size: 100,
        supports_range: true,
        filename: "a.bin".into(),
        segments: split(100, 8),
    }
}

#[test]
fn save_load_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let final_path = dir.path().join("a.bin");
    control_file::save(&final_path, &sample()).unwrap();
    assert!(control_file::path_for(&final_path).exists());
    let loaded = control_file::load(&control_file::path_for(&final_path)).unwrap();
    assert_eq!(loaded, sample());
}

#[test]
fn truncated_file_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let final_path = dir.path().join("a.bin");
    control_file::save(&final_path, &sample()).unwrap();
    let ctl = control_file::path_for(&final_path);
    let raw = std::fs::read(&ctl).unwrap();
    std::fs::write(&ctl, &raw[..raw.len() / 2]).unwrap(); // 模拟写一半崩溃
    let err = control_file::load(&ctl).unwrap_err();
    assert!(matches!(err, sparkling_core::SparklingError::CorruptControlFile(_)));
}

#[test]
fn bad_invariant_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let final_path = dir.path().join("a.bin");
    let mut cf = sample();
    cf.segments[0].downloaded = cf.segments[0].len() + 1; // downloaded > len
    control_file::save(&final_path, &cf).unwrap();
    let err = control_file::load(&control_file::path_for(&final_path)).unwrap_err();
    assert!(matches!(err, sparkling_core::SparklingError::CorruptControlFile(_)));
}

#[test]
fn missing_file_is_corrupt() {
    let dir = tempfile::tempdir().unwrap();
    let err = control_file::load(&dir.path().join("nope.sparkling")).unwrap_err();
    assert!(matches!(err, sparkling_core::SparklingError::CorruptControlFile(_)));
}

#[test]
fn atomic_save_leaves_no_tmp() {
    let dir = tempfile::tempdir().unwrap();
    let final_path = dir.path().join("a.bin");
    control_file::save(&final_path, &sample()).unwrap();
    control_file::save(&final_path, &sample()).unwrap(); // 覆盖保存
    let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert_eq!(files.len(), 1); // 只有 .sparkling，没有残留 tmp
}

mod throttle_tests {
    use sparkling_core::throttle::TokenBucket;

    #[tokio::test(start_paused = true)]
    async fn unlimited_acquires_immediately() {
        let bucket = TokenBucket::new(None);
        let t0 = tokio::time::Instant::now();
        for _ in 0..1000 {
            bucket.acquire(64 * 1024).await;
        }
        assert!(t0.elapsed().as_millis() < 10);
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limited_paces_output() {
        let bucket = TokenBucket::new(Some(1000)); // 1000 B/s
        bucket.acquire(1000).await; // 初始令牌覆盖（桶容量 = 1 秒配额，取尽后桶空）
        let t0 = tokio::time::Instant::now();
        bucket.acquire(500).await; // 需要等 0.5s 攒令牌
        assert!(t0.elapsed() >= std::time::Duration::from_millis(500));
    }

    #[tokio::test(start_paused = true)]
    async fn set_rate_takes_effect_immediately() {
        let bucket = TokenBucket::new(Some(10));
        bucket.set_rate(None);
        let t0 = tokio::time::Instant::now();
        for _ in 0..100 {
            bucket.acquire(64 * 1024).await;
        }
        assert!(t0.elapsed().as_millis() < 10);
    }
}
