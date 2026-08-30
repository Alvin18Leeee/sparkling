use sparkling_core::store::{TaskRecord, TaskStore};
use sparkling_core::task::{TaskKind, TaskState, VideoMeta, VideoParams};

fn http_rec(id: &str) -> TaskRecord {
    TaskRecord {
        id: id.into(),
        url: "http://example.com/a.bin".into(),
        state: TaskState::Queued,
        save_dir: "/tmp".into(),
        filename: Some("a.bin".into()),
        segments: 8,
        max_speed: None,
        total_size: None,
        downloaded: 0,
        error: None,
        created_at: 1700000000,
        kind: TaskKind::Http,
        video: None,
        video_meta: None,
        collection: None,
    }
}

/// 手工构造①期旧 schema 库（无新列、user_version=0）
fn legacy_db(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(
        "CREATE TABLE tasks (
            id TEXT PRIMARY KEY, url TEXT NOT NULL, state TEXT NOT NULL,
            save_dir TEXT NOT NULL, filename TEXT, segments INTEGER NOT NULL,
            max_speed INTEGER, total_size INTEGER,
            downloaded INTEGER NOT NULL DEFAULT 0, error TEXT,
            created_at INTEGER NOT NULL
        );
        INSERT INTO tasks (id, url, state, save_dir, filename, segments, downloaded, created_at)
        VALUES ('old1', 'http://e.com/x.zip', 'paused', 'D:\\\\dl', 'x.zip', 8, 1024, 1700000001);",
    )
    .unwrap();
}

#[test]
fn migrates_legacy_db_and_defaults_kind_http() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("tasks.db");
    legacy_db(&db);
    // 打开即迁移
    let store = TaskStore::open(&db).unwrap();
    let rec = store.get("old1").unwrap().unwrap();
    assert_eq!(rec.kind, TaskKind::Http);
    assert_eq!(rec.state, TaskState::Paused);
    assert_eq!(rec.downloaded, 1024);
    assert_eq!(rec.filename.as_deref(), Some("x.zip"));
    // user_version 已置 1（重复打开幂等）
    drop(store);
    let conn = rusqlite::Connection::open(&db).unwrap();
    let v: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 2, "①期旧库应连迁到当前版本");
    let again = TaskStore::open(&db).unwrap();
    assert_eq!(again.get("old1").unwrap().unwrap().kind, TaskKind::Http);
}

#[test]
fn video_record_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = TaskStore::open(&dir.path().join("tasks.db")).unwrap();
    let mut rec = http_rec("v1");
    rec.kind = TaskKind::Video;
    rec.video = Some(VideoParams {
        format: "bv*[height<=1080]+ba/b".into(),
        subtitles: vec!["zh-Hans".into()],
        auto_subs: true,
    });
    rec.video_meta = Some(VideoMeta {
        title: "测试视频标题".into(),
        duration_sec: Some(123),
        thumbnail: Some("https://example.com/t.jpg".into()),
        uploader: Some("上传者".into()),
        webpage_url: Some("https://example.com/watch?v=1".into()),
    });
    store.insert(&rec).unwrap();
    let back = store.get("v1").unwrap().unwrap();
    assert_eq!(back.kind, TaskKind::Video);
    assert_eq!(
        back.video.as_ref().unwrap().format,
        "bv*[height<=1080]+ba/b"
    );
    assert_eq!(
        back.video.as_ref().unwrap().subtitles,
        vec!["zh-Hans".to_string()]
    );
    assert_eq!(back.video_meta.as_ref().unwrap().title, "测试视频标题");
    assert_eq!(back.video_meta.as_ref().unwrap().duration_sec, Some(123));
}

/// 手工构造③期 v1 库（有 kind/video 列、无 collection、user_version=1）
fn v1_db(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(
        "CREATE TABLE tasks (
            id TEXT PRIMARY KEY, url TEXT NOT NULL, state TEXT NOT NULL,
            save_dir TEXT NOT NULL, filename TEXT, segments INTEGER NOT NULL,
            max_speed INTEGER, total_size INTEGER,
            downloaded INTEGER NOT NULL DEFAULT 0, error TEXT,
            created_at INTEGER NOT NULL,
            kind TEXT NOT NULL DEFAULT 'http', video_params TEXT, video_meta TEXT
        );
        INSERT INTO tasks (id, url, state, save_dir, filename, segments, downloaded,
                           created_at, kind, video_params, video_meta)
        VALUES ('v1task', 'https://www.youtube.com/watch?v=1', 'paused', 'D:\\dl',
                'v.mp4', 1, 0, 1700000002, 'video', NULL, NULL);
        PRAGMA user_version = 1;",
    )
    .unwrap();
}

#[test]
fn migrates_v1_to_v2_collection() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("tasks.db");
    v1_db(&db);
    let store = TaskStore::open(&db).unwrap();
    let rec = store.get("v1task").unwrap().unwrap();
    assert_eq!(rec.kind, TaskKind::Video);
    assert!(rec.collection.is_none(), "v1 行的 collection 默认 NULL");
    let conn = rusqlite::Connection::open(&db).unwrap();
    let v: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, 2);
    // 幂等：重复打开不重复 ALTER
    drop(store);
    assert!(TaskStore::open(&db).is_ok());
}

#[test]
fn collection_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let store = TaskStore::open(&dir.path().join("tasks.db")).unwrap();
    let mut rec = http_rec("c1");
    rec.collection = Some("测试合集".into());
    store.insert(&rec).unwrap();
    let back = store.get("c1").unwrap().unwrap();
    assert_eq!(back.collection.as_deref(), Some("测试合集"));
}
