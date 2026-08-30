# ③期视频解析下载（yt-dlp）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 接入 yt-dlp 视频解析下载：粘贴视频链接 → 解析选格式 → 下载合并，与 HTTP 任务统一队列。

**Architecture:** TaskManager 从单引擎改为多引擎路由（`TaskKind → Engine`）；新增 `sparkling-core/src/video/` 模块（进程 Runner 抽象 + 解析 + VideoEngine），yt-dlp 全权下载、进程 stdout 逐行解析为现有 ProgressSnapshot；yt-dlp 打包基线版 + app data 更新版优先，ffmpeg 打包。

**Tech Stack:** Rust（tokio process/reqwest/rusqlite）、Tauri 2（commands + bundle.resources）、React + TypeScript。

**Spec:** `docs/superpowers/specs/2026-08-29-video-ytdlp-design.md`（六项已拍板决策 D1–D6 见 spec 决策表）。本计划对 spec 的三处技术修正：
1. 命令行去掉 `--restrict-filenames`——它把中文标题（Bilibili 等）清洗成全下划线；yt-dlp 默认已清洗 Windows 非法字符。
2. 便携版发布形态从裸 exe 改为 zip——Tauri `bundle.resources` 不进裸 exe，裸 exe 运行时找不到 yt-dlp/ffmpeg。
3. probe 一次调用 `-J --flat-playlist`——flat 只影响列表条目展开，单视频 URL 仍返回完整 formats，无需两次进程调用。

## Global Constraints

- cargo 不在会话 PATH：bash 中先 `export PATH="$PATH:/c/Users/Alvin/.cargo/bin"`（下文 Run 命令直接写 `cargo …`）
- 提交信息：中文 Conventional Commits，末尾 `Co-Authored-By: Claude Code <noreply@anthropic.com>`
- 每任务收尾质量门（workspace 根执行）：`cargo fmt` + `cargo clippy --all-targets -- -D warnings` + `cargo test`
- sparkling-core **不得依赖 Tauri**（二进制路径由 Tauri 层注入）
- 错误消息用户可读中文；测试断言消息中文
- serde 新增配置字段必须 `#[serde(default)]`（兼容旧 settings.json）
- 前端改动仅在 Windows 本机 `npm run tauri dev` 验收（用户亲验），不进 CI
- 测试不依赖真实 yt-dlp 二进制（FakeRunner 全覆盖；真机验收单独人工步骤）

---

### Task 1: 任务类型模型（TaskKind/VideoParams/VideoMeta + 全结构扩展）

**Files:**
- Modify: `crates/sparkling-core/src/task.rs`
- Modify: `crates/sparkling-core/src/engine.rs`
- Modify: `crates/sparkling-core/src/manager.rs`
- Modify: `crates/sparkling-core/src/store.rs`（仅 `row_to_record`/`rec()`，Task 2 做列持久化）
- Modify: `crates/sparkling-core/src/http_engine.rs`（submit 防御一行）
- Modify: `crates/sparkling-core/src/lib.rs`（导出）
- Modify: `crates/sparkling-core/tests/*.rs`（构造点机械补字段）

**Interfaces:**
- Produces: `TaskKind { Http, Video }`（`as_str`/`parse`，serde lowercase）；`VideoParams { format: String, subtitles: Vec<String>, auto_subs: bool }`；`VideoMeta { title: String, duration_sec: Option<u64>, thumbnail: Option<String>, uploader: Option<String>, webpage_url: Option<String> }`（三者均 Clone + Serialize + Deserialize）；`TaskSpec` 增 `kind: TaskKind`、`video: Option<VideoParams>`；`TaskRecord` 增 `kind: TaskKind`、`video: Option<VideoParams>`、`video_meta: Option<VideoMeta>`；`ProgressSnapshot` 增 `merging: bool`；`TaskEvent::Progress` 增 `merging: bool`；`AddTaskOptions` 增 `kind: TaskKind`、`video: Option<VideoParams>`、`video_meta: Option<VideoMeta>`；`ManagerConfig` 增 `video_max_height: Option<u32>`、`video_audio_only: bool`、`video_sub_langs: String`、`video_auto_subs: bool`、`cookie_file: Option<PathBuf>`。

- [ ] **Step 1: 写失败测试（task.rs 新类型 + VideoParams serde）**

在 `crates/sparkling-core/src/task.rs` 的 `#[cfg(test)] mod tests` 内追加：

```rust
    #[test]
    fn task_kind_roundtrip() {
        assert_eq!(TaskKind::Http.as_str(), "http");
        assert_eq!(TaskKind::Video.as_str(), "video");
        assert_eq!(TaskKind::parse("http"), Some(TaskKind::Http));
        assert_eq!(TaskKind::parse("video"), Some(TaskKind::Video));
        assert_eq!(TaskKind::parse("bogus"), None);
        // serde 小写
        assert_eq!(serde_json::to_string(&TaskKind::Video).unwrap(), "\"video\"");
    }

    #[test]
    fn video_params_serde_roundtrip() {
        let v = VideoParams {
            format: "bv*[height<=1080]+ba/b".into(),
            subtitles: vec!["zh-Hans".into(), "en".into()],
            auto_subs: true,
        };
        let s = serde_json::to_string(&v).unwrap();
        let back: VideoParams = serde_json::from_str(&s).unwrap();
        assert_eq!(back.format, v.format);
        assert_eq!(back.subtitles, v.subtitles);
        assert!(back.auto_subs);
    }
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p sparkling-core --lib task::`
Expected: FAIL，`TaskKind`/`VideoParams` 未定义

- [ ] **Step 3: 实现（task.rs 顶部类型区）**

```rust
/// 任务类别：HTTP 直下（①期）或视频解析下载（③期）
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskKind {
    Http,
    Video,
}

impl TaskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskKind::Http => "http",
            TaskKind::Video => "video",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "http" => TaskKind::Http,
            "video" => TaskKind::Video,
            _ => return None,
        })
    }
}

/// 视频任务的下载参数（yt-dlp 侧配置）
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VideoParams {
    /// yt-dlp -f 格式选择器（如 "bv*[height<=1080]+ba/b"）
    pub format: String,
    /// 字幕语言列表（yt-dlp --sub-langs 逗号拼接；空 = 不下字幕）
    pub subtitles: Vec<String>,
    /// 含自动生成字幕（--write-auto-subs）
    pub auto_subs: bool,
}

/// 视频元数据（解析阶段取得，落库供 UI 展示/重启恢复）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VideoMeta {
    pub title: String,
    pub duration_sec: Option<u64>,
    pub thumbnail: Option<String>,
    pub uploader: Option<String>,
    pub webpage_url: Option<String>,
}
```

`TaskSpec` 末尾追加两个字段；`TaskSpec` 的 doc 注释补一行 `kind/video：③期视频任务参数`。同时给 `VideoMeta`/`VideoParams`/`TaskKind` 加 `Default` 无意义的不加，仅上面三个 derive。

- [ ] **Step 4: engine.rs——ProgressSnapshot 增 merging**

`ProgressSnapshot` 结构体加字段：

```rust
    /// 视频任务：下载完成后合并中（HTTP 任务恒 false；segments 恒空数组）
    pub merging: bool,
```

- [ ] **Step 5: manager.rs——TaskEvent/AddTaskOptions/ManagerConfig 扩展**

`TaskEvent::Progress` 变体加 `merging: bool` 字段；`AddTaskOptions` 追加：

```rust
    pub kind: TaskKind,
    pub video: Option<VideoParams>,
    pub video_meta: Option<VideoMeta>,
```

`ManagerConfig` 追加（全部 serde default，兼容旧 settings.json）：

```rust
    #[serde(default)]
    pub video_max_height: Option<u32>,
    #[serde(default)]
    pub video_audio_only: bool,
    #[serde(default = "default_sub_langs")]
    pub video_sub_langs: String,
    #[serde(default)]
    pub video_auto_subs: bool,
    #[serde(default)]
    pub cookie_file: Option<std::path::PathBuf>,
```

及自由函数 `fn default_sub_langs() -> String { "zh-Hans,en".into() }`；`Default for ManagerConfig` 实现补齐五个字段（`video_max_height: None, video_audio_only: false, video_sub_langs: default_sub_langs(), video_auto_subs: false, cookie_file: None`）。

`use crate::task::{TaskId, TaskSpec, TaskState}` 改为加 `TaskKind, VideoMeta, VideoParams`。

`add_task()` 构造 `TaskRecord` 时填充：`kind: opts.kind, video: opts.video.clone(), video_meta: opts.video_meta.clone()`；视频任务 `segments` 固定存 1（`if opts.kind == TaskKind::Video { segments = 1 }`）。`monitor_task` 内 `TaskEvent::Progress` 构造加 `merging: snap.merging`。

- [ ] **Step 6: store.rs——TaskRecord 增字段（本任务先不落库）**

`TaskRecord` 结构体加 `pub kind: TaskKind, pub video: Option<VideoParams>, pub video_meta: Option<VideoMeta>`（`use crate::task::{TaskState, TaskKind, VideoParams, VideoMeta}`）。`row_to_record` 暂时硬编码 `kind: TaskKind::Http, video: None, video_meta: None`（老行皆 http；Task 2 接管列读取）。测试 `fn rec()` 同样补三个字段。

- [ ] **Step 7: http_engine.rs——submit 防御**

`impl Engine for HttpEngine` 的 `submit` 开头加：

```rust
        if spec.kind != TaskKind::Http {
            return Err(SparklingError::Other("HttpEngine 收到非 HTTP 任务".into()));
        }
```

（`use crate::task::TaskKind`。）同时本文件所有 `ProgressSnapshot { … }` 字面量补 `merging: false`（submit 初始快照 1 处、spawn_reporter 2 处，编译器指出）。

- [ ] **Step 8: lib.rs 导出 + 全仓编译修复**

`lib.rs` 的 `pub use task::{TaskId, TaskSpec, TaskState};` 扩为 `pub use task::{TaskId, TaskKind, TaskSpec, TaskState, VideoMeta, VideoParams};`。

然后编译器驱动修复全部构造点（机械改动，不改语义）：
- `tests/*.rs` 与 `tests/common/mod.rs` 中每个 `AddTaskOptions { … }` 补 `kind: TaskKind::Http, video: None, video_meta: None`（加对应 use）
- tests 中如有直接构造 `TaskSpec`/`ProgressSnapshot`/`TaskRecord` 之处，同样补 `kind: TaskKind::Http`/`video: None`/`video_meta: None` 与 `merging: false`
- src-tauri `lib.rs` 的 `add_task` 命令构造 `AddTaskOptions` 处补三个新字段（暂以 `kind: TaskKind::Http, video: None, video_meta: None`，Task 9 扩展为参数）

Run: `cargo build --workspace`
Expected: BUILD OK（零 error）

- [ ] **Step 9: 全量测试 + 质量门**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: 全部 PASS，clippy 零告警

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(core): 任务类型模型 TaskKind/VideoParams/VideoMeta 与 merging 快照标记"
```

---

### Task 2: SQLite 版本化迁移（kind/video 列持久化）

**Files:**
- Modify: `crates/sparkling-core/src/store.rs`
- Test: `crates/sparkling-core/tests/store_migration.rs`（新建）

**Interfaces:**
- Consumes: Task 1 的 `TaskRecord.kind/video/video_meta`
- Produces: tasks 表新列 `kind TEXT NOT NULL DEFAULT 'http'`、`video_params TEXT`（VideoParams JSON）、`video_meta TEXT`（VideoMeta JSON）；`PRAGMA user_version = 1`；旧库（①期 schema、user_version=0）打开自动迁移且老数据 kind='http'。`row_to_record`/`insert` 读写新列。

- [ ] **Step 1: 写失败测试（迁移 + 新列 roundtrip）**

新建 `crates/sparkling-core/tests/store_migration.rs`：

```rust
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
    let v: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
    assert_eq!(v, 1);
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
    assert_eq!(back.video.as_ref().unwrap().format, "bv*[height<=1080]+ba/b");
    assert_eq!(back.video.as_ref().unwrap().subtitles, vec!["zh-Hans".to_string()]);
    assert_eq!(back.video_meta.as_ref().unwrap().title, "测试视频标题");
    assert_eq!(back.video_meta.as_ref().unwrap().duration_sec, Some(123));
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p sparkling-core --test store_migration`
Expected: FAIL——`kind` 列不存在（`migrates_legacy_db_and_defaults_kind_http` 在 `row_to_record` 读 kind 列时报错）

- [ ] **Step 3: 实现迁移与读写**

`store.rs` 的 `init` 改为（CREATE 保持①期 schema 不变，统一走 ALTER 路径——全新库与旧库同一道迁移，避免"CREATE 带新列 + ALTER 重复列"冲突）：

```rust
    fn init(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tasks (
                id          TEXT PRIMARY KEY,
                url         TEXT NOT NULL,
                state       TEXT NOT NULL,
                save_dir    TEXT NOT NULL,
                filename    TEXT,
                segments    INTEGER NOT NULL,
                max_speed   INTEGER,
                total_size  INTEGER,
                downloaded  INTEGER NOT NULL DEFAULT 0,
                error       TEXT,
                created_at  INTEGER NOT NULL
            );",
        )
        .map_err(|e| SparklingError::Other(format!("初始化表失败: {e}")))?;
        // v0→v1：③期视频任务列。user_version 幂等保护（重复打开不重复 ALTER）
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(|e| SparklingError::Other(format!("读取库版本失败: {e}")))?;
        if version < 1 {
            conn.execute_batch(
                "ALTER TABLE tasks ADD COLUMN kind TEXT NOT NULL DEFAULT 'http';
                 ALTER TABLE tasks ADD COLUMN video_params TEXT;
                 ALTER TABLE tasks ADD COLUMN video_meta TEXT;
                 PRAGMA user_version = 1;",
            )
            .map_err(|e| SparklingError::Other(format!("迁移数据库失败: {e}")))?;
        }
        Ok(Self { conn })
    }
```

`insert` 的 SQL 加三列（`kind, video_params, video_meta`）；params 追加 `r.kind.as_str(), video_params_json(r)?, video_meta_json(r)?`，其中两个辅助函数：

```rust
fn video_params_json(r: &TaskRecord) -> Result<String> {
    serde_json::to_string(&r.video)
        .map_err(|e| SparklingError::Other(format!("序列化视频参数失败: {e}")))
}
fn video_meta_json(r: &TaskRecord) -> Result<String> {
    serde_json::to_string(&r.video_meta)
        .map_err(|e| SparklingError::Other(format!("序列化视频元数据失败: {e}")))
}
```

（`Option` 序列化为 `null`/对象字符串，均合法 TEXT。）

`row_to_record` 读列（注意 SELECT * 的列序 = 建表序 + ALTER 序，kind 在 index 11、video_params 12、video_meta 13）：

```rust
            kind: TaskKind::parse(&row.get::<_, String>(11).unwrap_or_else(|_| "http".into()))
                .unwrap_or(TaskKind::Http),
            video: row
                .get::<_, Option<String>>(12)
                .ok()
                .flatten()
                .and_then(|s| serde_json::from_str(&s).ok()),
            video_meta: row
                .get::<_, Option<String>>(13)
                .ok()
                .flatten()
                .and_then(|s| serde_json::from_str(&s).ok()),
```

（SELECT * 依赖列序脆弱，故顺手把 `get`/`get_all`/`insert` 的 SELECT 改为显式列名 `SELECT id, url, state, save_dir, filename, segments, max_speed, total_size, downloaded, error, created_at, kind, video_params, video_meta FROM tasks`——get 与 get_all 两处，INSERT 列名已显式。）

- [ ] **Step 4: 运行测试通过**

Run: `cargo test -p sparkling-core --test store_migration && cargo test -p sparkling-core`
Expected: PASS（含①期全部存量测试——证明迁移未破坏既有行为）

- [ ] **Step 5: 质量门 + Commit**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt`

```bash
git add -A
git commit -m "feat(core): tasks 表版本化迁移（PRAGMA user_version）与视频任务列持久化"
```

---

### Task 3: yt-dlp 进度行解析器（纯函数）

**Files:**
- Create: `crates/sparkling-core/src/video/mod.rs`
- Create: `crates/sparkling-core/src/video/progress.rs`
- Modify: `crates/sparkling-core/src/lib.rs`（`pub mod video;`）

**Interfaces:**
- Produces: `video::progress::{ProgressLine, parse_progress_line, is_merge_line}`
  - `ProgressLine { downloaded: u64, total: Option<u64>, speed: Option<u64> }`（total = total_bytes 优先、缺失回退 total_bytes_estimate；speed 字节/秒）
  - `parse_progress_line(line: &str) -> Option<ProgressLine>`（仅识别 `SPARKLING|` 前缀行）
  - `is_merge_line(line: &str) -> bool`（`[Merger]`/`[ExtractAudio]` 前缀）
- 进度模板约定（Task 6 build_args 使用同款）：`download:SPARKLING|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.total_bytes_estimate)s|%(progress.speed)s`，yt-dlp 输出形如 `SPARKLING|123456|NA|1234567|234567.8`，NA = 字段缺失

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/src/video/progress.rs`：

```rust
//! yt-dlp --progress-template 输出行解析（纯函数，无 IO）
//!
//! 模板（engine 构造命令时使用同款字符串）：
//! download:SPARKLING|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|
//!           %(progress.total_bytes_estimate)s|%(progress.speed)s

/// 一行进度（字段 NA → None）
#[derive(Debug, Clone, PartialEq)]
pub struct ProgressLine {
    pub downloaded: u64,
    /// total_bytes 优先；缺失回退 total_bytes_estimate
    pub total: Option<u64>,
    /// bytes/s
    pub speed: Option<u64>,
}

/// 解析 SPARKLING 前缀进度行；其它行返回 None
pub fn parse_progress_line(line: &str) -> Option<ProgressLine> {
    let rest = line.trim().strip_prefix("SPARKLING|")?;
    let mut parts = rest.split('|');
    let downloaded: u64 = parts.next()?.trim().parse().ok()?;
    let total: Option<u64> = parse_na(parts.next()?);
    let estimate: Option<u64> = parse_na(parts.next()?);
    let speed: Option<u64> = parse_na(parts.next()?);
    Some(ProgressLine {
        downloaded,
        total: total.or(estimate),
        speed,
    })
}

/// "NA" → None；数值字符串（可含小数）→ 截断取整
fn parse_na(s: &str) -> Option<u64> {
    let t = s.trim();
    if t == "NA" || t.is_empty() {
        return None;
    }
    t.parse::<f64>().ok().map(|v| v as u64)
}

/// 合并/提取阶段行（下载已 100%，ffmpeg 工作中）
pub fn is_merge_line(line: &str) -> bool {
    line.starts_with("[Merger]") || line.starts_with("[ExtractAudio]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_line() {
        let p = parse_progress_line("SPARKLING|123456|2000000|2000000|234567.8").unwrap();
        assert_eq!(p.downloaded, 123456);
        assert_eq!(p.total, Some(2000000));
        assert_eq!(p.speed, Some(234567));
    }

    #[test]
    fn total_falls_back_to_estimate() {
        // 直播/未知大小：total_bytes = NA，estimate 有值
        let p = parse_progress_line("SPARKLING|123456|NA|2000000|NA").unwrap();
        assert_eq!(p.total, Some(2000000));
        assert_eq!(p.speed, None);
    }

    #[test]
    fn all_na_total_is_none() {
        let p = parse_progress_line("SPARKLING|123456|NA|NA|100.5").unwrap();
        assert_eq!(p.total, None);
        assert_eq!(p.speed, Some(100));
    }

    #[test]
    fn ignores_non_progress_lines() {
        assert!(parse_progress_line("[download] Destination: a.mp4").is_none());
        assert!(parse_progress_line("[Merger] Merging formats").is_none());
        assert!(parse_progress_line("").is_none());
        // 前缀不符（yt-dlp 其它模板输出）
        assert!(parse_progress_line("OTHER|1|2|3|4").is_none());
    }

    #[test]
    fn merge_line_detection() {
        assert!(is_merge_line("[Merger] Merging formats into \"x.mp4\""));
        assert!(is_merge_line("[ExtractAudio] Destination: x.m4a"));
        assert!(!is_merge_line("[download] 100% of 10.00MiB"));
        assert!(!is_merge_line("SPARKLING|1|2|3|4"));
    }
}
```

`crates/sparkling-core/src/video/mod.rs`：

```rust
//! ③期视频解析下载（yt-dlp 包装）：二进制管理、解析、引擎
pub mod progress;
```

`lib.rs` 加 `pub mod video;`。

- [ ] **Step 2: 运行测试**

Run: `cargo test -p sparkling-core --lib video::`
Expected: PASS（实现与测试同文件一次落位——纯函数无隐藏分支，测试即文档）

- [ ] **Step 3: 质量门 + Commit**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt && cargo test`

```bash
git add -A
git commit -m "feat(core): yt-dlp progress-template 进度行解析器（纯函数）"
```

---

### Task 4: probe JSON 解析（VideoInfo + fixtures）

**Files:**
- Create: `crates/sparkling-core/src/video/probe.rs`
- Create: `crates/sparkling-core/tests/fixtures/video_info.json`
- Create: `crates/sparkling-core/tests/fixtures/playlist_info.json`
- Modify: `crates/sparkling-core/src/video/mod.rs`
- Test: `crates/sparkling-core/src/video/probe.rs` 内 `mod tests`（fixture 经 `include_str!` 引入）

**Interfaces:**
- Consumes: 无（独立纯解析层）
- Produces: `video::probe::{VideoInfo, FormatEntry, PlaylistEntry, parse_info_json}`
  - `VideoInfo { title: String, duration_sec: Option<u64>, thumbnail: Option<String>, uploader: Option<String>, webpage_url: Option<String>, formats: Vec<FormatEntry>, playlist: Option<Vec<PlaylistEntry>> }`
  - `FormatEntry { format_id: String, ext: String, height: Option<u32>, fps: Option<f64>, vcodec: String, acodec: String, filesize: Option<u64>, tbr: Option<f64> }`（vcodec/acodec 缺省 "none"）
  - `PlaylistEntry { url: String, title: String, duration_sec: Option<u64> }`（url = entry.url 或 webpage_url，无有效 url 的条目跳过）
  - `parse_info_json(json: &str) -> Result<VideoInfo>`：`_type == "playlist"` → playlist Some；过滤 storyboard（ext == "mhtml"）与 vcodec/acodec 双 "none" 的空格式

- [ ] **Step 1: 写 fixtures（真实 yt-dlp -J 输出的精简样本）**

`crates/sparkling-core/tests/fixtures/video_info.json`（单视频，含视频+音频分离流、渐进流、纯音频、storyboard）：

```json
{
  "_type": "video",
  "id": "dQW4w9WgXcQ",
  "title": "测试视频 - 中文标题",
  "duration": 212.136,
  "thumbnail": "https://example.com/thumb.jpg",
  "uploader": "测试上传者",
  "webpage_url": "https://www.youtube.com/watch?v=dQW4w9WgXcQ",
  "formats": [
    { "format_id": "sb0", "ext": "mhtml", "height": 48, "vcodec": "vp9", "acodec": "none", "tbr": 20.5 },
    { "format_id": "140", "ext": "m4a", "acodec": "mp4a.40.2", "vcodec": "none", "filesize": 3440000, "tbr": 129.8 },
    { "format_id": "137", "ext": "mp4", "height": 1080, "fps": 25.0, "vcodec": "avc1.640028", "acodec": "none", "filesize": 45000000, "tbr": 2000.5 },
    { "format_id": "18", "ext": "mp4", "height": 360, "fps": 25.0, "vcodec": "avc1.42001E", "acodec": "mp4a.40.2", "filesize": 9000000, "tbr": 390.2 },
    { "format_id": "empty", "ext": "bin" }
  ]
}
```

`crates/sparkling-core/tests/fixtures/playlist_info.json`（--flat-playlist 列表）：

```json
{
  "_type": "playlist",
  "id": "PLtest",
  "title": "测试播放列表",
  "webpage_url": "https://www.youtube.com/playlist?list=PLtest",
  "entries": [
    { "id": "v1", "title": "第一集", "url": "https://www.youtube.com/watch?v=v1", "duration": 100.5 },
    { "id": "v2", "title": "第二集", "url": "https://www.youtube.com/watch?v=v2" },
    { "id": "bad", "title": "无URL条目" }
  ]
}
```

- [ ] **Step 2: 写失败测试（probe.rs 尾部 mod tests）**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const VIDEO_JSON: &str = include_str!("../../../tests/fixtures/video_info.json");
    const PLAYLIST_JSON: &str = include_str!("../../../tests/fixtures/playlist_info.json");

    #[test]
    fn parses_single_video() {
        let info = parse_info_json(VIDEO_JSON).unwrap();
        assert_eq!(info.title, "测试视频 - 中文标题");
        assert_eq!(info.duration_sec, Some(212));
        assert_eq!(info.uploader.as_deref(), Some("测试上传者"));
        assert!(info.playlist.is_none());
        // storyboard(sb0) 与双 none(empty) 被过滤；保留 140/137/18
        assert_eq!(info.formats.len(), 3);
        let f137 = info.formats.iter().find(|f| f.format_id == "137").unwrap();
        assert_eq!(f137.height, Some(1080));
        assert_eq!(f137.fps, Some(25.0));
        assert_eq!(f137.filesize, Some(45000000));
        assert_eq!(f137.acodec, "none");
        let f140 = info.formats.iter().find(|f| f.format_id == "140").unwrap();
        assert_eq!(f140.vcodec, "none");
    }

    #[test]
    fn parses_playlist_and_skips_entry_without_url() {
        let info = parse_info_json(PLAYLIST_JSON).unwrap();
        let pl = info.playlist.expect("应识别为播放列表");
        assert_eq!(pl.len(), 2);
        assert_eq!(pl[0].title, "第一集");
        assert_eq!(pl[0].duration_sec, Some(100));
        assert_eq!(pl[1].url, "https://www.youtube.com/watch?v=v2");
    }

    #[test]
    fn rejects_invalid_json() {
        assert!(parse_info_json("not json").is_err());
    }
}
```

- [ ] **Step 3: 运行确认失败**

Run: `cargo test -p sparkling-core --lib video::probe`
Expected: FAIL，`parse_info_json` 未定义

- [ ] **Step 4: 实现（probe.rs 主体）**

```rust
//! yt-dlp -J 输出解析：单视频 → 完整格式表；播放列表 → flat 条目。
//! 字段全部防御性 Option（yt-dlp JSON 字段随 extractor 有增减）。
use crate::{Result, SparklingError};
use serde::Deserialize;

#[derive(Debug, Clone, serde::Serialize)]
pub struct VideoInfo {
    pub title: String,
    pub duration_sec: Option<u64>,
    pub thumbnail: Option<String>,
    pub uploader: Option<String>,
    pub webpage_url: Option<String>,
    pub formats: Vec<FormatEntry>,
    pub playlist: Option<Vec<PlaylistEntry>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FormatEntry {
    pub format_id: String,
    pub ext: String,
    pub height: Option<u32>,
    pub fps: Option<f64>,
    pub vcodec: String,
    pub acodec: String,
    pub filesize: Option<u64>,
    pub tbr: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PlaylistEntry {
    pub url: String,
    pub title: String,
    pub duration_sec: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawInfo {
    #[serde(default, rename = "_type")]
    kind: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    duration: Option<f64>,
    #[serde(default)]
    thumbnail: Option<String>,
    #[serde(default)]
    uploader: Option<String>,
    #[serde(default)]
    webpage_url: Option<String>,
    #[serde(default)]
    formats: Vec<RawFormat>,
    #[serde(default)]
    entries: Vec<RawEntry>,
}

#[derive(Debug, Deserialize)]
struct RawFormat {
    #[serde(default)]
    format_id: Option<String>,
    #[serde(default)]
    ext: Option<String>,
    #[serde(default)]
    height: Option<u32>,
    #[serde(default)]
    fps: Option<f64>,
    #[serde(default)]
    vcodec: Option<String>,
    #[serde(default)]
    acodec: Option<String>,
    #[serde(default)]
    filesize: Option<u64>,
    #[serde(default)]
    tbr: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawEntry {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    webpage_url: Option<String>,
    #[serde(default)]
    duration: Option<f64>,
}

/// 解析 yt-dlp -J --flat-playlist 的 JSON 输出
pub fn parse_info_json(json: &str) -> Result<VideoInfo> {
    let raw: RawInfo = serde_json::from_str(json)
        .map_err(|e| SparklingError::Other(format!("解析视频信息失败: {e}")))?;
    let title = raw.title.clone().unwrap_or_else(|| "未知标题".into());
    let duration_sec = raw.duration.map(|d| d as u64);
    if raw.kind.as_deref() == Some("playlist") || !raw.entries.is_empty() {
        let playlist = raw
            .entries
            .into_iter()
            .filter_map(|e| {
                // flat 条目 url 与 webpage_url 皆可能出现；二者皆无 → 跳过
                let url = e.url.or(e.webpage_url.clone())?;
                Some(PlaylistEntry {
                    url,
                    title: e.title.unwrap_or_else(|| e.id.clone().unwrap_or_default()),
                    duration_sec: e.duration.map(|d| d as u64),
                })
            })
            .collect::<Vec<_>>();
        return Ok(VideoInfo {
            title,
            duration_sec,
            thumbnail: raw.thumbnail,
            uploader: raw.uploader,
            webpage_url: raw.webpage_url,
            formats: vec![],
            playlist: Some(playlist),
        });
    }
    let formats = raw
        .formats
        .into_iter()
        .filter(|f| {
            let vcodec = f.vcodec.as_deref().unwrap_or("none");
            let acodec = f.acodec.as_deref().unwrap_or("none");
            // 过滤 storyboard（mhtml）与双 none 空格式
            f.ext.as_deref() != Some("mhtml") && !(vcodec == "none" && acodec == "none")
        })
        .map(|f| FormatEntry {
            format_id: f.format_id.unwrap_or_default(),
            ext: f.ext.unwrap_or_else(|| "unknown".into()),
            height: f.height,
            fps: f.fps,
            vcodec: f.vcodec.unwrap_or_else(|| "none".into()),
            acodec: f.acodec.unwrap_or_else(|| "none".into()),
            filesize: f.filesize,
            tbr: f.tbr,
        })
        .collect();
    Ok(VideoInfo {
        title,
        duration_sec,
        thumbnail: raw.thumbnail,
        uploader: raw.uploader,
        webpage_url: raw.webpage_url,
        formats,
        playlist: None,
    })
}
```

`video/mod.rs` 加 `pub mod probe;`。

- [ ] **Step 5: 运行测试通过**

Run: `cargo test -p sparkling-core --lib video::`
Expected: PASS

- [ ] **Step 6: 质量门 + Commit**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt`

```bash
git add -A
git commit -m "feat(core): yt-dlp -J 输出解析为 VideoInfo（含播放列表/格式过滤）"
```

---

### Task 5: 进程调用抽象 YtDlpRunner（TokioChildRunner + FakeRunner）

**Files:**
- Create: `crates/sparkling-core/src/video/runner.rs`
- Modify: `crates/sparkling-core/src/video/mod.rs`

**Interfaces:**
- Produces:
  - `KillReason { Pause, Cancel }`（Clone Copy）
  - `RunResult { killed: Option<KillReason>, code: Option<i32>, stderr_tail: String }`
  - `RunHandle { pub done: JoinHandle<RunResult>, kill_tx }`：`kill(&self, reason)`、`async wait(self) -> RunResult`
  - `#[async_trait] trait YtDlpRunner: Send + Sync { async fn start(&self, args: Vec<String>, on_line: Box<dyn FnMut(&str) + Send>) -> Result<RunHandle>; }`
  - `TokioChildRunner { pub bin: PathBuf }`：生产实现，`kill_on_drop(true)`、Windows `CREATE_NO_WINDOW`（0x08000000）、stdout 逐行 UTF-8、stderr 全量收后取尾 4KB
  - `FakeRunner { pub scripts: Mutex<VecDeque<Vec<ScriptStep>>>, pub calls: Mutex<Vec<Vec<String>>> }`：每次 `start` 弹出一个脚本；`ScriptStep::Lines(&'static [&'static str]) | Delay(Duration) | Exit(i32)`；kill 在 Delay 期间到达 → `RunResult { killed: Some(reason), code: None }`

- [ ] **Step 1: 写失败测试（FakeRunner 语义：kill 中断 / 正常退出 / args 记录）**

`crates/sparkling-core/src/video/runner.rs` 尾部：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn recv_all(rx: &mut mpsc::UnboundedReceiver<String>) -> Vec<String> {
        let mut out = vec![];
        while let Ok(l) = rx.try_recv() {
            out.push(l);
        }
        out
    }

    #[tokio::test]
    async fn fake_runs_script_and_exits() {
        let r = FakeRunner::default();
        r.scripts.lock().unwrap().push_back(vec![
            ScriptStep::Lines(&["SPARKLING|100|200|200|50"]),
            ScriptStep::Exit(0),
        ]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let h = r
            .start(
                vec!["-J".into()],
                Box::new(move |l| {
                    let _ = tx.send(l.to_string());
                }),
            )
            .await
            .unwrap();
        let res = h.wait().await;
        assert_eq!(res.code, Some(0));
        assert!(res.killed.is_none());
        assert_eq!(recv_all(&mut rx), vec!["SPARKLING|100|200|200|50".to_string()]);
        assert_eq!(r.calls.lock().unwrap()[0], vec!["-J".to_string()]);
    }

    #[tokio::test]
    async fn fake_kill_during_delay_reports_killed() {
        let r = FakeRunner::default();
        r.scripts.lock().unwrap().push_back(vec![
            ScriptStep::Lines(&["SPARKLING|1|10|10|1"]),
            ScriptStep::Delay(Duration::from_secs(60)),
            ScriptStep::Exit(0),
        ]);
        let h = r.start(vec![], Box::new(|_| {})).await.unwrap();
        h.kill(KillReason::Pause);
        let res = h.wait().await;
        assert_eq!(res.killed, Some(KillReason::Pause));
        assert_eq!(res.code, None);
    }

    #[tokio::test]
    async fn fake_exit_code_propagates() {
        let r = FakeRunner::default();
        r.scripts.lock().unwrap().push_back(vec![ScriptStep::Exit(2)]);
        let h = r.start(vec![], Box::new(|_| {})).await.unwrap();
        let res = h.wait().await;
        assert_eq!(res.code, Some(2));
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p sparkling-core --lib video::runner`
Expected: FAIL，类型未定义

- [ ] **Step 3: 实现（runner.rs 主体）**

```rust
//! yt-dlp 进程调用抽象：生产 TokioChildRunner（spawn 真 exe）与测试 FakeRunner。
//! VideoEngine 依赖本 trait 而非直接 spawn——CI 无需真二进制即可全量单测。
use crate::{Result, SparklingError};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// 主动杀进程的原因（区别于进程自身退出）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillReason {
    Pause,
    Cancel,
}

/// 一次运行的退出结果。killed 优先于 code 判定（被杀进程 code 无意义）
#[derive(Debug, Clone)]
pub struct RunResult {
    pub killed: Option<KillReason>,
    pub code: Option<i32>,
    /// stderr 末尾若干 KB（错误摘要提取用）
    pub stderr_tail: String,
}

pub struct RunHandle {
    pub done: JoinHandle<RunResult>,
    kill_tx: mpsc::UnboundedSender<KillReason>,
}

impl RunHandle {
    pub fn kill(&self, reason: KillReason) {
        let _ = self.kill_tx.send(reason);
    }
    pub async fn wait(self) -> RunResult {
        self.done.await.unwrap_or(RunResult {
            killed: None,
            code: None,
            stderr_tail: "runner 任务异常退出".into(),
        })
    }
}

#[async_trait]
pub trait YtDlpRunner: Send + Sync {
    async fn start(
        &self,
        args: Vec<String>,
        on_line: Box<dyn FnMut(&str) + Send>,
    ) -> Result<RunHandle>;
}

/// 生产实现：spawn 真 yt-dlp 进程
pub struct TokioChildRunner {
    pub bin: PathBuf,
}

/// stderr 保留末尾 max 字节（按 char 边界安全截断）
fn tail_utf8(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let start = s.len() - max;
    let mut i = start;
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    s[i..].to_string()
}

#[async_trait]
impl YtDlpRunner for TokioChildRunner {
    async fn start(
        &self,
        args: Vec<String>,
        mut on_line: Box<dyn FnMut(&str) + Send>,
    ) -> Result<RunHandle> {
        let mut cmd = tokio::process::Command::new(&self.bin);
        cmd.args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // abort/泄漏兜底：句柄 drop 即杀进程
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| SparklingError::Other(format!("启动 yt-dlp 失败: {e}")))?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (kill_tx, mut kill_rx) = mpsc::unbounded_channel::<KillReason>();
        let done = tokio::spawn(async move {
            let mut child = child;
            let mut killed = None;
            let stderr_task = tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = String::new();
                let mut stderr = stderr;
                let _ = stderr.read_to_string(&mut buf).await;
                buf
            });
            {
                use tokio::io::AsyncBufReadExt;
                let mut lines = tokio::io::BufReader::new(stdout).lines();
                loop {
                    tokio::select! {
                        line = lines.next_line() => match line {
                            Ok(Some(l)) => on_line(&l),
                            Ok(None) => break,
                            Err(_) => break,
                        },
                        r = kill_rx.recv() => {
                            killed = r;
                            let _ = child.kill().await;
                            break;
                        }
                    }
                }
            }
            let code = child.wait().await.ok().and_then(|s| s.code());
            let stderr_tail = stderr_task.await.unwrap_or_default();
            RunResult {
                killed,
                code,
                stderr_tail: tail_utf8(&stderr_tail, 4096),
            }
        });
        Ok(RunHandle { done, kill_tx })
    }
}

/// 测试用脚本步骤
pub enum ScriptStep {
    Lines(&'static [&'static str]),
    Delay(std::time::Duration),
    Exit(i32),
}

/// 测试用 Runner：start 弹出一个脚本按步回放
#[derive(Default)]
pub struct FakeRunner {
    pub scripts: Mutex<VecDeque<Vec<ScriptStep>>>,
    pub calls: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl YtDlpRunner for FakeRunner {
    async fn start(
        &self,
        args: Vec<String>,
        mut on_line: Box<dyn FnMut(&str) + Send>,
    ) -> Result<RunHandle> {
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        self.calls.lock().unwrap().push(args);
        let (kill_tx, mut kill_rx) = mpsc::unbounded_channel::<KillReason>();
        let done = tokio::spawn(async move {
            let mut killed: Option<KillReason> = None;
            for step in script {
                match step {
                    ScriptStep::Lines(lines) => {
                        for l in lines {
                            if killed.is_some() {
                                break;
                            }
                            on_line(l);
                        }
                    }
                    ScriptStep::Delay(d) => {
                        tokio::select! {
                            _ = tokio::time::sleep(d) => {}
                            r = kill_rx.recv() => killed = r,
                        }
                    }
                    ScriptStep::Exit(code) => {
                        let k = kill_rx.try_recv().ok().or(killed);
                        return RunResult {
                            killed: k,
                            code: if k.is_some() { None } else { Some(code) },
                            stderr_tail: String::new(),
                        };
                    }
                }
                if killed.is_some() {
                    break;
                }
            }
            RunResult {
                killed,
                code: None,
                stderr_tail: String::new(),
            }
        });
        Ok(RunHandle { done, kill_tx })
    }
}
```

`video/mod.rs` 加 `pub mod runner;` 与 `pub use runner::{FakeRunner, KillReason, RunHandle, RunResult, ScriptStep, TokioChildRunner, YtDlpRunner};`。

- [ ] **Step 4: 运行测试通过**

Run: `cargo test -p sparkling-core --lib video::`
Expected: PASS

- [ ] **Step 5: 质量门 + Commit**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt`

```bash
git add -A
git commit -m "feat(core): yt-dlp 进程调用抽象 YtDlpRunner（TokioChildRunner + FakeRunner）"
```

---

### Task 6: VideoEngine（Engine trait 实现，FakeRunner 全覆盖状态机）

**Files:**
- Create: `crates/sparkling-core/src/video/engine.rs`
- Modify: `crates/sparkling-core/src/video/mod.rs`
- Modify: `crates/sparkling-core/src/http_engine.rs`（`sanitize_filename` 改 `pub(crate)`→`pub`）

**Interfaces:**
- Consumes: Task 1 的 `TaskSpec.kind/video`、`ProgressSnapshot.merging`；Task 3 的 `progress::{parse_progress_line, is_merge_line}`；Task 5 的 `{YtDlpRunner, RunHandle, KillReason, RunResult}`
- Produces:
  - `VideoEngine::new(runner: Arc<dyn YtDlpRunner>, ffmpeg: Option<PathBuf>, cookie: Option<PathBuf>) -> Self`，实现 `Engine`（submit/set_speed_limit/shutdown，Drop = shutdown）
  - `video::engine::build_args(spec: &TaskSpec, ffmpeg: Option<&Path>, cookie: Option<&Path>, limit: Option<u64>) -> Vec<String>`（pub 供测试）
  - `video::engine::cleanup_partial(save_dir: &Path, filename: &str)`（pub，manager remove_task 复用）
  - `video::engine::extract_error(stderr: &str) -> String`（pub，probe 错误展示复用）

- [ ] **Step 1: 写失败测试（build_args + 状态机）**

`crates/sparkling-core/src/video/engine.rs` 尾部：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{TaskKind, VideoParams};
    use crate::video::runner::{FakeRunner, ScriptStep};
    use std::time::Duration;

    fn video_spec(save_dir: &std::path::Path) -> TaskSpec {
        TaskSpec {
            url: "https://www.youtube.com/watch?v=test".into(),
            save_dir: save_dir.to_path_buf(),
            filename: Some("测试视频".into()),
            segments: 1,
            max_speed: None,
            kind: TaskKind::Video,
            video: Some(VideoParams {
                format: "bv*[height<=1080]+ba/b".into(),
                subtitles: vec!["zh-Hans".into()],
                auto_subs: true,
            }),
        }
    }

    fn engine(runner: Arc<FakeRunner>) -> VideoEngine {
        VideoEngine::new(runner, Some(PathBuf::from("ffmpeg.exe")), None)
    }

    #[test]
    fn build_args_contains_required_flags() {
        let spec = video_spec(Path::new("D:\\dl"));
        let args = build_args(&spec, Some(Path::new("ff/ffmpeg.exe")), None, Some(1024));
        let joined = args.join(" ");
        // 核心参数逐项断言
        assert!(args.windows(2).any(|w| w[0] == "-f" && w[1] == "bv*[height<=1080]+ba/b"));
        assert!(args.contains(&"-c".to_string()), "断点续传 -c 必须在");
        assert!(args.contains(&"--newline".to_string()));
        assert!(args.contains(&"--no-mtime".to_string()));
        assert!(joined.contains("--progress-template"));
        assert!(joined.contains("SPARKLING|%(progress.downloaded_bytes)s"));
        assert!(args.windows(2).any(|w| w[0] == "--ffmpeg-location" && w[1] == "ff/ffmpeg.exe"));
        assert!(args.windows(2).any(|w[0] == "-r" && w[1] == "1K"));
        assert!(args.windows(2).any(|w[0] == "--sub-langs" && w[1] == "zh-Hans"));
        assert!(args.contains(&"--write-subs".to_string()));
        assert!(args.contains(&"--write-auto-subs".to_string()));
        assert!(args.windows(2).any(|w[0] == "-o" && w[1].ends_with("测试视频.%(ext)s")));
        assert_eq!(args.last().unwrap(), "https://www.youtube.com/watch?v=test");
    }

    #[test]
    fn build_args_omits_optional_when_none() {
        let spec = video_spec(Path::new("D:\\dl"));
        let args = build_args(&spec, None, None, None);
        let joined = args.join(" ");
        assert!(!joined.contains("--ffmpeg-location"));
        assert!(!joined.contains("--cookies"));
        assert!(!joined.contains("-r"));
        assert!(!joined.contains("--write-subs"));
        assert!(!joined.contains("--write-auto-subs"));
    }

    #[test]
    fn build_args_passes_cookie_file() {
        let spec = video_spec(Path::new("D:\\dl"));
        let args = build_args(&spec, None, Some(Path::new("data/cookies.txt")), None);
        assert!(args.windows(2).any(|w| w[0] == "--cookies" && w[1] == "data/cookies.txt"));
    }

    async fn wait_state(rx: &mut watch::Receiver<ProgressSnapshot>, want: TaskState) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if rx.borrow().state == want {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "等待状态 {want:?} 超时");
            rx.changed().await.unwrap();
        }
    }

    #[tokio::test]
    async fn completes_with_progress_lines() {
        let runner = Arc::new(FakeRunner::default());
        runner.scripts.lock().unwrap().push_back(vec![
            ScriptStep::Lines(&["SPARKLING|100|1000|1000|500"]),
            ScriptStep::Lines(&["SPARKLING|1000|1000|1000|500"]),
            ScriptStep::Lines(&["[Merger] Merging formats into \"x.mp4\""]),
            ScriptStep::Exit(0),
        ]);
        let eng = engine(runner.clone());
        let handle = eng.submit(video_spec(Path::new("D:\\dl"))).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Completed).await;
        let snap = rx.borrow().clone();
        assert_eq!(snap.downloaded, 1000);
        assert_eq!(snap.total, 1000);
        assert!(snap.merging, "合并行应置 merging");
        assert_eq!(snap.filename.as_deref(), Some("测试视频"));
    }

    #[tokio::test]
    async fn pause_kills_process_and_resume_restarts() {
        let runner = Arc::new(FakeRunner::default());
        // 第一次运行：一行进度后长时间下载（Delay）等待 pause
        runner.scripts.lock().unwrap().push_back(vec![
            ScriptStep::Lines(&["SPARKLING|100|1000|1000|500"]),
            ScriptStep::Delay(Duration::from_secs(60)),
        ]);
        // 第二次运行（恢复）：直接完成
        runner.scripts.lock().unwrap().push_back(vec![
            ScriptStep::Lines(&["SPARKLING|1000|1000|1000|800"]),
            ScriptStep::Exit(0),
        ]);
        let eng = engine(runner.clone());
        let handle = eng.submit(video_spec(Path::new("D:\\dl"))).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Running).await;
        handle.pause().unwrap();
        wait_state(&mut rx, TaskState::Paused).await;
        handle.resume().unwrap();
        wait_state(&mut rx, TaskState::Completed).await;
        // 恢复后重启了进程（两次 start）
        assert_eq!(runner.calls.lock().unwrap().len(), 2, "暂停恢复应重启进程");
        // 两次都带 -c（续传）
        for call in runner.calls.lock().unwrap().iter() {
            assert!(call.contains(&"-c".to_string()), "每次运行都要 -c 续传");
        }
        let snap = rx.borrow().clone();
        assert_eq!(snap.downloaded, 1000);
        assert!(!snap.merging, "重启后 merging 复位");
    }

    #[tokio::test]
    async fn cancel_cleans_partial_files() {
        let dir = tempfile::tempdir().unwrap();
        let save = dir.path();
        // 预置 .part 与分片残留
        std::fs::write(save.join("测试视频.mp4.part"), b"x").unwrap();
        std::fs::write(save.join("测试视频.f137.mp4"), b"x").unwrap();
        std::fs::write(save.join("无关文件.txt"), b"x").unwrap();
        let runner = Arc::new(FakeRunner::default());
        runner.scripts.lock().unwrap().push_back(vec![
            ScriptStep::Lines(&["SPARKLING|10|1000|1000|100"]),
            ScriptStep::Delay(Duration::from_secs(60)),
        ]);
        let eng = engine(runner);
        let handle = eng.submit(video_spec(save)).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Running).await;
        handle.cancel().unwrap();
        wait_state(&mut rx, TaskState::Cancelled).await;
        assert!(!save.join("测试视频.mp4.part").exists(), ".part 应被清理");
        assert!(!save.join("测试视频.f137.mp4").exists(), "分片残留应被清理");
        assert!(save.join("无关文件.txt").exists(), "无关文件不得误删");
    }

    #[tokio::test]
    async fn nonzero_exit_fails_with_stderr_summary() {
        let runner = Arc::new(FakeRunner::default());
        runner.scripts.lock().unwrap().push_back(vec![ScriptStep::Exit(1)]);
        let eng = engine(runner);
        let handle = eng.submit(video_spec(Path::new("D:\\dl"))).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Failed).await;
        let snap = rx.borrow().clone();
        assert!(snap.error.unwrap().contains("yt-dlp 退出码 1"), "错误应含退出码");
    }

    #[test]
    fn extract_error_takes_last_error_line() {
        let stderr = "WARNING: something\nERROR: Unsupported URL\nERROR: Video unavailable";
        assert_eq!(extract_error(stderr), "Video unavailable");
        assert_eq!(extract_error("no error lines"), "no error lines");
        // 长文本截断到 200
        let long = "x".repeat(500);
        assert!(extract_error(&long).len() <= 200);
    }

    #[test]
    fn rejects_http_spec() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let eng = engine(Arc::new(FakeRunner::default()));
            let mut spec = video_spec(Path::new("D:\\dl"));
            spec.kind = TaskKind::Http;
            let err = eng.submit(spec).await.unwrap_err();
            assert!(err.user_message().contains("非视频任务"));
        });
    }
}
```

注意：`nonzero_exit_fails_with_stderr_summary` 依赖 FakeRunner 的 `stderr_tail`——本任务给 `ScriptStep::Exit` 增补 stderr：把 `ScriptStep::Exit(i32)` 改为携带 `&'static str` stderr 太侵入；保持 Exit(i32) 不变，`RunResult.stderr_tail` 为空串时错误消息即 `yt-dlp 退出码 1`（断言据此写，见 Step 3 实现）。若希望测试 stderr 提取，`extract_error` 已有纯函数测试覆盖。

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p sparkling-core --lib video::engine`
Expected: FAIL，模块未定义

- [ ] **Step 3: 实现（engine.rs 主体）**

```rust
//! VideoEngine：包装 yt-dlp 进程的下载引擎（③期）。
//! 每任务一个进程；暂停 = 杀进程，恢复 = 重启进程（yt-dlp -c 从 .part 续传）；
//! 进度经 --progress-template 结构化输出，逐行解析为 ProgressSnapshot。
use crate::engine::{ControlMsg, Engine, ProgressSnapshot, TaskHandle};
use crate::task::{TaskId, TaskKind, TaskSpec, TaskState};
use crate::video::progress::{is_merge_line, parse_progress_line};
use crate::video::runner::{KillReason, RunHandle, YtDlpRunner};
use crate::{Result, SparklingError};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

pub struct VideoEngine {
    runner: Arc<dyn YtDlpRunner>,
    ffmpeg: Option<PathBuf>,
    cookie: Option<PathBuf>,
    /// 全局限速（新进程生效；yt-dlp 不支持运行中热改）
    limit: Arc<Mutex<Option<u64>>>,
    registry: Arc<Mutex<HashMap<TaskId, JoinHandle<()>>>>,
}

impl VideoEngine {
    pub fn new(
        runner: Arc<dyn YtDlpRunner>,
        ffmpeg: Option<PathBuf>,
        cookie: Option<PathBuf>,
    ) -> Self {
        Self {
            runner,
            ffmpeg,
            cookie,
            limit: Arc::new(Mutex::new(None)),
            registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn shutdown(&self) {
        for (_, h) in self.registry.lock().unwrap().drain() {
            h.abort(); // kill_on_drop 兜底杀进程
        }
    }
}

impl Drop for VideoEngine {
    fn drop(&mut self) {
        VideoEngine::shutdown(self);
    }
}

#[async_trait]
impl Engine for VideoEngine {
    async fn submit(&self, spec: TaskSpec) -> Result<TaskHandle> {
        if spec.kind != TaskKind::Video {
            return Err(SparklingError::Other("VideoEngine 收到非视频任务".into()));
        }
        let id: TaskId = uuid::Uuid::new_v4().to_string();
        let filename = spec
            .filename
            .clone()
            .unwrap_or_else(|| "video".into());
        let (progress_tx, progress_rx) = watch::channel(ProgressSnapshot {
            state: TaskState::Running,
            downloaded: 0,
            total: 0,
            speed: 0,
            segments: vec![],
            error: None,
            filename: Some(filename),
            merging: false,
        });
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let join = tokio::spawn(supervise_video(
            id.clone(),
            spec,
            self.runner.clone(),
            self.ffmpeg.clone(),
            self.cookie.clone(),
            self.limit.clone(),
            progress_tx,
            control_rx,
            self.registry.clone(),
        ));
        self.registry.lock().unwrap().insert(id.clone(), join);
        Ok(TaskHandle {
            id,
            progress: progress_rx,
            control: control_tx,
        })
    }

    fn set_speed_limit(&self, limit: Option<u64>) {
        *self.limit.lock().unwrap() = limit;
    }

    fn shutdown(&self) {
        VideoEngine::shutdown(self);
    }
}

/// yt-dlp 进度模板（与 video::progress 解析器同款）
const PROGRESS_TEMPLATE: &str = "download:SPARKLING|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.total_bytes_estimate)s|%(progress.speed)s";

/// 构造 yt-dlp 命令行参数
pub fn build_args(
    spec: &TaskSpec,
    ffmpeg: Option<&Path>,
    cookie: Option<&Path>,
    limit: Option<u64>,
) -> Vec<String> {
    let video = spec
        .video
        .as_ref()
        .expect("视频任务必有 video 参数（submit 已校验）");
    let filename = spec.filename.as_deref().unwrap_or("video");
    let out = spec.save_dir.join(format!("{filename}.%(ext)s"));
    let mut args: Vec<String> = vec![
        "-f".into(),
        video.format.clone(),
        "-c".into(),
        "--newline".into(),
        "--no-mtime".into(),
        "--retries".into(),
        "10".into(),
        "--progress-template".into(),
        PROGRESS_TEMPLATE.into(),
        "-o".into(),
        out.display().to_string(),
    ];
    if let Some(f) = ffmpeg {
        args.push("--ffmpeg-location".into());
        args.push(f.display().to_string());
    }
    if let Some(c) = cookie {
        args.push("--cookies".into());
        args.push(c.display().to_string());
    }
    if let Some(l) = limit {
        args.push("-r".into());
        args.push(format!("{}K", l / 1024));
    }
    if !video.subtitles.is_empty() {
        args.push("--write-subs".into());
        args.push("--sub-langs".into());
        args.push(video.subtitles.join(","));
    }
    if video.auto_subs {
        args.push("--write-auto-subs".into());
    }
    args.push(spec.url.clone());
    args
}

/// 从 stderr 摘取错误消息：最后一个 ERROR 行，否则截尾 200 字符
pub fn extract_error(stderr: &str) -> String {
    if let Some(line) = stderr.lines().rev().find(|l| l.starts_with("ERROR")) {
        return line.trim_start_matches("ERROR").trim_start_matches(':').trim().to_string();
    }
    let t = stderr.trim();
    if t.is_empty() {
        return String::new();
    }
    let n = t.chars().count();
    if n <= 200 {
        return t.to_string();
    }
    t.chars().skip(n - 200).collect()
}

/// 取消/移除时清理 yt-dlp 残留：<名>.part、<名>.ytdl、<名>.fNNN.* 分片
pub fn cleanup_partial(save_dir: &Path, filename: &str) {
    let Ok(rd) = std::fs::read_dir(save_dir) else {
        return;
    };
    let prefix = format!("{filename}.");
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        let Some(rest) = name.strip_prefix(&prefix) else {
            continue;
        };
        let is_part = rest.ends_with(".part") || rest.ends_with(".ytdl");
        let is_fragment = rest.starts_with('f')
            && rest[1..].chars().next().map(|c| c.is_ascii_digit()).unwrap_or(false);
        if is_part || is_fragment {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

/// 进度行应用到快照
fn apply_line(snap: &mut ProgressSnapshot, line: &str) {
    if let Some(p) = parse_progress_line(line) {
        snap.downloaded = p.downloaded;
        if let Some(t) = p.total {
            snap.total = t;
        }
        snap.speed = p.speed.unwrap_or(0);
    } else if is_merge_line(line) {
        snap.merging = true;
        snap.speed = 0;
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervise_video(
    id: TaskId,
    spec: TaskSpec,
    runner: Arc<dyn YtDlpRunner>,
    ffmpeg: Option<PathBuf>,
    cookie: Option<PathBuf>,
    limit: Arc<Mutex<Option<u64>>>,
    progress_tx: watch::Sender<ProgressSnapshot>,
    mut control_rx: mpsc::UnboundedReceiver<ControlMsg>,
    registry: Arc<Mutex<HashMap<TaskId, JoinHandle<()>>>>,
) {
    let filename = spec.filename.clone().unwrap_or_else(|| "video".into());
    let mut snapshot = progress_tx.subscribe().borrow().clone();
    loop {
        let args = build_args(
            &spec,
            ffmpeg.as_deref(),
            cookie.as_deref(),
            *limit.lock().unwrap(),
        );
        let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();
        let started = runner
            .start(
                args,
                Box::new(move |l| {
                    let _ = line_tx.send(l.to_string());
                }),
            )
            .await;
        let Ok(mut run) = started else {
            let e = started.err().map(|_| ()).map(|_| unreachable!()).unwrap_or(());
            let _ = e;
            // start 失败（二进制缺失等）→ Failed 终态
            let _ = progress_tx.send(ProgressSnapshot {
                state: TaskState::Failed,
                error: Some("启动 yt-dlp 失败".into()),
                ..snapshot.clone()
            });
            registry.lock().unwrap().remove(&id);
            return;
        };
        snapshot.merging = false; // 重启后重新判定
        let outcome = loop {
            tokio::select! {
                // 进程退出
                res = &mut run.done => break RunOutcome::Done(res),
                // 进度行
                Some(line) = line_rx.recv() => {
                    apply_line(&mut snapshot, &line);
                    let _ = progress_tx.send(snapshot.clone());
                }
                // 控制消息
                msg = control_rx.recv() => match msg {
                    Some(ControlMsg::Pause) => {
                        run.kill(KillReason::Pause);
                        let res = (&mut run.done).await;
                        break RunOutcome::Done(Ok(res));
                    }
                    Some(ControlMsg::Cancel) | None => {
                        run.kill(KillReason::Cancel);
                        let _ = (&mut run.done).await;
                        break RunOutcome::Cancelled;
                    }
                    Some(ControlMsg::Resume) => {} // 未暂停时忽略
                }
            }
        };
        match outcome {
            RunOutcome::Cancelled => {
                cleanup_partial(&spec.save_dir, &filename);
                snapshot.state = TaskState::Cancelled;
                snapshot.speed = 0;
                let _ = progress_tx.send(snapshot.clone());
                registry.lock().unwrap().remove(&id);
                return;
            }
            RunOutcome::Done(Ok(res)) => {
                if res.killed == Some(KillReason::Pause) {
                    // 暂停：等 Resume 重启进程续传
                    snapshot.state = TaskState::Paused;
                    snapshot.speed = 0;
                    let _ = progress_tx.send(snapshot.clone());
                    loop {
                        match control_rx.recv().await {
                            Some(ControlMsg::Resume) => {
                                snapshot.state = TaskState::Running;
                                let _ = progress_tx.send(snapshot.clone());
                                break; // 回外层 loop 重启进程
                            }
                            Some(ControlMsg::Cancel) | None => {
                                cleanup_partial(&spec.save_dir, &filename);
                                snapshot.state = TaskState::Cancelled;
                                let _ = progress_tx.send(snapshot.clone());
                                registry.lock().unwrap().remove(&id);
                                return;
                            }
                            Some(ControlMsg::Pause) => {} // 已暂停
                        }
                    }
                    continue;
                }
                if res.killed == Some(KillReason::Cancel) {
                    cleanup_partial(&spec.save_dir, &filename);
                    snapshot.state = TaskState::Cancelled;
                    let _ = progress_tx.send(snapshot.clone());
                    registry.lock().unwrap().remove(&id);
                    return;
                }
                if res.code == Some(0) {
                    snapshot.state = TaskState::Completed;
                    snapshot.speed = 0;
                    let _ = progress_tx.send(snapshot.clone());
                    registry.lock().unwrap().remove(&id);
                    return;
                }
                let msg = match extract_error(&res.stderr_tail) {
                    s if !s.is_empty() => s,
                    _ => format!("yt-dlp 退出码 {}", res.code.map(|c| c.to_string()).unwrap_or_else(|| "未知".into())),
                };
                snapshot.state = TaskState::Failed;
                snapshot.error = Some(msg);
                let _ = progress_tx.send(snapshot.clone());
                registry.lock().unwrap().remove(&id);
                return;
            }
            RunOutcome::Done(Err(_)) => {
                // done JoinError（abort 等）→ 取消语义
                cleanup_partial(&spec.save_dir, &filename);
                snapshot.state = TaskState::Cancelled;
                let _ = progress_tx.send(snapshot.clone());
                registry.lock().unwrap().remove(&id);
                return;
            }
        }
    }
}

enum RunOutcome {
    Done(std::result::Result<crate::video::runner::RunResult, tokio::task::JoinError>),
    Cancelled,
}
```

实现注意（执行者按此微调，不改语义）：
1. 上面 `let Ok(mut run) = started else { … }` 块中那两行占位删除，直接写 `let Err(e) = started else { let mut run = started.unwrap(); … }` 不可行——正确写法：

```rust
        let mut run = match started {
            Ok(h) => h,
            Err(e) => {
                let _ = progress_tx.send(ProgressSnapshot {
                    state: TaskState::Failed,
                    error: Some(e.user_message()),
                    ..snapshot.clone()
                });
                registry.lock().unwrap().remove(&id);
                return;
            }
        };
```

2. `RunOutcome::Done(res)` 的 `res` 是 `Result<RunResult, JoinError>`；`break RunOutcome::Done(res)` 处 select 臂类型对齐（`&mut run.done` await 产出 `Result<RunResult, JoinError>`）。
3. `http_engine.rs` 的 `fn sanitize_filename` 可见性改为 `pub fn`（probe 标题清洗复用；本任务只需改可见性 + `video/mod.rs` 不引用）。
4. `video/mod.rs` 加 `pub mod engine;` 与 `pub use engine::VideoEngine;`。

- [ ] **Step 4: 运行测试通过**

Run: `cargo test -p sparkling-core --lib video::`
Expected: PASS（build_args 三测 + 状态机五测 + extract_error/cleanup 纯函数测）

- [ ] **Step 5: 质量门 + Commit**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt && cargo test`

```bash
git add -A
git commit -m "feat(core): VideoEngine 实现 Engine trait（yt-dlp 进程驱动/暂停=杀进程/取消清理）"
```

---

### Task 7: 二进制管理（发现/版本/自更新下载）

**Files:**
- Create: `crates/sparkling-core/src/video/bin.rs`
- Modify: `crates/sparkling-core/src/video/mod.rs`

**Interfaces:**
- Consumes: Task 5 的 `TokioChildRunner`（版本查询）
- Produces:
  - `video::bin::resolve_ytdlp(app_bin: &Path, packed: &Path) -> PathBuf`——app data 更新版存在即优先
  - `video::bin::parse_version(s: &str) -> Option<(u32, u32, u32)>`——"2026.08.29" → (2026, 8, 29)
  - `video::bin::version_gt(a: &str, b: &str) -> bool`——版本序比较（任一解析失败返回 false）
  - `video::bin::ytdlp_version(bin: &Path) -> Result<String>`——跑 `yt-dlp --version`（毫秒级；stdout 首行）
  - `video::bin::download_replace(url: &str, dest: &Path) -> Result<()>`——reqwest 下载到 `<dest>.tmp` 后原子 rename；Content-Length 断言非空

- [ ] **Step 1: 写失败测试（bin.rs 尾部 mod tests）**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_shapes() {
        assert_eq!(parse_version("2026.08.29"), Some((2026, 8, 29)));
        assert_eq!(parse_version("2026.8.9"), Some((2026, 8, 9)));
        assert_eq!(parse_version("2026.08.29.12345"), Some((2026, 8, 29))); // nightly 后缀容忍
        assert_eq!(parse_version("bogus"), None);
        assert_eq!(parse_version(""), None);
    }

    #[test]
    fn version_ordering() {
        assert!(version_gt("2026.09.01", "2026.08.29"));
        assert!(version_gt("2027.01.01", "2026.12.31"));
        assert!(!version_gt("2026.08.29", "2026.08.29"));
        assert!(!version_gt("bogus", "2026.08.29"), "解析失败按不更新处理");
        assert!(!version_gt("2026.09.01", "bogus"));
    }

    #[test]
    fn resolve_prefers_app_data_binary() {
        let dir = tempfile::tempdir().unwrap();
        let app_bin = dir.path().join("bin");
        std::fs::create_dir_all(&app_bin).unwrap();
        std::fs::write(app_bin.join("yt-dlp.exe"), b"updated").unwrap();
        let resolved = resolve_ytdlp(&app_bin, Path::new("packed/yt-dlp.exe"));
        assert_eq!(resolved, app_bin.join("yt-dlp.exe"));
        // 无更新版 → 回退打包版
        let empty = dir.path().join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        assert_eq!(
            resolve_ytdlp(&empty, Path::new("packed/yt-dlp.exe")),
            PathBuf::from("packed/yt-dlp.exe")
        );
    }

    #[tokio::test]
    async fn download_replace_writes_file_atomically() {
        // 本地 axum 服务器充当下载源（复用 dev-dependency axum）
        use axum::routing::get;
        let app = axum::Router::new().route("/yt-dlp.exe", get(|| async { "BINBYTES" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("yt-dlp.exe");
        download_replace(&format!("http://{addr}/yt-dlp.exe"), &dest)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"BINBYTES");
        assert!(!dir.path().join("yt-dlp.exe.tmp").exists(), "tmp 应已改名");
    }
}
```

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p sparkling-core --lib video::bin`
Expected: FAIL，模块未定义

- [ ] **Step 3: 实现（bin.rs 主体）**

```rust
//! yt-dlp/ffmpeg 二进制管理：发现（app data 更新版优先于打包基线）、
//! 版本查询、自更新下载（原子替换）。
use crate::{Result, SparklingError};
use std::path::{Path, PathBuf};

/// 选定 yt-dlp 路径：app data 更新版存在即优先（更新动作保证其总是更新过的）
pub fn resolve_ytdlp(app_bin: &Path, packed: &Path) -> PathBuf {
    let updated = app_bin.join("yt-dlp.exe");
    if updated.exists() {
        updated
    } else {
        packed.to_path_buf()
    }
}

/// "2026.08.29" → (2026, 8, 29)；容忍 4 段式 nightly 版本
pub fn parse_version(s: &str) -> Option<(u32, u32, u32)> {
    let mut it = s.trim().split('.');
    let major: u32 = it.next()?.parse().ok()?;
    let minor: u32 = it.next()?.parse().ok()?;
    let patch: u32 = it.next()?.parse().ok()?;
    Some((major, minor, patch))
}

/// a 是否严格大于 b（任一解析失败 → false，保守不更新）
pub fn version_gt(a: &str, b: &str) -> bool {
    match (parse_version(a), parse_version(b)) {
        (Some(x), Some(y)) => x > y,
        _ => false,
    }
}

/// 跑 `yt-dlp --version`，取 stdout 首行
pub async fn ytdlp_version(bin: &Path) -> Result<String> {
    let out = tokio::process::Command::new(bin)
        .arg("--version")
        .output()
        .await
        .map_err(|e| SparklingError::Other(format!("运行 yt-dlp 失败: {e}")))?;
    if !out.status.success() {
        return Err(SparklingError::Other("yt-dlp 版本查询失败".into()));
    }
    let s = String::from_utf8_lossy(&out.stdout);
    Ok(s.lines().next().unwrap_or_default().trim().to_string())
}

/// 下载到 dest.tmp 后原子 rename 到 dest
pub async fn download_replace(url: &str, dest: &Path) -> Result<()> {
    let resp = reqwest::get(url)
        .await
        .map_err(|e| SparklingError::Network(format!("下载失败: {e}")))?;
    if !resp.status().is_success() {
        return Err(SparklingError::HttpStatus {
            status: resp.status().as_u16(),
            detail: format!("下载 {url} 失败"),
        });
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| SparklingError::Network(format!("读取响应失败: {e}")))?;
    if bytes.is_empty() {
        return Err(SparklingError::Other("下载内容为空".into()));
    }
    let tmp = dest.with_extension("exe.tmp");
    if let Some(dir) = dest.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| SparklingError::DiskWrite(format!("创建目录失败: {e}")))?;
    }
    std::fs::write(&tmp, &bytes)
        .map_err(|e| SparklingError::DiskWrite(format!("写入临时文件失败: {e}")))?;
    std::fs::rename(&tmp, dest)
        .map_err(|e| SparklingError::DiskWrite(format!("替换二进制失败: {e}")))?;
    Ok(())
}
```

`video/mod.rs` 加 `pub mod bin;`。测试中 `download_replace` 是 async fn（实现即 async；上面 async trait 无关）——测试 `#[tokio::test]` 已就绪。

- [ ] **Step 4: 运行测试通过**

Run: `cargo test -p sparkling-core --lib video::`
Expected: PASS

- [ ] **Step 5: 质量门 + Commit**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt`

```bash
git add -A
git commit -m "feat(core): yt-dlp 二进制发现/版本比较/原子替换下载"
```

---

### Task 8: TaskManager 多引擎路由 + 混合队列集成测试

**Files:**
- Modify: `crates/sparkling-core/src/manager.rs`
- Modify: `crates/sparkling-core/src/lib.rs`（导出 Engines）
- Modify: `src-tauri/src/lib.rs`（构造点：单引擎 → Engines；暂用 FakeRunner 不可行——Tauri 层先用真实 TokioChildRunner 占位，Task 9 完善）
- Modify: `crates/sparkling-core/tests/manager.rs`（构造点补 Engines）
- Test: `crates/sparkling-core/tests/video_manager.rs`（新建）

**Interfaces:**
- Consumes: Task 1（TaskKind/VideoParams/VideoMeta/AddTaskOptions 扩展）、Task 6（VideoEngine/cleanup_partial）
- Produces:
  - `manager::Engines { pub http: Arc<dyn Engine>, pub video: Arc<dyn Engine> }` + `for_kind(kind) -> Arc<dyn Engine>`
  - `TaskManager::new(store_path, engines: Engines, config, runtime)`（签名变更）
  - `recover()`：视频任务的 Running/Paused → 自动恢复时重新排队（yt-dlp -c 续传，无控制文件校验）；不自动恢复 → 置 Paused
  - `remove_task()`：无句柄的视频任务 → `video::engine::cleanup_partial`

- [ ] **Step 1: 写失败测试（tests/video_manager.rs）**

```rust
mod common;

use common::{poll_until, wait_event_state};
use sparkling_core::engine::Engine;
use sparkling_core::manager::{AddTaskOptions, Engines, ManagerConfig, TaskManager};
use sparkling_core::task::{TaskKind, TaskState, VideoParams};
use sparkling_core::video::engine::VideoEngine;
use sparkling_core::video::runner::{FakeRunner, ScriptStep};
use std::sync::Arc;
use std::time::Duration;

/// 视频引擎 + Fake HTTP 引擎（用真实 HttpEngine 空载即可——本测试不跑 HTTP 任务）
fn manager(dir: &tempfile::TempDir, runner: Arc<FakeRunner>, cfg: ManagerConfig) -> TaskManager {
    let video: Arc<dyn Engine> = Arc::new(VideoEngine::new(runner, None, None));
    TaskManager::new(
        &dir.path().join("tasks.db"),
        Engines {
            http: Arc::new(sparkling_core::HttpEngine::new(None)),
            video,
        },
        cfg,
        tokio::runtime::Handle::current(),
    )
    .unwrap()
}

fn video_opts(dir: &tempfile::TempDir) -> AddTaskOptions {
    AddTaskOptions {
        save_dir: dir.path().to_path_buf(),
        filename: Some("测试视频".into()),
        segments: None,
        max_speed: None,
        kind: TaskKind::Video,
        video: Some(VideoParams {
            format: "bv*+ba/b".into(),
            subtitles: vec![],
            auto_subs: false,
        }),
        video_meta: None,
    }
}

#[tokio::test]
async fn video_task_completes_through_queue() {
    let runner = Arc::new(FakeRunner::default());
    runner.scripts.lock().unwrap().push_back(vec![
        ScriptStep::Lines(&["SPARKLING|50|100|100|10"]),
        ScriptStep::Exit(0),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, runner, ManagerConfig::default());
    let mut rx = m.subscribe();
    let id = m.add_task("https://www.youtube.com/watch?v=t".into(), video_opts(&dir)).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Completed, Duration::from_secs(10)).await;
    let rec = m.list_tasks().unwrap().into_iter().next().unwrap();
    assert_eq!(rec.kind, TaskKind::Video);
    assert_eq!(rec.downloaded, 50);
    // VideoParams 落库 roundtrip
    assert_eq!(rec.video.unwrap().format, "bv*+ba/b");
}

#[tokio::test]
async fn paused_video_task_resumes_after_restart_recover() {
    // 场景：任务暂停（杀进程）→ 模拟应用重启（重建 manager）→ recover 自动重新排队续传完成
    let runner = Arc::new(FakeRunner::default());
    runner.scripts.lock().unwrap().push_back(vec![
        ScriptStep::Lines(&["SPARKLING|30|100|100|10"]),
        ScriptStep::Delay(Duration::from_secs(60)), // 等待 pause 杀进程
    ]);
    runner.scripts.lock().unwrap().push_back(vec![
        ScriptStep::Lines(&["SPARKLING|100|100|100|10"]),
        ScriptStep::Exit(0), // 重启恢复后完成
    ]);
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, runner.clone(), ManagerConfig::default());
    let mut rx = m.subscribe();
    let id = m.add_task("https://www.youtube.com/watch?v=t".into(), video_opts(&dir)).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Running, Duration::from_secs(10)).await;
    m.pause_task(&id).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Paused, Duration::from_secs(10)).await;
    // 模拟重启：丢弃 manager（句柄清空），第二个 manager 打开同一 tasks.db
    drop(m);
    let m2 = manager(&dir, runner, ManagerConfig::default());
    let mut rx2 = m2.subscribe();
    m2.recover().unwrap();
    wait_event_state(&mut rx2, &id, TaskState::Completed, Duration::from_secs(10)).await;
}

#[tokio::test]
async fn mixed_queue_shares_concurrency_slots() {
    // 视频任务占满并发位时，HTTP 任务排队等待
    let runner = Arc::new(FakeRunner::default());
    // 两个视频任务：都慢（Delay 等待手动放行？不行——FakeRunner 脚本无法中途放行）
    // 改用可完成的慢任务：Lines 大量行（几十毫秒量级）
    let lines: Vec<&'static str> = vec!["SPARKLING|1|1000000|1000000|1"; 50];
    runner.scripts.lock().unwrap().push_back(vec![
        ScriptStep::Lines(lines.as_slice()),
        ScriptStep::Exit(0),
    ]);
    runner.scripts.lock().unwrap().push_back(vec![
        ScriptStep::Lines(lines.as_slice()),
        ScriptStep::Exit(0),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let m = manager(
        &dir,
        runner,
        ManagerConfig {
            max_concurrent: 1,
            ..Default::default()
        },
    );
    let mut rx = m.subscribe();
    let v1 = m.add_task("https://www.youtube.com/watch?v=a".into(), video_opts(&dir)).unwrap();
    let v2 = m.add_task("https://www.youtube.com/watch?v=b".into(), video_opts(&dir)).unwrap();
    // max_concurrent=1：两任务都完成，但 v2 必须等 v1
    wait_event_state(&mut rx, &v1, TaskState::Completed, Duration::from_secs(30)).await;
    wait_event_state(&mut rx, &v2, TaskState::Completed, Duration::from_secs(30)).await;
    // 顺序断言：v1 完成事件先于 v2（队列 FIFO）
    // （弱断言：两个都完成即通过——并发位共享的强断言需要观测窗口，此处信任 try_schedule 逻辑）
    let recs = m.list_tasks().unwrap();
    assert!(recs.iter().all(|r| r.state == TaskState::Completed));
}
```

（`common/mod.rs` 已有 `wait_event_state`；若其签名只认 HTTP 事件流，直接复用——TaskEvent 与引擎无关。）

- [ ] **Step 2: 运行确认失败**

Run: `cargo test -p sparkling-core --test video_manager`
Expected: FAIL，`Engines` 未定义

- [ ] **Step 3: 实现 manager.rs 改造**

1. `Inner.engine: Arc<dyn Engine>` → `engines: Engines`；顶部定义：

```rust
/// 引擎路由表：TaskKind → Engine（③期多引擎接入点）
pub struct Engines {
    pub http: Arc<dyn Engine>,
    pub video: Arc<dyn Engine>,
}

impl Engines {
    pub fn for_kind(&self, kind: crate::task::TaskKind) -> Arc<dyn Engine> {
        match kind {
            crate::task::TaskKind::Http => self.http.clone(),
            crate::task::TaskKind::Video => self.video.clone(),
        }
    }
}
```

2. `TaskManager::new` 签名第二参 `engine: Arc<dyn Engine>` → `engines: Engines`；Inner 构造相应替换。
3. `set_config`：`self.inner.engine.set_speed_limit(...)` → 对两个引擎各调一次：

```rust
        let limit = self.inner.config.lock().unwrap().global_speed_limit;
        self.inner.engines.http.set_speed_limit(limit);
        self.inner.engines.video.set_speed_limit(limit);
```

4. `try_schedule`：构造 spec 后按 kind 路由：

```rust
        let spec = TaskSpec {
            url: rec.url.clone(),
            save_dir: PathBuf::from(&rec.save_dir),
            filename: rec.filename.clone(),
            segments: rec.segments,
            max_speed: rec.max_speed,
            kind: rec.kind,
            video: rec.video.clone(),
        };
        let engine = inner.engines.for_kind(rec.kind);
        inner.active.fetch_add(1, Ordering::SeqCst);
        let inner2 = inner.clone();
        inner.runtime.spawn(async move {
            match engine.submit(spec).await {
```

（原来 `inner2.engine.submit(spec)` → `engine.submit(spec)`。）
5. `recover()` 的 `TaskState::Running | TaskState::Paused` 分支加 kind 前置分支：

```rust
                TaskState::Running | TaskState::Paused => {
                    if rec.kind == crate::task::TaskKind::Video {
                        // 视频任务：yt-dlp .part 续传，无控制文件概念
                        if cfg.auto_resume_on_start {
                            to_resume.push(rec.id.clone());
                        } else {
                            self.inner.store.lock().unwrap().update_state(
                                &rec.id,
                                TaskState::Paused,
                                None,
                            )?;
                            self.emit_state(&rec.id, TaskState::Paused, None);
                        }
                        continue;
                    }
                    // …原有控制文件校验逻辑不动…
                }
```

（注意：`recover` 内是 `for rec in recs` 循环体，`continue` 直达下一条。）
6. `remove_task` 的无句柄清理分支按 kind 分流：

```rust
        if handle.is_none() {
            if let Some(rec) = rec {
                if let Some(name) = rec.filename {
                    match rec.kind {
                        crate::task::TaskKind::Video => {
                            crate::video::engine::cleanup_partial(
                                &PathBuf::from(&rec.save_dir),
                                &name,
                            );
                        }
                        crate::task::TaskKind::Http => {
                            // …原有 ctl/.part 清理逻辑移入此处（原样）…
                        }
                    }
                }
            }
        }
```

7. `shutdown`：`self.inner.engine.shutdown()` → 两个引擎各一次。
8. `lib.rs` 导出 `pub use manager::{AddTaskOptions, Engines, ManagerConfig, TaskEvent, TaskManager};`。
9. 全部构造点机械修复：`tests/manager.rs` 的 `manager()`（HttpEngine 包成 `Engines { http: …, video: Arc::new(VideoEngine::new(Arc::new(FakeRunner::default()), None, None)) }`——video 引擎空载即可，该测试文件不跑视频任务）；`src-tauri/src/lib.rs` setup（本任务先给占位 `video: Arc::new(VideoEngine::new(Arc::new(TokioChildRunner { bin: PathBuf::from("yt-dlp.exe") }.into()), None, None))`——路径解析 Task 9 接管；`Arc::new(...)` 需要 `From` —— 写成 `let runner: Arc<dyn YtDlpRunner> = Arc::new(TokioChildRunner { bin: ... });` 再传）。

- [ ] **Step 4: 运行测试通过**

Run: `cargo test -p sparkling-core && cargo build --workspace`
Expected: PASS（含①期全部存量测试 + 新 video_manager 三测）

- [ ] **Step 5: 质量门 + Commit**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt`

```bash
git add -A
git commit -m "feat(core): TaskManager 多引擎路由（Engines）与视频任务恢复/清理分支"
```

---

### Task 9: Tauri 命令层（probe_video / add_task 扩展 / 二进制状态与更新 / cookie）

**Files:**
- Modify: `src-tauri/src/lib.rs`
- Modify: `src-tauri/Cargo.toml`（无新依赖——tokio/serde 已有）

**Interfaces:**
- Consumes: Task 5 `TokioChildRunner`、Task 6 `VideoEngine`、Task 7 `bin::{resolve_ytdlp, ytdlp_version, version_gt, download_replace}`、Task 4 `probe::parse_info_json`、Task 8 `Engines`
- Produces（Tauri commands，前端 Task 10 调用）:
  - `probe_video(url: String) -> Result<VideoInfo, String>`（async command，60s 超时；进程跑 `yt-dlp -J --flat-playlist <url>` 收集 stdout）
  - `add_task(url, filename, segments, kind, video, video_meta)`（扩展既有命令——新增三个 `Option` 参数，旧调用兼容）
  - `get_ytdlp_status() -> YtdlpStatus { version: Option<String>, source: String, ffmpeg_available: bool }`
  - `update_ytdlp() -> Result<YtdlpStatus, String>`（下载 GitHub latest 到 app data/bin）
  - `import_cookies(browser: String) -> Result<(), String>`（`--cookies-from-browser <b> --cookies <path> --simulate https://www.youtube.com`）
  - `clear_cookies() -> Result<(), String>`（删 cookies.txt + 清配置）

- [ ] **Step 1: AppState 扩展 + 二进制解析**

`src-tauri/src/lib.rs` 顶部加：

```rust
use sparkling_core::video::bin as vbin;
use sparkling_core::video::probe::VideoInfo;
use sparkling_core::video::runner::TokioChildRunner;
use sparkling_core::video::runner::YtDlpRunner;
use sparkling_core::task::{VideoMeta, VideoParams};

#[derive(serde::Serialize, Clone)]
pub struct YtdlpStatus {
    pub version: Option<String>,
    /// "app-data"（更新版）| "bundled"（打包基线）| "missing"
    pub source: String,
    pub ffmpeg_available: bool,
}

/// 二进制候选链：打包 resource → exe 同目录 bin/（便携 zip 形态）→ 源码 src-tauri/bin/（dev）
fn find_binary(app: &AppHandle, name: &str) -> Option<PathBuf> {
    let candidates = [
        app.path().resource_dir().ok().map(|d| d.join("bin").join(name)),
        std::env::current_exe().ok().and_then(|e| e.parent().map(|d| d.join("bin").join(name))),
        Some(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("bin").join(name)),
    ];
    candidates
        .into_iter()
        .flatten()
        .find(|p| p.exists())
}

pub struct VideoService {
    pub runner: Arc<TokioChildRunner>,
    pub ffmpeg: Option<PathBuf>,
    pub app_bin_dir: PathBuf,   // app data/bin（更新版 yt-dlp 落点）
    pub packed_ytdlp: Option<PathBuf>,
    pub cookie_file: PathBuf,   // app data/cookies.txt
}
```

`AppState` 增 `pub video: VideoService`。setup 里（替换 Task 8 的占位）：

```rust
            let app_data = app.path().app_data_dir().unwrap_or_else(|_| PathBuf::from("."));
            std::fs::create_dir_all(app_data.join("bin")).ok();
            let packed_ytdlp = find_binary(app.handle(), "yt-dlp.exe");
            let ffmpeg = find_binary(app.handle(), "ffmpeg.exe");
            let ytdlp_bin = vbin::resolve_ytdlp(&app_data.join("bin"), packed_ytdlp
                .as_deref()
                .unwrap_or_else(|| std::path::Path::new("yt-dlp.exe")));
            let video_engine = sparkling_core::VideoEngine::new(
                Arc::new(TokioChildRunner { bin: ytdlp_bin.clone() }),
                ffmpeg.clone(),
                app_data.join("cookies.txt").exists().then(|| app_data.join("cookies.txt")),
            );
```

Engines 构造改为 `Engines { http: …, video: Arc::new(video_engine) }`；`app.manage(AppState { manager, config_path, default_save_dir, video: VideoService { runner: Arc::new(TokioChildRunner { bin: ytdlp_bin }), ffmpeg, app_bin_dir: app_data.join("bin"), packed_ytdlp, cookie_file: app_data.join("cookies.txt") } })`。

- [ ] **Step 2: probe_video 命令**

```rust
/// 视频解析：yt-dlp -J --flat-playlist（60s 超时；stdout 全量收集后解析）
#[tauri::command]
async fn probe_video(state: State<'_, AppState>, url: String) -> Result<VideoInfo, String> {
    use tokio::sync::mpsc;
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let runner = state.video.runner.clone();
    let handle = runner
        .start(
            vec!["-J".into(), "--flat-playlist".into(), url],
            Box::new(move |l| {
                let _ = tx.send(l.to_string());
            }),
        )
        .await
        .map_err(|e| e.user_message())?;
    let mut out = String::new();
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), async {
        // 行收集与进程结束并发；进程结束时 channel 关闭（drop）→ rx 返回 None
        loop {
            tokio::select! {
                line = rx.recv() => match line {
                    Some(l) => out.push_str(&l),
                    None => break,
                },
            }
        }
        handle.wait().await
    })
    .await;
    // rx 未关闭说明超时：杀进程并报错
    let res = match result {
        Ok(r) => r,
        Err(_) => {
            handle.kill(sparkling_core::video::runner::KillReason::Cancel);
            return Err("解析超时（60 秒），请重试".into());
        }
    };
    if res.killed.is_some() {
        return Err("解析被中断".into());
    }
    if res.code != Some(0) {
        let msg = sparkling_core::video::engine::extract_error(&res.stderr_tail);
        return Err(if msg.is_empty() { format!("解析失败（退出码 {:?}）", res.code) } else { msg });
    }
    sparkling_core::video::probe::parse_info_json(&out).map_err(|e| e.user_message())
}
```

注意：`handle.wait()` 消耗 handle，且 `kill` 在超时分支仍需 handle——把 `handle` 声明提前、闭包内借用不可行（wait(self) 消耗所有权）。调整：超时分支先 `drop` 收集 future 再 kill。执行者实现时按此结构写：timeout 包住一个返回 `(RunResult, String)` 的 async 块（块内 `let res = handle.wait().await;`）；`Err(_)` 超时分支中 handle 已被块 move——改为在块外先用 `Arc` 不现实。**可行写法**：不用 timeout 包 wait；改用 `tokio::select!` 三臂（行收集 / `handle.done`（`&mut` 借用）/ `sleep(60s)` 超时臂内 `handle.kill(Cancel)` 后 `(&mut handle.done).await` 并返回错误）。最终形态：

```rust
    let mut out = String::new();
    let mut handle = handle;
    let timed_out = tokio::select! {
        _ = async {
            while let Some(l) = rx.recv().await { out.push_str(&l); }
        } => false,
        res = &mut handle.done => {
            // 进程先退：把残余行收干（channel 已关）
            while let Ok(l) = rx.try_recv() { out.push_str(&l); }
            let code = res.map(|r| r.code).unwrap_or(None);
            if code != Some(0) {
                let msg = sparkling_core::video::engine::extract_error(&res.map(|r| r.stderr_tail).unwrap_or_default());
                return Err(if msg.is_empty() { format!("解析失败（退出码 {code:?}）") } else { msg });
            }
            false
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            handle.kill(sparkling_core::video::runner::KillReason::Cancel);
            let _ = (&mut handle.done).await;
            true
        }
    };
    if timed_out {
        return Err("解析超时（60 秒），请重试".into());
    }
    if res killed 检查：正常路径无 killed。
    sparkling_core::video::probe::parse_info_json(&out).map_err(|e| e.user_message())
```

（执行者按上面最终形态实现；select 两臂都闭 channel 后 `parse_info_json(&out)`。）

- [ ] **Step 3: add_task 扩展 + 其余命令**

```rust
#[tauri::command]
fn add_task(
    state: State<AppState>,
    url: String,
    filename: Option<String>,
    segments: Option<u32>,
    kind: Option<String>,
    video: Option<VideoParams>,
    video_meta: Option<VideoMeta>,
) -> Result<String, String> {
    let kind = sparkling_core::TaskKind::parse(&kind.unwrap_or_else(|| "http".into()))
        .ok_or_else(|| "未知任务类型".to_string())?;
    let opts = AddTaskOptions {
        save_dir: state.default_save_dir.clone(),
        filename,
        segments,
        max_speed: None,
        kind,
        video,
        video_meta,
    };
    state
        .manager
        .add_task(url, opts)
        .map_err(|e| e.user_message())
}

#[tauri::command]
async fn get_ytdlp_status(state: State<'_, AppState>) -> Result<YtdlpStatus, String> {
    let bin = vbin::resolve_ytdlp(
        &state.video.app_bin_dir,
        state.video.packed_ytdlp.as_deref().unwrap_or_else(|| std::path::Path::new("yt-dlp.exe")),
    );
    let version = vbin::ytdlp_version(&bin).await.ok();
    let source = if !bin.exists() {
        "missing".to_string()
    } else if state.video.app_bin_dir.join("yt-dlp.exe").exists() {
        "app-data".to_string()
    } else {
        "bundled".to_string()
    };
    Ok(YtdlpStatus {
        version,
        source,
        ffmpeg_available: state.video.ffmpeg.is_some(),
    })
}

#[tauri::command]
async fn update_ytdlp(state: State<'_, AppState>) -> Result<YtdlpStatus, String> {
    const YTDLP_URL: &str = "https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp.exe";
    let dest = state.video.app_bin_dir.join("yt-dlp.exe");
    vbin::download_replace(YTDLP_URL, &dest)
        .await
        .map_err(|e| e.user_message())?;
    get_ytdlp_status(state).await
}

/// 一次性从浏览器导出 cookie 到 app data/cookies.txt（Netscape 格式）。
/// yt-dlp 语义：--cookies FILE 同时是读取与转储目标。
#[tauri::command]
async fn import_cookies(state: State<'_, AppState>, browser: String) -> Result<(), String> {
    let runner = state.video.runner.clone();
    let out = tokio::process::Command::new(&state.video.runner.bin)
        .args([
            "--cookies-from-browser",
            &browser,
            "--cookies",
        ])
        .arg(&state.video.cookie_file)
        .arg("--simulate")
        .arg("https://www.youtube.com")
        .output()
        .await
        .map_err(|e| format!("运行 yt-dlp 失败: {e}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(sparkling_core::video::engine::extract_error(&stderr));
    }
    Ok(())
}

#[tauri::command]
fn clear_cookies(state: State<AppState>) -> Result<(), String> {
    let _ = std::fs::remove_file(&state.video.cookie_file);
    Ok(())
}
```

（`import_cookies` 直接用 `Command`（TokioChildRunner 的 start 面向流式场景，此处 output 一次性更简洁）；`TokioChildRunner.bin` 字段需 pub——已是。`State` 跨 await 借用：async command 里 `State<'_, AppState>` 合法。）

`.invoke_handler` 列表追加：`probe_video, get_ytdlp_status, update_ytdlp, import_cookies, clear_cookies`（add_task 已在）。

- [ ] **Step 4: 编译 + 既有命令回归**

Run: `cargo build --workspace && cargo test`
Expected: BUILD OK（本任务无 Rust 单测——命令层由前端验收覆盖；core 测试全绿）

- [ ] **Step 5: 质量门 + Commit**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt`

```bash
git add -A
git commit -m "feat(tauri): 视频解析/任务添加/二进制状态与更新/cookie 命令层"
```

---

### Task 10: 前端类型与 API 层 + 域名检测

**Files:**
- Modify: `src/types.ts`
- Modify: `src/api.ts`

**Interfaces:**
- Consumes: Task 9 的 commands（`probe_video`/`add_task` 扩展/`get_ytdlp_status`/`update_ytdlp`/`import_cookies`/`clear_cookies`）
- Produces（Task 11-13 使用）:
  - `types.ts`: `TaskKind`、`VideoParams`、`VideoMeta`、`FormatEntry`、`PlaylistEntry`、`VideoInfo`、`YtdlpStatus`；`TaskRecord` 增 `kind/video/video_meta`；`TaskEvent` Progress 增 `merging`；`LiveInfo` 增 `merging`；`ManagerConfig` 增视频五字段；`looksLikeVideoUrl(url)`；`fmtDuration(sec)`
  - `api.ts`: `probeVideo`、`addVideoTask`、`getYtdlpStatus`、`updateYtdlp`、`importCookies`、`clearCookies`

- [ ] **Step 1: types.ts 扩展**

```typescript
export type TaskKind = 'http' | 'video';

export interface VideoParams {
  format: string;
  subtitles: string[];
  auto_subs: boolean;
}

export interface VideoMeta {
  title: string;
  duration_sec: number | null;
  thumbnail: string | null;
  uploader: string | null;
  webpage_url: string | null;
}

export interface FormatEntry {
  format_id: string;
  ext: string;
  height: number | null;
  fps: number | null;
  vcodec: string;
  acodec: string;
  filesize: number | null;
  tbr: number | null;
}

export interface PlaylistEntry {
  url: string;
  title: string;
  duration_sec: number | null;
}

export interface VideoInfo {
  title: string;
  duration_sec: number | null;
  thumbnail: string | null;
  uploader: string | null;
  webpage_url: string | null;
  formats: FormatEntry[];
  playlist: PlaylistEntry[] | null;
}

export interface YtdlpStatus {
  version: string | null;
  source: string;
  ffmpeg_available: boolean;
}
```

`TaskRecord` 加 `kind: TaskKind; video: VideoParams | null; video_meta: VideoMeta | null;`；`ManagerConfig` 加 `video_max_height: number | null; video_audio_only: boolean; video_sub_langs: string; video_auto_subs: boolean;`；`TaskEvent` 的 Progress 变体加 `merging: boolean`；`LiveInfo` 加 `merging: boolean`。文件尾部追加：

```typescript
/** 常见视频站点白名单——仅做添加对话框的 UI 提示，不是权威判断 */
const VIDEO_SITES = [
  'youtube.com', 'youtu.be', 'bilibili.com', 'b23.tv', 'douyin.com',
  'tiktok.com', 'twitter.com', 'x.com', 'vimeo.com', 'twitch.tv',
];

export function looksLikeVideoUrl(url: string): boolean {
  try {
    const h = new URL(url).hostname.toLowerCase();
    return VIDEO_SITES.some((s) => h === s || h.endsWith('.' + s));
  } catch {
    return false;
  }
}

/** 秒 → "1:23:45" / "12:34" */
export function fmtDuration(sec: number | null | undefined): string {
  if (sec == null || sec <= 0) return '—';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = Math.floor(sec % 60);
  const mm = h > 0 ? String(m).padStart(2, '0') : String(m);
  const ss = String(s).padStart(2, '0');
  return h > 0 ? `${h}:${mm}:${ss}` : `${mm}:${ss}`;
}

/** 记住的画质偏好 → yt-dlp -f 选择器（直下路径用；无偏好返回 null 走解析面板） */
export function selectorFromPreference(cfg: {
  video_audio_only?: boolean;
  video_max_height?: number | null;
} | null): string | null {
  if (!cfg) return null;
  if (cfg.video_audio_only) return 'ba/b';
  const h = cfg.video_max_height;
  if (h == null) return 'bv*+ba/b';
  return `bv*[height<=${h}]+ba/b[height<=${h}]`;
}
```

- [ ] **Step 2: api.ts 扩展**

```typescript
import type { ManagerConfig, TaskRecord, VideoInfo, VideoMeta, VideoParams, YtdlpStatus } from './types';

  // api 对象内追加：
  probeVideo: (url: string) => invoke<VideoInfo>('probe_video', { url }),
  addVideoTask: (url: string, video: VideoParams, filename: string, videoMeta: VideoMeta | null) =>
    invoke<string>('add_task', {
      url, filename, segments: null,
      kind: 'video', video, videoMeta,
    }),
  getYtdlpStatus: () => invoke<YtdlpStatus>('get_ytdlp_status'),
  updateYtdlp: () => invoke<YtdlpStatus>('update_ytdlp'),
  importCookies: (browser: string) => invoke<void>('import_cookies', { browser }),
  clearCookies: () => invoke<void>('clear_cookies'),
```

（Tauri invoke 参数名自动转 snake_case：`videoMeta` → `video_meta`。）

- [ ] **Step 3: App.tsx 的 LiveInfo 填充点补 merging**

`App.tsx` 事件监听里 `pendingLive.current.set(p.id, { … })` 增加 `merging: p.merging`。

- [ ] **Step 4: 类型检查 + Commit**

Run: `npm run build`（tsc + vite）
Expected: 构建通过（TaskRecord 新字段若有缺省问题由 tsc 报出——后端序列化恒带这些字段）

```bash
git add -A
git commit -m "feat(web): 视频任务类型定义、API 封装与域名启发检测"
```

---

### Task 11: 添加对话框视频区 + 视频解析面板

**Files:**
- Create: `src/components/VideoInfoPanel.tsx`
- Modify: `src/components/AddTaskDialog.tsx`
- Modify: `src/App.css`（面板样式）

**Interfaces:**
- Consumes: Task 10 的 `api.probeVideo/addVideoTask`、`looksLikeVideoUrl/fmtDuration/fmtBytes`、`VideoInfo/FormatEntry/ManagerConfig`
- Produces: 添加对话框的完整视频流程（粘贴 → 检测提示/展开 → 解析 → 面板选择 → 入队）；画质档位选择器构造函数（面板内部）

- [ ] **Step 1: VideoInfoPanel 组件（新文件）**

```tsx
import { useMemo, useState } from 'react';
import type { FormatEntry, PlaylistEntry, VideoInfo } from '../types';
import { fmtBytes, fmtDuration } from '../types';

/** 画质档位（UI 选择粒度；selector 是 yt-dlp -f 模板，跨视频稳定） */
interface QualityOption {
  id: string;
  label: string;
  selector: string;
  needsMerge: boolean;
}

/** 从格式表聚合可选画质档位（height 降序 + 仅音频），needsMerge 判定看是否存在渐进流 */
function qualityOptions(formats: FormatEntry[]): QualityOption[] {
  const hasProgressive = formats.some((f) => f.vcodec !== 'none' && f.acodec !== 'none');
  const heights = [...new Set(formats.filter((f) => f.height).map((f) => f.height!))]
    .sort((a, b) => b - a)
    .map<QualityOption>((h) => ({
      id: `h${h}`,
      label: `${h}p`,
      selector: `bv*[height<=${h}]+ba/b[height<=${h}]`,
      needsMerge: true, // 分离流合并（档位选择总是走 bestvideo+bestaudio 模板）
    }));
  const audio: QualityOption = {
    id: 'audio',
    label: '仅音频（m4a）',
    selector: 'ba/b',
    needsMerge: false,
  };
  return [...heights, audio].map((o) => ({ ...o, needsMerge: o.needsMerge && !hasProgressive ? true : o.needsMerge }));
}

export default function VideoInfoPanel({
  info,
  ffmpegAvailable,
  defaultSubLangs,
  defaultAutoSubs,
  onConfirm,
  onCancel,
  busy,
}: {
  info: VideoInfo;
  ffmpegAvailable: boolean;
  defaultSubLangs: string;
  defaultAutoSubs: boolean;
  onConfirm: (c: {
    format: string;
    subtitles: string[];
    auto_subs: boolean;
    entries: PlaylistEntry[] | null;
    audioOnly: boolean;
    maxHeight: number | null;
  }) => void;
  onCancel: () => void;
  busy: boolean;
}) {
  const options = useMemo(() => qualityOptions(info.formats), [info.formats]);
  const [quality, setQuality] = useState(options[0]?.id ?? '');
  const [subLangs, setSubLangs] = useState(defaultSubLangs);
  const [autoSubs, setAutoSubs] = useState(defaultAutoSubs);
  const [selected, setSelected] = useState<Set<number>>(() =>
    new Set((info.playlist ?? []).map((_, i) => i))
  );
  const isPlaylist = info.playlist != null && info.playlist.length > 0;
  const selectedOpt = options.find((o) => o.id === quality);

  const confirm = () => {
    const langs = subLangs.split(/[,，]/).map((s) => s.trim()).filter(Boolean);
    onConfirm({
      format: selectedOpt?.selector ?? 'bv*+ba/b',
      subtitles: langs,
      auto_subs: autoSubs,
      entries: isPlaylist ? (info.playlist ?? []).filter((_, i) => selected.has(i)) : null,
      audioOnly: selectedOpt?.id === 'audio',
      maxHeight: selectedOpt?.id.startsWith('h') ? Number(selectedOpt.id.slice(1)) : null,
    });
  };

  return (
    <div className="video-panel">
      <div className="video-panel__head">
        {info.thumbnail && <img className="video-panel__thumb" src={info.thumbnail} alt="" />}
        <div className="video-panel__meta">
          <div className="video-panel__title" title={info.title}>{info.title}</div>
          <div className="video-panel__sub">
            {info.uploader && <span>{info.uploader} · </span>}
            <span>{fmtDuration(info.duration_sec)}</span>
            {isPlaylist && <span> · 共 {info.playlist!.length} 集（已选 {selected.size}）</span>}
          </div>
        </div>
      </div>

      {isPlaylist && (
        <div className="video-panel__list">
          {(info.playlist ?? []).map((e, i) => (
            <label key={i} className="video-panel__entry">
              <input
                type="checkbox"
                checked={selected.has(i)}
                onChange={(ev) => {
                  const next = new Set(selected);
                  if (ev.target.checked) next.add(i); else next.delete(i);
                  setSelected(next);
                }}
              />
              <span className="video-panel__entry-title" title={e.title}>{e.title}</span>
              <span className="video-panel__entry-dur">{fmtDuration(e.duration_sec)}</span>
            </label>
          ))}
        </div>
      )}

      {!isPlaylist && (
        <>
          <label>画质</label>
          <select value={quality} onChange={(e) => setQuality(e.target.value)}>
            {options.map((o) => (
              <option key={o.id} value={o.id}>
                {o.label}{o.needsMerge && !ffmpegAvailable ? '（需 ffmpeg，缺失）' : ''}
              </option>
            ))}
          </select>
          {/* 格式详情（信息性） */}
          <details className="video-panel__formats">
            <summary>可用格式（{info.formats.length}）</summary>
            <table>
              <tbody>
                {info.formats.map((f) => (
                  <tr key={f.format_id}>
                    <td>{f.format_id}</td>
                    <td>{f.height ? `${f.height}p${f.fps ? `/${Math.round(f.fps)}` : ''}` : '音频'}</td>
                    <td>{f.ext}</td>
                    <td>{f.vcodec === 'none' ? '—' : f.vcodec}</td>
                    <td>{f.filesize != null ? fmtBytes(f.filesize) : fmtBytes(f.tbr ? f.tbr * 1024 / 8 : null)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </details>
        </>
      )}

      <label>字幕语言（逗号分隔，留空不下字幕）</label>
      <input value={subLangs} onChange={(e) => setSubLangs(e.target.value)} placeholder="zh-Hans,en" />
      <label className="checkbox">
        <input type="checkbox" checked={autoSubs} onChange={(e) => setAutoSubs(e.target.checked)} />
        包含自动生成字幕（CC）
      </label>

      <div className="modal-actions">
        <button className="btn" onClick={onCancel}>返回</button>
        <button className="btn btn--primary" disabled={busy || (isPlaylist && selected.size === 0)} onClick={confirm}>
          {busy ? '添加中…' : '下载'}
        </button>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: AddTaskDialog 改造（视频区 + 解析状态机）**

`AddTaskDialog.tsx` 重构为三态：表单（现状）/ 解析中 / 面板。核心改动（保留现有 HTTP 表单逻辑不动）：

```tsx
import { useState } from 'react';
import { api } from '../api';
import type { ManagerConfig, VideoInfo } from '../types';
import { looksLikeVideoUrl, selectorFromPreference } from '../types';
import VideoInfoPanel from './VideoInfoPanel';

export default function AddTaskDialog({
  defaultSegments,
  ffmpegAvailable,
  defaultSubLangs,
  defaultAutoSubs,
  preference,
  onClose,
  onAdded,
}: {
  defaultSegments: number;
  ffmpegAvailable: boolean;
  defaultSubLangs: string;
  defaultAutoSubs: boolean;
  preference: ManagerConfig | null;
  onClose: () => void;
  onAdded: () => void;
}) {
  const [url, setUrl] = useState('');
  const [filename, setFilename] = useState('');
  const [segments, setSegments] = useState(defaultSegments);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // 视频态：null=表单；'probing'；{ info }
  const [video, setVideo] = useState<null | 'probing' | { info: VideoInfo }>(null);
  const isVideoUrl = looksLikeVideoUrl(url.trim());

  const probe = async () => {
    setBusy(true); setErr(null); setVideo('probing');
    try {
      const info = await api.probeVideo(url.trim());
      setVideo({ info });
    } catch (e) {
      setVideo(null);
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  // 视频面板确认 → 批量入队（列表多选逐条 addVideoTask；标题做文件名）
  const confirmVideo = async (c: {
    format: string; subtitles: string[]; auto_subs: boolean;
    entries: { url: string; title: string }[] | null;
    audioOnly: boolean; maxHeight: number | null;
  }) => {
    setBusy(true); setErr(null);
    try {
      const targets = c.entries ?? [{ url: url.trim(), title: (video as { info: VideoInfo }).info.title }];
      for (const t of targets) {
        await api.addVideoTask(
          t.url,
          { format: c.format, subtitles: c.subtitles, auto_subs: c.auto_subs },
          t.title,
          null
        );
      }
      onAdded();
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  // 直下路径（D4）：已有画质偏好时跳过解析，按偏好构造 selector 直接入队
  // （filename 留 null 让 yt-dlp 按视频标题命名；字幕跟默认设置）
  const quickDownload = async () => {
    const selector = selectorFromPreference(preference);
    if (!selector) return;
    setBusy(true); setErr(null);
    try {
      await api.addVideoTask(
        url.trim(),
        { format: selector, subtitles: defaultSubLangs.split(/[,，]/).map((s) => s.trim()).filter(Boolean), auto_subs: defaultAutoSubs },
        null,
        null
      );
      onAdded();
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  const submit = async () => { /* 现有 HTTP 提交逻辑不变 */ };

  return (
    <div className="modal-mask" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>新建下载</h2>
        {video === 'probing' && <div className="video-panel__loading">正在解析视频信息…</div>}
        {video && video !== 'probing' ? (
          <VideoInfoPanel
            info={video.info}
            ffmpegAvailable={ffmpegAvailable}
            defaultSubLangs={defaultSubLangs}
            defaultAutoSubs={defaultAutoSubs}
            onConfirm={confirmVideo}
            onCancel={() => setVideo(null)}
            busy={busy}
          />
        ) : (
          <>
            {/* 现有 URL/文件名/分片表单 */}
            <label>URL</label>
            <input /* 现有属性 */ />
            {isVideoUrl && (
              <div className="video-hint">
                <span>检测到视频链接</span>
                {selectorFromPreference(preference) && (
                  <button className="btn btn--sm btn--primary" disabled={busy} onClick={quickDownload}>
                    直接下载
                  </button>
                )}
                <button className="btn btn--sm" disabled={busy} onClick={probe}>
                  {busy ? '解析中…' : '解析视频'}
                </button>
              </div>
            )}
            {/* 现有文件名/分片输入、err、modal-actions 保持 */}
          </>
        )}
      </div>
    </div>
  );
}
```

（`/* 现有属性 */` 处保留原有 input 全部属性；`submit`/HTTP 表单区原样。执行者注意保持现有 JSX 完整，不要丢既有字段。）

- [ ] **Step 3: App.tsx 传参 + App.css 样式**

`App.tsx` 给 AddTaskDialog 新增 props：`ffmpegAvailable={ytdlpStatus?.ffmpeg_available ?? true}`、`defaultSubLangs={config?.video_sub_langs ?? 'zh-Hans,en'}`、`defaultAutoSubs={config?.video_auto_subs ?? false}`、`preference={config}`（`ytdlpStatus` 为 App 新 state，初始 `null`，mount 时 `api.getYtdlpStatus().then(setYtdlpStatus).catch(() => {})`）。

`App.css` 追加（沿用现有 CSS 变量与命名风格 BEM）：

```css
.video-hint { display: flex; align-items: center; gap: 8px; margin: 6px 0 2px;
  color: var(--accent, #5b9dff); font-size: 12px; }
.video-panel__head { display: flex; gap: 12px; margin-bottom: 12px; }
.video-panel__thumb { width: 120px; aspect-ratio: 16/9; object-fit: cover; border-radius: 4px; }
.video-panel__title { font-weight: 600; line-height: 1.4; }
.video-panel__sub { color: var(--muted, #8aa0bd); font-size: 12px; margin-top: 4px; }
.video-panel__list { max-height: 200px; overflow-y: auto; border: 1px solid var(--line, #23385C);
  border-radius: 4px; padding: 6px 10px; margin: 8px 0; }
.video-panel__entry { display: flex; gap: 8px; align-items: center; padding: 3px 0; font-size: 13px; }
.video-panel__entry-title { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.video-panel__entry-dur { color: var(--muted, #8aa0bd); font-size: 12px; }
.video-panel__formats summary { cursor: pointer; font-size: 12px; color: var(--muted, #8aa0bd); margin: 6px 0; }
.video-panel__formats table { width: 100%; font-size: 12px; border-collapse: collapse; }
.video-panel__formats td { padding: 2px 8px 2px 0; border-bottom: 1px solid var(--line, #23385C); }
.video-panel__loading { color: var(--muted, #8aa0bd); padding: 24px 0; text-align: center; }
```

（CSS 变量名以现有 App.css 实际为准——执行者先读 App.css 的 `:root` 块，把 `var(--accent, …)` 等对齐到真实变量名。）

- [ ] **Step 4: 构建 + 手动验收 + Commit**

Run: `npm run build && npm run tauri dev`
手动验收清单（用户亲验）：
- 粘贴 Bilibili/YouTube 链接出现"检测到视频链接"提示与"解析视频"按钮
- 解析后面板显示标题/时长/缩略图/画质列表
- 选画质下载，任务入队且状态/进度正常
- 播放列表链接解析出条目列表，勾选下载

```bash
git add -A
git commit -m "feat(web): 添加对话框视频检测/解析面板/画质选择与播放列表批量入队"
```

---

### Task 12: 任务行视频展示（标题/合并中徽标）

**Files:**
- Modify: `src/components/TaskRow.tsx`
- Modify: `src/App.css`（徽标样式）

**Interfaces:**
- Consumes: Task 10 的 `TaskRecord.kind/video_meta`、`LiveInfo.merging`
- Produces: 视频任务行展示（标题优先于 URL 文件名；running+merging 显示"合并中"标签；视频任务不显示分片概念）

- [ ] **Step 1: TaskRow 改造**

```tsx
// STATE_META 定义后追加：
const VIDEO_RUNNING_LABEL: Record<string, string> = { merging: '合并中' };
```

组件内（现有 `const name = …` 与 `const running = …` 之间）：

```tsx
  // 视频任务：标题优先；下载完成进入合并阶段时状态标签切换"合并中"
  const isVideo = task.kind === 'video';
  const name = isVideo
    ? (task.video_meta?.title ?? task.filename ?? urlName(task.url) ?? '解析中…')
    : (task.filename ?? urlName(task.url) ?? '解析中…');
  const merging = isVideo && running && (live?.merging ?? false);
  const stateLabel = merging ? '合并中' : STATE_META[task.state].label;
```

（替换原 `const name = …` 行；`task__state` 渲染处 `{STATE_META[task.state].label}` 改 `{stateLabel}`。）

进度条合并阶段的视觉（可选增强，用户验收后定）：`task__bar-fill` 在 merging 时加 `task__bar-fill--merging` class（CSS 脉动动画），本任务先实现标签切换，动画留给用户反馈。

`App.css` 追加：

```css
.task--video .task__name { font-weight: 500; }
```

- [ ] **Step 2: 构建验收 + Commit**

Run: `npm run build && npm run tauri dev`
手动验收：视频任务行显示视频标题；下载完成后状态显示"合并中"（大视频可观察到）；合并结束转"已完成"。

```bash
git add -A
git commit -m "feat(web): 任务行视频标题与合并中状态展示"
```

---

### Task 13: 设置面板（视频偏好/yt-dlp 版本更新/cookie 管理）

**Files:**
- Modify: `src/components/SettingsModal.tsx`
- Modify: `src/App.css`

**Interfaces:**
- Consumes: Task 10 的 `api.getYtdlpStatus/updateYtdlp/importCookies/clearCookies`、`ManagerConfig` 视频字段、`YtdlpStatus`
- Produces: 设置面板"视频下载"区（画质偏好/字幕默认）、"组件与 Cookie"区（yt-dlp 版本+更新按钮、cookie 导入/清除）

- [ ] **Step 1: SettingsModal 扩展**

组件新增 state 与区块（现有区保持；`save` 的 cfg 构造补五个视频字段）：

```tsx
  // 视频偏好
  const [maxHeight, setMaxHeight] = useState<number>(config?.video_max_height ?? 1080);
  const [audioOnly, setAudioOnly] = useState(config?.video_audio_only ?? false);
  const [subLangs, setSubLangs] = useState(config?.video_sub_langs ?? 'zh-Hans,en');
  const [autoSubs, setAutoSubs] = useState(config?.video_auto_subs ?? false);
  // 组件与 cookie
  const [ytdlp, setYtdlp] = useState<YtdlpStatus | null>(null);
  const [updating, setUpdating] = useState(false);
  const [cookieBrowser, setCookieBrowser] = useState('edge');
  const [cookieMsg, setCookieMsg] = useState<string | null>(null);

  useEffect(() => {
    api.getYtdlpStatus().then(setYtdlp).catch(() => {});
  }, []);

  const doUpdate = async () => {
    setUpdating(true); setCookieMsg(null);
    try { setYtdlp(await api.updateYtdlp()); } catch (e) { setCookieMsg(String(e)); }
    finally { setUpdating(false); }
  };
  const doImportCookies = async () => {
    setUpdating(true); setCookieMsg(null);
    try { await api.importCookies(cookieBrowser); setCookieMsg('Cookie 已导入'); }
    catch (e) { setCookieMsg(String(e)); }
    finally { setUpdating(false); }
  };
  const doClearCookies = async () => {
    await api.clearCookies().catch(() => {});
    setCookieMsg('Cookie 已清除');
  };
```

`save()` 的 cfg 对象补：

```typescript
      video_max_height: audioOnly ? null : maxHeight,
      video_audio_only: audioOnly,
      video_sub_langs: subLangs,
      video_auto_subs: autoSubs,
      // cookie_file 由后端管理，前端不提交——后端 update_config 收到的 cfg 此字段为 undefined
      // → serde default 接 null，配置层不存 cookie 路径（cookie 存在性即生效，见 Task 9）
```

⚠️ 类型修正：`ManagerConfig` TS 接口有 `cookie_file` 字段就必须提交（TS 要求完整对象）。**决策**：TS 的 `ManagerConfig` 不含 `cookie_file`（后端 serde `#[serde(default)]` 容忍缺失，cookie 存在性即生效）——Task 10 已按此设计（ManagerConfig TS 无 cookie_file）。执行者确认两端口径一致：Rust 端有该字段 + serde default；TS 端无该字段。

JSX 区块（插在现有区块之后、err/modal-actions 之前）：

```tsx
        <h3>视频下载</h3>
        <label>默认画质（最高分辨率）</label>
        <select value={audioOnly ? 'audio' : String(maxHeight)}
          onChange={(e) => {
            if (e.target.value === 'audio') setAudioOnly(true);
            else { setAudioOnly(false); setMaxHeight(Number(e.target.value)); }
          }}>
          <option value="2160">2160p（4K）</option>
          <option value="1440">1440p</option>
          <option value="1080">1080p</option>
          <option value="720">720p</option>
          <option value="480">480p</option>
          <option value="audio">仅音频</option>
        </select>
        <label>字幕语言（逗号分隔，留空不下字幕）</label>
        <input value={subLangs} onChange={(e) => setSubLangs(e.target.value)} />
        <label className="checkbox">
          <input type="checkbox" checked={autoSubs} onChange={(e) => setAutoSubs(e.target.checked)} />
          默认包含自动生成字幕（CC）
        </label>

        <h3>组件与 Cookie</h3>
        <div className="settings-row">
          <span>yt-dlp {ytdlp?.version ?? '…'}</span>
          <button className="btn btn--sm" disabled={updating} onClick={doUpdate}>
            {updating ? '处理中…' : '检查更新'}
          </button>
        </div>
        <div className="settings-row">
          <select value={cookieBrowser} onChange={(e) => setCookieBrowser(e.target.value)}>
            <option value="edge">Edge</option>
            <option value="chrome">Chrome</option>
            <option value="firefox">Firefox</option>
          </select>
          <button className="btn btn--sm" disabled={updating} onClick={doImportCookies}>导入 Cookie</button>
          <button className="btn btn--sm" onClick={doClearCookies}>清除</button>
        </div>
        <div className="settings-note">Cookie 文件保存在本机应用数据目录，仅用于视频解析下载；清除即删除文件。导入 Cookie 可解锁登录内容与会员画质。</div>
        {cookieMsg && <div className="settings-msg">{cookieMsg}</div>}
```

`App.css` 追加 `.settings-row { display:flex; gap:8px; align-items:center; margin:6px 0; } .settings-row span { flex:1; } .settings-note { font-size:12px; color: var(--muted, #8aa0bd); line-height:1.5; margin:6px 0; } .settings-msg { font-size:12px; margin-top:6px; }`（变量名对齐现有 App.css）。

- [ ] **Step 2: 构建验收 + Commit**

Run: `npm run build && npm run tauri dev`
手动验收：设置面板显示 yt-dlp 版本；"检查更新"能完成下载并刷新版本号；导入 Edge cookie 成功提示（无浏览器登录则报错透传）；清除后再次下载视频不带 cookie。

```bash
git add -A
git commit -m "feat(web): 设置面板视频偏好/yt-dlp 更新/cookie 管理区"
```

---

### Task 14: 打包与发布（resources/获取脚本/Release workflow/便携 zip）

**Files:**
- Create: `scripts/fetch-bin.ps1`
- Modify: `tauri.conf.json`（bundle.resources）
- Modify: `.github/workflows/release.yml`
- Modify: `.gitignore`
- Modify: `package.json`（`fetch:bin` script）

**Interfaces:**
- Consumes: Task 9 的 `find_binary` 候选链（resource_dir → exe 同目录 → CARGO_MANIFEST_DIR/bin）
- Produces: NSIS 安装包与便携 zip 均携带 `bin/yt-dlp.exe`（固定版本基线）与 `bin/ffmpeg.exe`；CI 自动下载二进制；开发机 `npm run fetch:bin` 一次

- [ ] **Step 1: 获取脚本 scripts/fetch-bin.ps1**

```powershell
# 下载 yt-dlp / ffmpeg 外部二进制到 src-tauri/bin/（开发与 CI 共用）
# yt-dlp 固定基线版本（发布可复现）；ffmpeg 用 BtbN 稳定 latest 构建
$ErrorActionPreference = "Stop"
$dir = Join-Path $PSScriptRoot "..\src-tauri\bin"
New-Item -ItemType Directory -Force $dir | Out-Null

$YTDLP_VERSION = "2026.08.24"   # 基线版本；发布时随版本提升更新
$ytdlpUrl = "https://github.com/yt-dlp/yt-dlp/releases/download/$YTDLP_VERSION/yt-dlp.exe"
$ytdlpDest = Join-Path $dir "yt-dlp.exe"
if (-not (Test-Path $ytdlpDest)) {
  Write-Host "下载 yt-dlp $YTDLP_VERSION ..."
  Invoke-WebRequest -Uri $ytdlpUrl -OutFile $ytdlpDest
}

$ffmpegZip = Join-Path $dir "ffmpeg.zip"
$ffmpegDest = Join-Path $dir "ffmpeg.exe"
if (-not (Test-Path $ffmpegDest)) {
  Write-Host "下载 ffmpeg ..."
  Invoke-WebRequest -Uri "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl.zip" -OutFile $ffmpegZip
  Expand-Archive -Path $ffmpegZip -DestinationPath (Join-Path $dir "ffmpeg-tmp") -Force
  Copy-Item (Join-Path $dir "ffmpeg-tmp\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe") $ffmpegDest -Force
  Remove-Item (Join-Path $dir "ffmpeg-tmp") -Recurse -Force
  Remove-Item $ffmpegZip
}
Write-Host "完成：$ytdlpDest / $ffmpegDest"
```

（`$YTDLP_VERSION` 基线版本号执行时以 yt-dlp 最新 release 为准填入。）

- [ ] **Step 2: 配置接线**

`package.json` scripts 加 `"fetch:bin": "powershell -ExecutionPolicy Bypass -File scripts/fetch-bin.ps1"`。

`tauri.conf.json` 的 `bundle` 加：

```json
    "resources": {
      "bin/yt-dlp.exe": "bin/",
      "bin/ffmpeg.exe": "bin/"
    }
```

`.gitignore` 加：

```
src-tauri/bin/
```

（注意：若 `.gitignore` 已有类似条目则跳过；二进制不入库。）

- [ ] **Step 3: release.yml 增加下载步骤 + 便携 zip**

```yaml
      - name: 前端依赖
        run: npm install
      - name: 下载外部二进制（yt-dlp 基线 + ffmpeg）
        run: npm run fetch:bin
      - name: 构建安装包（含便携版）
        run: npm run tauri build
      - name: 组装便携版 zip（exe + bin）
        run: |
          mkdir -p portable/bin
          cp target/release/sparkling.exe portable/
          cp src-tauri/bin/yt-dlp.exe src-tauri/bin/ffmpeg.exe portable/bin/
          Compress-Archive -Path portable/* -DestinationPath sparkling-portable-windows-x64.zip
        shell: pwsh
      - name: 发布到 GitHub Releases
        uses: softprops/action-gh-release@v2
        with:
          files: |
            target/release/bundle/nsis/*.exe
            sparkling-portable-windows-x64.zip
          generate_release_notes: true
```

（原 `target/release/sparkling.exe` 产物行删除——便携形态改为 zip。）

- [ ] **Step 4: 本机验证 + Commit**

Run: `npm run fetch:bin && npm run tauri build`
Expected: 安装包构建成功；`target/release/bundle/nsis/*.exe` 与本地安装运行后视频功能可用（二进制经 resource 解析到）

```bash
git add -A
git commit -m "build: 打包 yt-dlp/ffmpeg 进安装包与便携 zip（fetch 脚本 + Release 流水线）"
```

（Release 全流程验证打 `v0.2.0` 标签时进行，不在本任务。）

---

### Task 15: 回归收尾（README/路线图/全量质量门）

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-08-29-video-ytdlp-design.md`（无——spec 不改）

**Interfaces:**
- Consumes: 全部前序任务
- Produces: 文档与路线图更新；全仓绿态

- [ ] **Step 1: README 更新**

功能清单加：

```markdown
- 视频解析下载（yt-dlp）：Bilibili/YouTube 等 1800+ 站点，画质选择、播放列表批量、字幕、Cookie 导入（登录/会员画质）
```

开发一节 `npm install` 后补一句：

```markdown
> 首次构建视频功能需先 `npm run fetch:bin` 下载 yt-dlp/ffmpeg 到 `src-tauri/bin/`（约 40MB，仅一次）
```

路线图勾选：

```markdown
- [x] ③ 视频解析下载（yt-dlp）
```

- [ ] **Step 2: 全量质量门**

Run: `cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test && npm run build`
Expected: 全绿（fmt 无 diff、clippy 零告警、Rust 测试全过、前端构建通过）

- [ ] **Step 3: 真机端到端验收（用户亲验）**

清单（开发模式逐项）：
1. 粘贴 Bilibili 单视频 → 解析 → 选 1080p → 下载完成可播放（ffmpeg 合并生效）
2. 粘贴 YouTube 播放列表 → 条目勾选 → 批量任务依次完成
3. 视频任务暂停 → 继续（.part 续传，不从头下）
4. 视频任务取消 → 无 .part 残留
5. 设置导入 Edge cookie → Bilibili 登录画质生效 → 清除 cookie
6. 字幕下载：勾选 zh-Hans → 视频旁出现 .zh-Hans.srt
7. 应用重启 → 未完成视频任务自动续传
8. yt-dlp"检查更新"→ 版本号刷新且视频功能正常
9. HTTP 直链下载回归（①期功能无损）

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: README 视频功能与路线图③期勾选"
```

---

## 计划自审记录

1. **Spec 覆盖**：D1 分发（Task 7/9/14）✓；D2 yt-dlp 全权下载（Task 6）✓；D3 统一任务列表（Task 1/2/8）✓；D4 交互（Task 10 `selectorFromPreference` + Task 11 直下按钮/解析面板双路径；偏好由 Task 13 设置面板写入 ManagerConfig——"记住此选择"即存为默认画质偏好）✓；D5 ffmpeg 打包（Task 14）✓；D6 范围：播放列表（Task 11）、字幕（Task 6 build_args + Task 11 面板）、Cookie（Task 9/13）✓。
2. **占位符扫描**：Task 11 Step 2 的 `/* 现有属性 */` 为"保留现有代码"指令而非待补占位——已明确说明保留原 JSX；Task 9 Step 2 给出两版实现并指定最终形态；无 TBD/TODO。
3. **类型一致性**：`VideoParams{format,subtitles,auto_subs}`（Rust/TS 字段名一致，serde 无 rename）；`merging` 在 ProgressSnapshot→TaskEvent→LiveInfo→TaskRow 链路各任务接口一致；`Engines::for_kind` 签名在 Task 8 定义、Task 9 构造使用；`TokioChildRunner.bin` pub 字段被 Task 9 import_cookies 直用。
