# Sparkling 子项目①：HTTP 多线程下载核心 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现可公开发布的 HTTP/HTTPS 多线程下载器核心（分片调度、断点续传、限速、任务队列、SQLite 持久化）+ Tauri 2 + React 壳。

**Architecture:** `sparkling-core` 为纯 Rust crate（不依赖 Tauri），内含 `Engine` trait 抽象、`HttpEngine`（分片调度器 + 控制文件断点续传 + 令牌桶限速）、`TaskStore`（SQLite）、`TaskManager`（并发调度）。Tauri 壳只做 command/event 桥接，React 前端消费事件渲染任务列表。

**Tech Stack:** Rust (tokio, reqwest, rusqlite, axum[仅测试]), Tauri 2, React + TypeScript + Vite。

**Spec:** `docs/superpowers/specs/2026-08-28-http-downloader-core-design.md`（本计划从 spec 出发，执行者需同时阅读两份文档）

## Global Constraints

- 分片默认 8，可配 1–64；并发任务数默认 3（可配）；进度事件节流 250ms；控制文件每 2 秒或有分片完成时落盘。
- 分片重试：指数退避 1s/2s/4s…上限 30s，5 次后该分片失败（测试可用快速策略）。
- 磁盘空间检查：文件大小 × 1.02。
- 数据正确性红线：ETag/Last-Modified 不一致 → 整任务从零重下；Content-MD5 不匹配 → Failed 且不产出正式文件。
- Windows 优先但禁止硬编码平台 API；`sparkling-core` 不得依赖 Tauri。
- 控制文件 `<文件名>.sparkling`，数据文件 `<文件名>.sparkling.part`。
- 提交信息用 Conventional Commits（feat/test/chore/docs）。

## 文件结构总览

```
sparkling/
├── Cargo.toml                      # workspace: crates/sparkling-core + src-tauri
├── package.json                    # React 前端（Task 15/16）
├── crates/sparkling-core/
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs                  # 模块导出
│   │   ├── error.rs                # SparklingError
│   │   ├── task.rs                 # TaskState 状态机 + TaskSpec
│   │   ├── segment.rs              # 分片数学（split / take_over）
│   │   ├── control_file.rs         # .sparkling 控制文件读写
│   │   ├── throttle.rs             # 令牌桶
│   │   ├── engine.rs               # Engine trait + TaskHandle + ProgressSnapshot
│   │   ├── probe.rs                # HTTP 探测（大小/Range/文件名/校验头）
│   │   ├── disk.rs                 # 磁盘空间检查
│   │   ├── http_engine.rs          # HttpEngine（supervisor + 分片 worker + 偷段）
│   │   ├── store.rs                # TaskStore（SQLite）
│   │   └── manager.rs              # TaskManager（队列/调度/恢复）
│   └── tests/
│       ├── common/mod.rs           # 可编程测试服务器（axum）
│       ├── segment_state.rs        # 纯单元测试（Task 2/3）
│       ├── control_throttle.rs     # 控制文件 + 令牌桶测试（Task 4/5）
│       ├── probe_seq.rs            # 探测 + 单线程降级（Task 7/8）
│       ├── segments.rs             # 多线程 + 偷段（Task 9/10）
│       ├── resume.rs               # 暂停/崩溃恢复/ETag 变化（Task 11）
│       └── errors.rs               # 重试/5xx/416/MD5（Task 12）
├── src-tauri/                      # Tauri 壳（Task 15）
└── src/                            # React 前端（Task 16）
```

---

### Task 1: Rust workspace 脚手架 + SparklingError

**Files:**
- Create: `Cargo.toml`（workspace 根）
- Create: `crates/sparkling-core/Cargo.toml`
- Create: `crates/sparkling-core/src/lib.rs`
- Create: `crates/sparkling-core/src/error.rs`
- Test: `crates/sparkling-core/src/error.rs`（内联 `#[cfg(test)]`）

**Interfaces:**
- Consumes: 无（首个任务）
- Produces: `SparklingError` 枚举（variants: `Network(String)`, `HttpStatus { status: u16, detail: String }`, `InsufficientDisk { required: u64, available: u64 }`, `CorruptControlFile(String)`, `RemoteChanged(String)`, `DiskWrite(String)`, `ChecksumMismatch { expected: String, actual: String }`, `TaskNotFound(String)`, `Other(String)`）与方法 `user_message(&self) -> String`、`technical(&self) -> String`。后续所有任务用它做错误类型。

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/src/error.rs`（先只写测试骨架，类型未实现时编译失败即 TDD 的 RED）：

```rust
use crate::error::SparklingError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_message_is_chinese_and_readable() {
        let e = SparklingError::InsufficientDisk { required: 1000, available: 500 };
        assert!(e.user_message().contains("磁盘空间不足"));
        assert!(e.user_message().contains("1000"));
    }

    #[test]
    fn technical_keeps_debug_detail() {
        let e = SparklingError::HttpStatus { status: 503, detail: "unavailable".into() };
        assert!(e.technical().contains("503"));
        assert!(e.technical().contains("HttpStatus"));
    }

    #[test]
    fn remote_changed_and_checksum_have_distinct_messages() {
        let e = SparklingError::RemoteChanged("etag".into());
        assert!(e.user_message().contains("已变化"));
        let e = SparklingError::ChecksumMismatch { expected: "a".into(), actual: "b".into() };
        assert!(e.user_message().contains("校验"));
    }
}
```

注意：实际文件中测试放在实现下方同一文件，先写测试再补实现。

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core`
Expected: 编译失败（`SparklingError` 未定义）

- [ ] **Step 3: 写实现**

`crates/sparkling-core/Cargo.toml`：

```toml
[package]
name = "sparkling-core"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["full"] }
reqwest = { version = "0.12", default-features = true, features = ["stream"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
uuid = { version = "1", features = ["v4"] }
rusqlite = { version = "0.32", features = ["bundled"] }
md-5 = "0.10"
base64 = "0.22"
async-trait = "0.1"
fs2 = "0.4"
percent-encoding = "2"
tracing = "0.1"

[dev-dependencies]
# start_paused 测试需要 test-util（不在 full 里）
tokio = { version = "1", features = ["test-util"] }
axum = "0.7"
tempfile = "3"
sha2 = "0.10"
futures = "0.3"
```

根 `Cargo.toml`：

```toml
[workspace]
resolver = "2"
members = ["crates/sparkling-core"]

[workspace.package]
edition = "2021"
```

`crates/sparkling-core/src/error.rs`：

```rust
use thiserror::Error;

/// 统一错误类型：user_message() 给用户看（中文），technical() 给详情面板看。
#[derive(Debug, Error)]
pub enum SparklingError {
    #[error("网络错误: {0}")]
    Network(String),

    #[error("服务器返回 {status}: {detail}")]
    HttpStatus { status: u16, detail: String },

    #[error("磁盘空间不足: 需要 {required} 字节, 剩余 {available} 字节")]
    InsufficientDisk { required: u64, available: u64 },

    #[error("控制文件损坏: {0}")]
    CorruptControlFile(String),

    #[error("远端文件已变化: {0}")]
    RemoteChanged(String),

    #[error("磁盘写入失败: {0}")]
    DiskWrite(String),

    #[error("完整性校验失败: 期望 {expected}, 实际 {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("任务不存在: {0}")]
    TaskNotFound(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, SparklingError>;

impl SparklingError {
    /// 用户可读的中文消息
    pub fn user_message(&self) -> String {
        self.to_string()
    }

    /// 技术细节（状态码、内部结构）
    pub fn technical(&self) -> String {
        format!("{self:?}")
    }
}
```

`crates/sparkling-core/src/lib.rs`：

```rust
pub mod error;

pub use error::{Result, SparklingError};
```

（把测试模块追加在 `error.rs` 末尾。）

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core`
Expected: 3 个测试 PASS

- [ ] **Step 5: 提交**

```bash
git add Cargo.toml Cargo.lock crates/
git commit -m "feat(core): workspace 脚手架与 SparklingError 错误类型"
```

---

### Task 2: 任务状态机 TaskState

**Files:**
- Create: `crates/sparkling-core/src/task.rs`
- Modify: `crates/sparkling-core/src/lib.rs`（加 `pub mod task;`）
- Test: `crates/sparkling-core/src/task.rs` 内联测试

**Interfaces:**
- Consumes: 无
- Produces: `TaskState`（`Queued/Running/Paused/Completed/Failed/Cancelled`），方法 `as_str() -> &'static str`、`from_str(&str) -> Option<Self>`、`can_transition_to(self, next) -> bool`；`TaskSpec { url: String, save_dir: PathBuf, filename: Option<String>, segments: u32, max_speed: Option<u64> }`；`TaskId = String`。Manager/Engine/Store 任务依赖这些名字。

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legal_transitions() {
        use TaskState::*;
        let legal = [
            (Queued, Running), (Queued, Cancelled),
            (Running, Paused), (Running, Completed), (Running, Failed), (Running, Cancelled),
            (Paused, Queued), (Paused, Cancelled), (Paused, Failed),
            (Failed, Queued), (Failed, Cancelled),
        ];
        for (from, to) in legal {
            assert!(from.can_transition_to(to), "{from:?} -> {to:?} 应合法");
        }
    }

    #[test]
    fn illegal_transitions() {
        use TaskState::*;
        let illegal = [
            (Completed, Running), (Completed, Queued), (Completed, Failed),
            (Cancelled, Running), (Cancelled, Queued),
            (Queued, Completed), (Queued, Failed), (Queued, Paused),
            (Failed, Running), (Failed, Completed),
        ];
        for (from, to) in illegal {
            assert!(!from.can_transition_to(to), "{from:?} -> {to:?} 应非法");
        }
    }

    #[test]
    fn str_roundtrip() {
        for s in [TaskState::Queued, TaskState::Running, TaskState::Paused,
                  TaskState::Completed, TaskState::Failed, TaskState::Cancelled] {
            assert_eq!(TaskState::from_str(s.as_str()), Some(s));
        }
        assert_eq!(TaskState::from_str("bogus"), None);
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core`
Expected: 编译失败（`TaskState` 未定义）

- [ ] **Step 3: 写实现**

`crates/sparkling-core/src/task.rs`：

```rust
use std::path::PathBuf;

pub type TaskId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Queued,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Queued => "queued",
            TaskState::Running => "running",
            TaskState::Paused => "paused",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "queued" => TaskState::Queued,
            "running" => TaskState::Running,
            "paused" => TaskState::Paused,
            "completed" => TaskState::Completed,
            "failed" => TaskState::Failed,
            "cancelled" => TaskState::Cancelled,
            _ => return None,
        })
    }

    /// 状态机（与 spec 一致）：
    /// Queued→Running/Cancelled；Running→Paused/Completed/Failed/Cancelled；
    /// Paused→Queued(重试)/Cancelled/Failed(恢复时校验失败)；
    /// Failed→Queued(手动重试)/Cancelled；Completed/Cancelled 为终态。
    pub fn can_transition_to(self, next: TaskState) -> bool {
        use TaskState::*;
        matches!((self, next),
            (Queued, Running) | (Queued, Cancelled)
            | (Running, Paused) | (Running, Completed) | (Running, Failed) | (Running, Cancelled)
            | (Paused, Queued) | (Paused, Cancelled) | (Paused, Failed)
            | (Failed, Queued) | (Failed, Cancelled))
    }
}

/// 提交给引擎的下载任务描述
#[derive(Debug, Clone)]
pub struct TaskSpec {
    pub url: String,
    pub save_dir: PathBuf,
    /// None = 从 Content-Disposition / URL 推断
    pub filename: Option<String>,
    /// 分片数 1–64
    pub segments: u32,
    /// 单任务限速 bytes/s，None = 不限
    pub max_speed: Option<u64>,
}
```

`lib.rs` 增加一行：

```rust
pub mod task;
pub use task::{TaskId, TaskSpec, TaskState};
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core`
Expected: 全部 PASS（含 Task 1 的 3 个）

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core/src
git commit -m "feat(core): 任务状态机 TaskState 与 TaskSpec"
```

---

### Task 3: 分片数学（split / take_over）

**Files:**
- Create: `crates/sparkling-core/src/segment.rs`
- Modify: `crates/sparkling-core/src/lib.rs`
- Test: `crates/sparkling-core/tests/segment_state.rs`

**Interfaces:**
- Consumes: 无
- Produces: `Segment { index: usize, start: u64, end: u64, downloaded: u64 }`（end 含端点；不变量：`downloaded` 是从 `start` 起连续已写字节数，取值 `0..=len()`），方法 `len()`、`remaining()`、`next_offset()`；自由函数 `split(total: u64, n: u32) -> Vec<Segment>`（`total==0` 返回空 vec）与 `take_over(from: &mut Segment, new_index: usize) -> Option<Segment>`（把 `from` 剩余部分右半切给新段）。http_engine / control_file 依赖。

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/tests/segment_state.rs`：

```rust
use sparkling_core::segment::{split, take_over, Segment};

#[test]
fn split_normal() {
    let segs = split(100, 8);
    assert_eq!(segs.len(), 8);
    // 100 = 8*12 + 4，前 4 段 13 字节
    let lens: Vec<u64> = segs.iter().map(|s| s.len()).collect();
    assert_eq!(lens, vec![13, 13, 13, 13, 12, 12, 12, 12]);
    assert_eq!(segs[0].start, 0);
    assert_eq!(segs[0].end, 12);
    assert_eq!(segs[7].end, 99);
    // 段间无缝衔接
    for w in segs.windows(2) {
        assert_eq!(w[0].end + 1, w[1].start);
    }
    for s in &segs {
        assert_eq!(s.downloaded, 0);
    }
}

#[test]
fn split_fewer_bytes_than_segments() {
    let segs = split(3, 8);
    assert_eq!(segs.len(), 3);
    assert!(segs.iter().all(|s| s.len() == 1));
}

#[test]
fn split_exact() {
    let segs = split(8, 8);
    assert_eq!(segs.len(), 8);
    assert!(segs.iter().all(|s| s.len() == 1));
}

#[test]
fn split_zero_and_guard() {
    assert!(split(0, 8).is_empty());
    let segs = split(100, 0); // n=0 视为 1
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].len(), 100);
}

#[test]
fn split_huge() {
    let segs = split(u64::MAX, 8);
    assert_eq!(segs.len(), 8);
    assert_eq!(segs[0].start, 0);
    // 含端点语义：sum == u64::MAX，但末段 end == u64::MAX - 1（end+1 才是总字节）
    assert_eq!(segs[7].end, u64::MAX - 1);
    let total: u128 = segs.iter().map(|s| s.len() as u128).sum();
    assert_eq!(total, u64::MAX as u128);
}

#[test]
fn take_over_splits_remaining_in_half() {
    let mut seg = Segment { index: 0, start: 0, end: 99, downloaded: 10 };
    // 剩余 [10,99] 共 90 字节，右半 [55,99]
    let stolen = take_over(&mut seg, 8).unwrap();
    assert_eq!(stolen.index, 8);
    assert_eq!(stolen.start, 55);
    assert_eq!(stolen.end, 99);
    assert_eq!(stolen.downloaded, 0);
    assert_eq!(seg.end, 54);
    assert_eq!(seg.downloaded, 10);
    // 两段剩余之和不变
    assert_eq!(seg.remaining() + stolen.remaining(), 90);
}

#[test]
fn take_over_refuses_tiny_remaining() {
    // 含端点语义：len = end - start + 1 = 11
    let mut seg = Segment { index: 0, start: 0, end: 10, downloaded: 11 }; // 剩余 0
    assert!(take_over(&mut seg, 1).is_none());
    let mut seg = Segment { index: 0, start: 0, end: 10, downloaded: 10 }; // 剩余 1
    assert!(take_over(&mut seg, 1).is_none());
}
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test segment_state`
Expected: 编译失败（`segment` 模块不存在）

- [ ] **Step 3: 写实现**

`crates/sparkling-core/src/segment.rs`：

```rust
use serde::{Deserialize, Serialize};

/// 一个下载分片。`end` 为含端点偏移。
/// 不变量：`downloaded` 表示从 `start` 起已连续写入的字节数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub index: usize,
    pub start: u64,
    pub end: u64,
    pub downloaded: u64,
}

impl Segment {
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn remaining(&self) -> u64 {
        self.len() - self.downloaded
    }
    /// 下一次 Range 请求的起始偏移
    pub fn next_offset(&self) -> u64 {
        self.start + self.downloaded
    }
}

/// 把 `total` 字节尽量均匀切成 `n` 段。total==0 返回空。
/// total < n 时实际段数 = total（每段至少 1 字节）。n==0 视为 1。
pub fn split(total: u64, n: u32) -> Vec<Segment> {
    if total == 0 {
        return Vec::new();
    }
    let n = (n.max(1) as u64).min(total);
    let base = total / n;
    let rem = total % n;
    let mut segs = Vec::with_capacity(n as usize);
    let mut start = 0u64;
    for i in 0..n {
        let len = base + if i < rem { 1 } else { 0 };
        segs.push(Segment { index: i as usize, start, end: start + len - 1, downloaded: 0 });
        start += len;
    }
    segs
}

/// 动态偷段：把 `from` 的剩余部分右半切出来作为新段（新 worker 接手）。
/// 剩余 < 2 字节时返回 None（不值得切）。
pub fn take_over(from: &mut Segment, new_index: usize) -> Option<Segment> {
    let rem = from.remaining();
    if rem < 2 {
        return None;
    }
    let half = rem / 2;
    let new_start = from.next_offset() + half;
    let stolen = Segment { index: new_index, start: new_start, end: from.end, downloaded: 0 };
    from.end = new_start - 1;
    Some(stolen)
}
```

`lib.rs` 增加：

```rust
pub mod segment;
pub use segment::{split, take_over, Segment};
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): 分片数学 split/take_over"
```

---

### Task 4: 控制文件（原子写 / 损坏检测）

**Files:**
- Create: `crates/sparkling-core/src/control_file.rs`
- Modify: `crates/sparkling-core/src/lib.rs`
- Test: `crates/sparkling-core/tests/control_throttle.rs`

**Interfaces:**
- Consumes: `Segment`（Task 3）
- Produces: `ControlFile { url, etag, last_modified, total_size, supports_range, filename, segments: Vec<Segment> }`（serde 可序列化）；`path_for(final_path: &Path) -> PathBuf`（`a.bin` → `a.bin.sparkling`）；`save(final_path: &Path, cf: &ControlFile) -> Result<()>`（原子：tmp + rename）；`load(ctl_path: &Path) -> Result<ControlFile>`（JSON 解析失败或分片不变量被破坏 → `CorruptControlFile`）；`exists(ctl_path: &Path) -> bool`。http_engine / manager 依赖。

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/tests/control_throttle.rs`（本文件同时容纳 Task 5 的令牌桶测试）：

```rust
use sparkling_core::control_file::{self, ControlFile};
use sparkling_core::segment::{split, Segment};

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

#[test]
fn inverted_segment_is_corrupt_not_panic() {
    // 倒置区间（end < start）必须返回 CorruptControlFile，而不是 debug 构建下溢 panic
    let dir = tempfile::tempdir().unwrap();
    let final_path = dir.path().join("a.bin");
    let mut cf = sample();
    cf.segments[0] = Segment { index: 0, start: 10, end: 5, downloaded: 0 };
    control_file::save(&final_path, &cf).unwrap();
    let err = control_file::load(&control_file::path_for(&final_path)).unwrap_err();
    assert!(matches!(err, sparkling_core::SparklingError::CorruptControlFile(_)));
}
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test control_throttle`
Expected: 编译失败（`control_file` 模块不存在）

- [ ] **Step 3: 写实现**

`crates/sparkling-core/src/control_file.rs`：

```rust
use crate::{Result, SparklingError};
use crate::segment::Segment;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// 断点续传控制文件（`<文件名>.sparkling`）的内容
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlFile {
    pub url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub total_size: u64,
    pub supports_range: bool,
    pub filename: String,
    pub segments: Vec<Segment>,
}

/// 正式文件 `a.bin` 对应的控制文件路径 `a.bin.sparkling`
pub fn path_for(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_os_string();
    s.push(".sparkling");
    PathBuf::from(s)
}

pub fn exists(ctl_path: &Path) -> bool {
    ctl_path.is_file()
}

/// 原子保存：写 `<名>.sparkling.tmp` 后 rename 覆盖
pub fn save(final_path: &Path, cf: &ControlFile) -> Result<()> {
    let ctl = path_for(final_path);
    let mut tmp = ctl.clone().into_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let data = serde_json::to_vec(cf)
        .map_err(|e| SparklingError::DiskWrite(format!("控制文件序列化失败: {e}")))?;
    std::fs::write(&tmp, &data)
        .map_err(|e| SparklingError::DiskWrite(format!("控制文件写入失败: {e}")))?;
    // std::fs::rename 在 Windows 上是 MoveFileExW + REPLACE_EXISTING，直接覆盖
    std::fs::rename(&tmp, &ctl)
        .map_err(|e| SparklingError::DiskWrite(format!("控制文件落盘失败: {e}")))?;
    Ok(())
}

/// 加载并校验。JSON 解析失败、IO 错误、分片不变量破坏都算损坏
/// （宁可控文件判损坏后重下，也不用可疑偏移续传）。
pub fn load(ctl_path: &Path) -> Result<ControlFile> {
    let raw = std::fs::read(ctl_path)
        .map_err(|e| SparklingError::CorruptControlFile(format!("读取失败: {e}")))?;
    let cf: ControlFile = serde_json::from_slice(&raw)
        .map_err(|e| SparklingError::CorruptControlFile(format!("JSON 解析失败: {e}")))?;
    for seg in &cf.segments {
        // 注意顺序：end < start 先判（短路），否则 len() 的 u64 减法在 debug 构建下溢 panic；
        // 错误消息同样不能用 len()（消息参数对倒置区间仍会求值）——用 saturating 计算
        if seg.end < seg.start || seg.downloaded > seg.len() {
            return Err(SparklingError::CorruptControlFile(format!(
                "分片 {} 不变量破坏: downloaded={} len={}",
                seg.index,
                seg.downloaded,
                seg.end.saturating_sub(seg.start) + 1
            )));
        }
    }
    Ok(cf)
}
```

`lib.rs` 增加：

```rust
pub mod control_file;
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core --test control_throttle`
Expected: 5 个控制文件测试 PASS（令牌桶测试尚未写）

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): 控制文件原子读写与损坏检测"
```

---

### Task 5: 令牌桶限速

**Files:**
- Create: `crates/sparkling-core/src/throttle.rs`
- Modify: `crates/sparkling-core/src/lib.rs`
- Test: `crates/sparkling-core/tests/control_throttle.rs`（追加）

**Interfaces:**
- Consumes: 无
- Produces: `TokenBucket::new(rate: Option<u64>) -> Self`（None 或 0 = 不限速）；`set_rate(&self, rate: Option<u64>)`（运行时实时生效）；`async acquire(&self, amount: u64)`（等到有足够令牌）。http_engine 依赖（全局桶 + 单任务桶）。内部用 `tokio::time::Instant` 保证可测试。

- [ ] **Step 1: 写失败测试**

追加到 `crates/sparkling-core/tests/control_throttle.rs`：

```rust
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
        let bucket = TokenBucket::new(Some(1000)); // 1000 B/s，初始满桶 1000
        bucket.acquire(1000).await; // 排空初始令牌
        let t0 = tokio::time::Instant::now();
        bucket.acquire(500).await; // 需要等 0.5s 攒令牌
        assert!(t0.elapsed() >= std::time::Duration::from_millis(500));
    }

    #[tokio::test(start_paused = true)]
    async fn oversize_acquire_terminates_and_paces() {
        // 限速低于单块大小（64 KiB）的场景：acquire 决不能挂死
        let bucket = TokenBucket::new(Some(60_000)); // 60 KB/s
        bucket.acquire(64 * 1024).await; // 初始满桶 60000 < 65536，须按差额等待
        let t0 = tokio::time::Instant::now();
        bucket.acquire(64 * 1024).await;
        // 第二块按速率线性等待：65536/60000 ≈ 1.09s
        assert!(t0.elapsed() >= std::time::Duration::from_millis(1000));
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
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test control_throttle`
Expected: 编译失败（`throttle` 模块不存在）

- [ ] **Step 3: 写实现**

`crates/sparkling-core/src/throttle.rs`：

```rust
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
    /// 支持 amount > rate（桶容量 = 1 秒配额）：按速率线性等待差额，等待期产生的
    /// 令牌直接折算进等待时长，睡醒清零完成扣减——低速限速下大块请求不会挂死。
    pub async fn acquire(&self, amount: u64) {
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
            ((amount as f64 - g.tokens) / g.rate as f64).max(0.001)
        };
        tokio::time::sleep(std::time::Duration::from_secs_f64(sleep_for)).await;
        // 等待期间的令牌额度已折算进等待时长：清零即完成本次扣减
        self.inner.lock().unwrap().tokens = 0.0;
    }
}
```

`lib.rs` 增加：

```rust
pub mod throttle;
pub use throttle::TokenBucket;
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core --test control_throttle`
Expected: 全部 PASS（含 Task 4 的 5 个）

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): 令牌桶限速器"
```

---

### Task 6: 可编程测试服务器（测试基础设施）

**Files:**
- Create: `crates/sparkling-core/tests/common/mod.rs`
- Test: 同文件内冒烟测试（服务器能起、Range 语义正确）

**Interfaces:**
- Consumes: 无（dev-dependency axum）
- Produces: `ServerConfig { size, support_range, fail_mode, slow_ranges, drop_after, disposition, send_md5 }`；`FailMode { None, Always5xx, FailFirstN(u32), Always416, WrongMd5 }`；`TestServer { url, data: Vec<u8> }` 与 `start(cfg: ServerConfig) -> TestServer`；`TestServer::set_content_v2()`（切换内容与 ETag，测 ETag 变化）；`sha256_hex(&[u8]) -> String` 辅助函数。Task 7–12 的所有集成测试依赖。

- [ ] **Step 1: 写冒烟测试**

`crates/sparkling-core/tests/common/mod.rs`：

```rust
//! 可编程 HTTP 测试服务器。行为由 ServerConfig 驱动，
//! 服务于 probe/多线程/偷段/续传/错误处理等所有集成测试。
#![allow(dead_code)]
use axum::body::Body;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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

async fn handler(State(st): State<Arc<ServerState>>, headers: HeaderMap) -> impl IntoResponse {
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
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| parse_range(v));

    if let Some((_, _)) = range {
        if matches!(cfg.fail_mode, FailMode::Always416) {
            return (StatusCode::RANGE_NOT_SATISFIABLE, "bad range").into_response();
        }
    }

    let (start, end) = match range {
        Some((a, b)) if cfg.support_range => (a, b.min(cfg.size - 1)),
        _ => (0, cfg.size.saturating_sub(1)),
    };

    if let Some(d) = cfg.slow_first_half {
        if start < cfg.size / 2 {
            tokio::time::sleep(d).await;
        }
    }

    let slice = &data[start as usize..=(end as usize)];
    let mut headers = HeaderMap::new();
    let status = if cfg.support_range && headers_contains_range(&headers) {
        // 下一行不可达占位删除——见下方真实实现说明
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    // ——真实实现见 Step 3，此处测试先行只写语义断言——
    let _ = status;
    let _ = &mut headers;
    (StatusCode::OK, Vec::from(slice)).into_response()
}

fn headers_contains_range(_h: &HeaderMap) -> bool { false }
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
```

**注意**：上面 handler 是示意骨架。Step 3 给出完整正确实现（含 206/Content-Range/ETag/Content-MD5/流式 body/drop_after/掐断），测试只依赖最终语义。

- [ ] **Step 2: 起服务器验证语义（先手动确认现状不足）**

Run: `cargo test -p sparkling-core --test probe_seq`（该测试文件 Task 7 才创建；本任务用下面的冒烟测试）

在 `tests/common/mod.rs` 底部追加：

```rust
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
}
```

Run: `cargo test -p sparkling-core --test common`
Expected: 编译失败（`start` 未实现）

- [ ] **Step 3: 写完整实现**

用以下完整实现替换 `handler`、`headers_contains_range` 及补上 `start`、`set_content_v2`（其余保持）：

```rust
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

    let range = req_headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_range);

    if range.is_some() && matches!(cfg.fail_mode, FailMode::Always416) {
        return (StatusCode::RANGE_NOT_SATISFIABLE, "bad range").into_response();
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
    let md5_of = if matches!(cfg.fail_mode, FailMode::WrongMd5) { content(cfg.size, !v2) } else { slice.clone() };
    use md5::{Digest, Md5};
    let digest = base64::engine::general_purpose::STANDARD
        .encode(Md5::digest(&md5_of));
    resp_headers.insert("content-md5", HeaderValue::from_str(&digest).unwrap());

    // 流式分块（64KiB），drop_after 时中途产生错误模拟掐断
    let drop_after = cfg.drop_after;
    let chunk_size = 64 * 1024usize;
    let mut chunks: Vec<Result<Vec<u8>, std::io::Error>> = Vec::new();
    let mut sent = 0u64;
    for chunk in slice.chunks(chunk_size) {
        if let Some(limit) = drop_after {
            if sent >= limit {
                chunks.push(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe, "dropped",
                )));
                break;
            }
        }
        sent += chunk.len() as u64;
        chunks.push(Ok(chunk.to_vec()));
    }
    // 注意：drop_after 模式下重新计算精确截断——发送到 limit 字节后断
    if let Some(limit) = drop_after {
        let mut sent2 = 0u64;
        let mut bounded: Vec<Result<Vec<u8>, std::io::Error>> = Vec::new();
        for chunk in slice.chunks(chunk_size) {
            let remaining = limit.saturating_sub(sent2);
            if remaining == 0 {
                bounded.push(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe, "dropped",
                )));
                break;
            }
            let take = (remaining as usize).min(chunk.len());
            sent2 += take as u64;
            bounded.push(Ok(chunk[..take].to_vec()));
        }
        if sent2 >= limit {
            bounded.push(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe, "dropped",
            )));
        }
        let body = Body::from_stream(futures::stream::iter(bounded));
        return (status, resp_headers, body).into_response();
    }

    let body = Body::from_stream(futures::stream::iter(chunks));
    (status, resp_headers, body).into_response()
}

pub async fn start(cfg: ServerConfig) -> TestServer {
    let state = Arc::new(ServerState { cfg: cfg.clone(), requests: AtomicU64::new(0), v2: AtomicBool::new(false) });
    let app = Router::new()
        .route("/file.bin", get(handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    TestServer { url: format!("http://{addr}/file.bin"), data: content(cfg.size, false), state }
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
```

同时删掉 Step 1 骨架里的 `headers_contains_range` 与示意 handler（被上面替换），`parse_range` 保留。`Cargo.toml` dev-dependencies 已含 `md-5`（主依赖，测试可用）与 `futures`。

**编译提示**：`handler` 里调用 `.encode()` 前需要 trait 在作用域内——文件顶部加 `use base64::Engine as _;`，md5 摘要用 `use md5::{Digest, Md5};`（`Md5::digest` 同样需要 `Digest` trait）。

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core --test common`
Expected: `server_serves_range_and_full` PASS

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "test(core): 可编程 axum 测试服务器"
```

---

### Task 7: Engine trait + 类型 + HTTP 探测 probe

**Files:**
- Create: `crates/sparkling-core/src/engine.rs`
- Create: `crates/sparkling-core/src/probe.rs`
- Modify: `crates/sparkling-core/src/lib.rs`、`crates/sparkling-core/Cargo.toml`（加 `bytes = "1"`）
- Test: `crates/sparkling-core/tests/probe_seq.rs`

**Interfaces:**
- Consumes: `TaskState/TaskSpec/TaskId`（Task 2）
- Produces:
  - `engine.rs`：`SegmentProgress { index, downloaded, len }`；`ProgressSnapshot { state: TaskState, downloaded, total, speed, segments, error }`；`ControlMsg { Pause, Resume, Cancel }`；`TaskHandle`（`Clone`；`id() -> &str`、`subscribe() -> watch::Receiver<ProgressSnapshot>`、`pause()/resume()/cancel() -> Result<()>`）；`trait Engine: Send + Sync { async fn submit(&self, spec: TaskSpec) -> Result<TaskHandle>; fn set_speed_limit(&self, Option<u64>) {} }`
  - `probe.rs`：`ProbeResult { total: u64, supports_range: bool, filename: String, etag/last_modified/content_md5: Option<String> }`；`pub async fn probe(client: &reqwest::Client, url: &str) -> Result<ProbeResult>`
  - 测试公共：`tests/common/mod.rs` 增加 `pub async fn wait_state(rx: &mut watch::Receiver<ProgressSnapshot>, want: TaskState, timeout: Duration) -> ProgressSnapshot`

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/tests/probe_seq.rs`：

```rust
mod common;

use common::{start, FailMode, ServerConfig};
use sparkling_core::probe::probe;
use std::time::Duration;

fn client() -> reqwest::Client {
    reqwest::Client::builder().build().unwrap()
}

#[tokio::test]
async fn probe_range_server() {
    let server = start(ServerConfig { size: 5000, ..Default::default() }).await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert_eq!(p.total, 5000);
    assert!(p.supports_range);
    assert_eq!(p.filename, "file.bin");
    assert_eq!(p.etag.as_deref(), Some("\"v1\""));
}

#[tokio::test]
async fn probe_no_range_server() {
    let server = start(ServerConfig { size: 5000, support_range: false, ..Default::default() }).await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert_eq!(p.total, 5000);
    assert!(!p.supports_range);
}

#[tokio::test]
async fn probe_disposition_overrides_filename() {
    let server = start(ServerConfig {
        size: 100,
        disposition: Some("报表.zip".into()),
        ..Default::default()
    }).await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert_eq!(p.filename, "报表.zip");
}

#[tokio::test]
async fn probe_http_error() {
    let server = start(ServerConfig { fail_mode: FailMode::Always5xx, ..Default::default() }).await;
    let err = probe(&client(), &server.url).await.unwrap_err();
    assert!(matches!(err, sparkling_core::SparklingError::HttpStatus { status: 500, .. }));
}

#[tokio::test]
async fn probe_content_md5_present() {
    let server = start(ServerConfig { size: 100, ..Default::default() }).await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert!(p.content_md5.is_some());
}
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test probe_seq`
Expected: 编译失败（`probe` 模块不存在）

- [ ] **Step 3: 写实现**

`crates/sparkling-core/src/engine.rs`：

```rust
use crate::task::{TaskId, TaskSpec, TaskState};
use crate::{Result, SparklingError};
use async_trait::async_trait;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentProgress {
    pub index: usize,
    pub downloaded: u64,
    pub len: u64,
}

/// 推送给 UI 的进度快照（引擎内部节流约 250ms 一次）
#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    pub state: TaskState,
    pub downloaded: u64,
    pub total: u64,
    pub speed: u64, // bytes/s
    pub segments: Vec<SegmentProgress>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlMsg {
    Pause,
    Resume,
    Cancel,
}

/// 提交后返回的任务句柄；Clone 后可多方持有（manager、事件转发等）
#[derive(Clone)]
pub struct TaskHandle {
    id: TaskId,
    progress: watch::Receiver<ProgressSnapshot>,
    control: mpsc::UnboundedSender<ControlMsg>,
}

impl TaskHandle {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn subscribe(&self) -> watch::Receiver<ProgressSnapshot> {
        self.progress.clone()
    }
    fn send_control(&self, msg: ControlMsg) -> Result<()> {
        self.control
            .send(msg)
            .map_err(|_| SparklingError::TaskNotFound(self.id.clone()))
    }
    pub fn pause(&self) -> Result<()> {
        self.send_control(ControlMsg::Pause)
    }
    pub fn resume(&self) -> Result<()> {
        self.send_control(ControlMsg::Resume)
    }
    pub fn cancel(&self) -> Result<()> {
        self.send_control(ControlMsg::Cancel)
    }
}

/// 下载引擎抽象 —— ②期 BtEngine、③期 VideoEngine 的接入点。
/// 上层只认识"提交 TaskSpec → TaskHandle（进度流 + 控制面）"。
#[async_trait]
pub trait Engine: Send + Sync {
    async fn submit(&self, spec: TaskSpec) -> Result<TaskHandle>;
    /// 引擎级（全局）限速，默认空实现
    fn set_speed_limit(&self, _limit: Option<u64>) {}
}
```

`crates/sparkling-core/src/probe.rs`：

```rust
use crate::{Result, SparklingError};
use percent_encoding::percent_decode_str;

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub total: u64,
    pub supports_range: bool,
    pub filename: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_md5: Option<String>,
}

/// 探测：GET + Range: bytes=0-0。
/// 206 → 支持 Range（Content-Range 尾段是总大小）；200 → 不支持。
/// 未提供文件大小的服务器暂不支持（已知限制，spec 范围外）。
pub async fn probe(client: &reqwest::Client, url: &str) -> Result<ProbeResult> {
    let resp = client
        .get(url)
        .header("Range", "bytes=0-0")
        .send()
        .await
        .map_err(|e| SparklingError::Network(e.to_string()))?;
    let status = resp.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(SparklingError::HttpStatus { status, detail: format!("探测失败: {url}") });
    }
    let supports_range = status == 206;
    let headers = resp.headers();

    let total = if supports_range {
        let cr = headers
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| SparklingError::Network("206 响应缺少 Content-Range".into()))?;
        cr.rsplit('/')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| SparklingError::Network(format!("Content-Range 无法解析: {cr}")))?
    } else {
        headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| SparklingError::Network("服务器未提供文件大小，暂不支持".into()))?
    };

    let filename = filename_from_headers(headers)
        .unwrap_or_else(|| filename_from_url(url));

    Ok(ProbeResult {
        total,
        supports_range,
        filename,
        etag: header_string(headers, reqwest::header::ETAG),
        last_modified: header_string(headers, reqwest::header::LAST_MODIFIED),
        content_md5: header_string(headers, "content-md5"),
    })
}

fn header_string(headers: &reqwest::header::HeaderMap, name: impl reqwest::header::AsHeaderName) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(|s| s.to_string())
}

/// 解析 Content-Disposition 的 filename= / filename*=UTF-8''
fn filename_from_headers(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let cd = headers.get(reqwest::header::CONTENT_DISPOSITION)?.to_str().ok()?;
    for part in cd.split(';') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("filename*=UTF-8''") {
            return Some(percent_decode_str(v).decode_utf8_lossy().into_owned());
        }
        if let Some(v) = part.strip_prefix("filename=") {
            return Some(v.trim_matches('"').to_string());
        }
    }
    None
}

/// URL 末段做文件名（percent-decode），失败回退 "download"
fn filename_from_url(url: &str) -> String {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let last = path.rsplit('/').next().unwrap_or("");
    let decoded = percent_decode_str(last).decode_utf8_lossy().into_owned();
    if decoded.is_empty() { "download".to_string() } else { decoded }
}
```

`lib.rs` 增加：

```rust
pub mod engine;
pub mod probe;
pub use engine::{ControlMsg, Engine, ProgressSnapshot, SegmentProgress, TaskHandle};
pub use probe::{probe, ProbeResult};
```

`Cargo.toml` `[dependencies]` 增加 `bytes = "1"`。

`tests/common/mod.rs` 底部追加（后续任务复用）：

```rust
use sparkling_core::engine::ProgressSnapshot;
use sparkling_core::task::TaskState;
use tokio::sync::watch;

/// 轮询 watch 通道直到出现目标状态或超时（集成测试核心辅助）
pub async fn wait_state(
    rx: &mut watch::Receiver<ProgressSnapshot>,
    want: TaskState,
    timeout: std::time::Duration,
) -> ProgressSnapshot {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let cur = rx.borrow().clone();
            if cur.state == want {
                return cur;
            }
            if !cur.error.is_none() && want != TaskState::Failed {
                panic!("任务提前失败: {}", cur.error.unwrap());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("等待状态 {want:?} 超时");
        }
        if tokio::time::timeout_at(deadline, rx.changed()).await.is_err() {
            panic!("等待状态 {want:?} 超时（通道关闭）");
        }
    }
}
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core --test probe_seq`
Expected: 5 个 probe 测试 PASS

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): Engine trait 与 HTTP 探测"
```

---

### Task 8: HttpEngine 骨架 + 单线程降级 + 磁盘空间检查

**Files:**
- Create: `crates/sparkling-core/src/http_engine.rs`
- Create: `crates/sparkling-core/src/disk.rs`
- Modify: `crates/sparkling-core/src/lib.rs`
- Test: `crates/sparkling-core/tests/probe_seq.rs`（追加）

**Interfaces:**
- Consumes: `Engine/TaskHandle/ProgressSnapshot`（Task 7）、`TokenBucket`（Task 5）、`control_file`（Task 4）、`probe`（Task 7）
- Produces: `HttpEngine::new(global_rate: Option<u64>) -> Self`；`HttpEngine::new_with_policy(global_rate, RetryPolicy) -> Self`；`pub struct RetryPolicy { max_retries: u32, initial: Duration, max: Duration }`（`Default` = 5 / 1s / 30s；`RetryPolicy::fast()` = 5 / 10ms / 50ms；`backoff(attempt: u32) -> Duration`）；`HttpEngine` 实现 `Engine`（`submit` 立即返回句柄，失败经快照 `state=Failed` 呈现）。内部：`Shared`（worker 共享态）、`WorkerExit { Done, Paused, Cancelled, Failed }`、`write_stream`、`fetch_range`、`finalize`。Task 9/11/12 在同一文件扩展。
- 说明：`submit` 后立即 `subscribe()` 读取快照即可观察终态；引擎 `Drop` 时 abort 所有运行中的下载（测试隔离 + 崩溃模拟依赖此行为）。

- [ ] **Step 1: 写失败测试**

追加到 `crates/sparkling-core/tests/probe_seq.rs`：

```rust
mod engine_tests {
    use crate::common::{sha256_hex, start, wait_state, FailMode, ServerConfig};
    use sparkling_core::engine::Engine;
    use sparkling_core::task::{TaskSpec, TaskState};
    use std::path::PathBuf;
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
        }
    }

    #[tokio::test]
    async fn no_range_downloads_sequentially() {
        let server = start(ServerConfig {
            size: 256 * 1024,
            support_range: false,
            ..Default::default()
        }).await;
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
        let server = start(ServerConfig { size: 0, ..Default::default() }).await;
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
        let server = start(ServerConfig {
            size: 100,
            disposition: Some("报告 q4.zip".into()),
            ..Default::default()
        }).await;
        let dir = tempfile::tempdir().unwrap();
        let e = engine().await;
        let handle = e.submit(spec(server.url.clone(), &dir)).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Completed, Duration::from_secs(10)).await;
        assert!(dir.path().join("报告 q4.zip").exists());
    }

    #[tokio::test]
    async fn user_filename_overrides() {
        let server = start(ServerConfig { size: 100, ..Default::default() }).await;
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
    async fn probe_error_fails_task() {
        let server = start(ServerConfig { fail_mode: FailMode::Always5xx, ..Default::default() }).await;
        let dir = tempfile::tempdir().unwrap();
        let e = engine().await;
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
        assert!(matches!(err, sparkling_core::SparklingError::InsufficientDisk { .. }));
    }

    #[test]
    fn enough_space_ok() {
        let dir = tempfile::tempdir().unwrap();
        assert!(check_space(dir.path(), 0).is_ok());
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test probe_seq`
Expected: 编译失败（`http_engine`、`disk` 模块不存在）

- [ ] **Step 3: 写实现**

`crates/sparkling-core/src/disk.rs`：

```rust
use crate::{Result, SparklingError};
use std::path::Path;

/// 需要的磁盘空间 = 文件大小 × 1.02
pub fn required_space(total: u64) -> u64 {
    total + total / 50
}

pub fn check_space(dir: &Path, required: u64) -> Result<()> {
    let available = fs2::available_space(dir)
        .map_err(|e| SparklingError::DiskWrite(format!("无法查询磁盘剩余空间: {e}")))?;
    if available < required {
        return Err(SparklingError::InsufficientDisk { required, available });
    }
    Ok(())
}
```

`crates/sparkling-core/src/http_engine.rs`：

```rust
use crate::control_file;
use crate::engine::{ControlMsg, Engine, ProgressSnapshot, SegmentProgress, TaskHandle};
use crate::probe::{self, ProbeResult};
use crate::segment::Segment;
use crate::task::{TaskId, TaskSpec, TaskState};
use crate::throttle::TokenBucket;
use crate::{Result, SparklingError};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

const CHUNK_SIZE: usize = 64 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// 分片重试策略：指数退避 initial * 2^(attempt-1)，封顶 max
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial: Duration,
    pub max: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_retries: 5, initial: Duration::from_secs(1), max: Duration::from_secs(30) }
    }
}

impl RetryPolicy {
    /// 测试用：快速退避
    pub fn fast() -> Self {
        Self { max_retries: 5, initial: Duration::from_millis(10), max: Duration::from_millis(50) }
    }
    pub fn backoff(&self, attempt: u32) -> Duration {
        let shift = (attempt - 1).min(30);
        self.initial.saturating_mul(1u32 << shift).min(self.max)
    }
}

/// worker 的退出原因
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerExit {
    Done,     // 我的段（们）全部完成
    Paused,   // 收到暂停标志
    Cancelled,
    Failed,   // 别的 worker 已把任务置为失败
}

/// 一次 run 的终态
enum DownloadEnd {
    Failed(SparklingError),
    Cancelled,
}

/// 所有 worker 共享的任务态
struct Shared {
    url: String,
    filename: String,
    probe: ProbeResult,
    segments: Mutex<Vec<Segment>>,
    downloaded: AtomicU64,
    paused: AtomicBool,
    cancelled: AtomicBool,
    failed: Mutex<Option<SparklingError>>,
    next_index: AtomicUsize,
    finished: AtomicBool,
}

impl Shared {
    fn new(url: String, filename: String, probe: ProbeResult, segments: Vec<Segment>) -> Arc<Self> {
        Arc::new(Self {
            url,
            filename,
            next_index: AtomicUsize::new(segments.len()),
            segments: Mutex::new(segments),
            downloaded: AtomicU64::new(0),
            paused: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            failed: Mutex::new(None),
            probe,
            finished: AtomicBool::new(false),
        })
    }

    fn segment(&self, index: usize) -> Segment {
        self.segments.lock().unwrap().iter().find(|s| s.index == index).cloned().expect("分片存在")
    }

    /// 更新某分片进度并重算总进度。
    /// 关键不变量：总 downloaded = 各分片 downloaded 之和（派生），单段 clamp 到当前 len ——
    /// 偷段会收缩 victim 的 end，在途 worker 的多余写入不会污染总量。
    fn add_progress(&self, index: usize, downloaded: u64, _delta: u64) {
        let sum = {
            let mut g = self.segments.lock().unwrap();
            if let Some(s) = g.iter_mut().find(|s| s.index == index) {
                s.downloaded = downloaded.min(s.len());
            }
            g.iter().map(|s| s.downloaded).sum()
        };
        self.downloaded.store(sum, Ordering::Relaxed);
    }

    /// 共享表中该段当前长度（偷段会实时收缩）
    fn segment_len(&self, index: usize) -> u64 {
        self.segments
            .lock()
            .unwrap()
            .iter()
            .find(|s| s.index == index)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    fn snapshot_segments(&self) -> Vec<SegmentProgress> {
        self.segments
            .lock()
            .unwrap()
            .iter()
            .map(|s| SegmentProgress { index: s.index, downloaded: s.downloaded, len: s.len() })
            .collect()
    }

    fn fail(&self, e: SparklingError) {
        tracing::error!("任务失败: {}", e.technical());
        *self.failed.lock().unwrap() = Some(e);
    }
}

pub struct HttpEngine {
    client: reqwest::Client,
    global_throttle: Arc<TokenBucket>,
    registry: Arc<Mutex<HashMap<TaskId, JoinHandle<()>>>>,
    retry_policy: RetryPolicy,
}

impl HttpEngine {
    pub fn new(global_rate: Option<u64>) -> Self {
        Self::new_with_policy(global_rate, RetryPolicy::default())
    }

    pub fn new_with_policy(global_rate: Option<u64>, retry: RetryPolicy) -> Self {
        Self {
            client: reqwest::Client::new(),
            global_throttle: Arc::new(TokenBucket::new(global_rate)),
            registry: Arc::new(Mutex::new(HashMap::new())),
            retry_policy: retry,
        }
    }

    pub fn set_retry_policy(&self, rp: RetryPolicy) {
        // 通过内部可变性：把字段改为 Mutex 包装（见下方说明）
        unreachable!("用 new_with_policy 构造");
    }
}

impl Drop for HttpEngine {
    fn drop(&mut self) {
        // 引擎销毁 → abort 全部下载（测试隔离、崩溃模拟）
        for (_, h) in self.registry.lock().unwrap().drain() {
            h.abort();
        }
    }
}
```

**注意（实现修正）**：`retry_policy` 无需运行时可变——去掉 `set_retry_policy` 方法（上面保留的 `unreachable!` 版本删除），测试用 `new_with_policy` 注入快速策略即可。

继续同文件（`submit` / `supervise` / `run_download` / worker）：

```rust
#[async_trait]
impl Engine for HttpEngine {
    async fn submit(&self, spec: TaskSpec) -> Result<TaskHandle> {
        let id: TaskId = uuid::Uuid::new_v4().to_string();
        let (progress_tx, progress_rx) = watch::channel(ProgressSnapshot {
            state: TaskState::Running,
            downloaded: 0,
            total: 0,
            speed: 0,
            segments: vec![],
            error: None,
        });
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let join = tokio::spawn(supervise(
            id.clone(),
            self.client.clone(),
            spec,
            self.global_throttle.clone(),
            self.retry_policy.clone(),
            progress_tx,
            control_rx,
            self.registry.clone(),
        ));
        self.registry.lock().unwrap().insert(id.clone(), join);
        Ok(TaskHandle { id, progress: progress_rx, control: control_tx })
    }

    fn set_speed_limit(&self, limit: Option<u64>) {
        self.global_throttle.set_rate(limit);
    }
}

#[allow(clippy::too_many_arguments)]
async fn supervise(
    id: TaskId,
    client: reqwest::Client,
    spec: TaskSpec,
    global: Arc<TokenBucket>,
    retry: RetryPolicy,
    progress_tx: watch::Sender<ProgressSnapshot>,
    mut control_rx: mpsc::UnboundedReceiver<ControlMsg>,
    registry: Arc<Mutex<HashMap<TaskId, JoinHandle<()>>>>,
) {
    let result = run_download(&client, &spec, &global, &retry, &progress_tx, &mut control_rx).await;
    let mut snap = progress_tx.subscribe().borrow().clone();
    match result {
        Ok(()) => {
            snap.state = TaskState::Completed;
            snap.error = None;
        }
        Err(DownloadEnd::Cancelled) => snap.state = TaskState::Cancelled,
        Err(DownloadEnd::Failed(e)) => {
            snap.state = TaskState::Failed;
            snap.error = Some(e.user_message());
        }
    }
    let _ = progress_tx.send(snap);
    registry.lock().unwrap().remove(&id);
}

#[allow(clippy::too_many_arguments)]
async fn run_download(
    client: &reqwest::Client,
    spec: &TaskSpec,
    global: &Arc<TokenBucket>,
    retry: &RetryPolicy,
    progress_tx: &watch::Sender<ProgressSnapshot>,
    control_rx: &mut mpsc::UnboundedReceiver<ControlMsg>, // Task 8 暂不消费，Task 11 接入暂停/恢复/取消
) -> std::result::Result<(), DownloadEnd> {
    // 1. 探测
    let probe = probe::probe(client, &spec.url).await.map_err(DownloadEnd::Failed)?;
    let filename = spec.filename.clone().unwrap_or_else(|| probe.filename.clone());
    let final_path = spec.save_dir.join(&filename);
    let part_path = part_path_for(&final_path);
    let ctl_path = control_file::path_for(&final_path);

    // 2. 磁盘空间
    crate::disk::check_space(&spec.save_dir, crate::disk::required_space(probe.total))
        .map_err(DownloadEnd::Failed)?;

    // 3. 空文件
    if probe.total == 0 {
        std::fs::write(&final_path, b"")
            .map_err(|e| DownloadEnd::Failed(SparklingError::DiskWrite(e.to_string())))?;
        return Ok(());
    }

    // 4. 旧任务残留清理（真正的续传在 Task 11 实现，当前先安全重下）
    if control_file::exists(&ctl_path) {
        let _ = std::fs::remove_file(&ctl_path);
        let _ = std::fs::remove_file(&part_path);
    }

    // 5. 预分配 .part
    std::fs::File::create(&part_path)
        .and_then(|f| f.set_len(probe.total))
        .map_err(|e| DownloadEnd::Failed(SparklingError::DiskWrite(e.to_string())))?;

    let task_throttle = spec.max_speed.map(|r| Arc::new(TokenBucket::new(Some(r))));

    // 6. 分支：不支持 Range → 单线程顺序
    if !probe.supports_range {
        let segments = vec![Segment { index: 0, start: 0, end: probe.total - 1, downloaded: 0 }];
        let shared = Shared::new(spec.url.clone(), filename.clone(), probe, segments);
        let reporter = spawn_reporter(shared.clone(), progress_tx.clone());
        let exit = sequential_worker(client, spec, &shared, &part_path, global, &task_throttle, retry).await;
        shared.finished.store(true, Ordering::Relaxed);
        let _ = reporter.await;
        match exit {
            Ok(WorkerExit::Done) => {
                finalize(&shared, &final_path, &part_path, &ctl_path).map_err(DownloadEnd::Failed)?;
                Ok(())
            }
            Ok(_) => Err(DownloadEnd::Cancelled), // Task 8 中标志只可能因 Drop abort 而中断
            Err(e) => Err(DownloadEnd::Failed(e)),
        }
    } else {
        // 多线程分片路径在 Task 9 实现
        Err(DownloadEnd::Failed(SparklingError::Other("多线程分片下载在 Task 9 实现".into())))
    }
}

fn part_path_for(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_os_string();
    s.push(".sparkling.part");
    PathBuf::from(s)
}

/// 进度上报器：250ms 快照 + 3 秒滑动窗口测速
fn spawn_reporter(
    shared: Arc<Shared>,
    progress_tx: watch::Sender<ProgressSnapshot>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        let mut window: std::collections::VecDeque<(tokio::time::Instant, u64)> =
            std::collections::VecDeque::new();
        loop {
            interval.tick().await;
            if shared.finished.load(Ordering::Relaxed) {
                break;
            }
            let now = tokio::time::Instant::now();
            let dl = shared.downloaded.load(Ordering::Relaxed);
            window.push_back((now, dl));
            while window
                .front()
                .map(|(t, _)| now.duration_since(*t) > Duration::from_secs(3))
                .unwrap_or(false)
            {
                window.pop_front();
            }
            let speed = window
                .front()
                .map(|(t, d)| ((dl.saturating_sub(*d)) as f64 / now.duration_since(*t).as_secs_f64()) as u64)
                .unwrap_or(0);
            let _ = progress_tx.send(ProgressSnapshot {
                state: TaskState::Running,
                downloaded: dl,
                total: shared.probe.total,
                speed,
                segments: shared.snapshot_segments(),
                error: None,
            });
        }
    })
}

/// 单线程顺序 worker（不支持 Range 的服务器；中断只能从头重下）
#[allow(clippy::too_many_arguments)]
async fn sequential_worker(
    client: &reqwest::Client,
    spec: &TaskSpec,
    shared: &Arc<Shared>,
    part_path: &Path,
    global: &Arc<TokenBucket>,
    task: &Option<Arc<TokenBucket>>,
    retry: &RetryPolicy,
) -> std::result::Result<WorkerExit, SparklingError> {
    let mut attempt: u32 = 0;
    loop {
        let seg = shared.segment(0);
        if seg.remaining() == 0 {
            return Ok(WorkerExit::Done);
        }
        let resp = match fetch_range(client, &spec.url, seg.next_offset(), seg.end).await {
            Ok(r) => r,
            Err(e) => {
                attempt += 1;
                if attempt > retry.max_retries {
                    return Err(e);
                }
                tokio::time::sleep(retry.backoff(attempt)).await;
                continue;
            }
        };
        attempt = 0;
        let mut seg = shared.segment(0);
        match write_stream(resp.bytes_stream(), shared, &mut seg, part_path, global, task).await {
            StreamOutcome::Eof => return Ok(WorkerExit::Done),
            StreamOutcome::Flag(exit) => return Ok(exit),
            StreamOutcome::Retry(e) => {
                attempt += 1;
                if attempt > retry.max_retries {
                    return Err(e);
                }
                // 不支持 Range：只能从头重下
                if !shared.probe.supports_range {
                    let waste = shared.downloaded.swap(0, Ordering::Relaxed);
                    let _ = waste;
                    shared.add_progress(0, 0, 0); // 重置分片 downloaded（delta=0）
                    // 注意：add_progress 的 delta 参数为 0，仅同步分片表
                }
                tokio::time::sleep(retry.backoff(attempt)).await;
            }
            StreamOutcome::Fatal(e) => return Err(e),
        }
    }
}

enum StreamOutcome {
    Eof,
    Flag(WorkerExit),
    Retry(SparklingError),
    Fatal(SparklingError),
}

/// 读取响应流并按偏移写入 .part；逐块过限速桶；每块检查暂停/取消/失败标志
#[allow(clippy::too_many_arguments)]
async fn write_stream<S>(
    mut stream: S,
    shared: &Arc<Shared>,
    seg: &mut Segment,
    part_path: &Path,
    global: &Arc<TokenBucket>,
    task: &Option<Arc<TokenBucket>>,
) -> StreamOutcome
where
    S: futures::Stream<Item = reqwest::Result<Bytes>> + Unpin,
{
    let mut file = match std::fs::OpenOptions::new().write(true).open(part_path) {
        Ok(f) => f,
        Err(e) => return StreamOutcome::Fatal(SparklingError::DiskWrite(e.to_string())),
    };
    if let Err(e) = file.seek(SeekFrom::Start(seg.next_offset())) {
        return StreamOutcome::Fatal(SparklingError::DiskWrite(e.to_string()));
    }
    loop {
        if shared.cancelled.load(Ordering::Relaxed) {
            return StreamOutcome::Flag(WorkerExit::Cancelled);
        }
        if shared.failed.lock().unwrap().is_some() {
            return StreamOutcome::Flag(WorkerExit::Failed);
        }
        if shared.paused.load(Ordering::Relaxed) {
            return StreamOutcome::Flag(WorkerExit::Paused);
        }
        // 偷段保护：共享表中该段 end 可能已被收缩，超出部分已归新段——立即停止本段
        if seg.downloaded >= shared.segment_len(seg.index) {
            return StreamOutcome::Eof;
        }
        match tokio::time::timeout(READ_TIMEOUT, stream.next()).await {
            Err(_) => return StreamOutcome::Retry(SparklingError::Network("读取超时".into())),
            Ok(None) => return StreamOutcome::Eof,
            Ok(Some(Err(e))) => {
                return StreamOutcome::Retry(SparklingError::Network(format!("连接中断: {e}")))
            }
            Ok(Some(Ok(chunk))) => {
                if let Some(t) = task {
                    t.acquire(chunk.len() as u64).await;
                }
                global.acquire(chunk.len() as u64).await;
                if let Err(e) = file.write_all(&chunk) {
                    return StreamOutcome::Fatal(SparklingError::DiskWrite(e.to_string()));
                }
                seg.downloaded += chunk.len() as u64;
                shared.add_progress(seg.index, seg.downloaded, chunk.len() as u64);
            }
        }
    }
}

/// GET 一段。end=None 且 start=0 → 不带 Range（全量）；start>0 → 开区间 Range
async fn fetch_range(
    client: &reqwest::Client,
    url: &str,
    start: u64,
    end: Option<u64>,
) -> Result<reqwest::Response> {
    let mut req = client.get(url);
    if let Some(e) = end {
        req = req.header("Range", format!("bytes={start}-{e}"));
    } else if start > 0 {
        req = req.header("Range", format!("bytes={start}-"));
    }
    let resp = req.send().await.map_err(|e| SparklingError::Network(e.to_string()))?;
    let status = resp.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(SparklingError::HttpStatus { status, detail: format!("GET 失败: {url}") });
    }
    Ok(resp)
}

/// 收尾：总量校验 → rename → 清理控制文件（Content-MD5 校验在 Task 12 接入）
fn finalize(
    shared: &Arc<Shared>,
    final_path: &Path,
    part_path: &Path,
    ctl_path: &Path,
) -> Result<()> {
    if shared.downloaded.load(Ordering::Relaxed) != shared.probe.total {
        return Err(SparklingError::Other(format!(
            "下载数量不一致: {} / {}",
            shared.downloaded.load(Ordering::Relaxed),
            shared.probe.total
        )));
    }
    // 同名覆盖：完成同名文件视为用户覆盖意图
    if final_path.exists() {
        std::fs::remove_file(final_path)
            .map_err(|e| SparklingError::DiskWrite(format!("覆盖旧文件失败: {e}")))?;
    }
    std::fs::rename(part_path, final_path)
        .map_err(|e| SparklingError::DiskWrite(format!("重命名失败: {e}")))?;
    let _ = std::fs::remove_file(ctl_path);
    Ok(())
}
```

`lib.rs` 增加：

```rust
pub mod disk;
pub mod http_engine;
pub use http_engine::{HttpEngine, RetryPolicy};
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core --test probe_seq`
Expected: probe 5 个 + engine 5 个 + disk 3 个全部 PASS

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): HttpEngine 骨架、单线程降级与磁盘检查"
```

---

### Task 9: 多线程分片下载 + 控制文件周期落盘

**Files:**
- Modify: `crates/sparkling-core/src/http_engine.rs`（else 分支替换为真实多线程路径；`spawn_reporter` 增加控制文件保存；`Shared` 增加 `build_control_file`）
- Test: `crates/sparkling-core/tests/segments.rs`

**Interfaces:**
- Consumes: `split`（Task 3）、`control_file::save`（Task 4）、Task 8 的 `write_stream/fetch_range/finalize/Shared`
- Produces: `segment_worker`（单段下载循环，Task 10 在其尾部扩展偷段）；`spawn_reporter(shared, progress_tx, ctl: Option<PathBuf>)`（Task 11 复用）；`Shared::build_control_file(&self) -> ControlFile`。运行中的多线程任务每 2 秒在正式文件旁生成 `.sparkling` 控制文件。

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/tests/segments.rs`：

```rust
mod common;

use common::{sha256_hex, start, wait_state, wait_until, ServerConfig};
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
    let handle = engine.submit(spec(server.url.clone(), &dir, None)).await.unwrap();
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
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test segments`
Expected: `multithread_download_completes` FAIL（多线程分支返回 "在 Task 9 实现" 错误，任务 Failed 超时）；`wait_until` 未定义编译失败

- [ ] **Step 3: 写实现**

`tests/common/mod.rs` 追加 `wait_until`：

```rust
pub async fn wait_until(
    rx: &mut watch::Receiver<ProgressSnapshot>,
    pred: impl Fn(&ProgressSnapshot) -> bool,
    timeout: std::time::Duration,
) -> ProgressSnapshot {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        {
            let cur = rx.borrow().clone();
            if pred(&cur) {
                return cur;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("wait_until 超时");
        }
        if tokio::time::timeout_at(deadline, rx.changed()).await.is_err() {
            panic!("wait_until 超时（通道关闭）");
        }
    }
}
```

`http_engine.rs` 修改。

(a) `Shared` 增加方法：

```rust
impl Shared {
    /// 由当前状态构造控制文件内容
    fn build_control_file(&self) -> control_file::ControlFile {
        control_file::ControlFile {
            url: self.url.clone(),
            etag: self.probe.etag.clone(),
            last_modified: self.probe.last_modified.clone(),
            total_size: self.probe.total,
            supports_range: self.probe.supports_range,
            filename: self.filename.clone(),
            segments: self.segments.lock().unwrap().clone(),
        }
    }
}
```

(b) `spawn_reporter` 签名改为 `fn spawn_reporter(shared: Arc<Shared>, progress_tx: watch::Sender<ProgressSnapshot>, ctl: Option<PathBuf>) -> JoinHandle<()>`，循环内加 `tick` 计数，`interval.tick()` 之后：

```rust
let mut tick: u32 = 0;
loop {
    interval.tick().await;
    if shared.finished.load(Ordering::Relaxed) {
        break;
    }
    // ...（原快照/测速/发送逻辑不变）...
    let _ = progress_tx.send(ProgressSnapshot { /* 原样 */ });
    tick += 1;
    if tick % 8 == 0 {
        // 每 2 秒保存控制文件
        if let Some(final_path) = &ctl {
            let _ = control_file::save(final_path, &shared.build_control_file());
        }
    }
}
```

(c) `run_download` 中两处 `spawn_reporter(...)` 调用改为带 ctl 参数（顺序路径传 `None`，多线程路径传 `Some(final_path.clone())`），并把 else 分支整体替换为：

```rust
    } else {
        let segments = crate::segment::split(probe.total, spec.segments);
        let shared = Shared::new(spec.url.clone(), filename.clone(), probe, segments);
        let reporter = spawn_reporter(shared.clone(), progress_tx.clone(), Some(final_path.clone()));
        let n = shared.segments.lock().unwrap().len();
        let mut workers = Vec::with_capacity(n);
        for i in 0..n {
            let seg = shared.segment(i);
            workers.push(tokio::spawn(segment_worker(
                client.clone(),
                spec.clone(),
                shared.clone(),
                seg,
                part_path.clone(),
                global.clone(),
                task_throttle.clone(),
                retry.clone(),
            )));
        }
        let mut failure: Option<SparklingError> = None;
        let mut cancelled = false;
        for w in workers {
            match w.await {
                Ok(Ok(WorkerExit::Done)) => {}
                Ok(Ok(_)) | Err(_) => cancelled = true, // Paused/Cancelled/Failed/abort
                Ok(Err(e)) => {
                    if failure.is_none() {
                        failure = Some(e);
                    }
                }
            }
        }
        shared.finished.store(true, Ordering::Relaxed);
        let _ = reporter.await;
        if let Some(e) = failure {
            // 保留控制文件与 .part：手动重试从分片断点继续（spec）
            return Err(DownloadEnd::Failed(e));
        }
        if cancelled {
            let _ = std::fs::remove_file(&part_path);
            let _ = std::fs::remove_file(&ctl_path);
            return Err(DownloadEnd::Cancelled);
        }
        finalize(&shared, &final_path, &part_path, &ctl_path).map_err(DownloadEnd::Failed)?;
        Ok(())
    }
```

(d) 新增 `segment_worker`：

```rust
/// 分片 worker：下载指派的段直至完成（偷段在 Task 10 加入尾部）
#[allow(clippy::too_many_arguments)]
async fn segment_worker(
    client: reqwest::Client,
    spec: TaskSpec,
    shared: Arc<Shared>,
    start_seg: Segment,
    part_path: PathBuf,
    global: Arc<TokenBucket>,
    task: Option<Arc<TokenBucket>>,
    retry: RetryPolicy,
) -> std::result::Result<WorkerExit, SparklingError> {
    let mut seg = start_seg;
    let mut attempt: u32 = 0;
    loop {
        // 用共享表判断剩余（偷段会收缩本段 end，本地副本可能过期）
        if shared.segment(seg.index).remaining() == 0 {
            return Ok(WorkerExit::Done);
        }
        let resp = match fetch_range(&client, &spec.url, seg.next_offset(), Some(seg.end)).await {
            Ok(r) => r,
            Err(e) => {
                attempt += 1;
                if attempt > retry.max_retries {
                    shared.fail(e.clone());
                    return Err(e);
                }
                tokio::time::sleep(retry.backoff(attempt)).await;
                continue;
            }
        };
        attempt = 0;
        match write_stream(resp.bytes_stream(), &shared, &mut seg, &part_path, &global, &task).await {
            StreamOutcome::Eof => {
                // 用共享表的最新 end 判断（可能已被偷段收缩，本地副本过期）
                if shared.segment(seg.index).remaining() == 0 {
                    return Ok(WorkerExit::Done);
                }
                // 提前 EOF：当作可重试错误
                attempt += 1;
                if attempt > retry.max_retries {
                    let e = SparklingError::Network(format!(
                        "分片 {} 提前结束，剩余 {} 字节",
                        seg.index,
                        seg.remaining()
                    ));
                    shared.fail(e.clone());
                    return Err(e);
                }
                tokio::time::sleep(retry.backoff(attempt)).await;
            }
            StreamOutcome::Flag(exit) => return Ok(exit),
            StreamOutcome::Retry(e) => {
                attempt += 1;
                if attempt > retry.max_retries {
                    shared.fail(e.clone());
                    return Err(e);
                }
                tokio::time::sleep(retry.backoff(attempt)).await;
            }
            StreamOutcome::Fatal(e) => {
                shared.fail(e.clone());
                return Err(e);
            }
        }
    }
}
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core`
Expected: 全部 PASS（新增 2 个）

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): 多线程分片下载与控制文件周期落盘"
```

---

### Task 10: 动态偷段（消除长尾）

**Files:**
- Modify: `crates/sparkling-core/src/http_engine.rs`
- Test: `crates/sparkling-core/tests/segments.rs`（追加）

**Interfaces:**
- Consumes: `take_over`（Task 3）、`segment_worker`（Task 9）
- Produces: `Shared::steal_largest(&self) -> Option<Segment>`（阈值 `STEAL_THRESHOLD = 256 * 1024`：victim 剩余不足则不偷）；worker 完成自己段后进入偷段循环。

- [ ] **Step 1: 写失败测试**

追加到 `tests/segments.rs`：

```rust
#[tokio::test]
async fn stealing_eliminates_tail() {
    // 前半段起始的请求被延迟 400ms：先完成的 worker 应从慢段偷走剩余部分
    let server = start(ServerConfig {
        size: 2 * 1024 * 1024,
        slow_first_half: Some(Duration::from_millis(400)),
        ..Default::default()
    }).await;
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
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test segments stealing`
Expected: FAIL —— `steal_largest` 不存在（编译失败）或断言 `max_segments > 8` 失败（恒为 8）

- [ ] **Step 3: 写实现**

(a) `http_engine.rs` 顶部常量区增加：

```rust
/// 剩余不足此值不偷（避免为几 KB 反复分裂）
const STEAL_THRESHOLD: u64 = 256 * 1024;
```

(b) `impl Shared` 增加：

```rust
    /// 偷段：把剩余最大段的右半切出来给空闲 worker。
    /// 剩余不足阈值（256KiB）时返回 None —— 收尾阶段不值得再切。
    /// 注意：stolen 段必须 push 进共享表 —— 派生总进度、快照、控制文件持久化
    /// 都以共享表为准，漏 push 会让该段的字节从计数里"消失"。
    fn steal_largest(&self) -> Option<Segment> {
        let mut g = self.segments.lock().unwrap();
        let victim = g.iter_mut().max_by_key(|s| s.remaining())?;
        if victim.remaining() < STEAL_THRESHOLD {
            return None;
        }
        let new_index = self.next_index.fetch_add(1, Ordering::Relaxed);
        let stolen = crate::segment::take_over(victim, new_index)?;
        g.push(stolen.clone());
        Some(stolen)
    }
```

(c) `segment_worker` 中两处 `if shared.segment(seg.index).remaining() == 0 { return Ok(WorkerExit::Done); }`（loop 顶部与 `Eof` 分支后）都改为调用偷段循环：

```rust
// segment_worker 内：两处完成点
//     if shared.segment(seg.index).remaining() == 0 { return Ok(WorkerExit::Done); }
// 都替换为：
        if shared.segment(seg.index).remaining() == 0 {
            match shared.steal_largest() {
                Some(stolen) => {
                    seg = stolen;
                    attempt = 0;
                    continue; // 回到外层 loop，下载新段
                }
                None => return Ok(WorkerExit::Done),
            }
        }
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core`
Expected: 全部 PASS（`stealing_eliminates_tail` 观察到 > 8 段且哈希正确）

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): 动态偷段消除长尾"
```

---

### Task 11: 暂停/恢复 + 断点续传 + 崩溃恢复 + ETag 变化重下

**Files:**
- Modify: `crates/sparkling-core/src/http_engine.rs`（`run_download` 重构：恢复逻辑 + `drive_download` 控制循环）
- Test: `crates/sparkling-core/tests/resume.rs`

**Interfaces:**
- Consumes: Task 9/10 的 `segment_worker/sequential_worker/Shared/finalize`、`control_file`（Task 4）
- Produces: `TaskHandle::pause()/resume()/cancel()` 生效；`drive_download(...)`（批次循环 + select 控制面）；辅助 `save_ctl/send_state/check_fatal/first_error`。**恢复语义**：submit 时若控制文件存在且 url/total/ETag 匹配 → 从分片偏移续传；不匹配 → 自动从零重下。**产品决策**：不支持 Range 的任务不提供暂停（无法续传，UI 将禁用），只支持取消。
- 重要修正：恢复时 `Shared.downloaded` 必须初始化为 `sum(segments.downloaded)`，否则 finalize 的总量校验必然失败。

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/tests/resume.rs`：

```rust
mod common;

use common::{sha256_hex, start, wait_state, wait_until, ServerConfig};
use sparkling_core::control_file;
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
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test resume`
Expected: 编译失败或全部 FAIL（暂停/恢复未实现）

- [ ] **Step 3: 写实现**

`http_engine.rs` 修改。

(a) 顶部 `use` 增加：

```rust
use tokio::sync::mpsc::UnboundedReceiver;
```

(b) 新增辅助函数：

```rust
fn save_ctl(shared: &Arc<Shared>, final_path: &Path) {
    let _ = control_file::save(final_path, &shared.build_control_file());
}

fn send_state(progress_tx: &watch::Sender<ProgressSnapshot>, state: TaskState) {
    let mut snap = progress_tx.subscribe().borrow().clone();
    snap.state = state;
    let _ = progress_tx.send(snap);
}

fn first_error(
    outcomes: &[std::result::Result<WorkerExit, SparklingError>],
) -> Option<SparklingError> {
    outcomes.iter().filter_map(|r| r.as_ref().err()).cloned().next()
}

fn check_fatal(
    outcomes: &[std::result::Result<WorkerExit, SparklingError>],
    shared: &Shared,
) -> Option<DownloadEnd> {
    if let Some(e) = first_error(outcomes) {
        return Some(DownloadEnd::Failed(e));
    }
    if let Some(e) = shared.failed.lock().unwrap().clone() {
        return Some(DownloadEnd::Failed(e));
    }
    if shared.cancelled.load(Ordering::Relaxed) {
        return Some(DownloadEnd::Cancelled);
    }
    None
}

/// 校验器（ETag / Last-Modified）记录值与当前值是否兼容。
/// 任一方缺失视为无法判定 → 兼容（按大小兜底）。
fn validator_matches(recorded: &Option<String>, current: &Option<String>) -> bool {
    match (recorded, current) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    }
}
```

(c) `run_download` 的 else（多线程）分支整体替换（顺序分支的修改见 (e)）。替换前先做两处小改动：`DownloadEnd` 枚举（Task 8 定义处）增加变体 `RestartNeeded`；`supervise` 的 match 增加臂 `Err(DownloadEnd::RestartNeeded) => unreachable!("RestartNeeded 由 run_download 内部消化"),`。

```rust
    } else {
        // 恢复或全新（首次构造；RestartNeeded 时会重建）
        let mut shared: Arc<Shared> = if control_file::exists(&ctl_path) {
            match control_file::load(&ctl_path) {
                Ok(cf)
                    if cf.url == spec.url
                        && cf.total_size == probe.total
                        && cf.supports_range
                        && validator_matches(&cf.etag, &probe.etag)
                        && validator_matches(&cf.last_modified, &probe.last_modified) =>
                {
                    let done: u64 = cf.segments.iter().map(|s| s.downloaded).sum();
                    let shared = Shared::new(spec.url.clone(), filename.clone(), probe, cf.segments);
                    // 关键：恢复时初始化总进度计数，否则 finalize 总量校验必挂
                    shared.downloaded.store(done, Ordering::Relaxed);
                    shared
                }
                _ => {
                    // 远端已变化 / 控制文件损坏：清掉从零重下（数据正确性红线）
                    let _ = std::fs::remove_file(&ctl_path);
                    let _ = std::fs::remove_file(&part_path);
                    fresh_shared(spec, filename, probe)
                }
            }
        } else {
            fresh_shared(spec, filename, probe)
        };
        // 恢复场景下 .part 可能缺失/被截：确保存在且大小正确
        {
            let ok = std::fs::metadata(&part_path)
                .map(|m| m.len() == probe.total)
                .unwrap_or(false);
            if !ok {
                std::fs::File::create(&part_path)
                    .and_then(|f| f.set_len(probe.total))
                    .map_err(|e| DownloadEnd::Failed(SparklingError::DiskWrite(e.to_string())))?;
            }
        }
        let mut reporter = spawn_reporter(shared.clone(), progress_tx.clone(), Some(final_path.clone()));
        loop {
            let result = drive_download(
                client.clone(),
                spec.clone(),
                shared.clone(),
                part_path.clone(),
                final_path.clone(),
                global.clone(),
                task_throttle.clone(),
                retry.clone(),
                progress_tx.clone(),
                &mut control_rx,
            )
            .await;
            shared.finished.store(true, Ordering::Relaxed);
            let _ = reporter.await;
            match result {
                Err(DownloadEnd::RestartNeeded) => {
                    // 暂停期间远端已变化：清零重下（数据正确性红线）
                    let _ = std::fs::remove_file(&ctl_path);
                    let _ = std::fs::remove_file(&part_path);
                    let probe2 = probe::probe(client, &spec.url)
                        .await
                        .map_err(DownloadEnd::Failed)?;
                    crate::disk::check_space(&spec.save_dir, crate::disk::required_space(probe2.total))
                        .map_err(DownloadEnd::Failed)?;
                    shared = fresh_shared(spec, filename.clone(), probe2);
                    std::fs::File::create(&part_path)
                        .and_then(|f| f.set_len(shared.probe.total))
                        .map_err(|e| DownloadEnd::Failed(SparklingError::DiskWrite(e.to_string())))?;
                    reporter = spawn_reporter(shared.clone(), progress_tx.clone(), Some(final_path.clone()));
                    continue;
                }
                Ok(()) => {
                    finalize(&shared, &final_path, &part_path, &ctl_path).map_err(DownloadEnd::Failed)?;
                    return Ok(());
                }
                Err(DownloadEnd::Cancelled) => {
                    let _ = std::fs::remove_file(&part_path);
                    let _ = std::fs::remove_file(&ctl_path);
                    return Err(DownloadEnd::Cancelled);
                }
                Err(e) => return Err(e),
            }
        }
    }
```

(d) 新增 `fresh_shared` 与 `drive_download`：

```rust
fn fresh_shared(spec: &TaskSpec, filename: String, probe: ProbeResult) -> Arc<Shared> {
    let segments = crate::segment::split(probe.total, spec.segments);
    Shared::new(spec.url.clone(), filename, probe, segments)
}

/// 批次驱动：每轮把剩余段中最大的 initial_workers 个交给 worker；
/// select 控制面处理暂停/恢复/取消；全批完成或偷段收尾后进入下一轮。
#[allow(clippy::too_many_arguments)]
async fn drive_download(
    client: reqwest::Client,
    spec: TaskSpec,
    shared: Arc<Shared>,
    part_path: PathBuf,
    final_path: PathBuf,
    global: Arc<TokenBucket>,
    task: Option<Arc<TokenBucket>>,
    retry: RetryPolicy,
    progress_tx: watch::Sender<ProgressSnapshot>,
    control_rx: &mut UnboundedReceiver<ControlMsg>,
) -> std::result::Result<(), DownloadEnd> {
    let initial_workers = (spec.segments.max(1) as usize).max(1);
    'batches: loop {
        let batch: Vec<Segment> = {
            let mut g = shared.segments.lock().unwrap();
            g.sort_by(|a, b| b.remaining().cmp(&a.remaining()));
            g.iter().filter(|s| s.remaining() > 0).take(initial_workers).cloned().collect()
        };
        if batch.is_empty() {
            return Ok(()); // 全部完成
        }
        let workers: Vec<_> = batch
            .into_iter()
            .map(|seg| {
                tokio::spawn(segment_worker(
                    client.clone(), spec.clone(), shared.clone(), seg,
                    part_path.clone(), global.clone(), task.clone(), retry.clone(),
                ))
            })
            .collect();
        let mut all = futures::future::join_all(workers);

        let outcomes = loop {
            tokio::select! {
                res = &mut all => break res,
                msg = control_rx.recv() => {
                    match msg {
                        Some(ControlMsg::Pause) => {
                            shared.paused.store(true, Ordering::Relaxed);
                            let outcomes = all.await; // worker 在块边界退出
                            if let Some(end) = check_fatal(&outcomes, &shared) {
                                return Err(end);
                            }
                            save_ctl(&shared, &final_path);
                            send_state(&progress_tx, TaskState::Paused);
                            loop {
                                match control_rx.recv().await {
                                    Some(ControlMsg::Resume) => {
                                        // 数据正确性红线：恢复前重新探测，
                                        // 远端已变化 → RestartNeeded → 整任务从零重下
                                        let p = probe::probe(&client, &spec.url)
                                            .await
                                            .map_err(DownloadEnd::Failed)?;
                                        let unchanged = p.total == shared.probe.total
                                            && validator_matches(&shared.probe.etag, &p.etag)
                                            && validator_matches(
                                                &shared.probe.last_modified,
                                                &p.last_modified,
                                            );
                                        if unchanged {
                                            shared.paused.store(false, Ordering::Relaxed);
                                            continue 'batches; // 重新组批续传
                                        } else {
                                            return Err(DownloadEnd::RestartNeeded);
                                        }
                                    }
                                    Some(ControlMsg::Cancel) | None => {
                                        shared.cancelled.store(true, Ordering::Relaxed);
                                        return Err(DownloadEnd::Cancelled);
                                    }
                                    Some(ControlMsg::Pause) => {}
                                }
                            }
                        }
                        Some(ControlMsg::Cancel) | None => {
                            shared.cancelled.store(true, Ordering::Relaxed);
                            let _ = all.await; // 等 worker 退出，避免与清理竞态
                            return Err(DownloadEnd::Cancelled);
                        }
                        Some(ControlMsg::Resume) => {} // 未暂停时的 Resume：忽略
                    }
                }
            }
        };
        if let Some(end) = check_fatal(&outcomes, &shared) {
            return Err(end);
        }
        // 本批全 Done → 下一轮（偷段阈值以下的尾段在这里收尾）
    }
}
```

(e) `run_download` 的顺序（no-range）分支替换为（支持取消；暂停不支持——无法续传）：

```rust
    if !probe.supports_range {
        let segments = vec![Segment { index: 0, start: 0, end: probe.total - 1, downloaded: 0 }];
        let shared = Shared::new(spec.url.clone(), filename.clone(), probe, segments);
        let reporter = spawn_reporter(shared.clone(), progress_tx.clone(), None);
        // sequential_worker 参数保持引用形态（Task 8 签名）；fut 借用局部变量，
        // select! 挂在 &mut fut 上，不需要 'static
        let mut fut = Box::pin(sequential_worker(
            client, spec, &shared, &part_path,
            global, &task_throttle, retry,
        ));
        let exit = tokio::select! {
            res = &mut fut => res,
            msg = control_rx.recv() => {
                if matches!(msg, Some(ControlMsg::Cancel)) || msg.is_none() {
                    shared.cancelled.store(true, Ordering::Relaxed);
                    let _ = (&mut fut).await;
                    shared.finished.store(true, Ordering::Relaxed);
                    let _ = reporter.await;
                    let _ = std::fs::remove_file(&part_path);
                    let _ = std::fs::remove_file(&ctl_path);
                    return Err(DownloadEnd::Cancelled);
                }
                tracing::warn!("不支持 Range 的任务无法暂停，忽略 Pause");
                (&mut fut).await
            }
        };
        shared.finished.store(true, Ordering::Relaxed);
        let _ = reporter.await;
        match exit {
            Ok(WorkerExit::Done) => {
                finalize(&shared, &final_path, &part_path, &ctl_path).map_err(DownloadEnd::Failed)
            }
            Ok(_) => Err(DownloadEnd::Cancelled),
            Err(e) => Err(DownloadEnd::Failed(e)),
        }
    } else {
```

同时删除 Task 8 中"旧任务残留清理"与"预分配 .part"的旧代码（.part 保证移入 else 分支 (c)；空文件分支保留在探测/磁盘检查之后）。

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core`
Expected: 全部 PASS（新增 5 个 resume 测试）

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): 暂停恢复、断点续传、崩溃恢复与 ETag 变化检测"
```

---

### Task 12: 错误处理路径（重试退避 / 5xx / 掐断 / 416 / Content-MD5）

**Files:**
- Modify: `crates/sparkling-core/src/http_engine.rs`（`with_retry` 包装探测、`finalize` 增加 MD5 校验、416 处理、顺序 worker 修正）
- Modify: `crates/sparkling-core/src/probe.rs`（416 → 退回无 Range 探测）
- Modify: `crates/sparkling-core/tests/common/mod.rs`（`drop_only_first: bool`）
- Test: `crates/sparkling-core/tests/errors.rs`

**Interfaces:**
- Consumes: `RetryPolicy`（Task 8）、`drive_download`（Task 11）
- Produces: `with_retry(retry, f)` 泛型重试包装；`finalize` 的 Content-MD5 校验（不匹配 → `ChecksumMismatch`，**不产出正式文件**，`.part` 保留排查）；probe 收到 416 时退回 plain GET（视为不支持 Range）；`fetch_range` 对 416 → 重置该分片从头下（计入重试）。测试服务器新增 `drop_only_first`（只掐断第一次响应，用于验证 no-range 从头重下）。

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/tests/errors.rs`：

```rust
mod common;

use common::{sha256_hex, start, wait_state, FailMode, ServerConfig};
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
    // 每个响应体发 100KB 后掐断：worker 每次从新偏移续传
    let server = start(ServerConfig {
        size: 1024 * 1024,
        drop_after: Some(100_000),
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
async fn no_range_drop_restarts_from_zero() {
    // 不支持 Range + 第一次响应掐断 → 从头重下并完成
    let server = start(ServerConfig {
        size: 256 * 1024,
        support_range: false,
        drop_after: Some(100_000),
        drop_only_first: true,
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
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test errors`
Expected: 编译失败（`drop_only_first` 字段不存在）或各用例 FAIL

- [ ] **Step 3: 写实现**

(a) `tests/common/mod.rs`：`ServerConfig` 增加字段 `pub drop_only_first: bool`（`Default` 里置 `false`）；`ServerState` 增加 `dropped: AtomicBool`；`handler` 中 drop 分支改为：

```rust
// 在计算 bounded 截断前：
let should_drop = cfg.drop_after.is_some()
    && (!cfg.drop_only_first || !st.dropped.swap(true, Ordering::SeqCst));
let drop_after = if should_drop { cfg.drop_after } else { None };
// 后续截断逻辑用这个本地 drop_after（原 cfg.drop_after 全部替换）
```

(b) `http_engine.rs` 增加重试包装，并让探测走它（`run_download` 第 1 步替换）：

```rust
/// 带重试的执行器（探测等单次请求复用分片重试策略）
async fn with_retry<T, F, Fut>(retry: &RetryPolicy, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                attempt += 1;
                if attempt > retry.max_retries {
                    return Err(e);
                }
                tokio::time::sleep(retry.backoff(attempt)).await;
            }
        }
    }
}

// run_download 内：
let probe = with_retry(retry, || probe::probe(client, &spec.url))
    .await
    .map_err(DownloadEnd::Failed)?;
```

(c) `fetch_range` 对 416 抛出可识别错误；`segment_worker` 捕获后重置分片（在 fetch 的 Err 分支之前插入判断）：

```rust
// fetch_range 中，状态非 2xx 分支前插入：
    if status == 416 {
        return Err(SparklingError::HttpStatus { status, detail: "Range 不可满足，将重置分片".into() });
    }

// segment_worker 中，Err(e) 分支改为：
            Err(e) => {
                // 416：Range 失效（例如服务器端状态漂移）→ 重置该分片从头下
                if matches!(e, SparklingError::HttpStatus { status: 416, .. }) {
                    let waste = seg.downloaded;
                    seg.downloaded = 0;
                    shared.add_progress(seg.index, 0, 0);
                    let _ = waste;
                }
                attempt += 1;
                if attempt > retry.max_retries {
                    shared.fail(e.clone());
                    return Err(e);
                }
                tokio::time::sleep(retry.backoff(attempt)).await;
                continue;
            }
```

(d) `probe.rs`：`probe` 收到 416 时退回 plain GET。把主请求替换为：

```rust
pub async fn probe(client: &reqwest::Client, url: &str) -> Result<ProbeResult> {
    let resp = client
        .get(url)
        .header("Range", "bytes=0-0")
        .send()
        .await
        .map_err(|e| SparklingError::Network(e.to_string()))?;
    // 服务器对 Range=0-0 也回 416：Range 实现有问题 → 退回无 Range 探测
    let resp = if resp.status().as_u16() == 416 {
        client.get(url).send().await.map_err(|e| SparklingError::Network(e.to_string()))?
    } else {
        resp
    };
    // ……（其余逻辑不变）
```

(e) `sequential_worker` 修正：不支持 Range 的服务器一律 plain GET 并从头重下（替换原 `fetch_range(client, &spec.url, seg.next_offset(), seg.end)` 调用与重试分支）：

```rust
        // 不支持 Range：只发 plain GET；中断只能从头
        let resp = if shared.probe.supports_range {
            fetch_range(&client, &spec.url, seg.next_offset(), Some(seg.end)).await
        } else {
            fetch_range(&client, &spec.url, 0, None).await
        };
        // ……Err 重试分支里：
                if !shared.probe.supports_range {
                    // 从头重下：清零进度
                    let done = shared.downloaded.swap(0, Ordering::Relaxed);
                    let _ = done;
                    shared.add_progress(0, 0, 0);
                }
```

(f) `finalize` 增加内容校验（rename 之前插入）：

```rust
    // Content-MD5（若服务器提供）：不匹配则不产出正式文件
    if let Some(expected) = &shared.probe.content_md5 {
        use md5::{Digest, Md5};
        use base64::Engine as _;
        let mut h = Md5::new();
        let mut f = std::fs::File::open(part_path)
            .map_err(|e| SparklingError::DiskWrite(e.to_string()))?;
        let mut buf = vec![0u8; 64 * 1024];
        use std::io::Read;
        loop {
            let n = f.read(&mut buf).map_err(|e| SparklingError::DiskWrite(e.to_string()))?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
        }
        let actual = base64::engine::general_purpose::STANDARD.encode(h.finalize());
        if actual != *expected {
            return Err(SparklingError::ChecksumMismatch {
                expected: expected.clone(),
                actual,
            });
        }
    }
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core`
Expected: 全部 PASS（新增 7 个）

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): 重试退避、416 回退、Content-MD5 校验与 no-range 重下修正"
```

---

### Task 13: TaskStore（SQLite 持久化）

**Files:**
- Create: `crates/sparkling-core/src/store.rs`
- Modify: `crates/sparkling-core/src/lib.rs`
- Test: `crates/sparkling-core/src/store.rs` 内联 `#[cfg(test)]`

**Interfaces:**
- Consumes: `TaskState`（Task 2）
- Produces: `TaskRecord { id, url, state, save_dir, filename: Option<String>, segments, max_speed: Option<u64>, total_size: Option<u64>, downloaded, error: Option<String>, created_at }`（`Serialize`）；`TaskStore::open(path: &Path)` / `TaskStore::open_in_memory()`；方法 `insert(&TaskRecord)`、`get(id: &str) -> Option<TaskRecord>`、`get_all() -> Vec<TaskRecord>`、`update_state(id, state, error: Option<&str>)`、`update_progress(id, downloaded, total)`、`delete(id)`，全部返回 `Result<()>`。Manager（Task 14）依赖。

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/src/store.rs` 底部：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str) -> TaskRecord {
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
        }
    }

    #[test]
    fn insert_get_all_roundtrip() {
        let store = TaskStore::open_in_memory().unwrap();
        store.insert(&rec("t1")).unwrap();
        store.insert(&rec("t2")).unwrap();
        let all = store.get_all().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(store.get("t1").unwrap().unwrap().url, "http://example.com/a.bin");
        assert!(store.get("missing").unwrap().is_none());
    }

    #[test]
    fn update_state_and_progress() {
        let store = TaskStore::open_in_memory().unwrap();
        store.insert(&rec("t1")).unwrap();
        store.update_state("t1", TaskState::Failed, Some("网络错误")).unwrap();
        store.update_progress("t1", 12345, 67890).unwrap();
        let r = store.get("t1").unwrap().unwrap();
        assert_eq!(r.state, TaskState::Failed);
        assert_eq!(r.error.as_deref(), Some("网络错误"));
        assert_eq!(r.downloaded, 12345);
        assert_eq!(r.total_size, Some(67890));
        // 更新不存在的行不报错（幂等）
        store.update_state("nope", TaskState::Queued, None).unwrap();
    }

    #[test]
    fn delete_removes() {
        let store = TaskStore::open_in_memory().unwrap();
        store.insert(&rec("t1")).unwrap();
        store.delete("t1").unwrap();
        assert!(store.get("t1").unwrap().is_none());
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("tasks.db");
        {
            let store = TaskStore::open(&db).unwrap();
            store.insert(&rec("t1")).unwrap();
        }
        let store2 = TaskStore::open(&db).unwrap();
        assert_eq!(store2.get_all().unwrap().len(), 1);
    }
}
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core store`
Expected: 编译失败（`store` 模块不存在）

- [ ] **Step 3: 写实现**

`crates/sparkling-core/src/store.rs`（实现部分）：

```rust
use crate::task::TaskState;
use crate::{Result, SparklingError};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct TaskRecord {
    pub id: String,
    pub url: String,
    pub state: TaskState,
    pub save_dir: String,
    pub filename: Option<String>,
    pub segments: u32,
    pub max_speed: Option<u64>,
    pub total_size: Option<u64>,
    pub downloaded: u64,
    pub error: Option<String>,
    pub created_at: i64,
}

pub struct TaskStore {
    conn: Connection,
}

impl TaskStore {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .map_err(|e| SparklingError::Other(format!("打开数据库失败: {e}")))?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .map_err(|e| SparklingError::Other(format!("创建内存库失败: {e}")))?;
        Self::init(conn)
    }

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
        Ok(Self { conn })
    }

    pub fn insert(&self, r: &TaskRecord) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO tasks (id, url, state, save_dir, filename, segments, max_speed,
                                    total_size, downloaded, error, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    r.id, r.url, r.state.as_str(), r.save_dir, r.filename, r.segments,
                    r.max_speed, r.total_size, r.downloaded, r.error, r.created_at
                ],
            )
            .map_err(|e| SparklingError::Other(format!("插入失败: {e}")))?;
        Ok(())
    }

    fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRecord> {
        Ok(TaskRecord {
            id: row.get(0)?,
            url: row.get(1)?,
            state: TaskState::from_str(&row.get::<_, String>(2)?).unwrap_or(TaskState::Failed),
            save_dir: row.get(3)?,
            filename: row.get(4)?,
            segments: row.get::<_, u32>(5)?,
            max_speed: row.get(6)?,
            total_size: row.get(7)?,
            downloaded: row.get(8)?,
            error: row.get(9)?,
            created_at: row.get(10)?,
        })
    }

    pub fn get(&self, id: &str) -> Result<Option<TaskRecord>> {
        self.conn
            .query_row("SELECT * FROM tasks WHERE id = ?1", params![id], Self::row_to_record)
            .optional()
            .map_err(|e| SparklingError::Other(format!("查询失败: {e}")))
    }

    pub fn get_all(&self) -> Result<Vec<TaskRecord>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM tasks ORDER BY created_at DESC")
            .map_err(|e| SparklingError::Other(format!("查询失败: {e}")))?;
        let rows = stmt
            .query_map([], Self::row_to_record)
            .map_err(|e| SparklingError::Other(format!("查询失败: {e}")))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| SparklingError::Other(format!("读取行失败: {e}")))?);
        }
        Ok(out)
    }

    pub fn update_state(&self, id: &str, state: TaskState, error: Option<&str>) -> Result<()> {
        self.conn
            .execute(
                "UPDATE tasks SET state = ?2, error = ?3 WHERE id = ?1",
                params![id, state.as_str(), error],
            )
            .map_err(|e| SparklingError::Other(format!("更新状态失败: {e}")))?;
        Ok(())
    }

    pub fn update_progress(&self, id: &str, downloaded: u64, total: u64) -> Result<()> {
        self.conn
            .execute(
                "UPDATE tasks SET downloaded = ?2, total_size = ?3 WHERE id = ?1",
                params![id, downloaded, total],
            )
            .map_err(|e| SparklingError::Other(format!("更新进度失败: {e}")))?;
        Ok(())
    }

    pub fn delete(&self, id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM tasks WHERE id = ?1", params![id])
            .map_err(|e| SparklingError::Other(format!("删除失败: {e}")))?;
        Ok(())
    }
}
```

`lib.rs` 增加：

```rust
pub mod store;
pub use store::{TaskRecord, TaskStore};
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core`
Expected: 全部 PASS（新增 4 个）

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): SQLite 任务仓库"
```

---

### Task 14: TaskManager（队列调度 / 置顶 / 自动恢复 / 事件流）

**Files:**
- Create: `crates/sparkling-core/src/manager.rs`
- Modify: `crates/sparkling-core/src/task.rs`（`TaskState` 加 serde derive）、`crates/sparkling-core/src/engine.rs`（trait 加 `shutdown`）、`crates/sparkling-core/src/http_engine.rs`（实现 `shutdown`）、`crates/sparkling-core/src/lib.rs`
- Modify: `crates/sparkling-core/tests/common/mod.rs`（`fail()/relax()` 与 `wait_event_state/poll_until`）
- Test: `crates/sparkling-core/tests/manager.rs`

**Interfaces:**
- Consumes: `Engine/TaskHandle`（Task 7/11）、`TaskStore`（Task 13）、`control_file`（Task 4）
- Produces:
  - `ManagerConfig { max_concurrent: usize, auto_resume_on_start: bool, global_speed_limit: Option<u64>, default_segments: u32 }`（Default = 3 / true / None / 8；Serialize + Deserialize）
  - `TaskEvent { State { id, state, error } | Progress { id, downloaded, total, speed } }`（Serialize）
  - `TaskManager::new(store_path: &Path, engine: Arc<dyn Engine>, config: ManagerConfig) -> Result<Self>`
  - 方法：`subscribe() -> broadcast::Receiver<TaskEvent>`、`config()/set_config()`、`add_task(url: String, opts: AddTaskOptions) -> Result<TaskId>`、`pause_task/resume_task/cancel_task/retry_task/remove_task/move_to_top(&str) -> Result<()>`、`list_tasks() -> Result<Vec<TaskRecord>>`、`recover() -> Result<()>`（重启自动恢复）、`shutdown()`
  - `AddTaskOptions { save_dir: PathBuf, filename: Option<String>, segments: Option<u32>, max_speed: Option<u64> }`
  - `Engine` trait 增加 `fn shutdown(&self) {}`（默认空实现）
- 语义：暂停的任务不占并发位；`resume` 有句柄则直达引擎，无句柄（重启后）走重新排队（引擎检测控制文件续传）；Failed 手动重试 = 重新排队。

- [ ] **Step 1: 写失败测试**

`crates/sparkling-core/tests/manager.rs`：

```rust
mod common;

use common::{poll_until, sha256_hex, start, wait_event_state, ServerConfig};
use sparkling_core::http_engine::{HttpEngine, RetryPolicy};
use sparkling_core::manager::{AddTaskOptions, ManagerConfig, TaskEvent, TaskManager};
use sparkling_core::task::TaskState;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn manager(dir: &tempfile::TempDir, cfg: ManagerConfig) -> TaskManager {
    TaskManager::new(
        &dir.path().join("tasks.db"),
        Arc::new(HttpEngine::new_with_policy(None, RetryPolicy::fast())),
        cfg,
    )
    .unwrap()
}

fn opts(dir: &tempfile::TempDir, max_speed: Option<u64>) -> AddTaskOptions {
    AddTaskOptions {
        save_dir: dir.path().to_path_buf(),
        filename: None,
        segments: Some(4),
        max_speed,
    }
}

#[tokio::test]
async fn add_task_completes_and_persists() {
    let server = start(ServerConfig { size: 256 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig::default());
    let mut rx = m.subscribe();
    let id = m.add_task(server.url.clone(), opts(&dir, None)).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Completed, Duration::from_secs(30)).await;
    assert!(dir.path().join("file.bin").exists());
    let recs = m.list_tasks().unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].state, TaskState::Completed);
    assert_eq!(recs[0].downloaded, 256 * 1024);
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
}

#[tokio::test]
async fn queue_respects_max_concurrent() {
    let server_a = start(ServerConfig { size: 512 * 1024, ..Default::default() }).await;
    let server_b = start(ServerConfig { size: 512 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig { max_concurrent: 1, ..Default::default() });
    let id_a = m.add_task(server_a.url.clone(), opts(&dir, Some(200_000))).unwrap();
    let id_b = m.add_task(server_b.url.clone(), opts(&dir, Some(200_000))).unwrap();
    // B 必须等 A 完成后才 Running；全程不得出现两个 Running
    let mut saw_double_running = false;
    poll_until(Duration::from_secs(60), || {
        let recs = m.list_tasks().unwrap();
        let running = recs.iter().filter(|r| r.state == TaskState::Running).count();
        if running > 1 {
            saw_double_running = true;
        }
        recs.iter().all(|r| r.state == TaskState::Completed)
    })
    .await;
    assert!(!saw_double_running, "不得超过 max_concurrent");
    assert!(dir.path().join("file.bin").exists());
    let _ = (id_a, id_b);
}

#[tokio::test]
async fn pause_resume_cancel_via_manager() {
    let server = start(ServerConfig { size: 2 * 1024 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig::default());
    let mut rx = m.subscribe();
    let id = m.add_task(server.url.clone(), opts(&dir, Some(300_000))).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Running, Duration::from_secs(10)).await;

    m.pause_task(&id).unwrap();
    poll_until(Duration::from_secs(10), || {
        let r = m.list_tasks().unwrap().into_iter().find(|r| r.id == id).unwrap();
        (r.state == TaskState::Paused).then_some(())
    })
    .await;

    m.resume_task(&id).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Completed, Duration::from_secs(60)).await;

    // 取消路径
    let id2 = m.add_task(server.url.clone(), opts(&dir, Some(200_000))).unwrap();
    wait_event_state(&mut rx, &id2, TaskState::Running, Duration::from_secs(10)).await;
    m.cancel_task(&id2).unwrap();
    wait_event_state(&mut rx, &id2, TaskState::Cancelled, Duration::from_secs(10)).await;
    let r = m.list_tasks().unwrap().into_iter().find(|r| r.id == id2).unwrap();
    assert_eq!(r.state, TaskState::Cancelled);
}

#[tokio::test]
async fn retry_failed_continues_from_control_file() {
    let server = start(ServerConfig { size: 512 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig::default());
    let mut rx = m.subscribe();
    let id = m.add_task(server.url.clone(), opts(&dir, Some(400_000))).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Running, Duration::from_secs(10)).await;
    // 中途服务器开始 500 → 任务 Failed，控制文件保留
    server.fail();
    wait_event_state(&mut rx, &id, TaskState::Failed, Duration::from_secs(60)).await;
    let ctl = dir.path().join("file.bin.sparkling");
    assert!(ctl.exists(), "失败后控制文件应保留（断点续传）");

    // 恢复服务器 + 手动重试 → 从分片断点继续完成
    server.relax();
    m.retry_task(&id).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Completed, Duration::from_secs(60)).await;
    let file = std::fs::read(dir.path().join("file.bin")).unwrap();
    assert_eq!(sha256_hex(&file), sha256_hex(&server.data));
}

#[tokio::test]
async fn recovery_auto_resumes() {
    let server = start(ServerConfig { size: 1 * 1024 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    {
        let m = manager(&dir, ManagerConfig::default());
        let id = m.add_task(server.url.clone(), opts(&dir, Some(200_000))).unwrap();
        let _ = id;
        poll_until(Duration::from_secs(20), || {
            let r = m.list_tasks().unwrap().into_iter().next().unwrap();
            (r.downloaded > 100_000).then_some(r.downloaded)
        })
        .await;
        m.shutdown(); // 模拟应用退出
    }
    assert!(dir.path().join("file.bin.sparkling").exists());

    // 重启：同一数据库 + 新引擎 → 自动恢复
    let m2 = TaskManager::new(
        &dir.path().join("tasks.db"),
        Arc::new(HttpEngine::new_with_policy(None, RetryPolicy::fast())),
        ManagerConfig::default(),
    )
    .unwrap();
    let mut rx = m2.subscribe();
    m2.recover().unwrap();
    wait_event_state(&mut rx, "重启后应有任务恢复", TaskState::Completed, Duration::from_secs(60)).await_err_or_any();
}

#[tokio::test]
async fn recovery_corrupt_ctl_marks_failed() {
    let server = start(ServerConfig { size: 512 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig::default());
    let _id = m.add_task(server.url.clone(), opts(&dir, Some(200_000))).unwrap();
    poll_until(Duration::from_secs(20), || {
        let r = m.list_tasks().unwrap().into_iter().next().unwrap();
        (r.downloaded > 50_000).then_some(())
    })
    .await;
    m.shutdown();
    // 破坏控制文件
    std::fs::write(dir.path().join("file.bin.sparkling"), b"broken!!!").unwrap();

    let m2 = manager(&dir, ManagerConfig::default());
    m2.recover().unwrap();
    let recs = m2.list_tasks().unwrap();
    assert_eq!(recs[0].state, TaskState::Failed);
    assert!(recs[0].error.as_deref().unwrap().contains("控制文件"));
}

#[tokio::test]
async fn recovery_disabled_stays_paused_then_manual_resume() {
    let server = start(ServerConfig { size: 512 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    {
        let m = manager(&dir, ManagerConfig::default());
        let _id = m.add_task(server.url.clone(), opts(&dir, Some(200_000))).unwrap();
        poll_until(Duration::from_secs(20), || {
            let r = m.list_tasks().unwrap().into_iter().next().unwrap();
            (r.downloaded > 50_000).then_some(())
        })
        .await;
        m.shutdown();
    }
    let m2 = manager(&dir, ManagerConfig { auto_resume_on_start: false, ..Default::default() });
    m2.recover().unwrap();
    let recs = m2.list_tasks().unwrap();
    assert_eq!(recs[0].state, TaskState::Paused);

    let mut rx = m2.subscribe();
    let id = recs[0].id.clone();
    m2.resume_task(&id).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Completed, Duration::from_secs(60)).await;
}

#[tokio::test]
async fn move_to_top_reorders_queue() {
    let server_a = start(ServerConfig { size: 512 * 1024, ..Default::default() }).await;
    let server_b = start(ServerConfig { size: 512 * 1024, ..Default::default() }).await;
    let server_c = start(ServerConfig { size: 512 * 1024, ..Default::default() }).await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig { max_concurrent: 1, ..Default::default() });
    let mut rx = m.subscribe();
    let id_a = m.add_task(server_a.url.clone(), opts(&dir, Some(300_000))).unwrap();
    let id_b = m.add_task(server_b.url.clone(), opts(&dir, Some(300_000))).unwrap();
    let id_c = m.add_task(server_c.url.clone(), opts(&dir, Some(300_000))).unwrap();
    m.move_to_top(&id_c).unwrap();
    wait_event_state(&mut rx, &id_a, TaskState::Completed, Duration::from_secs(60)).await;
    // A 完成后下一个运行的应是 C（被置顶）
    let next_running = poll_until(Duration::from_secs(30), || {
        let recs = m.list_tasks().unwrap();
        recs.iter().find(|r| r.state == TaskState::Running).map(|r| r.id.clone())
    })
    .await;
    assert_eq!(next_running, id_c);
    wait_event_state(&mut rx, &id_c, TaskState::Completed, Duration::from_secs(60)).await;
    wait_event_state(&mut rx, &id_b, TaskState::Completed, Duration::from_secs(60)).await;
}
```

**注意**：`recovery_auto_resumes` 里 `wait_event_state(...).await_err_or_any()` 是示意——实际写法：

```rust
    // 重启后只有一个任务，直接等待任意 Completed 事件
    let ev = loop {
        let ev = tokio::time::timeout(Duration::from_secs(60), rx.recv())
            .await
            .expect("等待恢复完成超时")
            .expect("事件通道关闭");
        if let TaskEvent::State { state: TaskState::Completed, .. } = &ev {
            break ev;
        }
    };
    let _ = ev;
```

- [ ] **Step 2: 运行验证失败**

Run: `cargo test -p sparkling-core --test manager`
Expected: 编译失败（`manager` 模块不存在）

- [ ] **Step 3: 写实现**

(a) `task.rs`：`TaskState` 加 `serde::Serialize, serde::Deserialize` derive（`as_str/from_str` 保留）。

(b) `engine.rs`：`Engine` trait 增加：

```rust
    /// 关停引擎：abort 所有运行中的下载（应用退出时调用）
    fn shutdown(&self) {}
```

(c) `http_engine.rs`：`impl HttpEngine` 增加：

```rust
    pub fn shutdown(&self) {
        for (_, h) in self.registry.lock().unwrap().drain() {
            h.abort();
        }
    }
```

并在 `impl Engine for HttpEngine` 中加 `fn shutdown(&self) { HttpEngine::shutdown(self) }`。

(d) `tests/common/mod.rs`：

- `ServerState` 增加 `force_fail: AtomicBool`（`start` 里初始化 false）；`handler` 在 fail 判定处加 `|| st.force_fail.load(Ordering::SeqCst)`。
- `TestServer` 增加方法：

```rust
    /// 让服务器立刻开始对所有请求返回 500
    pub fn fail(&self) {
        self.state.force_fail.store(true, Ordering::SeqCst);
    }
    /// 解除 fail 状态
    pub fn relax(&self) {
        self.state.force_fail.store(false, Ordering::SeqCst);
    }
```

- 追加辅助：

```rust
use sparkling_core::manager::TaskEvent;
use tokio::sync::broadcast;

pub async fn wait_event_state(
    rx: &mut broadcast::Receiver<TaskEvent>,
    id: &str,
    want: TaskState,
    timeout: std::time::Duration,
) -> TaskEvent {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let ev = match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Ok(ev)) => ev,
            Ok(Err(_)) => panic!("事件通道关闭"),
            Err(_) => panic!("等待事件状态 {want:?} 超时（id={id}）"),
        };
        if let TaskEvent::State { id: eid, state, .. } = &ev {
            if eid == id && *state == want {
                return ev;
            }
        }
    }
}

pub async fn poll_until<T>(timeout: std::time::Duration, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return v;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("poll_until 超时");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
```

(e) `crates/sparkling-core/src/manager.rs`：

```rust
use crate::control_file;
use crate::engine::{Engine, TaskHandle};
use crate::store::{TaskRecord, TaskStore};
use crate::task::{TaskId, TaskSpec, TaskState};
use crate::{Result, SparklingError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagerConfig {
    pub max_concurrent: usize,
    pub auto_resume_on_start: bool,
    pub global_speed_limit: Option<u64>,
    pub default_segments: u32,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 3,
            auto_resume_on_start: true,
            global_speed_limit: None,
            default_segments: 8,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind")]
pub enum TaskEvent {
    State { id: TaskId, state: TaskState, error: Option<String> },
    Progress { id: TaskId, downloaded: u64, total: u64, speed: u64 },
}

#[derive(Debug, Clone)]
pub struct AddTaskOptions {
    pub save_dir: PathBuf,
    pub filename: Option<String>,
    pub segments: Option<u32>,
    pub max_speed: Option<u64>,
}

#[derive(Clone)]
struct Inner {
    engine: Arc<dyn Engine>,
    store: Arc<Mutex<TaskStore>>,
    config: Arc<Mutex<ManagerConfig>>,
    handles: Arc<Mutex<HashMap<TaskId, TaskHandle>>>,
    queue: Arc<Mutex<VecDeque<TaskId>>>,
    events: broadcast::Sender<TaskEvent>,
    /// 运行中（不含暂停）任务数
    active: Arc<AtomicUsize>,
    shutting_down: Arc<AtomicBool>,
}

pub struct TaskManager {
    inner: Arc<Inner>,
}

impl TaskManager {
    pub fn new(store_path: &std::path::Path, engine: Arc<dyn Engine>, config: ManagerConfig) -> Result<Self> {
        let store = TaskStore::open(store_path)?;
        let (events, _) = broadcast::channel(1024);
        Ok(Self {
            inner: Arc::new(Inner {
                engine,
                store: Arc::new(Mutex::new(store)),
                config: Arc::new(Mutex::new(config)),
                handles: Arc::new(Mutex::new(HashMap::new())),
                queue: Arc::new(Mutex::new(VecDeque::new())),
                events,
                active: Arc::new(AtomicUsize::new(0)),
                shutting_down: Arc::new(AtomicBool::new(false)),
            }),
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.inner.events.subscribe()
    }

    pub fn config(&self) -> ManagerConfig {
        self.inner.config.lock().unwrap().clone()
    }

    pub fn set_config(&self, cfg: ManagerConfig) {
        *self.inner.config.lock().unwrap() = cfg;
        self.inner.engine.set_speed_limit(self.inner.config.lock().unwrap().global_speed_limit);
    }

    pub fn add_task(&self, url: String, opts: AddTaskOptions) -> Result<TaskId> {
        let id = uuid::Uuid::new_v4().to_string();
        let segments = opts
            .segments
            .unwrap_or(self.config().default_segments)
            .clamp(1, 64);
        let rec = TaskRecord {
            id: id.clone(),
            url,
            state: TaskState::Queued,
            save_dir: opts.save_dir.display().to_string(),
            filename: opts.filename.clone(),
            segments,
            max_speed: opts.max_speed,
            total_size: None,
            downloaded: 0,
            error: None,
            created_at: now_unix(),
        };
        self.inner.store.lock().unwrap().insert(&rec)?;
        self.inner.queue.lock().unwrap().push_back(id.clone());
        self.emit_state(&id, TaskState::Queued, None);
        try_schedule(&self.inner);
        Ok(id)
    }

    pub fn pause_task(&self, id: &str) -> Result<()> {
        let h = self.handle(id)?;
        h.pause()
    }

    /// 有句柄（引擎里有该任务）→ 直达；无句柄（重启恢复的 Paused）→ 重新排队
    pub fn resume_task(&self, id: &str) -> Result<()> {
        if let Some(h) = self.inner.handles.lock().unwrap().get(id).cloned() {
            h.resume()
        } else {
            self.retry_task(id)
        }
    }

    pub fn cancel_task(&self, id: &str) -> Result<()> {
        if let Some(h) = self.inner.handles.lock().unwrap().get(id).cloned() {
            h.cancel()?;
        }
        Ok(())
    }

    /// Failed/Paused → Queued，重新调度（引擎检测控制文件从断点续传）
    pub fn retry_task(&self, id: &str) -> Result<()> {
        self.inner.store.lock().unwrap().update_state(id, TaskState::Queued, None)?;
        self.inner.queue.lock().unwrap().push_back(id.to_string());
        self.emit_state(id, TaskState::Queued, None);
        try_schedule(&self.inner);
        Ok(())
    }

    pub fn remove_task(&self, id: &str) -> Result<()> {
        if let Some(h) = self.inner.handles.lock().unwrap().get(id).cloned() {
            let _ = h.cancel();
        }
        self.inner.queue.lock().unwrap().retain(|q| q != id);
        self.inner.store.lock().unwrap().delete(id)?;
        Ok(())
    }

    pub fn move_to_top(&self, id: &str) -> Result<()> {
        let mut q = self.inner.queue.lock().unwrap();
        q.retain(|q| q != id);
        q.push_front(id.to_string());
        Ok(())
    }

    pub fn list_tasks(&self) -> Result<Vec<TaskRecord>> {
        self.inner.store.lock().unwrap().get_all()
    }

    /// 重启恢复：Queued 直接入队；Running/Paused 校验控制文件后
    /// 按配置自动恢复（重新排队，引擎续传）或保持 Paused；损坏 → Failed。
    pub fn recover(&self) -> Result<()> {
        let cfg = self.config();
        let recs = self.inner.store.lock().unwrap().get_all()?;
        let mut to_resume = Vec::new();
        for rec in recs {
            match rec.state {
                TaskState::Queued => {
                    self.inner.queue.lock().unwrap().push_back(rec.id.clone());
                }
                TaskState::Running | TaskState::Paused => {
                    let ctl_ok = rec.filename.as_ref().and_then(|f| {
                        let p = control_file::path_for(&PathBuf::from(&rec.save_dir).join(f));
                        control_file::exists(&p).then_some(p)
                    });
                    match ctl_ok.map(|p| control_file::load(&p)) {
                        Some(Ok(_)) if cfg.auto_resume_on_start => to_resume.push(rec.id.clone()),
                        Some(Ok(_)) => {
                            // 不自动恢复：保持 Paused（无句柄，resume 时重新排队）
                            self.inner.store.lock().unwrap().update_state(&rec.id, TaskState::Paused, None)?;
                            self.emit_state(&rec.id, TaskState::Paused, None);
                        }
                        _ => {
                            let msg = "控制文件缺失或损坏，请重试";
                            self.inner.store.lock().unwrap().update_state(&rec.id, TaskState::Failed, Some(msg))?;
                            self.emit_state(&rec.id, TaskState::Failed, Some(msg.to_string()));
                        }
                    }
                }
                _ => {}
            }
        }
        for id in to_resume {
            self.retry_task(&id)?;
        }
        try_schedule(&self.inner);
        Ok(())
    }

    /// 应用退出：关停引擎、清理句柄
    pub fn shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::SeqCst);
        self.inner.engine.shutdown();
        self.inner.handles.lock().unwrap().clear();
        self.inner.queue.lock().unwrap().clear();
    }

    fn handle(&self, id: &str) -> Result<TaskHandle> {
        self.inner
            .handles
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| SparklingError::TaskNotFound(id.to_string()))
    }

    fn emit_state(&self, id: &str, state: TaskState, error: Option<String>) {
        let _ = self.inner.events.send(TaskEvent::State { id: id.to_string(), state, error });
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 调度：把队列头的任务提交给引擎，直到占满并发位
fn try_schedule(inner: &Arc<Inner>) {
    loop {
        if inner.shutting_down.load(Ordering::SeqCst) {
            return;
        }
        let max = inner.config.lock().unwrap().max_concurrent;
        if inner.active.load(Ordering::SeqCst) >= max {
            return;
        }
        let id = inner.queue.lock().unwrap().pop_front();
        let Some(id) = id else { return };
        let Some(rec) = inner.store.lock().unwrap().get(&id).ok().flatten() else { continue };
        if matches!(rec.state, TaskState::Completed | TaskState::Cancelled) {
            continue;
        }
        let spec = TaskSpec {
            url: rec.url.clone(),
            save_dir: PathBuf::from(&rec.save_dir),
            filename: rec.filename.clone(),
            segments: rec.segments,
            max_speed: rec.max_speed,
        };
        inner.active.fetch_add(1, Ordering::SeqCst);
        let inner2 = inner.clone();
        tokio::spawn(async move {
            match inner2.engine.submit(spec).await {
                Ok(handle) => {
                    inner2.handles.lock().unwrap().insert(id.clone(), handle.clone());
                    monitor_task(inner2, id, handle).await;
                }
                Err(e) => {
                    inner2.active.fetch_sub(1, Ordering::SeqCst);
                    inner2.store.lock().unwrap().update_state(&id, TaskState::Failed, Some(&e.user_message())).ok();
                    let _ = inner2.events.send(TaskEvent::State {
                        id,
                        state: TaskState::Failed,
                        error: Some(e.user_message()),
                    });
                    try_schedule(&inner2);
                }
            }
        });
    }
}

/// 监控一个运行中的任务：进度写库 + 事件广播 + 终态调度下一个
async fn monitor_task(inner: Arc<Inner>, id: TaskId, handle: TaskHandle) {
    inner.store.lock().unwrap().update_state(&id, TaskState::Running, None).ok();
    let _ = inner.events.send(TaskEvent::State { id: id.clone(), state: TaskState::Running, error: None });
    let mut rx = handle.subscribe();
    let mut prev = TaskState::Running;
    loop {
        if rx.changed().await.is_err() {
            break;
        }
        let snap = rx.borrow().clone();
        inner.store.lock().unwrap().update_progress(&id, snap.downloaded, snap.total).ok();
        let _ = inner.events.send(TaskEvent::Progress {
            id: id.clone(),
            downloaded: snap.downloaded,
            total: snap.total,
            speed: snap.speed,
        });
        if snap.state != prev {
            let was_running = prev == TaskState::Running;
            prev = snap.state;
            inner.store.lock().unwrap().update_state(&id, snap.state, snap.error.as_deref()).ok();
            let _ = inner.events.send(TaskEvent::State {
                id: id.clone(),
                state: snap.state,
                error: snap.error.clone(),
            });
            match snap.state {
                TaskState::Paused => {
                    if was_running {
                        inner.active.fetch_sub(1, Ordering::SeqCst);
                        try_schedule(&inner); // 暂停让出并发位
                    }
                }
                TaskState::Running => {
                    if !was_running {
                        inner.active.fetch_add(1, Ordering::SeqCst);
                    }
                }
                TaskState::Completed | TaskState::Failed | TaskState::Cancelled => {
                    if was_running {
                        inner.active.fetch_sub(1, Ordering::SeqCst);
                    }
                    inner.handles.lock().unwrap().remove(&id);
                    try_schedule(&inner);
                    break;
                }
                _ => {}
            }
        }
    }
}
```

`lib.rs` 增加：

```rust
pub mod manager;
pub use manager::{AddTaskOptions, ManagerConfig, TaskEvent, TaskManager};
```

- [ ] **Step 4: 运行验证通过**

Run: `cargo test -p sparkling-core`
Expected: 全部 PASS（新增 8 个 manager 测试）

- [ ] **Step 5: 提交**

```bash
git add crates/sparkling-core
git commit -m "feat(core): TaskManager 队列调度与重启自动恢复"
```

---

### Task 15: Tauri 2 壳（commands + 事件转发）

**Files:**
- Create: `src-tauri/Cargo.toml`、`src-tauri/build.rs`、`src-tauri/tauri.conf.json`、`src-tauri/src/main.rs`、`src-tauri/src/lib.rs`
- Create: `dist/index.html`（占位，Task 16 由前端构建替换）
- Create: `.gitignore`
- Modify: `Cargo.toml`（workspace members 加 `"src-tauri"`）

**Interfaces:**
- Consumes: `TaskManager/TaskEvent/ManagerConfig`（Task 14）、`TaskRecord`（Task 13）
- Produces: Tauri commands：`add_task(url, filename, segments) -> String`、`pause_task/resume_task/cancel_task/retry_task/remove_task/move_to_top(id)`、`list_tasks() -> Vec<TaskRecord>`、`get_config() -> ManagerConfig`、`update_config(cfg)`；事件 `task-event`（payload = `TaskEvent` JSON）。配置持久化到应用配置目录 `settings.json`，任务库 `tasks.db` 同目录。默认保存目录 = 系统下载目录。

- [ ] **Step 1: 写配置与壳文件**

`Cargo.toml`（根，members 替换）：

```toml
[workspace]
resolver = "2"
members = ["crates/sparkling-core", "src-tauri"]

[workspace.package]
edition = "2021"
```

`src-tauri/Cargo.toml`：

```toml
[package]
name = "sparkling"
version = "0.1.0"
edition = "2021"
description = "Sparkling 下载器"

[lib]
name = "sparkling_lib"
crate-type = ["staticlib", "cdylib", "rlib"]

[build-dependencies]
tauri-build = { version = "2", features = [] }

[dependencies]
tauri = { version = "2", features = [] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sparkling-core = { path = "../crates/sparkling-core" }
```

`src-tauri/build.rs`：

```rust
fn main() {
    tauri_build::build()
}
```

`src-tauri/tauri.conf.json`：

```json
{
  "$schema": "https://schema.tauri.app/config/2",
  "productName": "Sparkling",
  "version": "0.1.0",
  "identifier": "com.sparkling.downloader",
  "build": {
    "beforeDevCommand": "npm run dev",
    "devUrl": "http://localhost:5173",
    "beforeBuildCommand": "npm run build",
    "frontendDist": "../dist"
  },
  "app": {
    "windows": [
      { "title": "Sparkling", "width": 1000, "height": 680, "minWidth": 760, "minHeight": 480 }
    ],
    "security": { "csp": null }
  },
  "bundle": { "active": false }
}
```

（`bundle.active: false`：安装包/图标/自动更新属④期。）

`dist/index.html`（占位）：

```html
<!doctype html>
<html lang="zh-CN">
  <head><meta charset="UTF-8" /><title>Sparkling</title></head>
  <body>占位页面（Task 16 替换为 React 构建产物）</body>
</html>
```

`.gitignore`：

```
/target
node_modules/
dist/*
!dist/index.html
*.sparkling
*.sparkling.part
tasks.db
settings.json
```

- [ ] **Step 2: 写 Rust 壳**

`src-tauri/src/main.rs`：

```rust
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    sparkling_lib::run()
}
```

`src-tauri/src/lib.rs`：

```rust
use sparkling_core::http_engine::HttpEngine;
use sparkling_core::manager::{AddTaskOptions, ManagerConfig, TaskManager};
use sparkling_core::store::TaskRecord;
use std::path::PathBuf;
use std::sync::Arc;
use tauri::{AppHandle, Emitter, Manager, State};

pub struct AppState {
    pub manager: TaskManager,
    pub config_path: PathBuf,
    pub default_save_dir: PathBuf,
}

fn load_or_default_config(path: &std::path::Path) -> ManagerConfig {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn persist_config(path: &std::path::Path, cfg: &ManagerConfig) -> Result<(), String> {
    serde_json::to_string_pretty(cfg)
        .map_err(|e| e.to_string())
        .and_then(|s| std::fs::write(path, s).map_err(|e| e.to_string()))
}

#[tauri::command]
fn add_task(
    state: State<AppState>,
    url: String,
    filename: Option<String>,
    segments: Option<u32>,
) -> Result<String, String> {
    let opts = AddTaskOptions {
        save_dir: state.default_save_dir.clone(),
        filename,
        segments,
        max_speed: None,
    };
    state.manager.add_task(url, opts).map_err(|e| e.user_message())
}

#[tauri::command]
fn pause_task(state: State<AppState>, id: String) -> Result<(), String> {
    state.manager.pause_task(&id).map_err(|e| e.user_message())
}

#[tauri::command]
fn resume_task(state: State<AppState>, id: String) -> Result<(), String> {
    state.manager.resume_task(&id).map_err(|e| e.user_message())
}

#[tauri::command]
fn cancel_task(state: State<AppState>, id: String) -> Result<(), String> {
    state.manager.cancel_task(&id).map_err(|e| e.user_message())
}

#[tauri::command]
fn retry_task(state: State<AppState>, id: String) -> Result<(), String> {
    state.manager.retry_task(&id).map_err(|e| e.user_message())
}

#[tauri::command]
fn remove_task(state: State<AppState>, id: String) -> Result<(), String> {
    state.manager.remove_task(&id).map_err(|e| e.user_message())
}

#[tauri::command]
fn move_to_top(state: State<AppState>, id: String) -> Result<(), String> {
    state.manager.move_to_top(&id).map_err(|e| e.user_message())
}

#[tauri::command]
fn list_tasks(state: State<AppState>) -> Result<Vec<TaskRecord>, String> {
    state.manager.list_tasks().map_err(|e| e.user_message())
}

#[tauri::command]
fn get_config(state: State<AppState>) -> ManagerConfig {
    state.manager.config()
}

#[tauri::command]
fn update_config(state: State<AppState>, cfg: ManagerConfig) -> Result<(), String> {
    state.manager.set_config(cfg.clone());
    persist_config(&state.config_path, &cfg)
}

pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            let config_dir = app
                .path()
                .app_config_dir()
                .unwrap_or_else(|_| PathBuf::from("."));
            std::fs::create_dir_all(&config_dir).ok();
            let config_path = config_dir.join("settings.json");
            let cfg = load_or_default_config(&config_path);
            let default_save_dir = app
                .path()
                .download_dir()
                .unwrap_or_else(|_| PathBuf::from("."));
            let engine: Arc<dyn sparkling_core::engine::Engine> =
                Arc::new(HttpEngine::new(cfg.global_speed_limit));
            let manager = TaskManager::new(&config_dir.join("tasks.db"), engine, cfg)
                .expect("初始化任务管理器失败");
            app.manage(AppState { manager, config_path, default_save_dir });

            // 恢复上次未完成任务（默认自动续传）
            {
                let state: State<AppState> = app.state();
                state.manager.recover().ok();
            }

            // 事件转发：core broadcast → 前端 listen("task-event")
            let handle: AppHandle = app.handle().clone();
            let state: State<AppState> = app.state();
            let mut rx = state.manager.subscribe();
            tauri::async_runtime::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(ev) => {
                            let _ = handle.emit("task-event", &ev);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            add_task, pause_task, resume_task, cancel_task, retry_task,
            remove_task, move_to_top, list_tasks, get_config, update_config
        ])
        .run(tauri::generate_context!())
        .expect("运行 Sparkling 失败");
}
```

- [ ] **Step 3: 编译验证**

Run: `cargo build -p sparkling`
Expected: 编译通过（占位 dist/index.html 已就位）

- [ ] **Step 4: 提交**

```bash
git add Cargo.toml Cargo.lock src-tauri dist .gitignore
git commit -m "feat(app): Tauri 2 壳与 command/event 桥接"
```

---

### Task 16: React + TypeScript 前端

**Files:**
- Create: `package.json`、`vite.config.ts`、`tsconfig.json`、`index.html`、`src/main.tsx`、`src/types.ts`、`src/api.ts`、`src/App.tsx`、`src/App.css`、`src/components/AddTaskDialog.tsx`、`src/components/TaskList.tsx`、`src/components/TaskRow.tsx`、`src/components/SettingsModal.tsx`
- Modify: `dist/*`（构建产物替换占位）

**Interfaces:**
- Consumes: Task 15 的 commands 与 `task-event` 事件
- Produces: 完整 UI（任务列表 + 添加任务 + 设置），与后端字段约定见 `src/types.ts`（`TaskState` 为小写字符串，与 `TaskState` 的 serde `rename_all = "lowercase"` 对应）

- [ ] **Step 1: 工程文件**

`package.json`：

```json
{
  "name": "sparkling-ui",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc && vite build",
    "tauri": "tauri"
  },
  "dependencies": {
    "@tauri-apps/api": "^2.0.0",
    "react": "^18.3.1",
    "react-dom": "^18.3.1"
  },
  "devDependencies": {
    "@tauri-apps/cli": "^2.0.0",
    "@types/react": "^18.3.0",
    "@types/react-dom": "^18.3.0",
    "@vitejs/plugin-react": "^4.3.0",
    "typescript": "^5.5.0",
    "vite": "^5.4.0"
  }
}
```

`vite.config.ts`：

```ts
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  clearScreen: false,
  server: { port: 5173, strictPort: true },
  build: { outDir: 'dist' },
});
```

`tsconfig.json`：

```json
{
  "compilerOptions": {
    "target": "ES2021",
    "lib": ["ES2021", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "skipLibCheck": true,
    "noEmit": true,
    "isolatedModules": true
  },
  "include": ["src"]
}
```

`index.html`：

```html
<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Sparkling</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 2: 类型与 API 层**

`src/types.ts`：

```ts
export type TaskState =
  | 'queued' | 'running' | 'paused' | 'completed' | 'failed' | 'cancelled';

export interface TaskRecord {
  id: string;
  url: string;
  state: TaskState;
  save_dir: string;
  filename: string | null;
  segments: number;
  max_speed: number | null;
  total_size: number | null;
  downloaded: number;
  error: string | null;
  created_at: number;
}

export interface ManagerConfig {
  max_concurrent: number;
  auto_resume_on_start: boolean;
  global_speed_limit: number | null;
  default_segments: number;
}

export type TaskEvent =
  | { kind: 'State'; id: string; state: TaskState; error: string | null }
  | { kind: 'Progress'; id: string; downloaded: number; total: number; speed: number };

export function fmtBytes(n: number | null | undefined): string {
  if (n == null) return '—';
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}
```

`src/api.ts`：

```ts
import { invoke } from '@tauri-apps/api/core';
import type { ManagerConfig, TaskRecord } from './types';

export const api = {
  addTask: (url: string, filename?: string | null, segments?: number | null) =>
    invoke<string>('add_task', { url, filename: filename ?? null, segments: segments ?? null }),
  pauseTask: (id: string) => invoke<void>('pause_task', { id }),
  resumeTask: (id: string) => invoke<void>('resume_task', { id }),
  cancelTask: (id: string) => invoke<void>('cancel_task', { id }),
  retryTask: (id: string) => invoke<void>('retry_task', { id }),
  removeTask: (id: string) => invoke<void>('remove_task', { id }),
  moveTaskToTop: (id: string) => invoke<void>('move_to_top', { id }),
  listTasks: () => invoke<TaskRecord[]>('list_tasks'),
  getConfig: () => invoke<ManagerConfig>('get_config'),
  updateConfig: (cfg: ManagerConfig) => invoke<void>('update_config', { cfg }),
};
```

- [ ] **Step 3: 组件**

`src/main.tsx`：

```tsx
import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './App.css';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
```

`src/App.tsx`：

```tsx
import { useCallback, useEffect, useRef, useState } from 'react';
import { listen } from '@tauri-apps/api/event';
import { api } from './api';
import type { ManagerConfig, TaskEvent, TaskRecord } from './types';
import { fmtBytes } from './types';
import AddTaskDialog from './components/AddTaskDialog';
import SettingsModal from './components/SettingsModal';
import TaskList from './components/TaskList';

export default function App() {
  const [tasks, setTasks] = useState<TaskRecord[]>([]);
  const [config, setConfig] = useState<ManagerConfig | null>(null);
  const [showAdd, setShowAdd] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const speeds = useRef<Map<string, number>>(new Map());

  const refresh = useCallback(async () => {
    try {
      setTasks(await api.listTasks());
    } catch {
      /* 后端未就绪时静默 */
    }
  }, []);

  useEffect(() => {
    refresh();
    api.getConfig().then(setConfig).catch(() => {});
    // 事件驱动更新 + 2s 轮询兜底（防丢事件）
    const un = listen<TaskEvent>('task-event', (ev) => {
      const p = ev.payload;
      if (p.kind === 'Progress') {
        speeds.current.set(p.id, p.speed);
      }
      // 状态/进度最终以 list_tasks 为准，事件触发即时刷新
      refresh();
    });
    const timer = setInterval(refresh, 2000);
    return () => {
      un.then((f) => f());
      clearInterval(timer);
    };
  }, [refresh]);

  const totalSpeed = [...speeds.current.values()].reduce((a, b) => a + b, 0);

  return (
    <div className="app">
      <header className="toolbar">
        <h1>✨ Sparkling</h1>
        <span className="speed">总速度 {fmtBytes(totalSpeed)}/s</span>
        <div className="actions">
          <button className="primary" onClick={() => setShowAdd(true)}>＋ 新建下载</button>
          <button onClick={() => setShowSettings(true)}>设置</button>
        </div>
      </header>
      <main>
        <TaskList tasks={tasks} onChanged={refresh} />
      </main>
      {showAdd && (
        <AddTaskDialog
          defaultSegments={config?.default_segments ?? 8}
          onClose={() => setShowAdd(false)}
          onAdded={() => {
            setShowAdd(false);
            refresh();
          }}
        />
      )}
      {showSettings && (
        <SettingsModal
          config={config}
          onClose={() => setShowSettings(false)}
          onSaved={(c) => {
            setConfig(c);
            setShowSettings(false);
          }}
        />
      )}
    </div>
  );
}
```

`src/components/TaskList.tsx`：

```tsx
import type { TaskRecord } from '../types';
import TaskRow from './TaskRow';

export default function TaskList({
  tasks,
  onChanged,
}: {
  tasks: TaskRecord[];
  onChanged: () => void;
}) {
  if (tasks.length === 0) {
    return <div className="empty">还没有任务 —— 点击「新建下载」开始</div>;
  }
  return (
    <div className="task-list">
      {tasks.map((t) => (
        <TaskRow key={t.id} task={t} onChanged={onChanged} />
      ))}
    </div>
  );
}
```

`src/components/TaskRow.tsx`：

```tsx
import { api } from '../api';
import type { TaskRecord } from '../types';
import { fmtBytes } from '../types';

const STATE_LABEL: Record<string, string> = {
  queued: '排队中',
  running: '下载中',
  paused: '已暂停',
  completed: '已完成',
  failed: '失败',
  cancelled: '已取消',
};

export default function TaskRow({ task, onChanged }: { task: TaskRecord; onChanged: () => void }) {
  const pct =
    task.total_size && task.total_size > 0
      ? Math.floor((task.downloaded / task.total_size) * 100)
      : 0;
  const name = task.filename ?? '解析中…';
  const act = (fn: (id: string) => Promise<void>) => () => {
    fn(task.id).then(onChanged).catch((e) => alert(String(e)));
  };

  return (
    <div className={`task-row ${task.state}`}>
      <div className="row-head">
        <span className="name" title={task.url}>{name}</span>
        <span className={`chip ${task.state}`}>{STATE_LABEL[task.state]}</span>
      </div>
      <div className="progress">
        <div className="bar">
          <div className="fill" style={{ width: `${pct}%` }} />
        </div>
        <span className="pct">{pct}%</span>
      </div>
      <div className="row-meta">
        <span>{fmtBytes(task.downloaded)} / {fmtBytes(task.total_size)}</span>
        {task.error && <span className="error" title={task.error}>{task.error}</span>}
      </div>
      <div className="row-actions">
        {task.state === 'queued' && (
          <>
            <button onClick={act(api.moveTaskToTop)}>置顶</button>
            <button onClick={act(api.cancelTask)}>取消</button>
          </>
        )}
        {task.state === 'running' && (
          <>
            <button onClick={act(api.pauseTask)}>暂停</button>
            <button onClick={act(api.cancelTask)}>取消</button>
          </>
        )}
        {task.state === 'paused' && (
          <>
            <button className="primary" onClick={act(api.resumeTask)}>继续</button>
            <button onClick={act(api.cancelTask)}>取消</button>
          </>
        )}
        {task.state === 'failed' && (
          <>
            <button className="primary" onClick={act(api.retryTask)}>重试</button>
            <button onClick={act(api.removeTask)}>移除</button>
          </>
        )}
        {(task.state === 'completed' || task.state === 'cancelled') && (
          <button onClick={act(api.removeTask)}>移除</button>
        )}
      </div>
    </div>
  );
}
```

`src/components/AddTaskDialog.tsx`：

```tsx
import { useState } from 'react';
import { api } from '../api';

export default function AddTaskDialog({
  defaultSegments,
  onClose,
  onAdded,
}: {
  defaultSegments: number;
  onClose: () => void;
  onAdded: () => void;
}) {
  const [url, setUrl] = useState('');
  const [filename, setFilename] = useState('');
  const [segments, setSegments] = useState(defaultSegments);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (!url.trim()) {
      setErr('请输入 URL');
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      await api.addTask(url.trim(), filename.trim() || null, segments);
      onAdded();
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="modal-mask" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>新建下载</h2>
        <label>URL</label>
        <input
          autoFocus
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          placeholder="https://example.com/file.zip"
          onKeyDown={(e) => e.key === 'Enter' && submit()}
        />
        <label>文件名（可选，留空自动识别）</label>
        <input value={filename} onChange={(e) => setFilename(e.target.value)} />
        <label>分片数（1–64）</label>
        <input
          type="number"
          min={1}
          max={64}
          value={segments}
          onChange={(e) => setSegments(Number(e.target.value) || defaultSegments)}
        />
        {err && <div className="error">{err}</div>}
        <div className="modal-actions">
          <button onClick={onClose}>取消</button>
          <button className="primary" disabled={busy} onClick={submit}>
            {busy ? '添加中…' : '添加'}
          </button>
        </div>
      </div>
    </div>
  );
}
```

`src/components/SettingsModal.tsx`：

```tsx
import { useState } from 'react';
import { api } from '../api';
import type { ManagerConfig } from '../types';

export default function SettingsModal({
  config,
  onClose,
  onSaved,
}: {
  config: ManagerConfig | null;
  onClose: () => void;
  onSaved: (c: ManagerConfig) => void;
}) {
  const [maxConcurrent, setMaxConcurrent] = useState(config?.max_concurrent ?? 3);
  const [defaultSegments, setDefaultSegments] = useState(config?.default_segments ?? 8);
  const [limitKb, setLimitKb] = useState(
    config?.global_speed_limit ? Math.round(config.global_speed_limit / 1024) : 0
  );
  const [autoResume, setAutoResume] = useState(config?.auto_resume_on_start ?? true);
  const [err, setErr] = useState<string | null>(null);

  const save = async () => {
    const cfg: ManagerConfig = {
      max_concurrent: Math.max(1, Math.min(10, maxConcurrent)),
      auto_resume_on_start: autoResume,
      global_speed_limit: limitKb > 0 ? limitKb * 1024 : null,
      default_segments: Math.max(1, Math.min(64, defaultSegments)),
    };
    try {
      await api.updateConfig(cfg);
      onSaved(cfg);
    } catch (e) {
      setErr(String(e));
    }
  };

  return (
    <div className="modal-mask" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>设置</h2>
        <label>同时下载任务数（1–10）</label>
        <input type="number" min={1} max={10} value={maxConcurrent}
          onChange={(e) => setMaxConcurrent(Number(e.target.value) || 1)} />
        <label>默认分片数（1–64）</label>
        <input type="number" min={1} max={64} value={defaultSegments}
          onChange={(e) => setDefaultSegments(Number(e.target.value) || 8)} />
        <label>全局限速 KB/s（0 = 不限）</label>
        <input type="number" min={0} value={limitKb}
          onChange={(e) => setLimitKb(Number(e.target.value) || 0)} />
        <label className="checkbox">
          <input type="checkbox" checked={autoResume}
            onChange={(e) => setAutoResume(e.target.checked)} />
          重启后自动恢复未完成任务
        </label>
        {err && <div className="error">{err}</div>}
        <div className="modal-actions">
          <button onClick={onClose}>取消</button>
          <button className="primary" onClick={save}>保存</button>
        </div>
      </div>
    </div>
  );
}
```

`src/App.css`：

```css
:root { color-scheme: dark; }
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: "Segoe UI", "Microsoft YaHei", system-ui, sans-serif;
  background: #14161c;
  color: #e6e9f0;
}
.app { display: flex; flex-direction: column; height: 100vh; }
.toolbar {
  display: flex; align-items: center; gap: 16px;
  padding: 12px 20px; background: #1b1e26; border-bottom: 1px solid #2a2e3a;
}
.toolbar h1 { font-size: 18px; margin: 0; flex: 1; }
.toolbar .speed { color: #8b93a7; font-size: 13px; }
.toolbar .actions { display: flex; gap: 8px; }
main { flex: 1; overflow-y: auto; padding: 16px 20px; }

button {
  background: #262a35; color: #e6e9f0; border: 1px solid #333846;
  border-radius: 6px; padding: 6px 14px; cursor: pointer; font-size: 13px;
}
button:hover { background: #2f3441; }
button.primary { background: #3b74f0; border-color: #3b74f0; }
button.primary:hover { background: #5487f2; }

.empty { color: #6b7386; text-align: center; margin-top: 80px; }
.task-list { display: flex; flex-direction: column; gap: 12px; max-width: 900px; }
.task-row {
  background: #1b1e26; border: 1px solid #2a2e3a; border-radius: 10px;
  padding: 12px 16px;
}
.row-head { display: flex; align-items: center; gap: 10px; }
.row-head .name { flex: 1; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.chip {
  font-size: 12px; padding: 2px 10px; border-radius: 999px; background: #2a2e3a;
}
.chip.running { background: #123a25; color: #5fd68a; }
.chip.completed { background: #123055; color: #6db3ff; }
.chip.failed { background: #45161c; color: #ff7b8a; }
.chip.paused { background: #453a12; color: #ffc861; }
.progress { display: flex; align-items: center; gap: 10px; margin: 10px 0 6px; }
.bar { flex: 1; height: 8px; background: #262a35; border-radius: 4px; overflow: hidden; }
.fill { height: 100%; background: linear-gradient(90deg, #3b74f0, #6db3ff); transition: width 0.3s; }
.pct { font-size: 12px; color: #8b93a7; width: 44px; text-align: right; }
.row-meta { display: flex; gap: 16px; font-size: 12px; color: #8b93a7; }
.row-meta .error { color: #ff7b8a; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }
.row-actions { display: flex; gap: 8px; margin-top: 10px; }

.modal-mask {
  position: fixed; inset: 0; background: rgba(0, 0, 0, 0.55);
  display: flex; align-items: center; justify-content: center; z-index: 10;
}
.modal {
  background: #1b1e26; border: 1px solid #2a2e3a; border-radius: 12px;
  padding: 20px 24px; width: 460px; max-width: 92vw;
}
.modal h2 { margin: 0 0 14px; font-size: 16px; }
.modal label { display: block; font-size: 12px; color: #8b93a7; margin: 12px 0 4px; }
.modal input {
  width: 100%; background: #14161c; color: #e6e9f0;
  border: 1px solid #333846; border-radius: 6px; padding: 8px 10px; font-size: 13px;
}
.modal label.checkbox { display: flex; align-items: center; gap: 8px; margin-top: 14px; }
.modal label.checkbox input { width: auto; }
.modal-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 18px; }
.error { color: #ff7b8a; font-size: 12px; margin-top: 8px; }
```

- [ ] **Step 4: 构建验证**

Run: `npm install`，然后 `npm run build`
Expected: `tsc` 无错误，`dist/` 生成构建产物

Run: `cargo build -p sparkling`
Expected: PASS（新 dist 被嵌入）

- [ ] **Step 5: 手动冒烟（`npm run tauri dev`）**

清单（全部通过才算完成）：
1. 窗口打开，工具栏与空列表渲染正常
2. 新建下载：粘贴一个大文件 URL（如某发行版 ISO 直链）→ 列表出现任务、进度条推进、总速度非零
3. 暂停 → 状态变"已暂停"，进度冻结；继续 → 恢复推进
4. 等待完成 → 状态"已完成"，系统下载目录出现文件
5. 设置修改"同时下载任务数 = 1" → 保存 → 再添加两个任务，第二个保持"排队中"
6. 下载中途关闭应用 → 重新启动 → 任务自动恢复下载
7. 新建一个会 404 的 URL → 任务"失败"并显示中文错误

- [ ] **Step 6: 提交**

```bash
git add package.json package-lock.json vite.config.ts tsconfig.json index.html src dist
git commit -m "feat(ui): React 任务列表/新建下载/设置"
```

---

### Task 17: 收尾（README + 全量回归）

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: 全部
- Produces: 项目文档与最终回归确认

- [ ] **Step 1: 写 README**

`README.md`：

```markdown
# Sparkling ✨

PC 端全功能下载器。当前为子项目①：HTTP/HTTPS 多线程下载核心。

## 功能

- 多线程分片下载（默认 8 线程，可配 1–64），动态偷段消除长尾
- 断点续传（`.sparkling` 控制文件），崩溃/重启后自动恢复
- 全局 + 单任务限速（令牌桶）
- 任务队列：并发控制（默认 3）、置顶、失败重试（从分片断点继续）
- 完整性校验（Content-MD5）、ETag 变化检测（远端变化自动重下）
- SQLite 任务持久化

## 开发

前置：Rust（stable）、Node.js 18+。

​```bash
npm install          # 前端依赖
npm run tauri dev    # 开发模式（热重载）
npm run tauri build  # 构建（bundling 属于后续阶段）
cargo test           # 核心库测试（含可编程 HTTP 测试服务器）
​```

## 架构

- `crates/sparkling-core`：纯 Rust 核心（不依赖 Tauri）——引擎、调度、持久化
- `src-tauri`：Tauri 2 壳，command/event 桥接
- `src`：React + TypeScript 前端

详见 `docs/superpowers/specs/2026-08-28-http-downloader-core-design.md`。

## 路线图

- [x] ① HTTP 多线程下载核心
- [ ] ② BT / 磁力引擎
- [ ] ③ 视频解析下载（yt-dlp）
- [ ] ④ 浏览器接管、自动更新、多语言、安装包
```

（注意：README 中的代码围栏需要真实的三反引号，上面用 `​` 转义仅为排版展示。）

- [ ] **Step 2: 全量回归**

Run: `cargo test --workspace`
Expected: 全部 PASS

Run: `npm run build`
Expected: 无错误

- [ ] **Step 3: 手动最终冒烟**

重复 Task 16 Step 5 清单一遍（含重启恢复）。

- [ ] **Step 4: 提交**

```bash
git add README.md
git commit -m "docs: README 与项目说明"
```

---

## 计划自审记录（writing-plans Self-Review）

1. **Spec 覆盖**：spec 第 2 节（分片/续传/限速/TaskStore）→ Task 3/4/5/9/11/13；第 3 节（状态机/数据流/重启自动恢复）→ Task 2/11/14；第 4 节（错误处理）→ Task 12；第 5 节（测试策略）→ Task 6 测试服务器 + 各任务 TDD + Task 11 崩溃恢复测试。UI 冒烟 → Task 16 Step 5。**无缺口。**
2. **占位符扫描**：Task 8 的"多线程在 Task 9 实现"与 Task 11 前的顺序分支为刻意的增量桩，后续任务给出完整替换代码；无 TBD/TODO。
3. **类型一致性**：`TaskState` serde 序列化在 Task 14(a) 统一为 `rename_all = "lowercase"`，与前端 `types.ts` 对齐；`TaskEvent` 的 serde tag `kind` 与前端判别联合对齐；`control_rx` 在 Task 11 起为值传递（supervise → run_download → drive_download）。执行者注意按任务顺序实施。

