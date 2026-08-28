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

#[allow(dead_code)] // Task 9 多线程分片读取缓冲使用
const CHUNK_SIZE: usize = 64 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(30);
/// 剩余不足此值不偷（避免为几 KB 反复分裂）
const STEAL_THRESHOLD: u64 = 256 * 1024;

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
    /// 偷段时分配新段号（单调递增，不复用旧号）
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

    fn snapshot_segments(&self) -> Vec<SegmentProgress> {
        self.segments
            .lock()
            .unwrap()
            .iter()
            .map(|s| SegmentProgress { index: s.index, downloaded: s.downloaded, len: s.len() })
            .collect()
    }

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
}

impl Drop for HttpEngine {
    fn drop(&mut self) {
        // 引擎销毁 → abort 全部下载（测试隔离、崩溃模拟）
        for (_, h) in self.registry.lock().unwrap().drain() {
            h.abort();
        }
    }
}

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
    _control_rx: &mut mpsc::UnboundedReceiver<ControlMsg>, // Task 8 暂不消费，Task 11 接入暂停/恢复/取消
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
        let reporter = spawn_reporter(shared.clone(), progress_tx.clone(), None);
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
                Some(final_path.clone()),
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
}

fn part_path_for(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_os_string();
    s.push(".sparkling.part");
    PathBuf::from(s)
}

/// 进度上报器：250ms 快照 + 3 秒滑动窗口测速。
/// 终局保证：finished 置位后发送**最后一帧最新快照**再退出——supervise 的终态
/// 快照从 watch 借用的是最后一帧，缺这一帧则终态 downloaded 最多旧 250ms。
/// 周期帧发送失败（接收端全部掉线，abort 场景）时退出，防止任务泄漏。
/// ctl = Some(正式文件路径) 时每 2 秒（8 × 250ms）原子保存控制文件，
/// 运行中的任务崩溃后可凭 `.sparkling` 控制文件续传（Task 11 接入）。
fn spawn_reporter(
    shared: Arc<Shared>,
    progress_tx: watch::Sender<ProgressSnapshot>,
    ctl: Option<PathBuf>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        let mut window: std::collections::VecDeque<(tokio::time::Instant, u64)> =
            std::collections::VecDeque::new();
        let mut tick: u32 = 0;
        loop {
            interval.tick().await;
            if shared.finished.load(Ordering::Relaxed) {
                // 最后一帧：downloaded 取当下值（终态快照依赖）
                let _ = progress_tx.send(ProgressSnapshot {
                    state: TaskState::Running,
                    downloaded: shared.downloaded.load(Ordering::Relaxed),
                    total: shared.probe.total,
                    speed: 0,
                    segments: shared.snapshot_segments(),
                    error: None,
                });
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
            if progress_tx
                .send(ProgressSnapshot {
                    state: TaskState::Running,
                    downloaded: dl,
                    total: shared.probe.total,
                    speed,
                    segments: shared.snapshot_segments(),
                    error: None,
                })
                .is_err()
            {
                break; // 接收端全部掉线（abort）：退出防泄漏
            }
            tick += 1;
            if tick % 8 == 0 {
                // 每 2 秒保存控制文件
                if let Some(final_path) = &ctl {
                    let _ = control_file::save(final_path, &shared.build_control_file());
                }
            }
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
        let resp = match fetch_range(client, &spec.url, seg.next_offset(), Some(seg.end)).await {
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

/// 分片 worker：下载指派的段直至完成；完成后落盘控制文件并尝试偷段，
/// 偷不到（所有段剩余均低于阈值）才退出 —— 消除慢段长尾。
/// `ctl` = Some(正式文件路径)：分片完成即保存控制文件（Task 11 续传依赖）。
#[allow(clippy::too_many_arguments)]
async fn segment_worker(
    client: reqwest::Client,
    spec: TaskSpec,
    shared: Arc<Shared>,
    start_seg: Segment,
    part_path: PathBuf,
    ctl: Option<PathBuf>,
    global: Arc<TokenBucket>,
    task: Option<Arc<TokenBucket>>,
    retry: RetryPolicy,
) -> std::result::Result<WorkerExit, SparklingError> {
    let mut seg = start_seg;
    let mut attempt: u32 = 0;
    loop {
        // 用共享表判断剩余（偷段会收缩本段 end，本地副本可能过期）
        if shared.segment(seg.index).remaining() == 0 {
            // 分片完成即落盘控制文件（spec：每 2 秒或有分片完成时）
            if let Some(final_path) = &ctl {
                let _ = control_file::save(final_path, &shared.build_control_file());
            }
            match shared.steal_largest() {
                Some(stolen) => {
                    seg = stolen;
                    attempt = 0;
                    continue; // 回到外层 loop，下载新段
                }
                None => return Ok(WorkerExit::Done),
            }
        }
        let resp = match fetch_range(&client, &spec.url, seg.next_offset(), Some(seg.end)).await {
            // 206 守卫：带 Range 的请求若被中间层忽略返回 200 全量，
            // 按段偏移写入会损坏 .part —— 视为可重试错误
            Ok(r) if r.status().as_u16() == 206 => r,
            Ok(_) => {
                attempt += 1;
                if attempt > retry.max_retries {
                    let e = SparklingError::Network("服务器忽略 Range 请求".into());
                    shared.fail(e.clone());
                    return Err(e);
                }
                tokio::time::sleep(retry.backoff(attempt)).await;
                continue;
            }
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
                    // 分片完成即落盘控制文件（spec：每 2 秒或有分片完成时）
                    if let Some(final_path) = &ctl {
                        let _ = control_file::save(final_path, &shared.build_control_file());
                    }
                    match shared.steal_largest() {
                        Some(stolen) => {
                            seg = stolen;
                            attempt = 0;
                            continue; // 回到外层 loop，下载新段
                        }
                        None => return Ok(WorkerExit::Done),
                    }
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
