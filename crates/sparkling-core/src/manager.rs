//! TaskManager：队列调度 / 并发位 / 置顶 / 重启自动恢复 / 事件流。
//! 上层（Tauri 命令层）只与本模块打交道，不直接触碰 Engine/TaskStore。
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

/// 推送给 UI 的事件流（broadcast：晚订阅者只看新事件，不回放）
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
    pub fn new(
        store_path: &std::path::Path,
        engine: Arc<dyn Engine>,
        config: ManagerConfig,
    ) -> Result<Self> {
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
        self.inner
            .engine
            .set_speed_limit(self.inner.config.lock().unwrap().global_speed_limit);
        // 调大 max_concurrent 时立即把排队任务提上来
        try_schedule(&self.inner);
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
        // 先查句柄再分支：if-let 条件里的锁守卫会活到整个 if-else 结束，
        // else 分支里 retry_task 再锁 handles 即自死锁（Mutex 不可重入，D36）
        let h = self.inner.handles.lock().unwrap().get(id).cloned();
        if let Some(h) = h {
            h.resume()
        } else {
            self.retry_task(id)
        }
    }

    /// 取消：有句柄（运行/暂停中）→ 送达引擎；纯排队任务 → 出队并标记
    /// Cancelled（D36：状态机 Queued→Cancelled 合法，静默空操作是最坏结果）
    pub fn cancel_task(&self, id: &str) -> Result<()> {
        if let Some(h) = self.inner.handles.lock().unwrap().get(id).cloned() {
            return h.cancel();
        }
        let was_queued = {
            let mut q = self.inner.queue.lock().unwrap();
            let was = q.iter().any(|x| x == id);
            q.retain(|x| x != id);
            was
        };
        if was_queued {
            self.inner.store.lock().unwrap().update_state(id, TaskState::Cancelled, None).ok();
            self.emit_state(id, TaskState::Cancelled, None);
        }
        Ok(())
    }

    /// Failed/Paused → Queued，重新调度（引擎检测控制文件从断点续传）。
    /// 防重入（D36）：运行/暂停中（有句柄）或已在队列的任务直接 Ok 忽略——
    /// 否则 UI 双击重试/重复恢复会把同一任务二次提交，两个引擎写同一 .part
    pub fn retry_task(&self, id: &str) -> Result<()> {
        if self.inner.handles.lock().unwrap().contains_key(id) {
            return Ok(()); // 运行/暂停中：由 pause/resume 管理
        }
        let q = self.inner.queue.lock().unwrap();
        if q.iter().any(|x| x == id) {
            return Ok(()); // 已在队列
        }
        drop(q);
        // 终态保护（D36 测试「终态不得被重试/取消改写」）：Completed/Cancelled
        // 直接忽略——否则 update_state(Queued) 先落库，出队侧的终态过滤永远
        // 看不到 Completed，任务会被重新下载。重启残留的 Running（无句柄）
        // 不在此列：recover() 正是靠重新排队续传它（有句柄的 Running 已被
        // 上面的句柄守卫拦下）
        if let Some(rec) = self.inner.store.lock().unwrap().get(id)? {
            if matches!(rec.state, TaskState::Completed | TaskState::Cancelled) {
                return Ok(());
            }
        }
        self.inner.queue.lock().unwrap().push_back(id.to_string());
        self.inner.store.lock().unwrap().update_state(id, TaskState::Queued, None)?;
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
                            self.inner
                                .store
                                .lock()
                                .unwrap()
                                .update_state(&rec.id, TaskState::Paused, None)?;
                            self.emit_state(&rec.id, TaskState::Paused, None);
                        }
                        _ => {
                            let msg = "控制文件缺失或损坏，请重试";
                            self.inner.store.lock().unwrap().update_state(
                                &rec.id,
                                TaskState::Failed,
                                Some(msg),
                            )?;
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
        let _ = self
            .inner
            .events
            .send(TaskEvent::State { id: id.to_string(), state, error });
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
                    inner2
                        .store
                        .lock()
                        .unwrap()
                        .update_state(&id, TaskState::Failed, Some(&e.user_message()))
                        .ok();
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
    inner
        .store
        .lock()
        .unwrap()
        .update_state(&id, TaskState::Running, None)
        .ok();
    let _ = inner.events.send(TaskEvent::State {
        id: id.clone(),
        state: TaskState::Running,
        error: None,
    });
    let mut rx = handle.subscribe();
    let mut prev = TaskState::Running;
    // 引擎解析出文件名后落库一次（重启恢复按文件名定位控制文件，D35）
    let mut filename_persisted = false;
    loop {
        if rx.changed().await.is_err() {
            break;
        }
        let snap = rx.borrow().clone();
        if !filename_persisted {
            if let Some(name) = &snap.filename {
                inner.store.lock().unwrap().update_filename(&id, name).ok();
                filename_persisted = true;
            }
        }
        inner
            .store
            .lock()
            .unwrap()
            .update_progress(&id, snap.downloaded, snap.total)
            .ok();
        let _ = inner.events.send(TaskEvent::Progress {
            id: id.clone(),
            downloaded: snap.downloaded,
            total: snap.total,
            speed: snap.speed,
        });
        if snap.state != prev {
            let was_running = prev == TaskState::Running;
            prev = snap.state;
            inner
                .store
                .lock()
                .unwrap()
                .update_state(&id, snap.state, snap.error.as_deref())
                .ok();
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
