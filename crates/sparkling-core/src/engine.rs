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
    /// 引擎解析出的最终文件名（探测前 None；manager 落库供重启恢复/UI 展示）
    pub filename: Option<String>,
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
    pub(crate) id: TaskId,
    pub(crate) progress: watch::Receiver<ProgressSnapshot>,
    pub(crate) control: mpsc::UnboundedSender<ControlMsg>,
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
    /// 关停引擎：abort 所有运行中的下载（应用退出时调用）
    fn shutdown(&self) {}
}
