use std::path::PathBuf;

pub type TaskId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
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
