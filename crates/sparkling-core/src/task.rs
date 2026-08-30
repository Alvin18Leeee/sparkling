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

    pub fn parse(s: &str) -> Option<Self> {
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
        matches!(
            (self, next),
            (Queued, Running)
                | (Queued, Cancelled)
                | (Running, Paused)
                | (Running, Completed)
                | (Running, Failed)
                | (Running, Cancelled)
                | (Paused, Queued)
                | (Paused, Cancelled)
                | (Paused, Failed)
                | (Failed, Queued)
                | (Failed, Cancelled)
        )
    }
}

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

/// 提交给引擎的下载任务描述
/// kind/video：③期视频任务参数
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
    pub kind: TaskKind,
    pub video: Option<VideoParams>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legal_transitions() {
        use TaskState::*;
        let legal = [
            (Queued, Running),
            (Queued, Cancelled),
            (Running, Paused),
            (Running, Completed),
            (Running, Failed),
            (Running, Cancelled),
            (Paused, Queued),
            (Paused, Cancelled),
            (Paused, Failed),
            (Failed, Queued),
            (Failed, Cancelled),
        ];
        for (from, to) in legal {
            assert!(from.can_transition_to(to), "{from:?} -> {to:?} 应合法");
        }
    }

    #[test]
    fn illegal_transitions() {
        use TaskState::*;
        let illegal = [
            (Completed, Running),
            (Completed, Queued),
            (Completed, Failed),
            (Cancelled, Running),
            (Cancelled, Queued),
            (Queued, Completed),
            (Queued, Failed),
            (Queued, Paused),
            (Failed, Running),
            (Failed, Completed),
        ];
        for (from, to) in illegal {
            assert!(!from.can_transition_to(to), "{from:?} -> {to:?} 应非法");
        }
    }

    #[test]
    fn str_roundtrip() {
        for s in [
            TaskState::Queued,
            TaskState::Running,
            TaskState::Paused,
            TaskState::Completed,
            TaskState::Failed,
            TaskState::Cancelled,
        ] {
            assert_eq!(TaskState::parse(s.as_str()), Some(s));
        }
        assert_eq!(TaskState::parse("bogus"), None);
    }

    #[test]
    fn task_kind_roundtrip() {
        assert_eq!(TaskKind::Http.as_str(), "http");
        assert_eq!(TaskKind::Video.as_str(), "video");
        assert_eq!(TaskKind::parse("http"), Some(TaskKind::Http));
        assert_eq!(TaskKind::parse("video"), Some(TaskKind::Video));
        assert_eq!(TaskKind::parse("bogus"), None);
        // serde 小写
        assert_eq!(
            serde_json::to_string(&TaskKind::Video).unwrap(),
            "\"video\""
        );
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
}
