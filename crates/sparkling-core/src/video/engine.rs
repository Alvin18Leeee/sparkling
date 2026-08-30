//! VideoEngine：包装 yt-dlp 进程的下载引擎（③期）。
//! 每任务一个进程；暂停 = 杀进程，恢复 = 重启进程（yt-dlp -c 从 .part 续传）；
//! 进度经 --progress-template 结构化输出，逐行解析为 ProgressSnapshot。
use crate::engine::{ControlMsg, Engine, ProgressSnapshot, TaskHandle};
use crate::task::{TaskId, TaskKind, TaskSpec, TaskState};
use crate::video::progress::{is_merge_line, parse_progress_line};
use crate::video::runner::{KillReason, YtDlpRunner};
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

    /// 关停：abort 全部运行中的下载（Drop 同款逻辑，供应用退出主动调用）
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
        if spec.video.is_none() {
            // build_args 对 video 参数有 expect 兜底，这里先拒之（否则 panic 发生在
            // spawn 出的任务里，watch 永远等不到终态）
            return Err(SparklingError::Other("视频任务缺少视频参数".into()));
        }
        let id: TaskId = uuid::Uuid::new_v4().to_string();
        let (progress_tx, progress_rx) = watch::channel(ProgressSnapshot {
            state: TaskState::Running,
            downloaded: 0,
            total: 0,
            speed: 0,
            segments: vec![],
            error: None,
            // 未指定时保持 None：manager 只在 Some 时落库，假名会污染 TaskRecord 与重启恢复
            filename: spec.filename.clone(),
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
    // filename 未指定（直下路径）时按 yt-dlp 标题模板命名（spec 规定），避免全部产出 video.mp4
    let out = match spec.filename.as_deref() {
        Some(f) => spec.save_dir.join(format!("{f}.%(ext)s")),
        None => spec.save_dir.join("%(title).200B [%(id)s].%(ext)s"),
    };
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
        return line
            .trim_start_matches("ERROR")
            .trim_start_matches(':')
            .trim()
            .to_string();
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

/// 分片名结构校验：`f` + 全数字 + `.` + 非空扩展名（可选 `.part` 后缀）。
/// 旧的"f + 首字符数字"启发式会误删 `X.f4v` 等真实完成文件（remove_task
/// 对已完成任务也调 cleanup）
fn is_fragment_name(rest: &str) -> bool {
    let body = rest.strip_suffix(".part").unwrap_or(rest);
    let Some(after_f) = body.strip_prefix('f') else {
        return false;
    };
    let Some((digits, ext)) = after_f.split_once('.') else {
        return false;
    };
    !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) && !ext.is_empty()
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
        if is_part || is_fragment_name(rest) {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

/// 进度行应用到快照。
/// downloaded/total 单调钳制（只增不减）：分离流（bv+ba）的第二段（音频流）
/// 进度行从 0 重涨，不钳制则进度条视觉回退；钳制后第二段期间停在 100%，
/// 合并阶段由 merging 标签体现（无独立进度）。
fn apply_line(snap: &mut ProgressSnapshot, line: &str) {
    if let Some(p) = parse_progress_line(line) {
        snap.downloaded = snap.downloaded.max(p.downloaded);
        if let Some(t) = p.total {
            snap.total = snap.total.max(t);
        }
        snap.speed = p.speed.unwrap_or(0);
    } else if is_merge_line(line) {
        snap.merging = true;
        snap.speed = 0;
    }
}

/// 进程退出后排干残留进度行：退出与末几行同拍就绪时 select 可能先取退出分支，
/// 不排干会丢末行进度（终态 downloaded/merging 缺失）。行先于 RunResult 入队
/// （runner 在返回前发完全部行），done 就绪后 try_recv 必然取尽。
fn drain_lines(line_rx: &mut mpsc::UnboundedReceiver<String>, snap: &mut ProgressSnapshot) {
    while let Ok(line) = line_rx.try_recv() {
        apply_line(snap, &line);
    }
}

enum RunOutcome {
    Done(std::result::Result<crate::video::runner::RunResult, tokio::task::JoinError>),
    Cancelled,
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
    // None（直下路径）时跳过 cleanup_partial：残留文件名是 yt-dlp 标题模板产物，
    // 无法前缀匹配（与 manager remove_task 的 filename=Some 前提一致）
    let filename = spec.filename.clone();
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
        let mut run = match started {
            Ok(h) => h,
            Err(e) => {
                // start 失败（二进制缺失等）→ Failed 终态
                let _ = progress_tx.send(ProgressSnapshot {
                    state: TaskState::Failed,
                    error: Some(e.user_message()),
                    ..snapshot.clone()
                });
                registry.lock().unwrap().remove(&id);
                return;
            }
        };
        snapshot.merging = false; // 重启后重新判定
        let outcome = loop {
            tokio::select! {
                // 进程退出
                res = &mut run.done => {
                    drain_lines(&mut line_rx, &mut snapshot);
                    break RunOutcome::Done(res);
                }
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
                        drain_lines(&mut line_rx, &mut snapshot);
                        break RunOutcome::Done(res);
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
                if let Some(f) = &filename {
                    cleanup_partial(&spec.save_dir, f);
                }
                snapshot.state = TaskState::Cancelled;
                snapshot.speed = 0;
                let _ = progress_tx.send(snapshot.clone());
                registry.lock().unwrap().remove(&id);
                return;
            }
            RunOutcome::Done(Ok(res)) => {
                // 终态判定：killed 优先于 code（被杀进程的退出码无意义）
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
                                if let Some(f) = &filename {
                                    cleanup_partial(&spec.save_dir, f);
                                }
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
                    if let Some(f) = &filename {
                        cleanup_partial(&spec.save_dir, f);
                    }
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
                    _ => format!(
                        "yt-dlp 退出码 {}",
                        res.code
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| "未知".into())
                    ),
                };
                snapshot.state = TaskState::Failed;
                snapshot.error = Some(msg);
                let _ = progress_tx.send(snapshot.clone());
                registry.lock().unwrap().remove(&id);
                return;
            }
            RunOutcome::Done(Err(_)) => {
                // done JoinError（abort 等）→ 取消语义
                if let Some(f) = &filename {
                    cleanup_partial(&spec.save_dir, f);
                }
                snapshot.state = TaskState::Cancelled;
                let _ = progress_tx.send(snapshot.clone());
                registry.lock().unwrap().remove(&id);
                return;
            }
        }
    }
}

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
        assert!(args
            .windows(2)
            .any(|w| w[0] == "-f" && w[1] == "bv*[height<=1080]+ba/b"));
        assert!(args.contains(&"-c".to_string()), "断点续传 -c 必须在");
        assert!(args.contains(&"--newline".to_string()));
        assert!(args.contains(&"--no-mtime".to_string()));
        assert!(joined.contains("--progress-template"));
        assert!(joined.contains("SPARKLING|%(progress.downloaded_bytes)s"));
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--ffmpeg-location" && w[1] == "ff/ffmpeg.exe"));
        assert!(args.windows(2).any(|w| w[0] == "-r" && w[1] == "1K"));
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--sub-langs" && w[1] == "zh-Hans"));
        assert!(args.contains(&"--write-subs".to_string()));
        assert!(args.contains(&"--write-auto-subs".to_string()));
        assert!(args
            .windows(2)
            .any(|w| w[0] == "-o" && w[1].ends_with("测试视频.%(ext)s")));
        assert_eq!(args.last().unwrap(), "https://www.youtube.com/watch?v=test");
    }

    #[test]
    fn build_args_none_filename_uses_title_template() {
        // 直下路径（quickDownload）filename 传 None：应按 yt-dlp 标题模板命名，
        // 不得兜底 "video"（否则重复直下全部产出 video.mp4 同名冲突）
        let mut spec = video_spec(Path::new("D:\\dl"));
        spec.filename = None;
        let args = build_args(&spec, None, None, None);
        assert!(
            args.windows(2)
                .any(|w| w[0] == "-o" && w[1].ends_with("%(title).200B [%(id)s].%(ext)s")),
            "filename 未指定时 -o 应以标题模板结尾：{:?}",
            args
        );
    }

    #[test]
    fn build_args_omits_optional_when_none() {
        let mut spec = video_spec(Path::new("D:\\dl"));
        if let Some(v) = spec.video.as_mut() {
            v.subtitles.clear();
            v.auto_subs = false;
        }
        let args = build_args(&spec, None, None, None);
        let joined = args.join(" ");
        assert!(!joined.contains("--ffmpeg-location"));
        assert!(!joined.contains("--cookies"));
        // 独立参数项判断：joined 子串会把 "--retries" 误含 "-r"
        assert!(!args.contains(&"-r".to_string()));
        assert!(!args.contains(&"--write-subs".to_string()));
        assert!(!args.contains(&"--write-auto-subs".to_string()));
        assert!(!args.contains(&"--sub-langs".to_string()));
    }

    #[test]
    fn build_args_passes_cookie_file() {
        let spec = video_spec(Path::new("D:\\dl"));
        let args = build_args(&spec, None, Some(Path::new("data/cookies.txt")), None);
        assert!(args
            .windows(2)
            .any(|w| w[0] == "--cookies" && w[1] == "data/cookies.txt"));
    }

    async fn wait_state(rx: &mut watch::Receiver<ProgressSnapshot>, want: TaskState) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if rx.borrow().state == want {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "等待状态 {want:?} 超时"
            );
            rx.changed().await.unwrap();
        }
    }

    #[tokio::test]
    async fn progress_never_regresses_across_format_streams() {
        // 分离流（bv+ba）：视频流下到 100% 后，音频流的进度行从 0 重涨——
        // 不钳制则进度条视觉回退。钳制后 downloaded/total 只增不减。
        let runner = Arc::new(FakeRunner::default());
        runner.scripts.lock().unwrap().push_back(vec![
            ScriptStep::Lines(&["SPARKLING|1000|1000|1000|500"]), // 视频流完成
            ScriptStep::Lines(&["SPARKLING|10|300|300|100"]),     // 音频流从 0 重涨
            ScriptStep::Lines(&["SPARKLING|300|300|300|100"]),    // 音频流完成
            ScriptStep::Lines(&["[Merger] Merging formats into \"x.mp4\""]),
            ScriptStep::Exit(0),
        ]);
        let eng = engine(runner);
        let handle = eng.submit(video_spec(Path::new("D:\\dl"))).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Completed).await;
        let snap = rx.borrow().clone();
        assert_eq!(
            snap.downloaded, 1000,
            "第二段流的 downloaded 不得回退到小值"
        );
        assert_eq!(snap.total, 1000, "total 只增不减");
        assert!(snap.merging);
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
        // f4v 是真实完成文件（remove_task 对已完成任务也调 cleanup），
        // 不得被"f + 首字符数字"启发式误删
        std::fs::write(save.join("测试视频.f4v"), b"x").unwrap();
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
        assert!(
            save.join("测试视频.f4v").exists(),
            "f4v 真实完成文件不得误删"
        );
    }

    #[test]
    fn cleanup_partial_fragment_heuristic_is_structural() {
        let dir = tempfile::tempdir().unwrap();
        let save = dir.path();
        for name in [
            "测试视频.mp4.part",
            "测试视频.f137.mp4",
            "测试视频.f137.mp4.part",
        ] {
            std::fs::write(save.join(name), b"x").unwrap();
        }
        for name in [
            "测试视频.f4v",
            "测试视频.f137",      // 无扩展名：不匹配 f+数字+.+ext 结构
            "测试视频.final.mp4", // f 后非全数字
            "无关文件.txt",
        ] {
            std::fs::write(save.join(name), b"x").unwrap();
        }
        cleanup_partial(save, "测试视频");
        for name in [
            "测试视频.mp4.part",
            "测试视频.f137.mp4",
            "测试视频.f137.mp4.part",
        ] {
            assert!(!save.join(name).exists(), "{name} 应被清理");
        }
        for name in [
            "测试视频.f4v",
            "测试视频.f137",
            "测试视频.final.mp4",
            "无关文件.txt",
        ] {
            assert!(save.join(name).exists(), "{name} 不得误删");
        }
    }

    #[tokio::test]
    async fn nonzero_exit_fails_with_stderr_summary() {
        let runner = Arc::new(FakeRunner::default());
        runner
            .scripts
            .lock()
            .unwrap()
            .push_back(vec![ScriptStep::Exit(1)]);
        let eng = engine(runner);
        let handle = eng.submit(video_spec(Path::new("D:\\dl"))).await.unwrap();
        let mut rx = handle.subscribe();
        wait_state(&mut rx, TaskState::Failed).await;
        let snap = rx.borrow().clone();
        assert!(
            snap.error.unwrap().contains("yt-dlp 退出码 1"),
            "错误应含退出码"
        );
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
            let err = eng.submit(spec).await.err().expect("非视频任务应被拒绝");
            assert!(err.user_message().contains("非视频任务"));
        });
    }
}
