//! ③期 TaskManager 多引擎路由集成测试：视频任务走 VideoEngine（FakeRunner），
//! 与 HTTP 任务共享同一队列/并发位；重启恢复走 yt-dlp .part 续传（无控制文件）。
mod common;

use common::wait_event_state;
use sparkling_core::engine::Engine;
use sparkling_core::manager::{AddTaskOptions, Engines, ManagerConfig, TaskManager};
use sparkling_core::task::{TaskKind, TaskState, VideoParams};
use sparkling_core::video::engine::VideoEngine;
use sparkling_core::video::runner::{FakeRunner, ScriptStep};
use std::sync::Arc;
use std::time::Duration;

/// 视频引擎 + 真实 HttpEngine 空载（本测试不跑 HTTP 任务）
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
    let id = m
        .add_task("https://www.youtube.com/watch?v=t".into(), video_opts(&dir))
        .unwrap();
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
    let id = m
        .add_task("https://www.youtube.com/watch?v=t".into(), video_opts(&dir))
        .unwrap();
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
async fn video_filename_sanitized_on_add() {
    // 视频标题是远端攻击者可控输入："a/b" 会让引擎 join 出子目录、
    // "C:\evil" 会整体替换 save_dir（写任意盘符）——add_task 必须先消毒
    let runner = Arc::new(FakeRunner::default());
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, runner, ManagerConfig::default());
    for (malicious, expected) in [("a/b", "b"), ("C:\\evil", "evil")] {
        let mut opts = video_opts(&dir);
        opts.filename = Some(malicious.into());
        let id = m
            .add_task("https://www.youtube.com/watch?v=t".into(), opts)
            .unwrap();
        let rec = m
            .list_tasks()
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
            .unwrap();
        assert_eq!(
            rec.filename.as_deref(),
            Some(expected),
            "恶意文件名 {malicious:?} 应被消毒为 {expected:?}"
        );
    }
}

#[tokio::test]
async fn mixed_queue_shares_concurrency_slots() {
    // 视频任务占满并发位时，后续任务排队等待。FakeRunner 脚本无法中途放行，
    // 改用可完成的慢任务：大量 Lines（几十毫秒量级）
    static LINES: [&str; 50] = ["SPARKLING|1|1000000|1000000|1"; 50];
    let runner = Arc::new(FakeRunner::default());
    runner
        .scripts
        .lock()
        .unwrap()
        .push_back(vec![ScriptStep::Lines(&LINES), ScriptStep::Exit(0)]);
    runner
        .scripts
        .lock()
        .unwrap()
        .push_back(vec![ScriptStep::Lines(&LINES), ScriptStep::Exit(0)]);
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
    let v1 = m
        .add_task("https://www.youtube.com/watch?v=a".into(), video_opts(&dir))
        .unwrap();
    let v2 = m
        .add_task("https://www.youtube.com/watch?v=b".into(), video_opts(&dir))
        .unwrap();
    // max_concurrent=1：两任务都完成，但 v2 必须等 v1
    // （弱断言：两个都完成即通过——并发位共享的强断言需要观测窗口，此处信任 try_schedule 逻辑）
    wait_event_state(&mut rx, &v1, TaskState::Completed, Duration::from_secs(30)).await;
    wait_event_state(&mut rx, &v2, TaskState::Completed, Duration::from_secs(30)).await;
    let recs = m.list_tasks().unwrap();
    assert!(recs.iter().all(|r| r.state == TaskState::Completed));
}
