mod common;

use common::{poll_until, sha256_hex, start, wait_event_state, ServerConfig};
use sparkling_core::http_engine::{HttpEngine, RetryPolicy};
use sparkling_core::manager::{AddTaskOptions, ManagerConfig, TaskEvent, TaskManager};
use sparkling_core::task::{TaskKind, TaskState};
use std::sync::Arc;
use std::time::Duration;

fn manager(dir: &tempfile::TempDir, cfg: ManagerConfig) -> TaskManager {
    TaskManager::new(
        &dir.path().join("tasks.db"),
        Arc::new(HttpEngine::new_with_policy(None, RetryPolicy::fast())),
        cfg,
        tokio::runtime::Handle::current(),
    )
    .unwrap()
}

fn opts(dir: &tempfile::TempDir, max_speed: Option<u64>) -> AddTaskOptions {
    AddTaskOptions {
        save_dir: dir.path().to_path_buf(),
        filename: None,
        segments: Some(4),
        max_speed,
        kind: TaskKind::Http,
        video: None,
        video_meta: None,
    }
}

#[tokio::test]
async fn add_task_completes_and_persists() {
    let server = start(ServerConfig {
        size: 256 * 1024,
        ..Default::default()
    })
    .await;
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
    let server_a = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let server_b = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(
        &dir,
        ManagerConfig {
            max_concurrent: 1,
            ..Default::default()
        },
    );
    let id_a = m
        .add_task(server_a.url.clone(), opts(&dir, Some(200_000)))
        .unwrap();
    let id_b = m
        .add_task(server_b.url.clone(), opts(&dir, Some(200_000)))
        .unwrap();
    // B 必须等 A 完成后才 Running；全程不得出现两个 Running
    let mut saw_double_running = false;
    poll_until(Duration::from_secs(60), || {
        let recs = m.list_tasks().unwrap();
        let running = recs
            .iter()
            .filter(|r| r.state == TaskState::Running)
            .count();
        if running > 1 {
            saw_double_running = true;
        }
        recs.iter()
            .all(|r| r.state == TaskState::Completed)
            .then_some(())
    })
    .await;
    assert!(!saw_double_running, "不得超过 max_concurrent");
    assert!(dir.path().join("file.bin").exists());
    let _ = (id_a, id_b);
}

#[tokio::test]
async fn pause_resume_cancel_via_manager() {
    let server = start(ServerConfig {
        size: 2 * 1024 * 1024,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig::default());
    let mut rx = m.subscribe();
    let id = m
        .add_task(server.url.clone(), opts(&dir, Some(300_000)))
        .unwrap();
    wait_event_state(&mut rx, &id, TaskState::Running, Duration::from_secs(10)).await;

    m.pause_task(&id).unwrap();
    poll_until(Duration::from_secs(10), || {
        let r = m
            .list_tasks()
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
            .unwrap();
        (r.state == TaskState::Paused).then_some(())
    })
    .await;

    m.resume_task(&id).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Completed, Duration::from_secs(60)).await;

    // 取消路径
    let id2 = m
        .add_task(server.url.clone(), opts(&dir, Some(200_000)))
        .unwrap();
    wait_event_state(&mut rx, &id2, TaskState::Running, Duration::from_secs(10)).await;
    m.cancel_task(&id2).unwrap();
    wait_event_state(&mut rx, &id2, TaskState::Cancelled, Duration::from_secs(10)).await;
    let r = m
        .list_tasks()
        .unwrap()
        .into_iter()
        .find(|r| r.id == id2)
        .unwrap();
    assert_eq!(r.state, TaskState::Cancelled);
}

#[tokio::test]
async fn retry_is_idempotent() {
    // D36：双击重试不得二次入队/二次提交（两个引擎写同一 .part 会损坏）
    let server = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig::default());
    let id = m
        .add_task(server.url.clone(), opts(&dir, Some(400_000)))
        .unwrap();
    let mut rx = m.subscribe();
    wait_event_state(&mut rx, &id, TaskState::Completed, Duration::from_secs(60)).await;
    // 完成后连点两次重试 + 一次取消：都应是安全 no-op
    m.retry_task(&id).unwrap();
    m.retry_task(&id).unwrap();
    m.cancel_task(&id).unwrap();
    let rec = m
        .list_tasks()
        .unwrap()
        .into_iter()
        .find(|r| r.id == id)
        .unwrap();
    assert_eq!(rec.state, TaskState::Completed, "终态不得被重试/取消改写");
}

#[tokio::test]
async fn cancel_queued_task_dequeues() {
    // D36：排队任务（无句柄）取消 → 出队 + Cancelled，不再被调度
    let server_a = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let server_b = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    let m = manager(
        &dir_a,
        ManagerConfig {
            max_concurrent: 1,
            ..Default::default()
        },
    );
    let mut rx = m.subscribe();
    let id_a = m
        .add_task(server_a.url.clone(), opts(&dir_a, Some(300_000)))
        .unwrap();
    let id_b = m
        .add_task(server_b.url.clone(), opts(&dir_b, Some(300_000)))
        .unwrap();
    // B 在排队（A 占用唯一并发位）→ 取消 B
    poll_until(Duration::from_secs(10), || {
        let recs = m.list_tasks().unwrap();
        recs.iter()
            .find(|r| r.id == id_b)
            .filter(|r| r.state == TaskState::Queued)
            .map(|_| ())
    })
    .await;
    m.cancel_task(&id_b).unwrap();
    let rec_b = m
        .list_tasks()
        .unwrap()
        .into_iter()
        .find(|r| r.id == id_b)
        .unwrap();
    assert_eq!(rec_b.state, TaskState::Cancelled);
    // A 正常完成；B 被取消不落盘
    wait_event_state(
        &mut rx,
        &id_a,
        TaskState::Completed,
        Duration::from_secs(60),
    )
    .await;
    assert!(dir_a.path().join("file.bin").exists());
    assert!(!dir_b.path().join("file.bin").exists());
}

#[tokio::test]
async fn retry_failed_continues_from_control_file() {
    // 参数偏离 brief（512KB / segments 4 → 2MB / segments 2 + drop_after）：
    // 引擎开工即把所有分段的请求全部发出（每段一个 worker，一批覆盖全部段），
    // 已建立的响应不受 fail() 影响——512KB/4 段一批全在途，fail() 后任务照样
    // 完成，永远到不了 Failed。drop_after=512KB 让每个响应中途掐断，worker
    // 必须发新请求续传，撞上 fail() 的 500 → 任务 Failed（D33：掐断限额须
    // 大于 hyper 写缓冲 ~400KB，且分段须大于限额）。限速沿用 brief 的 400KB/s。
    let server = start(ServerConfig {
        size: 2 * 1024 * 1024,
        drop_after: Some(512 * 1024),
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig::default());
    let mut rx = m.subscribe();
    let mut o = opts(&dir, Some(400_000));
    o.segments = Some(2);
    let id = m.add_task(server.url.clone(), o).unwrap();
    wait_event_state(&mut rx, &id, TaskState::Running, Duration::from_secs(10)).await;
    // Running 事件早于引擎探测完成——等到有真实进度再注入 500（"中途"失败，
    // 也保证控制文件已落盘；直接 fail 会打死探测，任务根本没下过字节）
    poll_until(Duration::from_secs(10), || {
        let r = m
            .list_tasks()
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
            .unwrap();
        (r.downloaded > 50_000).then_some(())
    })
    .await;
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
    let server = start(ServerConfig {
        size: 1024 * 1024,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    {
        let m = manager(&dir, ManagerConfig::default());
        let id = m
            .add_task(server.url.clone(), opts(&dir, Some(200_000)))
            .unwrap();
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
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut rx = m2.subscribe();
    m2.recover().unwrap();
    // 重启后只有一个任务，直接等待任意 Completed 事件
    let ev = loop {
        let ev = tokio::time::timeout(Duration::from_secs(60), rx.recv())
            .await
            .expect("等待恢复完成超时")
            .expect("事件通道关闭");
        if let TaskEvent::State {
            state: TaskState::Completed,
            ..
        } = &ev
        {
            break ev;
        }
    };
    let _ = ev;
}

#[tokio::test]
async fn recovery_corrupt_ctl_marks_failed() {
    let server = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(&dir, ManagerConfig::default());
    let _id = m
        .add_task(server.url.clone(), opts(&dir, Some(200_000)))
        .unwrap();
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
    let server = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    {
        let m = manager(&dir, ManagerConfig::default());
        let _id = m
            .add_task(server.url.clone(), opts(&dir, Some(200_000)))
            .unwrap();
        poll_until(Duration::from_secs(20), || {
            let r = m.list_tasks().unwrap().into_iter().next().unwrap();
            (r.downloaded > 50_000).then_some(())
        })
        .await;
        m.shutdown();
    }
    let m2 = manager(
        &dir,
        ManagerConfig {
            auto_resume_on_start: false,
            ..Default::default()
        },
    );
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
    let server_a = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let server_b = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let server_c = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let m = manager(
        &dir,
        ManagerConfig {
            max_concurrent: 1,
            ..Default::default()
        },
    );
    let mut rx = m.subscribe();
    let id_a = m
        .add_task(server_a.url.clone(), opts(&dir, Some(300_000)))
        .unwrap();
    let id_b = m
        .add_task(server_b.url.clone(), opts(&dir, Some(300_000)))
        .unwrap();
    let id_c = m
        .add_task(server_c.url.clone(), opts(&dir, Some(300_000)))
        .unwrap();
    m.move_to_top(&id_c).unwrap();
    wait_event_state(
        &mut rx,
        &id_a,
        TaskState::Completed,
        Duration::from_secs(60),
    )
    .await;
    // A 完成后下一个运行的应是 C（被置顶）
    let next_running = poll_until(Duration::from_secs(30), || {
        let recs = m.list_tasks().unwrap();
        recs.iter()
            .find(|r| r.state == TaskState::Running)
            .map(|r| r.id.clone())
    })
    .await;
    assert_eq!(next_running, id_c);
    wait_event_state(
        &mut rx,
        &id_c,
        TaskState::Completed,
        Duration::from_secs(60),
    )
    .await;
    wait_event_state(
        &mut rx,
        &id_b,
        TaskState::Completed,
        Duration::from_secs(60),
    )
    .await;
}

#[tokio::test]
async fn add_task_works_off_runtime_thread() {
    // C1 回归：TaskManager 持 runtime Handle 时，无 ambient runtime 的裸线程
    // （Tauri 的 WebView2 COM 回调线程同款处境）调用 add_task 不得 panic，
    // 且任务经 Handle::spawn 正常落在 runtime 上完成。
    // 旧实现（裸 tokio::spawn）会在该线程里 panic，首个「新建下载」即崩
    let server = start(ServerConfig {
        size: 64 * 1024,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let handle = tokio::runtime::Handle::current();
    let url = server.url.clone();
    let save_dir = dir.path().to_path_buf();
    let db = dir.path().join("tasks.db");
    let engine: std::sync::Arc<dyn sparkling_core::engine::Engine> =
        std::sync::Arc::new(HttpEngine::new_with_policy(None, RetryPolicy::fast()));
    // 构造 + add_task 全在裸线程上完成；manager 一并带回（同一条 store 连接
    // 观察完成态，避免第二个连接碰 SQLite 锁）
    let (m, id) = std::thread::spawn(move || {
        let m = TaskManager::new(&db, engine, ManagerConfig::default(), handle).unwrap();
        let id = m
            .add_task(
                url,
                AddTaskOptions {
                    save_dir,
                    filename: None,
                    segments: Some(2),
                    max_speed: None,
                    kind: TaskKind::Http,
                    video: None,
                    video_meta: None,
                },
            )
            .unwrap();
        (m, id)
    })
    .join()
    .expect("裸线程调用不得 panic");
    poll_until(Duration::from_secs(30), || {
        m.list_tasks()
            .unwrap()
            .into_iter()
            .find(|r| r.id == id)
            .filter(|r| r.state == TaskState::Completed)
            .map(|_| ())
    })
    .await;
    assert!(dir.path().join("file.bin").exists());
}

#[tokio::test]
async fn remove_task_cleans_orphan_ctl_and_part() {
    // M2 回归：无句柄任务（重启残留的 Running/Paused）删除时须一并清掉
    // ctl/.part——否则之后同 URL 重新添加会静默从旧控制文件续传
    let server = start(ServerConfig {
        size: 512 * 1024,
        ..Default::default()
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    {
        let m = manager(&dir, ManagerConfig::default());
        let _id = m
            .add_task(server.url.clone(), opts(&dir, Some(200_000)))
            .unwrap();
        poll_until(Duration::from_secs(20), || {
            let r = m.list_tasks().unwrap().into_iter().next().unwrap();
            (r.downloaded > 50_000).then_some(())
        })
        .await;
        m.shutdown(); // 模拟应用退出：ctl/.part 留盘，任务残留 Running（无句柄）
    }
    assert!(dir.path().join("file.bin.sparkling").exists());
    assert!(dir.path().join("file.bin.sparkling.part").exists());

    let m2 = manager(&dir, ManagerConfig::default());
    let recs = m2.list_tasks().unwrap();
    assert_eq!(recs.len(), 1);
    m2.remove_task(&recs[0].id).unwrap();
    assert!(
        !dir.path().join("file.bin.sparkling").exists(),
        "残留 ctl 应被清理"
    );
    assert!(
        !dir.path().join("file.bin.sparkling.part").exists(),
        "残留 .part 应被清理"
    );
    assert!(m2.list_tasks().unwrap().is_empty());
}
