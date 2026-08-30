use sparkling_core::http_engine::HttpEngine;
use sparkling_core::manager::{AddTaskOptions, Engines, ManagerConfig, TaskManager};
use sparkling_core::store::TaskRecord;
use sparkling_core::task::{TaskKind, VideoMeta, VideoParams};
use sparkling_core::video::bin as vbin;
use sparkling_core::video::engine::extract_error;
use sparkling_core::video::probe::{parse_info_json, VideoInfo};
use sparkling_core::video::runner::{KillReason, RunResult, TokioChildRunner, YtDlpRunner};
use sparkling_core::video::VideoEngine;
use std::path::PathBuf;
use std::sync::Arc;
use tauri::{AppHandle, Emitter, Manager, State};

pub struct AppState {
    pub manager: TaskManager,
    pub config_path: PathBuf,
    pub default_save_dir: PathBuf,
    pub video: VideoService,
}

/// yt-dlp 运行环境状态（前端设置页展示）
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
        app.path()
            .resource_dir()
            .ok()
            .map(|d| d.join("bin").join(name)),
        std::env::current_exe()
            .ok()
            .and_then(|e| e.parent().map(|d| d.join("bin").join(name))),
        Some(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("bin")
                .join(name),
        ),
    ];
    candidates.into_iter().flatten().find(|p| p.exists())
}

/// ③期视频服务：解析/cookie 导出共用的 runner 与二进制布局信息
pub struct VideoService {
    pub runner: Arc<TokioChildRunner>,
    pub ffmpeg: Option<PathBuf>,
    /// app data/bin（更新版 yt-dlp 落点）
    pub app_bin_dir: PathBuf,
    pub packed_ytdlp: Option<PathBuf>,
    /// app data/cookies.txt
    pub cookie_file: PathBuf,
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
    kind: Option<String>,
    video: Option<VideoParams>,
    video_meta: Option<VideoMeta>,
) -> Result<String, String> {
    // 新增参数全 Option：旧前端 invoke 不传 → http 任务（①期行为不变）
    let kind = TaskKind::parse(kind.as_deref().unwrap_or("http")).ok_or("未知任务类型")?;
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

/// 视频解析：yt-dlp -J --flat-playlist（60s 超时；stdout 全量收集后解析）。
/// cookie 文件存在时带上——否则 B 站会员等登录内容的 probe 看不到会员档位
#[tauri::command]
async fn probe_video(state: State<'_, AppState>, url: String) -> Result<VideoInfo, String> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let runner = state.video.runner.clone();
    let mut args = vec!["-J".into(), "--flat-playlist".into()];
    if state.video.cookie_file.exists() {
        args.push("--cookies".into());
        args.push(state.video.cookie_file.display().to_string());
    }
    args.push(url);
    let mut handle = runner
        .start(
            args,
            Box::new(move |l| {
                let _ = tx.send(l.to_string());
            }),
        )
        .await
        .map_err(|e| e.user_message())?;
    let mut out = String::new();
    let timeout = tokio::time::sleep(std::time::Duration::from_secs(60));
    tokio::pin!(timeout);
    // 三臂 select：行收集 / 进程退出 / 超时。tokio::select! 的 handler 在其余
    // 分支的 future 全部 drop 之后才执行，因此跨臂借用 rx/out/handle 均合法
    //（与 core VideoEngine 的 run 循环同款形态）。
    let res = tokio::select! {
        // 行收干：channel 关闭（sender 随进程任务结束 drop）→ 全部行已收完，
        // 顺势取进程退出结果（done 此时必已就绪，await 立即返回）
        _ = async {
            while let Some(l) = rx.recv().await {
                out.push_str(&l);
            }
        } => (&mut handle.done).await,
        // 进程先退：把 channel 缓冲里的残余行收干（sender 已 drop，干涸即止）
        res = &mut handle.done => {
            while let Ok(l) = rx.try_recv() {
                out.push_str(&l);
            }
            res
        }
        // 60s 超时：杀进程并等待回收后再报错
        _ = &mut timeout => {
            handle.kill(KillReason::Cancel);
            let _ = (&mut handle.done).await;
            return Err("解析超时（60 秒），请重试".into());
        }
    };
    let res = res.unwrap_or_else(|_| RunResult {
        killed: None,
        code: None,
        stderr_tail: "runner 任务异常退出".into(),
    });
    if res.code != Some(0) {
        let msg = extract_error(&res.stderr_tail);
        return Err(if msg.is_empty() {
            format!("解析失败（退出码 {:?}）", res.code)
        } else {
            msg
        });
    }
    parse_info_json(&out).map_err(|e| e.user_message())
}

#[tauri::command]
async fn get_ytdlp_status(state: State<'_, AppState>) -> Result<YtdlpStatus, String> {
    let bin = vbin::resolve_ytdlp(
        &state.video.app_bin_dir,
        state
            .video
            .packed_ytdlp
            .as_deref()
            .unwrap_or_else(|| std::path::Path::new("yt-dlp.exe")),
    );
    let version = vbin::ytdlp_version(&bin).await.ok();
    let source = if !bin.exists() {
        "missing"
    } else if state.video.app_bin_dir.join("yt-dlp.exe").exists() {
        "app-data"
    } else {
        "bundled"
    };
    Ok(YtdlpStatus {
        version,
        source: source.to_string(),
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
    // 不走 runner.start（面向流式下载）：一次性 output() 更简洁
    let mut cmd = tokio::process::Command::new(&state.video.runner.bin);
    cmd.arg("--cookies-from-browser")
        .arg(&browser)
        .arg("--cookies")
        .arg(&state.video.cookie_file)
        .arg("--simulate")
        .arg("https://www.youtube.com")
        // 中文 Windows 的 GBK stdout 编码防御（与 TokioChildRunner 同因，见其注释）
        .env("PYTHONUTF8", "1");
    #[cfg(windows)]
    {
        // CREATE_NO_WINDOW：GUI 进程 spawn 控制台二进制不闪窗（与 runner/bin.rs 一致）
        cmd.creation_flags(0x0800_0000);
    }
    let out = cmd
        .output()
        .await
        .map_err(|e| format!("运行 yt-dlp 失败: {e}"))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        let msg = extract_error(&stderr);
        return Err(if msg.is_empty() {
            format!(
                "导出 cookie 失败（退出码 {}）",
                out.status.code().unwrap_or(-1)
            )
        } else {
            msg
        });
    }
    Ok(())
}

#[tauri::command]
fn clear_cookies(state: State<AppState>) -> Result<(), String> {
    let _ = std::fs::remove_file(&state.video.cookie_file);
    Ok(())
}

/// Windows 标题栏定制：
/// - WS_EX_DLGMODALFRAME（对话框框架）：标题栏结构性无图标槽 —— 图标照常设置
///   （任务栏/Alt+Tab/悬停缩略图正常取用），但标题栏不绘制。这是 Win32 经典方案
/// - 标题栏染成 chrome（#102235，与前端工具栏色带一致），边框染成 line（#23385C）
/// - 标题文字颜色 = 标题栏底色（标题在标题栏内隐身，Alt+Tab/任务栏/缩略图正常显示）
/// - 窗口大/小图标 = exe 资源星标（tauri-build 以 ID 32512 嵌入 icons/icon.ico）
/// - 系统按钮、原生拖拽、双击最大化、Snap Layouts 全部保留
#[cfg(target_os = "windows")]
fn style_title_bar(hwnd: windows_sys::Win32::Foundation::HWND) {
    use windows_sys::Win32::Graphics::Dwm::{
        DwmSetWindowAttribute, DWMWA_BORDER_COLOR, DWMWA_CAPTION_COLOR, DWMWA_TEXT_COLOR,
    };
    use windows_sys::Win32::Graphics::Gdi::{
        RedrawWindow, RDW_FRAME, RDW_INVALIDATE, RDW_UPDATENOW,
    };
    use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        GetSystemMetrics, GetWindowLongPtrW, LoadImageW, SendMessageW, SetWindowLongPtrW,
        SetWindowPos, GWL_EXSTYLE, ICON_BIG, ICON_SMALL, IMAGE_ICON, LR_DEFAULTSIZE, LR_SHARED,
        SM_CXSMICON, SM_CYSMICON, SWP_FRAMECHANGED, SWP_NOACTIVATE, SWP_NOMOVE, SWP_NOSIZE,
        SWP_NOZORDER, WM_SETICON, WS_EX_DLGMODALFRAME,
    };
    unsafe {
        // 大图标（任务栏/Alt+Tab）与小图标（任务栏悬停缩略图）都设为 exe 资源星标
        let module = GetModuleHandleW(std::ptr::null());
        let big = LoadImageW(
            module,
            32512 as windows_sys::core::PCWSTR,
            IMAGE_ICON,
            0,
            0,
            LR_DEFAULTSIZE | LR_SHARED,
        );
        if !big.is_null() {
            SendMessageW(hwnd, WM_SETICON, ICON_BIG as usize, big as isize);
        }
        let small = LoadImageW(
            module,
            32512 as windows_sys::core::PCWSTR,
            IMAGE_ICON,
            GetSystemMetrics(SM_CXSMICON),
            GetSystemMetrics(SM_CYSMICON),
            LR_SHARED,
        );
        if !small.is_null() {
            SendMessageW(hwnd, WM_SETICON, ICON_SMALL as usize, small as isize);
        }
        // 对话框框架：标题栏不再有图标槽（图标本身照设，别处照常取用）
        let ex = GetWindowLongPtrW(hwnd, GWL_EXSTYLE);
        SetWindowLongPtrW(hwnd, GWL_EXSTYLE, ex | WS_EX_DLGMODALFRAME as isize);
        SetWindowPos(
            hwnd,
            std::ptr::null_mut(),
            0,
            0,
            0,
            0,
            SWP_FRAMECHANGED | SWP_NOMOVE | SWP_NOSIZE | SWP_NOZORDER | SWP_NOACTIVATE,
        );
        // COLORREF 布局为 0x00BBGGRR
        let chrome: u32 = 0x0035_2210; // #102235 chrome（与工具栏色带一致）
        DwmSetWindowAttribute(
            hwnd,
            DWMWA_CAPTION_COLOR as u32,
            &chrome as *const _ as *const core::ffi::c_void,
            4,
        );
        // 标题文字与标题栏同色：标题栏内隐身，Alt+Tab/任务栏照常显示标题
        DwmSetWindowAttribute(
            hwnd,
            DWMWA_TEXT_COLOR as u32,
            &chrome as *const _ as *const core::ffi::c_void,
            4,
        );
        let border: u32 = 0x005C_3823; // #23385C line
        DwmSetWindowAttribute(
            hwnd,
            DWMWA_BORDER_COLOR as u32,
            &border as *const _ as *const core::ffi::c_void,
            4,
        );
        // 强制非客户区立即重绘
        RedrawWindow(
            hwnd,
            std::ptr::null(),
            std::ptr::null_mut(),
            RDW_FRAME | RDW_INVALIDATE | RDW_UPDATENOW,
        );
    }
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
            // ③期视频引擎接线：候选链发现 yt-dlp/ffmpeg，app data 更新版优先；
            // cookie 文件存在即启用（导入见 import_cookies 命令）
            let app_data = app
                .path()
                .app_data_dir()
                .unwrap_or_else(|_| PathBuf::from("."));
            std::fs::create_dir_all(app_data.join("bin")).ok();
            let packed_ytdlp = find_binary(app.handle(), "yt-dlp.exe");
            let ffmpeg = find_binary(app.handle(), "ffmpeg.exe");
            let ytdlp_bin = vbin::resolve_ytdlp(
                &app_data.join("bin"),
                packed_ytdlp
                    .as_deref()
                    .unwrap_or_else(|| std::path::Path::new("yt-dlp.exe")),
            );
            let video_engine = VideoEngine::new(
                Arc::new(TokioChildRunner {
                    bin: ytdlp_bin.clone(),
                }),
                ffmpeg.clone(),
                app_data
                    .join("cookies.txt")
                    .exists()
                    .then(|| app_data.join("cookies.txt")),
            );
            // C1：命令层是同步 fn，tauri 在 WebView2 COM 回调线程上内联执行
            // （无 ambient runtime）——manager 必须持 Handle 才能从那里 spawn。
            // tauri::async_runtime::handle() 返回自身包装的 RuntimeHandle，
            // inner() 借出内部的 tokio Handle
            let manager = TaskManager::new(
                &config_dir.join("tasks.db"),
                Engines {
                    http: Arc::new(HttpEngine::new(cfg.global_speed_limit)),
                    video: Arc::new(video_engine),
                },
                cfg,
                tauri::async_runtime::handle().inner().clone(),
            )
            .expect("初始化任务管理器失败");
            app.manage(AppState {
                manager,
                config_path,
                default_save_dir,
                video: VideoService {
                    runner: Arc::new(TokioChildRunner { bin: ytdlp_bin }),
                    ffmpeg,
                    app_bin_dir: app_data.join("bin"),
                    packed_ytdlp,
                    cookie_file: app_data.join("cookies.txt"),
                },
            });

            // 恢复上次未完成任务（默认自动续传）
            {
                let state: State<AppState> = app.state();
                state.manager.recover().ok();
            }

            // 标题栏定制：系统标题栏染成主题色，隐藏图标与标题文字
            #[cfg(target_os = "windows")]
            if let Some(win) = app.get_webview_window("main") {
                if let Ok(hwnd) = win.hwnd() {
                    style_title_bar(hwnd.0);
                }
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
            add_task,
            pause_task,
            resume_task,
            cancel_task,
            retry_task,
            remove_task,
            move_to_top,
            list_tasks,
            get_config,
            update_config,
            probe_video,
            get_ytdlp_status,
            update_ytdlp,
            import_cookies,
            clear_cookies
        ])
        .run(tauri::generate_context!())
        .expect("运行 Sparkling 失败");
}
