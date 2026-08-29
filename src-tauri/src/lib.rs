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

/// Windows 标题栏定制：
/// - 标题栏染成 chrome（#102235，与前端工具栏色带一致），边框染成 line（#23385C）
/// - 标题文字颜色 = 标题栏底色（标题在标题栏内隐身，但 Alt+Tab/任务栏正常显示"Sparkling"）
/// - 窗口小图标设为全透明 1×1（标题栏不显示 icon；两级图标清空会回落到
///   exe 资源/默认占位图，透明图标才能真隐藏），类图标清空防回退
/// - 任务栏/Alt-Tab 图标走窗口大图标 → exe 资源（icons/icon.ico 真图标）
/// - 系统按钮、原生拖拽、双击最大化、Snap Layouts 全部保留
#[cfg(target_os = "windows")]
fn style_title_bar(hwnd: windows_sys::Win32::Foundation::HWND) {
    use windows_sys::Win32::Graphics::Dwm::{
        DwmSetWindowAttribute, DWMWA_BORDER_COLOR, DWMWA_CAPTION_COLOR, DWMWA_TEXT_COLOR,
    };
    use windows_sys::Win32::Graphics::Gdi::{
        CreateBitmap, DeleteObject, RedrawWindow, RDW_FRAME, RDW_INVALIDATE, RDW_UPDATENOW,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        CreateIconIndirect, SendMessageW, SetClassLongPtrW, GCLP_HICON, GCLP_HICONSM, ICONINFO,
        ICON_SMALL, WM_SETICON,
    };
    unsafe {
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
        // 全透明 1×1 单色图标：AND 位 1 = 不绘制（清空图标只会回落到占位图）
        // 单色图标 hbmColor 为空时，mask 高度 = 2（上行 AND、下行 XOR）
        let bits: [u8; 4] = [0x80, 0x00, 0x00, 0x00];
        let mask = CreateBitmap(1, 2, 1, 1, bits.as_ptr() as *const core::ffi::c_void);
        let mut info = ICONINFO {
            fIcon: 1,
            xHotspot: 0,
            yHotspot: 0,
            hbmMask: mask,
            hbmColor: std::ptr::null_mut(),
        };
        let invisible = CreateIconIndirect(&mut info);
        // 类图标清空 + 小图标换透明
        SetClassLongPtrW(hwnd, GCLP_HICON, 0);
        SetClassLongPtrW(hwnd, GCLP_HICONSM, 0);
        SendMessageW(hwnd, WM_SETICON, ICON_SMALL as usize, invisible as isize);
        // 强制非客户区立即重绘（防图标清除后残影）
        RedrawWindow(
            hwnd,
            std::ptr::null(),
            std::ptr::null_mut(),
            RDW_FRAME | RDW_INVALIDATE | RDW_UPDATENOW,
        );
        // CreateIconIndirect 复制了位图，临时 mask 可回收；invisible 归 WM_SETICON 所有
        DeleteObject(mask);
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
            let engine: Arc<dyn sparkling_core::engine::Engine> =
                Arc::new(HttpEngine::new(cfg.global_speed_limit));
            // C1：命令层是同步 fn，tauri 在 WebView2 COM 回调线程上内联执行
            // （无 ambient runtime）——manager 必须持 Handle 才能从那里 spawn。
            // tauri::async_runtime::handle() 返回自身包装的 RuntimeHandle，
            // inner() 借出内部的 tokio Handle
            let manager = TaskManager::new(
                &config_dir.join("tasks.db"),
                engine,
                cfg,
                tauri::async_runtime::handle().inner().clone(),
            )
            .expect("初始化任务管理器失败");
            app.manage(AppState { manager, config_path, default_save_dir });

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
            add_task, pause_task, resume_task, cancel_task, retry_task,
            remove_task, move_to_top, list_tasks, get_config, update_config
        ])
        .run(tauri::generate_context!())
        .expect("运行 Sparkling 失败");
}
