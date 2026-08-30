//! yt-dlp/ffmpeg 二进制管理：发现（app data 更新版优先于打包基线）、
//! 版本查询、自更新下载（原子替换）。
use crate::{Result, SparklingError};
use std::path::{Path, PathBuf};

/// 选定 yt-dlp 路径：app data 更新版存在即优先（更新动作保证其总是更新过的）
pub fn resolve_ytdlp(app_bin: &Path, packed: &Path) -> PathBuf {
    let updated = app_bin.join("yt-dlp.exe");
    if updated.exists() {
        updated
    } else {
        packed.to_path_buf()
    }
}

/// "2026.08.29" → (2026, 8, 29)；容忍 4 段式 nightly 版本
pub fn parse_version(s: &str) -> Option<(u32, u32, u32)> {
    let mut it = s.trim().split('.');
    let major: u32 = it.next()?.parse().ok()?;
    let minor: u32 = it.next()?.parse().ok()?;
    let patch: u32 = it.next()?.parse().ok()?;
    Some((major, minor, patch))
}

/// a 是否严格大于 b（任一解析失败 → false，保守不更新）
pub fn version_gt(a: &str, b: &str) -> bool {
    match (parse_version(a), parse_version(b)) {
        (Some(x), Some(y)) => x > y,
        _ => false,
    }
}

/// 跑 `yt-dlp --version`，取 stdout 首行
pub async fn ytdlp_version(bin: &Path) -> Result<String> {
    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("--version")
        // 中文 Windows 的 GBK stdout 编码防御（与 TokioChildRunner 同因，见其注释）
        .env("PYTHONUTF8", "1");
    #[cfg(windows)]
    {
        // CREATE_NO_WINDOW：GUI 进程 spawn 控制台二进制不闪窗，与 TokioChildRunner 一致
        cmd.creation_flags(0x0800_0000);
    }
    let out = cmd
        .output()
        .await
        .map_err(|e| SparklingError::Other(format!("运行 yt-dlp 失败: {e}")))?;
    if !out.status.success() {
        return Err(SparklingError::Other("yt-dlp 版本查询失败".into()));
    }
    let s = String::from_utf8_lossy(&out.stdout);
    Ok(s.lines().next().unwrap_or_default().trim().to_string())
}

/// 下载到 dest.tmp 后原子 rename 到 dest
pub async fn download_replace(url: &str, dest: &Path) -> Result<()> {
    // connect_timeout 限连接建立；read_timeout 限两次读之间——不限总时长
    // （大文件慢网允许慢慢下），但完全停滞的连接（GitHub 直连在国内网络
    // 常见"连上但不动"）30 秒报错，不再无限挂起（真机验收复现"始终处理中"）
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .read_timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| SparklingError::Other(format!("构建 HTTP 客户端失败: {e}")))?;
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| SparklingError::Network(format!("下载失败: {e}")))?;
    if !resp.status().is_success() {
        return Err(SparklingError::HttpStatus {
            status: resp.status().as_u16(),
            detail: format!("下载 {url} 失败"),
        });
    }
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| SparklingError::Network(format!("读取响应失败: {e}")))?;
    if bytes.is_empty() {
        return Err(SparklingError::Other("下载内容为空".into()));
    }
    let tmp = dest.with_extension("exe.tmp");
    if let Some(dir) = dest.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| SparklingError::DiskWrite(format!("创建目录失败: {e}")))?;
    }
    std::fs::write(&tmp, &bytes)
        .map_err(|e| SparklingError::DiskWrite(format!("写入临时文件失败: {e}")))?;
    std::fs::rename(&tmp, dest)
        .map_err(|e| SparklingError::DiskWrite(format!("替换二进制失败: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_shapes() {
        assert_eq!(parse_version("2026.08.29"), Some((2026, 8, 29)));
        assert_eq!(parse_version("2026.8.9"), Some((2026, 8, 9)));
        assert_eq!(parse_version("2026.08.29.12345"), Some((2026, 8, 29))); // nightly 后缀容忍
        assert_eq!(parse_version("bogus"), None);
        assert_eq!(parse_version(""), None);
    }

    #[test]
    fn version_ordering() {
        assert!(version_gt("2026.09.01", "2026.08.29"));
        assert!(version_gt("2027.01.01", "2026.12.31"));
        assert!(!version_gt("2026.08.29", "2026.08.29"));
        assert!(!version_gt("bogus", "2026.08.29"), "解析失败按不更新处理");
        assert!(!version_gt("2026.09.01", "bogus"));
    }

    #[test]
    fn resolve_prefers_app_data_binary() {
        let dir = tempfile::tempdir().unwrap();
        let app_bin = dir.path().join("bin");
        std::fs::create_dir_all(&app_bin).unwrap();
        std::fs::write(app_bin.join("yt-dlp.exe"), b"updated").unwrap();
        let resolved = resolve_ytdlp(&app_bin, Path::new("packed/yt-dlp.exe"));
        assert_eq!(resolved, app_bin.join("yt-dlp.exe"));
        // 无更新版 → 回退打包版
        let empty = dir.path().join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        assert_eq!(
            resolve_ytdlp(&empty, Path::new("packed/yt-dlp.exe")),
            PathBuf::from("packed/yt-dlp.exe")
        );
    }

    #[tokio::test]
    async fn download_replace_writes_file_atomically() {
        // 本地 axum 服务器充当下载源（复用 dev-dependency axum）
        use axum::routing::get;
        let app = axum::Router::new().route("/yt-dlp.exe", get(|| async { "BINBYTES" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("yt-dlp.exe");
        download_replace(&format!("http://{addr}/yt-dlp.exe"), &dest)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), b"BINBYTES");
        assert!(!dir.path().join("yt-dlp.exe.tmp").exists(), "tmp 应已改名");
    }
}
