use crate::segment::Segment;
use crate::{Result, SparklingError};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// 断点续传控制文件（`<文件名>.sparkling`）的内容
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlFile {
    pub url: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub total_size: u64,
    pub supports_range: bool,
    pub filename: String,
    pub segments: Vec<Segment>,
}

/// 正式文件 `a.bin` 对应的控制文件路径 `a.bin.sparkling`
pub fn path_for(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_os_string();
    s.push(".sparkling");
    PathBuf::from(s)
}

pub fn exists(ctl_path: &Path) -> bool {
    ctl_path.is_file()
}

/// 原子保存：写 `<名>.sparkling.tmp` 后 rename 覆盖
pub fn save(final_path: &Path, cf: &ControlFile) -> Result<()> {
    let ctl = path_for(final_path);
    let mut tmp = ctl.clone().into_os_string();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let data = serde_json::to_vec(cf)
        .map_err(|e| SparklingError::DiskWrite(format!("控制文件序列化失败: {e}")))?;
    std::fs::write(&tmp, &data)
        .map_err(|e| SparklingError::DiskWrite(format!("控制文件写入失败: {e}")))?;
    // std::fs::rename 在 Windows 上是 MoveFileExW + REPLACE_EXISTING，直接覆盖
    std::fs::rename(&tmp, &ctl)
        .map_err(|e| SparklingError::DiskWrite(format!("控制文件落盘失败: {e}")))?;
    Ok(())
}

/// 加载并校验。JSON 解析失败、IO 错误、分片不变量破坏都算损坏
/// （宁可控文件判损坏后重下，也不用可疑偏移续传）。
pub fn load(ctl_path: &Path) -> Result<ControlFile> {
    let raw = std::fs::read(ctl_path)
        .map_err(|e| SparklingError::CorruptControlFile(format!("读取失败: {e}")))?;
    let cf: ControlFile = serde_json::from_slice(&raw)
        .map_err(|e| SparklingError::CorruptControlFile(format!("JSON 解析失败: {e}")))?;
    for seg in &cf.segments {
        // 注意顺序：end < start 先判（短路），否则 len() 的 u64 减法在 debug 构建下溢 panic
        if seg.end < seg.start || seg.downloaded > seg.len() {
            // 倒置区间下 len() 会下溢：诊断消息用饱和计算，避免诊断路径自身 panic
            let len = seg.end.saturating_sub(seg.start) + 1;
            return Err(SparklingError::CorruptControlFile(format!(
                "分片 {} 不变量破坏: downloaded={} len={}",
                seg.index, seg.downloaded, len
            )));
        }
    }
    Ok(cf)
}
