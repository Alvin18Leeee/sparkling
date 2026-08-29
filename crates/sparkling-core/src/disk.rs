use crate::{Result, SparklingError};
use std::path::Path;

/// 需要的磁盘空间 = 文件大小 × 1.02
pub fn required_space(total: u64) -> u64 {
    total + total / 50
}

pub fn check_space(dir: &Path, required: u64) -> Result<()> {
    let available = fs2::available_space(dir)
        .map_err(|e| SparklingError::DiskWrite(format!("无法查询磁盘剩余空间: {e}")))?;
    if available < required {
        return Err(SparklingError::InsufficientDisk {
            required,
            available,
        });
    }
    Ok(())
}
