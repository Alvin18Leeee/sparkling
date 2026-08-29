use thiserror::Error;

/// 统一错误类型：user_message() 给用户看（中文），technical() 给详情面板看。
/// Clone：worker 失败上报（shared.fail）与返回值各需一份。
#[derive(Debug, Clone, Error)]
pub enum SparklingError {
    #[error("网络错误: {0}")]
    Network(String),

    #[error("服务器返回 {status}: {detail}")]
    HttpStatus { status: u16, detail: String },

    #[error("磁盘空间不足: 需要 {required} 字节, 剩余 {available} 字节")]
    InsufficientDisk { required: u64, available: u64 },

    #[error("控制文件损坏: {0}")]
    CorruptControlFile(String),

    #[error("远端文件已变化: {0}")]
    RemoteChanged(String),

    #[error("磁盘写入失败: {0}")]
    DiskWrite(String),

    #[error("完整性校验失败: 期望 {expected}, 实际 {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("任务不存在: {0}")]
    TaskNotFound(String),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, SparklingError>;

impl SparklingError {
    /// 用户可读的中文消息
    pub fn user_message(&self) -> String {
        self.to_string()
    }

    /// 技术细节（状态码、内部结构）
    pub fn technical(&self) -> String {
        format!("{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_message_is_chinese_and_readable() {
        let e = SparklingError::InsufficientDisk {
            required: 1000,
            available: 500,
        };
        assert!(e.user_message().contains("磁盘空间不足"));
        assert!(e.user_message().contains("1000"));
    }

    #[test]
    fn technical_keeps_debug_detail() {
        let e = SparklingError::HttpStatus {
            status: 503,
            detail: "unavailable".into(),
        };
        assert!(e.technical().contains("503"));
        assert!(e.technical().contains("HttpStatus"));
    }

    #[test]
    fn remote_changed_and_checksum_have_distinct_messages() {
        let e = SparklingError::RemoteChanged("etag".into());
        assert!(e.user_message().contains("已变化"));
        let e = SparklingError::ChecksumMismatch {
            expected: "a".into(),
            actual: "b".into(),
        };
        assert!(e.user_message().contains("校验"));
    }
}
