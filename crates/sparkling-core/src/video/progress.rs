//! yt-dlp --progress-template 输出行解析（纯函数，无 IO）
//!
//! 模板（engine 构造命令时使用同款字符串）：
//! download:SPARKLING|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|
//!           %(progress.total_bytes_estimate)s|%(progress.speed)s

/// 一行进度（字段 NA → None）
#[derive(Debug, Clone, PartialEq)]
pub struct ProgressLine {
    pub downloaded: u64,
    /// total_bytes 优先；缺失回退 total_bytes_estimate
    pub total: Option<u64>,
    /// bytes/s
    pub speed: Option<u64>,
}

/// 解析 SPARKLING 前缀进度行；其它行返回 None
pub fn parse_progress_line(line: &str) -> Option<ProgressLine> {
    let rest = line.trim().strip_prefix("SPARKLING|")?;
    let mut parts = rest.split('|');
    let downloaded: u64 = parts.next()?.trim().parse().ok()?;
    let total: Option<u64> = parse_na(parts.next()?);
    let estimate: Option<u64> = parse_na(parts.next()?);
    let speed: Option<u64> = parse_na(parts.next()?);
    Some(ProgressLine {
        downloaded,
        total: total.or(estimate),
        speed,
    })
}

/// "NA" → None；数值字符串（可含小数）→ 截断取整
fn parse_na(s: &str) -> Option<u64> {
    let t = s.trim();
    if t == "NA" || t.is_empty() {
        return None;
    }
    t.parse::<f64>().ok().map(|v| v as u64)
}

/// 合并/提取阶段行（下载已 100%，ffmpeg 工作中）
pub fn is_merge_line(line: &str) -> bool {
    line.starts_with("[Merger]") || line.starts_with("[ExtractAudio]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_line() {
        let p = parse_progress_line("SPARKLING|123456|2000000|2000000|234567.8").unwrap();
        assert_eq!(p.downloaded, 123456);
        assert_eq!(p.total, Some(2000000));
        assert_eq!(p.speed, Some(234567));
    }

    #[test]
    fn total_falls_back_to_estimate() {
        // 直播/未知大小：total_bytes = NA，estimate 有值
        let p = parse_progress_line("SPARKLING|123456|NA|2000000|NA").unwrap();
        assert_eq!(p.total, Some(2000000));
        assert_eq!(p.speed, None);
    }

    #[test]
    fn all_na_total_is_none() {
        let p = parse_progress_line("SPARKLING|123456|NA|NA|100.5").unwrap();
        assert_eq!(p.total, None);
        assert_eq!(p.speed, Some(100));
    }

    #[test]
    fn ignores_non_progress_lines() {
        assert!(parse_progress_line("[download] Destination: a.mp4").is_none());
        assert!(parse_progress_line("[Merger] Merging formats").is_none());
        assert!(parse_progress_line("").is_none());
        // 前缀不符（yt-dlp 其它模板输出）
        assert!(parse_progress_line("OTHER|1|2|3|4").is_none());
    }

    #[test]
    fn merge_line_detection() {
        assert!(is_merge_line("[Merger] Merging formats into \"x.mp4\""));
        assert!(is_merge_line("[ExtractAudio] Destination: x.m4a"));
        assert!(!is_merge_line("[download] 100% of 10.00MiB"));
        assert!(!is_merge_line("SPARKLING|1|2|3|4"));
    }
}
