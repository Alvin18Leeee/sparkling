use crate::{Result, SparklingError};
use percent_encoding::percent_decode_str;

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub total: u64,
    pub supports_range: bool,
    pub filename: String,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub content_md5: Option<String>,
}

/// 探测：GET + Range: bytes=0-0。
/// 206 → 支持 Range（Content-Range 尾段是总大小）；200 → 不支持。
/// 未提供文件大小的服务器暂不支持（已知限制，spec 范围外）。
pub async fn probe(client: &reqwest::Client, url: &str) -> Result<ProbeResult> {
    let resp = client
        .get(url)
        .header("Range", "bytes=0-0")
        .send()
        .await
        .map_err(|e| SparklingError::Network(e.to_string()))?;
    let status = resp.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(SparklingError::HttpStatus { status, detail: format!("探测失败: {url}") });
    }
    let supports_range = status == 206;
    let headers = resp.headers();

    let total = if supports_range {
        let cr = headers
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| SparklingError::Network("206 响应缺少 Content-Range".into()))?;
        cr.rsplit('/')
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| SparklingError::Network(format!("Content-Range 无法解析: {cr}")))?
    } else {
        headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| SparklingError::Network("服务器未提供文件大小，暂不支持".into()))?
    };

    let filename = filename_from_headers(headers)
        .unwrap_or_else(|| filename_from_url(url));

    Ok(ProbeResult {
        total,
        supports_range,
        filename,
        etag: header_string(headers, reqwest::header::ETAG),
        last_modified: header_string(headers, reqwest::header::LAST_MODIFIED),
        content_md5: header_string(headers, "content-md5"),
    })
}

fn header_string(headers: &reqwest::header::HeaderMap, name: impl reqwest::header::AsHeaderName) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(|s| s.to_string())
}

/// 解析 Content-Disposition 的 filename= / filename*=UTF-8''
fn filename_from_headers(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let cd = headers.get(reqwest::header::CONTENT_DISPOSITION)?.to_str().ok()?;
    for part in cd.split(';') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("filename*=UTF-8''") {
            return Some(percent_decode_str(v).decode_utf8_lossy().into_owned());
        }
        if let Some(v) = part.strip_prefix("filename=") {
            return Some(v.trim_matches('"').to_string());
        }
    }
    None
}

/// URL 末段做文件名（percent-decode），失败回退 "download"
fn filename_from_url(url: &str) -> String {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let last = path.rsplit('/').next().unwrap_or("");
    let decoded = percent_decode_str(last).decode_utf8_lossy().into_owned();
    if decoded.is_empty() { "download".to_string() } else { decoded }
}
