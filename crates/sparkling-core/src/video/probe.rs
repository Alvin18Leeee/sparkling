//! yt-dlp -J 输出解析：单视频 → 完整格式表；播放列表 → flat 条目。
//! 字段全部防御性 Option（yt-dlp JSON 字段随 extractor 有增减）。
use crate::{Result, SparklingError};
use serde::Deserialize;

#[derive(Debug, Clone, serde::Serialize)]
pub struct VideoInfo {
    pub title: String,
    pub duration_sec: Option<u64>,
    pub thumbnail: Option<String>,
    pub uploader: Option<String>,
    pub webpage_url: Option<String>,
    pub formats: Vec<FormatEntry>,
    pub playlist: Option<Vec<PlaylistEntry>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FormatEntry {
    pub format_id: String,
    pub ext: String,
    pub height: Option<u32>,
    pub fps: Option<f64>,
    pub vcodec: String,
    pub acodec: String,
    pub filesize: Option<u64>,
    pub tbr: Option<f64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PlaylistEntry {
    pub url: String,
    pub title: String,
    pub duration_sec: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RawInfo {
    #[serde(default, rename = "_type")]
    kind: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    duration: Option<f64>,
    #[serde(default)]
    thumbnail: Option<String>,
    #[serde(default)]
    uploader: Option<String>,
    #[serde(default)]
    webpage_url: Option<String>,
    #[serde(default)]
    formats: Vec<RawFormat>,
    #[serde(default)]
    entries: Vec<RawEntry>,
}

#[derive(Debug, Deserialize)]
struct RawFormat {
    #[serde(default)]
    format_id: Option<String>,
    #[serde(default)]
    ext: Option<String>,
    #[serde(default)]
    height: Option<u32>,
    #[serde(default)]
    fps: Option<f64>,
    #[serde(default)]
    vcodec: Option<String>,
    #[serde(default)]
    acodec: Option<String>,
    #[serde(default)]
    filesize: Option<u64>,
    #[serde(default)]
    tbr: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawEntry {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    webpage_url: Option<String>,
    #[serde(default)]
    duration: Option<f64>,
}

/// 解析 yt-dlp -J --flat-playlist 的 JSON 输出
pub fn parse_info_json(json: &str) -> Result<VideoInfo> {
    let raw: RawInfo = serde_json::from_str(json)
        .map_err(|e| SparklingError::Other(format!("解析视频信息失败: {e}")))?;
    let title = raw.title.clone().unwrap_or_else(|| "未知标题".into());
    let duration_sec = raw.duration.map(|d| d as u64);
    if raw.kind.as_deref() == Some("playlist") || !raw.entries.is_empty() {
        let playlist = raw
            .entries
            .into_iter()
            .filter_map(|e| {
                // flat 条目 url 与 webpage_url 皆可能出现；二者皆无 → 跳过
                let url = e.url.or(e.webpage_url.clone())?;
                Some(PlaylistEntry {
                    url,
                    title: e.title.unwrap_or_else(|| e.id.clone().unwrap_or_default()),
                    duration_sec: e.duration.map(|d| d as u64),
                })
            })
            .collect::<Vec<_>>();
        return Ok(VideoInfo {
            title,
            duration_sec,
            thumbnail: raw.thumbnail,
            uploader: raw.uploader,
            webpage_url: raw.webpage_url,
            formats: vec![],
            playlist: Some(playlist),
        });
    }
    let formats = raw
        .formats
        .into_iter()
        .filter(|f| {
            let vcodec = f.vcodec.as_deref().unwrap_or("none");
            let acodec = f.acodec.as_deref().unwrap_or("none");
            // 过滤 storyboard（mhtml）与双 none 空格式
            f.ext.as_deref() != Some("mhtml") && !(vcodec == "none" && acodec == "none")
        })
        .map(|f| FormatEntry {
            format_id: f.format_id.unwrap_or_default(),
            ext: f.ext.unwrap_or_else(|| "unknown".into()),
            height: f.height,
            fps: f.fps,
            vcodec: f.vcodec.unwrap_or_else(|| "none".into()),
            acodec: f.acodec.unwrap_or_else(|| "none".into()),
            filesize: f.filesize,
            tbr: f.tbr,
        })
        .collect();
    Ok(VideoInfo {
        title,
        duration_sec,
        thumbnail: raw.thumbnail,
        uploader: raw.uploader,
        webpage_url: raw.webpage_url,
        formats,
        playlist: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const VIDEO_JSON: &str = include_str!("../../tests/fixtures/video_info.json");
    const PLAYLIST_JSON: &str = include_str!("../../tests/fixtures/playlist_info.json");

    #[test]
    fn parses_single_video() {
        let info = parse_info_json(VIDEO_JSON).unwrap();
        assert_eq!(info.title, "测试视频 - 中文标题");
        assert_eq!(info.duration_sec, Some(212));
        assert_eq!(info.uploader.as_deref(), Some("测试上传者"));
        assert!(info.playlist.is_none());
        // storyboard(sb0) 与双 none(empty) 被过滤；保留 140/137/18
        assert_eq!(info.formats.len(), 3);
        let f137 = info.formats.iter().find(|f| f.format_id == "137").unwrap();
        assert_eq!(f137.height, Some(1080));
        assert_eq!(f137.fps, Some(25.0));
        assert_eq!(f137.filesize, Some(45000000));
        assert_eq!(f137.acodec, "none");
        let f140 = info.formats.iter().find(|f| f.format_id == "140").unwrap();
        assert_eq!(f140.vcodec, "none");
    }

    #[test]
    fn parses_playlist_and_skips_entry_without_url() {
        let info = parse_info_json(PLAYLIST_JSON).unwrap();
        let pl = info.playlist.expect("应识别为播放列表");
        assert_eq!(pl.len(), 2);
        assert_eq!(pl[0].title, "第一集");
        assert_eq!(pl[0].duration_sec, Some(100));
        assert_eq!(pl[1].url, "https://www.youtube.com/watch?v=v2");
    }

    #[test]
    fn rejects_invalid_json() {
        assert!(parse_info_json("not json").is_err());
    }
}
