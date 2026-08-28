mod common;

use common::{start, FailMode, ServerConfig};
use sparkling_core::probe::probe;

fn client() -> reqwest::Client {
    reqwest::Client::builder().build().unwrap()
}

#[tokio::test]
async fn probe_range_server() {
    let server = start(ServerConfig { size: 5000, ..Default::default() }).await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert_eq!(p.total, 5000);
    assert!(p.supports_range);
    assert_eq!(p.filename, "file.bin");
    assert_eq!(p.etag.as_deref(), Some("\"v1\""));
}

#[tokio::test]
async fn probe_no_range_server() {
    let server = start(ServerConfig { size: 5000, support_range: false, ..Default::default() }).await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert_eq!(p.total, 5000);
    assert!(!p.supports_range);
}

#[tokio::test]
async fn probe_disposition_overrides_filename() {
    // 注意：这里用 ASCII 文件名。测试服务器的 Content-Disposition 头由
    // HeaderValue::from_str 构造，非 ASCII（如 "报表.zip"）虽能作为 obs-text
    // 字节发出，但 probe 侧 HeaderValue::to_str() 拒绝非可见 ASCII 字节，
    // 解析会回退到 URL 文件名。filename*=UTF-8'' 百分号编码路径可表达非
    // ASCII 文件名，但服务器封装格式（filename="..."）不支持注入该形式。
    let server = start(ServerConfig {
        size: 100,
        disposition: Some("report-q4.zip".into()),
        ..Default::default()
    }).await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert_eq!(p.filename, "report-q4.zip");
}

#[tokio::test]
async fn probe_http_error() {
    let server = start(ServerConfig { fail_mode: FailMode::Always5xx, ..Default::default() }).await;
    let err = probe(&client(), &server.url).await.unwrap_err();
    assert!(matches!(err, sparkling_core::SparklingError::HttpStatus { status: 500, .. }));
}

#[tokio::test]
async fn probe_content_md5_present() {
    let server = start(ServerConfig { size: 100, ..Default::default() }).await;
    let p = probe(&client(), &server.url).await.unwrap();
    assert!(p.content_md5.is_some());
}
