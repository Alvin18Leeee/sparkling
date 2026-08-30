# 下载 yt-dlp / ffmpeg 外部二进制到 src-tauri/bin/（开发与 CI 共用）
# yt-dlp 固定基线版本（发布可复现）；ffmpeg 用 BtbN 稳定 latest 构建
$ErrorActionPreference = "Stop"
$dir = Join-Path $PSScriptRoot "..\src-tauri\bin"
New-Item -ItemType Directory -Force $dir | Out-Null

$YTDLP_VERSION = "2026.08.19"   # 基线版本；发布时随版本提升更新
$ytdlpUrl = "https://github.com/yt-dlp/yt-dlp/releases/download/$YTDLP_VERSION/yt-dlp.exe"
$ytdlpDest = Join-Path $dir "yt-dlp.exe"
if (-not (Test-Path $ytdlpDest)) {
  Write-Host "下载 yt-dlp $YTDLP_VERSION ..."
  Invoke-WebRequest -Uri $ytdlpUrl -OutFile $ytdlpDest
}

$ffmpegZip = Join-Path $dir "ffmpeg.zip"
$ffmpegDest = Join-Path $dir "ffmpeg.exe"
if (-not (Test-Path $ffmpegDest)) {
  Write-Host "下载 ffmpeg ..."
  Invoke-WebRequest -Uri "https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl.zip" -OutFile $ffmpegZip
  Expand-Archive -Path $ffmpegZip -DestinationPath (Join-Path $dir "ffmpeg-tmp") -Force
  Copy-Item (Join-Path $dir "ffmpeg-tmp\ffmpeg-master-latest-win64-gpl\bin\ffmpeg.exe") $ffmpegDest -Force
  Remove-Item (Join-Path $dir "ffmpeg-tmp") -Recurse -Force
  Remove-Item $ffmpegZip
}
Write-Host "完成：$ytdlpDest / $ffmpegDest"
