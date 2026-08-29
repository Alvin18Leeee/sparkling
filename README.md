# Sparkling ✨

[![CI](https://github.com/Alvin18Leeee/sparkling/actions/workflows/ci.yml/badge.svg)](https://github.com/Alvin18Leeee/sparkling/actions/workflows/ci.yml)

PC 端全功能下载器。当前为子项目①：HTTP/HTTPS 多线程下载核心。

## 功能

- 多线程分片下载（默认 8 线程，可配 1–64），动态偷段消除长尾
- 断点续传（`.sparkling` 控制文件），崩溃/重启后自动恢复
- 全局限速（令牌桶）；单任务限速核心已支持，UI 接入在后续阶段
- 任务队列：并发控制（默认 3）、置顶、失败重试（从分片断点继续）
- 完整性校验（Content-MD5）、ETag 变化检测（远端变化自动重下）
- SQLite 任务持久化
- 视频解析下载（yt-dlp）：Bilibili/YouTube 等 1800+ 站点，画质选择、播放列表批量、字幕、Cookie 导入（登录/会员画质）

## 开发

前置：Rust（stable）、Node.js 18+。

```bash
npm install          # 前端依赖
npm run tauri dev    # 开发模式（热重载）
npm run tauri build  # 构建（bundling 属于后续阶段）
cargo test           # 核心库测试（含可编程 HTTP 测试服务器）
```

> 首次构建视频功能需先 `npm run fetch:bin` 下载 yt-dlp/ffmpeg 到 `src-tauri/bin/`（约 40MB，仅一次）

> 注：首次 `cargo build` / `cargo test` 需要 `dist/` 已存在，而
> `dist/assets` 被 git 忽略、不入库。全新 checkout 请先执行
> `npm install && npm run build`；`npm run tauri build` 也会通过
> `beforeBuildCommand` 自动完成前端构建。

## 架构

- `crates/sparkling-core`：纯 Rust 核心（不依赖 Tauri）——引擎、调度、持久化
- `src-tauri`：Tauri 2 壳，command/event 桥接
- `src`：React + TypeScript 前端

详见 `docs/superpowers/specs/2026-08-28-http-downloader-core-design.md`。

## 路线图

- [x] ① HTTP 多线程下载核心
- [ ] ② BT / 磁力引擎
- [x] ③ 视频解析下载（yt-dlp）
- [ ] ④ 浏览器接管、自动更新、多语言、安装包
