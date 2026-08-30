# ③期设计：视频解析下载（yt-dlp）

日期：2026-08-29
状态：已与用户逐项确认（方案 A + 六项关键决策拍板）
前置：①期 [HTTP 多线程下载核心](2026-08-28-http-downloader-core-design.md) 已发布 v0.1.0

## 背景与目标

Sparkling ①期交付了 HTTP/HTTPS 多线程下载核心（Engine trait 边界、任务队列、断点续传、限速、SQLite 持久化）。③期在此之上接入视频解析下载：用户粘贴视频链接 → 解析 → 选择画质/字幕 → 下载合并，与 HTTP 任务统一在同一个任务列表。

**为什么提前于②期（BT）**：用户拍板（2026-08-29），②期顺延。

**为什么包装 yt-dlp**（沿①期 spec 决策）：yt-dlp 支持 1800+ 网站（Bilibili、YouTube、抖音、Twitter 等），且 generic extractor 兜底直链/m3u8。网站适配是维护无底洞，外包给社区高频维护的 yt-dlp，本产品只负责调度与体验。

## 用户已拍板的关键决策

| # | 决策点 | 结论 |
|---|--------|------|
| D1 | yt-dlp 二进制分发 | **混合**：安装包打包基线版（resource），app data 存更新版（优先使用），设置页提供"检查更新" |
| D2 | 下载执行 | **yt-dlp 全权下载**（解析+下载+合并），不自研引擎接管直链 |
| D3 | 任务模型 | **统一进现有任务列表**（VideoEngine 实现 Engine trait，复用队列/并发/持久化） |
| D4 | 交互流程 | 默认直接下载（用记住的画质偏好）+ **展开按钮进实时解析面板**（展示该视频真实格式）；选择记住为下次默认 |
| D5 | ffmpeg | **打包进安装包**（资源随应用分发，开箱即用） |
| D6 | ③期范围 | 播放列表批量展开、字幕下载、Cookie 导入纳入；**音频提取转码（-x）descope** |

## 总体架构

```
┌─ 前端 (React) ──────────────────────────────────────────┐
│ AddTaskDialog（+视频链接检测/解析按钮）                    │
│ VideoInfoPanel（新：标题/时长/缩略图/格式列表/字幕/列表勾选）│
│ TaskRow（视频任务：标题+阶段徽标[下载中/合并中]+连续进度条） │
│ SettingsModal（画质偏好/字幕/cookie/yt-dlp 版本+更新）      │
└──────────────┬────────────────────────────────────────┘
               │ invoke: probe_video / add_task(kind=video) / update_ytdlp / import_cookies
┌──────────────▼────────────────────────────────────────┐
│ src-tauri（Tauri 壳）                                   │
│ · resource_dir() 定位打包的 yt-dlp.exe / ffmpeg.exe      │
│ · app_data_dir()/bin/ 存更新版 yt-dlp.exe（优先使用）     │
│ · TaskManager 构造时注入两个引擎                          │
└──────────────┬────────────────────────────────────────┘
┌──────────────▼────────────────────────────────────────┐
│ sparkling-core（保持零 Tauri 依赖）                      │
│ ├─ manager.rs（改）：单引擎 → 路由表 Kind→Engine；       │
│ │   recover()/remove_task() 按 kind 分支                │
│ ├─ video/（新模块）                                      │
│ │   ├─ bin.rs     二进制发现/版本查询/自更新下载          │
│ │   ├─ probe.rs   yt-dlp -J 解析 → VideoInfo            │
│ │   └─ engine.rs  VideoEngine：spawn 进程下载，           │
│ │                 stdout 逐行解析 → ProgressSnapshot     │
│ └─ task.rs/store.rs（改）：TaskKind + 视频字段 + 迁移     │
└───────────────────────────────────────────────────────┘
```

实施路径为**方案 A（多引擎路由）**，已对比否决：
- 方案 B（RoutingEngine 组合代理）：manager 内 recover/remove 的 HTTP 特化逻辑必须感知 kind，"零改动"是伪命题；
- 方案 C（独立视频管理器）：违背 D3 统一任务列表，重复实现队列/并发/持久化。

## 核心组件设计

### 1. 任务模型扩展（task.rs / store.rs）

```rust
pub enum TaskKind { Http, Video }   // serde lowercase，持久化 TEXT

pub struct VideoParams {
    pub format: String,        // yt-dlp -f 选择器，如 "137+140" 或 "bv*[height<=1080]+ba/b"
    pub subtitles: Vec<String>, // 字幕语言列表（空 = 不下）
    pub auto_subs: bool,        // 含自动生成字幕（--write-auto-subs）
}

// TaskSpec / TaskRecord 新增：
//   kind: TaskKind（默认 Http，向后兼容）
//   video: Option<VideoParams>
//   video_meta: Option<VideoMeta>（title/duration_sec/thumbnail/uploader/webpage_url，仅 TaskRecord 持久化）
```

- TaskSpec 增 `kind` 与 `video: Option<VideoParams>`；`segments` 对视频任务无意义（存 1）。
- TaskRecord 增 `kind TEXT NOT NULL DEFAULT 'http'`、`video_format TEXT`、`video_meta TEXT`（JSON）。**SQLite 版本化迁移**（`PRAGMA user_version`）：①期旧库打开时 `ALTER TABLE ADD COLUMN`，老数据默认 kind='http' 无损升级。
- `ManagerConfig` 增：`video_max_height: Option<u32>`（画质偏好，D4 记忆项）、`video_audio_only: bool`（仅音频偏好）、`video_sub_langs: String`（默认 `"zh-Hans,en"`）、`video_auto_subs: bool`、`cookie_file: Option<PathBuf>`。
- **偏好记忆的映射**（D4）：面板的「记住此选择」把当前格式选择**折算为偏好元组**（最高分辨率上限、是否仅音频）——不是精确 format_id（不同视频可用格式不同，format_id 不可跨视频复用）。直下时用偏好构造格式选择器（如 `bv*[height<=1080]+ba/b` 或 `ba/b`）。

### 2. VideoEngine（video/engine.rs）

实现①期预留的 `Engine` trait，生产实现 spawn yt-dlp 进程：

- **命令构造**（每任务一次）：
  ```
  yt-dlp -f <format> -c --newline --no-mtime
    --progress-template "download:SPARKLING|%(progress.downloaded_bytes)s|%(progress.total_bytes_estimate)s|%(progress.total_bytes)s|%(progress._speed_str)s"
    -o "<save_dir>/%(title).200B [%(id)s].%(ext)s" --restrict-filenames
    --ffmpeg-location <path> [-r <limit>] [--write-subs --sub-langs ...] [--cookies <file>]
    <url>
  ```
- **进度解析**：stdout 逐行读，`SPARKLING|...` 前缀行 → `ProgressSnapshot { downloaded, total, speed }`；`segments` 恒为空数组（前端自然渲染连续进度条）；节流沿用现有 watch 通道语义。总大小优先 `total_bytes`，缺失用 `total_bytes_estimate`。
- **阶段识别**：`[Merger]`/`[ExtractAudio]` 行 → 快照带 `merging` 标记（前端显示"合并中"，进度条停在 100%）。
- **控制语义**：
  - Pause → 杀进程（`.part` 保留），快照状态 Paused；
  - Resume → 重启进程，yt-dlp `-c` 从 `.part` 续传；
  - Cancel → 杀进程 + 删 `.part`/`.ytdl` 残留。
- **退出码映射**：0 → Completed；被杀（暂停/取消）→ 对应状态；非零 → Failed，提取 stderr 末尾 `ERROR:` 行作 `error` 字段。
- **限速**：单任务 `max_speed` → `-r`；全局限速对运行中视频任务不热更（yt-dlp 无此能力），下个调度周期生效。
- **文件名**：filename 由解析阶段（probe）从 VideoInfo 取标题预填 TaskRecord，不依赖引擎回调落库（重启恢复直接可用）。

### 3. 进程调用抽象（可测试性核心）

```rust
#[async_trait]
pub trait YtDlpRunner: Send + Sync {
    async fn run(&self, args: Vec<String>, on_line: mpsc::UnboundedSender<String>) -> RunHandle;
}
// 生产：TokioChildRunner（tokio::process spawn 真 yt-dlp，杀进程句柄）
// 测试：FakeRunner（按脚本回放 stdout 行/退出码/延迟）
```

VideoEngine 依赖 `YtDlpRunner` 而非直接 spawn——状态机、控制语义、进度解析全部可用 FakeRunner 单测，CI 不需要真 yt-dlp 二进制。

### 4. 解析（video/probe.rs）

```
probe(url, opts) → yt-dlp -J [--flat-playlist] --no-playlist <url>
  → 解析 JSON → VideoInfo {
      title, duration_sec, thumbnail, uploader, webpage_url,
      formats: Vec<FormatEntry{ format_id, ext, height, fps, vcodec, acodec,
                                filesize, tbr, note }>,   // 已过滤纯 storyboard/低价值项
      is_video_only_available: bool,  // 前端禁用需合并的格式（若 ffmpeg 不可用）
      playlist: Option<Vec<PlaylistEntry{ url, title, duration }>>,
    }
```

- 单视频链接与播放列表 URL 都先 `-J`（列表用 `--flat-playlist` 轻量模式）。
- 60s 超时杀进程。
- JSON 解析为纯函数：真实 `yt-dlp -J` 输出样本做测试 fixture。

### 5. 二进制管理（video/bin.rs）

- **发现顺序**：`app_data/bin/yt-dlp.exe`（若存在且版本更新）> 打包 resource 基线版。版本比较调 `yt-dlp --version`（本地毫秒级）。
- **自更新**：GitHub Releases `yt-dlp.exe` → 下载到 `app_data/bin/`（原子替换：先下 `.tmp` 再改名）。设置页显示当前版本 +「检查更新」。
- ffmpeg 仅打包 resource 版（无需更新），路径经 `--ffmpeg-location` 传给每次调用。
- core 不感知路径来源：二进制路径由 Tauri 层解析后注入 VideoEngine/probe 构造参数。

### 6. 播放列表（D6）

解析面板列出全部条目（复选框默认全选 + 显示总数）→ 确认后**批量创建 N 条独立任务**入队（每条带各自 VideoParams），受 `max_concurrent` 自然分批，可单独暂停/取消/重试。

### 7. Cookie（D6，隐私敏感设计）

- 设置页「从浏览器导入」：`--cookies-from-browser chrome|edge|firefox` 一次性导出 → 落 `app_data/cookies.txt`（Netscape 格式），下载时传 `--cookies`。
- 设置页明确显示：cookie 文件位置、包含站点、**「清除」按钮**（一键删除文件 + 清空配置）。
- 不做云同步、无遥测。会员画质（Bilibili 1080p+ 等）依赖此机制。

### 8. URL 识别（添加对话框）

- **域名启发白名单**（youtube/bilibili/抖音/twitter 等 top 站点）仅做 UI 提示——出现"视频下载"展开区；不是权威判断。
- 任何链接用户可手动点「解析」强制走视频流程（兜住长尾站点）。
- 白名单外的链接默认 HTTP 下载（现状不变）。

### 9. Tauri 命令层新增

| 命令 | 作用 |
|---|---|
| `probe_video(url)` | 解析 → VideoInfo（面板数据；含超时/错误返回） |
| `add_task`（扩展） | 增 `kind`/`video` 参数，视频任务带 VideoParams |
| `get_ytdlp_status()` | 当前版本、二进制来源（打包/更新）、ffmpeg 可用性 |
| `update_ytdlp()` | 下载最新版到 app data |
| `import_cookies(browser)` / `clear_cookies()` | cookie 导入/清除 |

### 10. 前端

- **AddTaskDialog**：URL 粘贴 → 域名检测出视频链接时出现展开区；展开后「解析」按钮 → `probe_video` → VideoInfoPanel；已有画质偏好时提供直接「下载」（跳过解析）。
- **VideoInfoPanel**（新组件）：标题/时长/缩略图/uploader；格式列表（按分辨率分组：1080p60/720p/…/仅音频 m4a，显示容器/码率/预估大小）；字幕勾选（默认跟随设置）；播放列表条目勾选；「记住此选择」默认开。
- **TaskRow**：视频任务显示标题（URL 文件名位置）+ 阶段徽标（下载中/合并中）；连续进度条（segments 空）。
- **SettingsModal**：视频区（默认画质偏好/字幕语言/自动字幕开关）、Cookie 导入与清除、yt-dlp 版本 + 检查更新按钮。

## 数据流：添加与生命周期

```
粘贴链接 ──► 域名启发检测
   │                    │
   │ 普通链接            │ 视频站点链接
   ▼                    ▼
HTTP 下载（现状）    AddTaskDialog「视频下载」展开区
                        │
              ┌─────────┴──────────┐
              ▼ 有记忆的画质偏好      ▼ 点击「解析」
        直接「下载」入队          VideoProbe: yt-dlp -J
        （yt-dlp 自动选最接近       → VideoInfoPanel
          偏好的格式）
                                ▼ 确认 → add_task(kind=Video)
                                选择存为默认偏好（下次直下）

任务状态机（视频任务）：
Queued ─► Running[解析准备(秒级)] ─► Running[下载中 %] ─► Running[合并中] ─► Completed
           ▲                                                │失败（重试从 .part 续传）
           └── 恢复 = 重启进程，yt-dlp -c 从 .part 续传 ◄─── Paused/Failed

注：「解析准备」（yt-dlp 进程内部先解析再下载，秒级）不引入独立状态标记，
Running + 进度 0 自然呈现；「合并中」是唯一有显式标记的阶段（快照 merging）。
播放列表批量任务：每条使用各自的网页 URL + 共享的 VideoParams。
```

## 错误处理

| 场景 | 处理 |
|---|---|
| URL 不支持/视频删除/地区限制 | 解析阶段失败 → 面板显示 yt-dlp `ERROR:` 行摘要（中文包装） |
| 下载中网络错误 | yt-dlp `--retries 10` 自带重试，耗尽后非零退出 → Failed + stderr 摘要 |
| 合并失败/磁盘满 | 非零退出 → Failed；`.part` 保留，重试续传 |
| 二进制缺失/损坏 | 版本探测失败 → 提示；打包基线版兜底始终可用 |
| 解析超时 | 60s 杀进程，报"解析超时，请重试" |
| 输出编码 | stdout 按 UTF-8 解码 |

## 测试策略

1. **FakeRunner 单测**：VideoEngine 状态机/控制语义（pause=杀、resume=重启续传、cancel=清理）、退出码映射、合并阶段标记。
2. **进度解析器纯函数单测**：样例行 → downloaded/total/speed 断言（含 total 缺失回退 estimate）。
3. **probe JSON 解析单测**：真实 `-J` 输出 fixture（单视频 + 播放列表）→ VideoInfo 断言（格式过滤、字段提取）。
4. **SQLite 迁移测试**：①期旧 schema 库 → 打开迁移 → 老数据完好、新列默认值正确。
5. **manager 路由测试**：HTTP+视频混合队列的调度/并发位共享/暂停让位。
6. **真机验收**（本机 dev，不进 CI）：YouTube/Bilibili 真实链接端到端（含播放列表、字幕、cookie 画质）。

## 发布与 CI 影响

- Release workflow：CI 下载**固定版本** yt-dlp.exe（基线）与 ffmpeg.exe，随 NSIS/便携版打包（`bundle.resources`）。
- 安装包体积预计 +50~60MB（压缩后）。
- Ubuntu 质量门不受影响（core 测试不依赖真二进制，FakeRunner 全覆盖）。

## 明确不做（③期，YAGNI）

- 音频提取转码（`-x` mp3）——纯音频原生流（m4a）在格式列表可选
- yt-dlp 高级参数面板（自定义 output template/任意 flag 透传）
- 频道订阅源/频道批量抓取（仅单链接的播放列表展开）
- 下载镜像源配置（GitHub 直连失败先报错重试，后续按需评估）
- 浏览器接管、剪贴板监听、应用自身自动更新、多语言（④期）

## 风险与开放问题

- **GitHub 直连下载**（yt-dlp 自更新、CI 拉取二进制）在国内网络可能慢/失败：③期先官方源 + 明确报错，镜像配置留作后续。
- **yt-dlp 输出格式变化**：progress-template 是 yt-dlp 稳定公开接口，风险低；`-J` JSON 字段偶有增减，解析层做防御性默认值。
- **Bilibili 等站点风控**（429/需登录）：错误信息透传给用户，cookie 导入作为缓解手段；无法根治，跟随 yt-dlp 版本更新修复。
