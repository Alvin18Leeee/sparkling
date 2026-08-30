export type TaskState =
  | 'queued' | 'running' | 'paused' | 'completed' | 'failed' | 'cancelled';

export type TaskKind = 'http' | 'video';

export interface VideoParams {
  format: string;
  subtitles: string[];
  auto_subs: boolean;
}

export interface VideoMeta {
  title: string;
  duration_sec: number | null;
  thumbnail: string | null;
  uploader: string | null;
  webpage_url: string | null;
}

export interface FormatEntry {
  format_id: string;
  ext: string;
  height: number | null;
  fps: number | null;
  vcodec: string;
  acodec: string;
  filesize: number | null;
  tbr: number | null;
}

export interface PlaylistEntry {
  url: string;
  title: string;
  duration_sec: number | null;
}

export interface VideoInfo {
  title: string;
  duration_sec: number | null;
  thumbnail: string | null;
  uploader: string | null;
  webpage_url: string | null;
  formats: FormatEntry[];
  playlist: PlaylistEntry[] | null;
}

export interface YtdlpStatus {
  version: string | null;
  source: string;
  ffmpeg_available: boolean;
}

export interface TaskRecord {
  id: string;
  url: string;
  state: TaskState;
  save_dir: string;
  filename: string | null;
  segments: number;
  max_speed: number | null;
  total_size: number | null;
  downloaded: number;
  error: string | null;
  created_at: number;
  kind: TaskKind;
  video: VideoParams | null;
  video_meta: VideoMeta | null;
}

export interface ManagerConfig {
  max_concurrent: number;
  auto_resume_on_start: boolean;
  global_speed_limit: number | null;
  default_segments: number;
  video_max_height: number | null;
  video_audio_only: boolean;
  video_sub_langs: string;
  video_auto_subs: boolean;
}

/** 引擎真实分片（光道的一块 = 一个分片） */
export interface SegmentInfo {
  index: number;
  downloaded: number;
  len: number;
}

export type TaskEvent =
  | { kind: 'State'; id: string; state: TaskState; error: string | null }
  | { kind: 'Progress'; id: string; downloaded: number; total: number; speed: number; segments: SegmentInfo[]; merging: boolean };

/** 进度事件的最新快照（4Hz 直渲，不经 IPC 整表刷新） */
export interface LiveInfo {
  downloaded: number;
  total: number;
  speed: number;
  segments: SegmentInfo[];
  merging: boolean;
}

export function fmtBytes(n: number | null | undefined): string {
  if (n == null) return '—';
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}

/** http 资源升级为 https：B 站等站点返回 http 缩略图，安全上下文页面
 * （tauri://localhost）对 http 图片执行 mixed content 阻断导致坏图 */
export function httpsUpgrade(url: string): string {
  return url.startsWith('http://') ? `https://${url.slice('http://'.length)}` : url;
}

/** 常见视频站点白名单——仅做添加对话框的 UI 提示，不是权威判断 */
const VIDEO_SITES = [
  'youtube.com', 'youtu.be', 'bilibili.com', 'b23.tv', 'douyin.com',
  'tiktok.com', 'twitter.com', 'x.com', 'vimeo.com', 'twitch.tv',
];

export function looksLikeVideoUrl(url: string): boolean {
  try {
    const h = new URL(url).hostname.toLowerCase();
    return VIDEO_SITES.some((s) => h === s || h.endsWith('.' + s));
  } catch {
    return false;
  }
}

/** 秒 → "1:23:45" / "12:34"；缺失 → "时长未知" */
export function fmtDuration(sec: number | null | undefined): string {
  if (sec == null || sec <= 0) return '时长未知';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = Math.floor(sec % 60);
  const mm = h > 0 ? String(m).padStart(2, '0') : String(m);
  const ss = String(s).padStart(2, '0');
  return h > 0 ? `${h}:${mm}:${ss}` : `${mm}:${ss}`;
}

/** 记住的画质偏好 → yt-dlp -f 选择器（直下路径用；无偏好返回 null 走解析面板） */
export function selectorFromPreference(cfg: {
  video_audio_only?: boolean;
  video_max_height?: number | null;
} | null): string | null {
  if (!cfg) return null;
  if (cfg.video_audio_only) return 'ba/b';
  const h = cfg.video_max_height;
  if (h == null) return 'bv*+ba/b';
  return `bv*[height<=${h}]+ba/b[height<=${h}]`;
}
