export type TaskState =
  | 'queued' | 'running' | 'paused' | 'completed' | 'failed' | 'cancelled';

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
}

export interface ManagerConfig {
  max_concurrent: number;
  auto_resume_on_start: boolean;
  global_speed_limit: number | null;
  default_segments: number;
}

/** 引擎真实分片（光道的一块 = 一个分片） */
export interface SegmentInfo {
  index: number;
  downloaded: number;
  len: number;
}

export type TaskEvent =
  | { kind: 'State'; id: string; state: TaskState; error: string | null }
  | { kind: 'Progress'; id: string; downloaded: number; total: number; speed: number; segments: SegmentInfo[] };

/** 进度事件的最新快照（4Hz 直渲，不经 IPC 整表刷新） */
export interface LiveInfo {
  downloaded: number;
  total: number;
  speed: number;
  segments: SegmentInfo[];
}

export function fmtBytes(n: number | null | undefined): string {
  if (n == null) return '—';
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}
