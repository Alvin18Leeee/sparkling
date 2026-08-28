import { invoke } from '@tauri-apps/api/core';
import type { ManagerConfig, TaskRecord } from './types';

export const api = {
  addTask: (url: string, filename?: string | null, segments?: number | null) =>
    invoke<string>('add_task', { url, filename: filename ?? null, segments: segments ?? null }),
  pauseTask: (id: string) => invoke<void>('pause_task', { id }),
  resumeTask: (id: string) => invoke<void>('resume_task', { id }),
  cancelTask: (id: string) => invoke<void>('cancel_task', { id }),
  retryTask: (id: string) => invoke<void>('retry_task', { id }),
  removeTask: (id: string) => invoke<void>('remove_task', { id }),
  moveTaskToTop: (id: string) => invoke<void>('move_to_top', { id }),
  listTasks: () => invoke<TaskRecord[]>('list_tasks'),
  getConfig: () => invoke<ManagerConfig>('get_config'),
  updateConfig: (cfg: ManagerConfig) => invoke<void>('update_config', { cfg }),
};
