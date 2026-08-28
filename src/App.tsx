import { useCallback, useEffect, useRef, useState } from 'react';
import { listen } from '@tauri-apps/api/event';
import { api } from './api';
import type { ManagerConfig, TaskEvent, TaskRecord } from './types';
import { fmtBytes } from './types';
import AddTaskDialog from './components/AddTaskDialog';
import SettingsModal from './components/SettingsModal';
import TaskList from './components/TaskList';

export default function App() {
  const [tasks, setTasks] = useState<TaskRecord[]>([]);
  const [config, setConfig] = useState<ManagerConfig | null>(null);
  const [showAdd, setShowAdd] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const speeds = useRef<Map<string, number>>(new Map());

  const refresh = useCallback(async () => {
    try {
      setTasks(await api.listTasks());
    } catch {
      /* 后端未就绪时静默 */
    }
  }, []);

  useEffect(() => {
    refresh();
    api.getConfig().then(setConfig).catch(() => {});
    // 事件驱动更新 + 2s 轮询兜底（防丢事件）
    const un = listen<TaskEvent>('task-event', (ev) => {
      const p = ev.payload;
      if (p.kind === 'Progress') {
        speeds.current.set(p.id, p.speed);
      } else if (p.state !== 'running') {
        // 暂停/终态的速度不再计入总速度（paused 快照携带旧速度且 reporter 已静默）
        speeds.current.delete(p.id);
      }
      // 状态/进度最终以 list_tasks 为准，事件触发即时刷新
      refresh();
    });
    const timer = setInterval(refresh, 2000);
    return () => {
      un.then((f) => f());
      clearInterval(timer);
    };
  }, [refresh]);

  const totalSpeed = [...speeds.current.values()].reduce((a, b) => a + b, 0);

  return (
    <div className="app">
      <header className="toolbar">
        <h1>✨ Sparkling</h1>
        <span className="speed">总速度 {fmtBytes(totalSpeed)}/s</span>
        <div className="actions">
          <button className="primary" onClick={() => setShowAdd(true)}>＋ 新建下载</button>
          <button onClick={() => setShowSettings(true)}>设置</button>
        </div>
      </header>
      <main>
        <TaskList tasks={tasks} onChanged={refresh} />
      </main>
      {showAdd && (
        <AddTaskDialog
          defaultSegments={config?.default_segments ?? 8}
          onClose={() => setShowAdd(false)}
          onAdded={() => {
            setShowAdd(false);
            refresh();
          }}
        />
      )}
      {showSettings && (
        <SettingsModal
          config={config}
          onClose={() => setShowSettings(false)}
          onSaved={(c) => {
            setConfig(c);
            setShowSettings(false);
          }}
        />
      )}
    </div>
  );
}
