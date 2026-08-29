import { useCallback, useEffect, useRef, useState } from 'react';
import { listen } from '@tauri-apps/api/event';
import { getCurrentWindow } from '@tauri-apps/api/window';
import { api } from './api';
import type { LiveInfo, ManagerConfig, TaskEvent, TaskRecord } from './types';
import { fmtBytes } from './types';
import AddTaskDialog from './components/AddTaskDialog';
import SettingsModal from './components/SettingsModal';
import TaskList from './components/TaskList';

const appWindow = getCurrentWindow();

function Spark({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 16 16" aria-hidden="true">
      <path
        d="M8 0 L9.7 6.3 L16 8 L9.7 9.7 L8 16 L6.3 9.7 L0 8 L6.3 6.3 Z"
        fill="currentColor"
      />
    </svg>
  );
}

/** 自绘窗口控制（无边框窗口）：最小化 / 最大化 / 关闭 */
function WindowControls() {
  return (
    <div className="win-controls">
      <button className="win-btn" aria-label="最小化" onClick={() => appWindow.minimize()}>
        <svg viewBox="0 0 10 10" aria-hidden="true"><path d="M0 5h10" stroke="currentColor" strokeWidth="1" /></svg>
      </button>
      <button className="win-btn" aria-label="最大化" onClick={() => appWindow.toggleMaximize()}>
        <svg viewBox="0 0 10 10" aria-hidden="true"><rect x="0.5" y="0.5" width="9" height="9" fill="none" stroke="currentColor" strokeWidth="1" /></svg>
      </button>
      <button className="win-btn win-btn--close" aria-label="关闭" onClick={() => appWindow.close()}>
        <svg viewBox="0 0 10 10" aria-hidden="true"><path d="M0 0l10 10M10 0L0 10" stroke="currentColor" strokeWidth="1" /></svg>
      </button>
    </div>
  );
}

export default function App() {
  const [tasks, setTasks] = useState<TaskRecord[]>([]);
  const [live, setLive] = useState<Map<string, LiveInfo>>(new Map());
  const pendingLive = useRef<Map<string, LiveInfo>>(new Map());
  const [config, setConfig] = useState<ManagerConfig | null>(null);
  const [showAdd, setShowAdd] = useState(false);
  const [showSettings, setShowSettings] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const recs = await api.listTasks();
      setTasks(recs);
      // 对账：清掉已不在列表 / 已非 running 的 live 项（防幻影速度）
      setLive((prev) => {
        if (prev.size === 0) return prev;
        const running = new Set(recs.filter((r) => r.state === 'running').map((r) => r.id));
        let changed = false;
        const next = new Map(prev);
        for (const id of next.keys()) {
          if (!running.has(id)) {
            next.delete(id);
            changed = true;
          }
        }
        return changed ? next : prev;
      });
    } catch {
      /* 后端未就绪时静默 */
    }
  }, []);

  useEffect(() => {
    refresh();
    api.getConfig().then(setConfig).catch(() => {});
    // Progress 事件（后端 250ms 节奏）先进缓冲，按 1s 节拍统一上屏——
    // 视觉更新周期 1 秒，数字与进度条不闪烁；State 事件即时对账 + 2s 轮询兜底
    const un = listen<TaskEvent>('task-event', (ev) => {
      const p = ev.payload;
      if (p.kind === 'Progress') {
        pendingLive.current.set(p.id, {
          downloaded: p.downloaded,
          total: p.total,
          speed: p.speed,
          segments: p.segments,
        });
      } else {
        if (p.state !== 'running') {
          pendingLive.current.delete(p.id);
          setLive((prev) => {
            if (!prev.has(p.id)) return prev;
            const next = new Map(prev);
            next.delete(p.id);
            return next;
          });
        }
        refresh();
      }
    });
    const flushTimer = setInterval(() => {
      if (pendingLive.current.size === 0) return;
      const batch = pendingLive.current;
      pendingLive.current = new Map();
      setLive((prev) => {
        const next = new Map(prev);
        for (const [id, info] of batch) next.set(id, info);
        return next;
      });
    }, 1000);
    const timer = setInterval(refresh, 2000);
    return () => {
      un.then((f) => f());
      clearInterval(flushTimer);
      clearInterval(timer);
    };
  }, [refresh]);

  const totalSpeed = [...live.values()].reduce((a, b) => a + b.speed, 0);
  const hasRunning = tasks.some((t) => t.state === 'running');

  return (
    <div className="app">
      <header className="toolbar" data-tauri-drag-region>
        <div className="brand">
          <Spark className="brand__spark" />
          <span className="brand__name">SPARKLING</span>
        </div>
        {hasRunning && (
          <div className="speed" aria-label="总下载速度">
            <span className="speed__label">总速度</span>
            <span className="speed__value">
              {fmtBytes(totalSpeed)}<span className="speed__unit">/s</span>
            </span>
          </div>
        )}
        <div className="toolbar__actions">
          <button className="btn btn--primary" onClick={() => setShowAdd(true)}>
            ＋ 新建下载
          </button>
          <button className="btn" onClick={() => setShowSettings(true)}>设置</button>
        </div>
        <WindowControls />
      </header>
      <main>
        <TaskList tasks={tasks} live={live} onChanged={refresh} onAdd={() => setShowAdd(true)} />
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
