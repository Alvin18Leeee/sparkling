import { api } from '../api';
import type { TaskRecord } from '../types';
import { fmtBytes } from '../types';

const STATE_LABEL: Record<string, string> = {
  queued: '排队中',
  running: '下载中',
  paused: '已暂停',
  completed: '已完成',
  failed: '失败',
  cancelled: '已取消',
};

export default function TaskRow({ task, onChanged }: { task: TaskRecord; onChanged: () => void }) {
  const pct =
    task.total_size && task.total_size > 0
      ? Math.floor((task.downloaded / task.total_size) * 100)
      : 0;
  const name = task.filename ?? '解析中…';
  const act = (fn: (id: string) => Promise<void>) => () => {
    fn(task.id).then(onChanged).catch((e) => alert(String(e)));
  };

  return (
    <div className={`task-row ${task.state}`}>
      <div className="row-head">
        <span className="name" title={task.url}>{name}</span>
        <span className={`chip ${task.state}`}>{STATE_LABEL[task.state]}</span>
      </div>
      <div className="progress">
        <div className="bar">
          <div className="fill" style={{ width: `${pct}%` }} />
        </div>
        <span className="pct">{pct}%</span>
      </div>
      <div className="row-meta">
        <span>{fmtBytes(task.downloaded)} / {fmtBytes(task.total_size)}</span>
        {task.error && <span className="error" title={task.error}>{task.error}</span>}
      </div>
      <div className="row-actions">
        {task.state === 'queued' && (
          <>
            <button onClick={act(api.moveTaskToTop)}>置顶</button>
            <button onClick={act(api.cancelTask)}>取消</button>
          </>
        )}
        {task.state === 'running' && (
          <>
            <button onClick={act(api.pauseTask)}>暂停</button>
            <button onClick={act(api.cancelTask)}>取消</button>
          </>
        )}
        {task.state === 'paused' && (
          <>
            <button className="primary" onClick={act(api.resumeTask)}>继续</button>
            <button onClick={act(api.cancelTask)}>取消</button>
          </>
        )}
        {task.state === 'failed' && (
          <>
            <button className="primary" onClick={act(api.retryTask)}>重试</button>
            <button onClick={act(api.removeTask)}>移除</button>
          </>
        )}
        {(task.state === 'completed' || task.state === 'cancelled') && (
          <button onClick={act(api.removeTask)}>移除</button>
        )}
      </div>
    </div>
  );
}
