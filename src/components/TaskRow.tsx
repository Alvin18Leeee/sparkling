import { api } from '../api';
import type { LiveInfo, TaskRecord, TaskState } from '../types';
import { fmtBytes } from '../types';

const STATE_META: Record<TaskState, { label: string }> = {
  queued: { label: '排队中' },
  running: { label: '下载中' },
  paused: { label: '已暂停' },
  completed: { label: '已完成' },
  failed: { label: '失败' },
  cancelled: { label: '已取消' },
};

/** 从 URL 末段取显示名（percent-decode；无可用段时返回 null） */
function urlName(url: string): string | null {
  try {
    const u = new URL(url);
    const last = u.pathname.split('/').filter(Boolean).pop();
    if (!last) return null;
    return decodeURIComponent(last);
  } catch {
    return null;
  }
}

export default function TaskRow({
  task,
  live,
  onChanged,
}: {
  task: TaskRecord;
  live?: LiveInfo;
  onChanged: () => void;
}) {
  const total = live?.total ?? task.total_size ?? 0;
  const downloaded = live?.downloaded ?? task.downloaded;
  const pct = total > 0 ? Math.min(100, Math.floor((downloaded / total) * 100)) : 0;
  // 探测完成前显示 URL 末段；探测失败的不再永远卡在"解析中"
  const name = task.filename ?? urlName(task.url) ?? '解析中…';
  const running = task.state === 'running';
  const act = (fn: (id: string) => Promise<void>) => () => {
    fn(task.id).then(onChanged).catch((e) => alert(String(e)));
  };

  return (
    <div className={`task task--${task.state}`}>
      <div className="task__head">
        <span className="task__name" title={task.url}>{name}</span>
        <span className="task__pct">{pct}%</span>
        <span className="task__state">
          <i className="task__state-dot" aria-hidden="true" />
          {STATE_META[task.state].label}
        </span>
      </div>

      <div
        className="task__bar"
        role="progressbar"
        aria-valuenow={pct}
        aria-valuemin={0}
        aria-valuemax={100}
      >
        <div className="task__bar-fill" style={{ width: `${pct}%` }} />
      </div>

      <div className="task__meta">
        <span className="task__stat">{fmtBytes(downloaded)} / {fmtBytes(total)}</span>
        {running && live && <span className="task__stat task__stat--speed">{fmtBytes(live.speed)}/s</span>}
        {task.error && (
          <span className="task__error" title={task.error}>{task.error}</span>
        )}
        <div className="task__actions">
          {task.state === 'queued' && (
            <>
              <button className="btn btn--sm" onClick={act(api.moveTaskToTop)}>置顶</button>
              <button className="btn btn--sm" onClick={act(api.cancelTask)}>取消</button>
            </>
          )}
          {running && (
            <>
              <button className="btn btn--sm" onClick={act(api.pauseTask)}>暂停</button>
              <button className="btn btn--sm" onClick={act(api.cancelTask)}>取消</button>
            </>
          )}
          {task.state === 'paused' && (
            <>
              <button className="btn btn--sm btn--primary" onClick={act(api.resumeTask)}>继续</button>
              <button className="btn btn--sm" onClick={act(api.cancelTask)}>取消</button>
            </>
          )}
          {task.state === 'failed' && (
            <>
              <button className="btn btn--sm btn--primary" onClick={act(api.retryTask)}>重试</button>
              <button className="btn btn--sm" onClick={act(api.removeTask)}>移除</button>
            </>
          )}
          {(task.state === 'completed' || task.state === 'cancelled') && (
            <button className="btn btn--sm" onClick={act(api.removeTask)}>移除</button>
          )}
        </div>
      </div>
    </div>
  );
}
