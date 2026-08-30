import { useState } from 'react';
import { api } from '../api';
import type { LiveInfo, TaskRecord } from '../types';
import { fmtBytes } from '../types';
import TaskRow from './TaskRow';

/** 合集条目：N 个同 collection 的播放列表子任务聚合为一行，
 *  展开后子任务保留独立控制（暂停/取消/重试）。 */
export default function CollectionRow({
  name,
  items,
  live,
  onChanged,
}: {
  name: string;
  items: TaskRecord[];
  live: Map<string, LiveInfo>;
  onChanged: () => void;
}) {
  // 有未完成任务默认展开（下载中想看进度），终态默认收起
  const [expanded, setExpanded] = useState(() =>
    items.some((t) => t.state === 'running' || t.state === 'queued')
  );

  let downloaded = 0;
  let total = 0;
  let completed = 0;
  let running = 0;
  let failed = 0;
  for (const t of items) {
    const l = live.get(t.id);
    downloaded += l?.downloaded ?? t.downloaded;
    total += l?.total ?? t.total_size ?? 0;
    if (t.state === 'completed') completed++;
    if (t.state === 'running') running++;
    if (t.state === 'failed') failed++;
  }
  const pct = total > 0 ? Math.min(100, Math.floor((downloaded / total) * 100)) : 0;
  const allDone = completed === items.length;

  // 汇总状态标签：进行语义优先级 running > failed > queued > 终态
  const label = allDone
    ? '已完成'
    : running > 0
      ? `下载中 · ${completed}/${items.length}`
      : failed > 0
        ? `部分失败 · ${completed}/${items.length}`
        : `排队中 · ${completed}/${items.length}`;
  // 全组取消：未完成子任务逐个取消（已终态的跳过）
  const unfinished = items.filter((t) => t.state !== 'completed' && t.state !== 'cancelled');
  const cancelAll = async () => {
    for (const t of unfinished) {
      await api.cancelTask(t.id).catch(() => {});
    }
    onChanged();
  };

  return (
    <div className={`collection collection--${allDone ? 'completed' : 'active'}`}>
      <button className="collection__head" onClick={() => setExpanded(!expanded)}>
        <span className={`collection__chevron ${expanded ? 'collection__chevron--open' : ''}`} aria-hidden="true">
          ›
        </span>
        <span className="collection__name" title={name}>{name}</span>
        <span className="collection__count">{items.length} 项</span>
        <span className="collection__pct">{pct}%</span>
        <span className="collection__state">{label}</span>
        {!allDone && (
          <span className="collection__actions">
            <button
              className="btn btn--sm"
              onClick={(e) => {
                e.stopPropagation(); // 别触发头部展开/收起
                void cancelAll();
              }}
            >
              全部取消
            </button>
          </span>
        )}
      </button>
      <div className="task__bar" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
        <div className="task__bar-fill" style={{ width: `${pct}%` }} />
      </div>
      <div className="collection__meta">
        <span className="task__stat">{fmtBytes(downloaded)} / {fmtBytes(total)}</span>
      </div>
      {expanded && (
        <div className="collection__items">
          {items.map((t) => (
            <TaskRow key={t.id} task={t} live={live.get(t.id)} onChanged={onChanged} />
          ))}
        </div>
      )}
    </div>
  );
}
