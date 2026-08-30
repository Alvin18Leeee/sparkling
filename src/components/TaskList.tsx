import type { LiveInfo, TaskRecord } from '../types';
import CollectionRow from './CollectionRow';
import TaskRow from './TaskRow';

export default function TaskList({
  tasks,
  live,
  onChanged,
  onAdd,
}: {
  tasks: TaskRecord[];
  live: Map<string, LiveInfo>;
  onChanged: () => void;
  onAdd: () => void;
}) {
  if (tasks.length === 0) {
    return (
      <div className="empty">
        <svg className="empty__spark" viewBox="0 0 16 16" aria-hidden="true">
          <path
            d="M8 0 L9.7 6.3 L16 8 L9.7 9.7 L8 16 L6.3 9.7 L0 8 L6.3 6.3 Z"
            fill="currentColor"
          />
        </svg>
        <p className="empty__title">还没有下载任务</p>
        <p className="empty__hint">粘贴一个链接，开始第一次下载</p>
        <button className="btn btn--primary" onClick={onAdd}>＋ 新建下载</button>
      </div>
    );
  }
  // 合集聚合：collection 非空的任务归为一组（组位置 = 组内最新任务的位置，
  // 传入已按 created_at 倒序）；独立任务照常渲染
  const rows: Array<{ kind: 'task'; task: TaskRecord } | { kind: 'collection'; name: string; items: TaskRecord[] }> =
    [];
  const grouped = new Map<string, { kind: 'collection'; name: string; items: TaskRecord[] }>();
  for (const t of tasks) {
    if (t.collection) {
      let g = grouped.get(t.collection);
      if (!g) {
        g = { kind: 'collection', name: t.collection, items: [] };
        grouped.set(t.collection, g);
        rows.push(g);
      }
      g.items.push(t);
    } else {
      rows.push({ kind: 'task', task: t });
    }
  }
  return (
    <div className="task-list">
      {rows.map((r) =>
        r.kind === 'task' ? (
          <TaskRow key={r.task.id} task={r.task} live={live.get(r.task.id)} onChanged={onChanged} />
        ) : (
          <CollectionRow key={`c:${r.name}`} name={r.name} items={r.items} live={live} onChanged={onChanged} />
        )
      )}
    </div>
  );
}
