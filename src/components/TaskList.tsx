import type { LiveInfo, TaskRecord } from '../types';
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
  return (
    <div className="task-list">
      {tasks.map((t) => (
        <TaskRow key={t.id} task={t} live={live.get(t.id)} onChanged={onChanged} />
      ))}
    </div>
  );
}
