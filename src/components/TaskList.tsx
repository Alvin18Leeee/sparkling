import type { TaskRecord } from '../types';
import TaskRow from './TaskRow';

export default function TaskList({
  tasks,
  onChanged,
}: {
  tasks: TaskRecord[];
  onChanged: () => void;
}) {
  if (tasks.length === 0) {
    return <div className="empty">还没有任务 —— 点击「新建下载」开始</div>;
  }
  return (
    <div className="task-list">
      {tasks.map((t) => (
        <TaskRow key={t.id} task={t} onChanged={onChanged} />
      ))}
    </div>
  );
}
