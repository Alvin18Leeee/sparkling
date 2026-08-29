import { api } from '../api';
import type { LiveInfo, SegmentInfo, TaskRecord, TaskState } from '../types';
import { fmtBytes } from '../types';

const STATE_META: Record<TaskState, { label: string }> = {
  queued: { label: '排队中' },
  running: { label: '下载中' },
  paused: { label: '已暂停' },
  completed: { label: '已完成' },
  failed: { label: '失败' },
  cancelled: { label: '已取消' },
};

/** 光道最多渲染的块数（引擎偷段可产生超过初始分片数的段，超出按组合并） */
const MAX_BLOCKS = 32;

function toBlocks(live: LiveInfo | undefined, task: TaskRecord): SegmentInfo[] {
  if (live && live.segments.length > 0) return live.segments;
  const total = live?.total ?? task.total_size ?? 0;
  const dl = live?.downloaded ?? task.downloaded;
  if (total > 0) return [{ index: 0, downloaded: dl, len: total }];
  return [];
}

function capBlocks(segs: SegmentInfo[]): SegmentInfo[] {
  if (segs.length <= MAX_BLOCKS) return segs;
  const group = Math.ceil(segs.length / MAX_BLOCKS);
  const out: SegmentInfo[] = [];
  for (let i = 0; i < segs.length; i += group) {
    const chunk = segs.slice(i, i + group);
    out.push({
      index: out.length,
      downloaded: chunk.reduce((a, s) => a + s.downloaded, 0),
      len: chunk.reduce((a, s) => a + s.len, 0),
    });
  }
  return out;
}

export default function TaskRow({
  task,
  live,
  index,
  onChanged,
}: {
  task: TaskRecord;
  live?: LiveInfo;
  index: number;
  onChanged: () => void;
}) {
  const blocks = capBlocks(toBlocks(live, task));
  const total = live?.total ?? task.total_size ?? 0;
  const downloaded = live?.downloaded ?? task.downloaded;
  const pct = total > 0 ? Math.floor((downloaded / total) * 100) : 0;
  const name = task.filename ?? '解析中…';
  const running = task.state === 'running';
  const act = (fn: (id: string) => Promise<void>) => () => {
    fn(task.id).then(onChanged).catch((e) => alert(String(e)));
  };

  return (
    <div
      className={`task task--${task.state}`}
      style={{ animationDelay: `${Math.min(index, 8) * 40}ms` }}
    >
      <div className="task__head">
        <span className="task__name" title={task.url}>{name}</span>
        <span className="task__pct">{pct}%</span>
        <span className="task__state">
          <i className="task__state-dot" aria-hidden="true" />
          {STATE_META[task.state].label}
        </span>
      </div>

      <div className="task__lane" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
        {blocks.map((s, i) => {
          const fill = s.len > 0 ? Math.min(100, (s.downloaded / s.len) * 100) : 0;
          // 运行中：沿青→紫光谱按块位取样（水面焦散）；其余状态由 CSS 语义色接管
          const hue = blocks.length > 1 ? 195 + (i / (blocks.length - 1)) * 70 : 215;
          return (
            <div key={s.index} className="task__block" style={{ ['--i' as string]: i }}>
              <div
                className="task__block-fill"
                style={
                  running
                    ? { width: `${fill}%`, background: `hsl(${hue} 78% 64%)` }
                    : { width: `${fill}%` }
                }
              />
            </div>
          );
        })}
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
