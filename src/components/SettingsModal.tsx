import { useState } from 'react';
import { api } from '../api';
import type { ManagerConfig } from '../types';

export default function SettingsModal({
  config,
  onClose,
  onSaved,
}: {
  config: ManagerConfig | null;
  onClose: () => void;
  onSaved: (c: ManagerConfig) => void;
}) {
  const [maxConcurrent, setMaxConcurrent] = useState(config?.max_concurrent ?? 3);
  const [defaultSegments, setDefaultSegments] = useState(config?.default_segments ?? 8);
  const [limitKb, setLimitKb] = useState(
    config?.global_speed_limit ? Math.round(config.global_speed_limit / 1024) : 0
  );
  const [autoResume, setAutoResume] = useState(config?.auto_resume_on_start ?? true);
  const [err, setErr] = useState<string | null>(null);

  const save = async () => {
    const cfg: ManagerConfig = {
      max_concurrent: Math.max(1, Math.min(10, maxConcurrent)),
      auto_resume_on_start: autoResume,
      global_speed_limit: limitKb > 0 ? limitKb * 1024 : null,
      default_segments: Math.max(1, Math.min(64, defaultSegments)),
    };
    try {
      await api.updateConfig(cfg);
      onSaved(cfg);
    } catch (e) {
      setErr(String(e));
    }
  };

  return (
    <div className="modal-mask" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>设置</h2>
        <label>同时下载任务数（1–10）</label>
        <input type="number" min={1} max={10} value={maxConcurrent}
          onChange={(e) => setMaxConcurrent(Number(e.target.value) || 1)} />
        <label>默认分片数（1–64）</label>
        <input type="number" min={1} max={64} value={defaultSegments}
          onChange={(e) => setDefaultSegments(Number(e.target.value) || 8)} />
        <label>全局限速 KB/s（0 = 不限）</label>
        <input type="number" min={0} value={limitKb}
          onChange={(e) => setLimitKb(Number(e.target.value) || 0)} />
        <label className="checkbox">
          <input type="checkbox" checked={autoResume}
            onChange={(e) => setAutoResume(e.target.checked)} />
          重启后自动恢复未完成任务
        </label>
        {err && <div className="error">{err}</div>}
        <div className="modal-actions">
          <button onClick={onClose}>取消</button>
          <button className="primary" onClick={save}>保存</button>
        </div>
      </div>
    </div>
  );
}
