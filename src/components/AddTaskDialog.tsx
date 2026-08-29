import { useState } from 'react';
import { api } from '../api';

export default function AddTaskDialog({
  defaultSegments,
  onClose,
  onAdded,
}: {
  defaultSegments: number;
  onClose: () => void;
  onAdded: () => void;
}) {
  const [url, setUrl] = useState('');
  const [filename, setFilename] = useState('');
  const [segments, setSegments] = useState(defaultSegments);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    if (!url.trim()) {
      setErr('请输入 URL');
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      await api.addTask(url.trim(), filename.trim() || null, segments);
      onAdded();
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="modal-mask" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>新建下载</h2>
        <label>URL</label>
        <input
          autoFocus
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          placeholder="https://example.com/file.zip"
          onKeyDown={(e) => e.key === 'Enter' && submit()}
        />
        <label>文件名（可选，留空自动识别）</label>
        <input value={filename} onChange={(e) => setFilename(e.target.value)} />
        <label>分片数（1–64）</label>
        <input
          type="number"
          min={1}
          max={64}
          value={segments}
          onChange={(e) => setSegments(Number(e.target.value) || defaultSegments)}
        />
        {err && <div className="error">{err}</div>}
        <div className="modal-actions">
          <button className="btn" onClick={onClose}>取消</button>
          <button className="btn btn--primary" disabled={busy} onClick={submit}>
            {busy ? '添加中…' : '添加'}
          </button>
        </div>
      </div>
    </div>
  );
}
