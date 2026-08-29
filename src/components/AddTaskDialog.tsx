import { useState } from 'react';
import { api } from '../api';
import type { ManagerConfig, VideoInfo } from '../types';
import { looksLikeVideoUrl, selectorFromPreference } from '../types';
import VideoInfoPanel from './VideoInfoPanel';

export default function AddTaskDialog({
  defaultSegments,
  ffmpegAvailable,
  defaultSubLangs,
  defaultAutoSubs,
  preference,
  onClose,
  onAdded,
}: {
  defaultSegments: number;
  ffmpegAvailable: boolean;
  defaultSubLangs: string;
  defaultAutoSubs: boolean;
  preference: ManagerConfig | null;
  onClose: () => void;
  onAdded: () => void;
}) {
  const [url, setUrl] = useState('');
  const [filename, setFilename] = useState('');
  const [segments, setSegments] = useState(defaultSegments);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  // 视频态：null=表单；'probing'=解析中；{ info }=解析结果面板
  const [video, setVideo] = useState<null | 'probing' | { info: VideoInfo }>(null);
  const isVideoUrl = looksLikeVideoUrl(url.trim());

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

  // 视频链接 → 实时解析（probe）出格式/播放列表，进 VideoInfoPanel 选择
  const probe = async () => {
    setBusy(true);
    setErr(null);
    setVideo('probing');
    try {
      const info = await api.probeVideo(url.trim());
      setVideo({ info });
    } catch (e) {
      setVideo(null);
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  // 视频面板确认 → 批量入队（列表多选逐条 addVideoTask；标题做文件名）
  const confirmVideo = async (c: {
    format: string;
    subtitles: string[];
    auto_subs: boolean;
    entries: { url: string; title: string }[] | null;
    audioOnly: boolean;
    maxHeight: number | null;
  }) => {
    if (!video || video === 'probing') return; // 仅面板态可达；顺带收窄类型
    setBusy(true);
    setErr(null);
    try {
      const targets = c.entries ?? [{ url: url.trim(), title: video.info.title }];
      for (const t of targets) {
        await api.addVideoTask(
          t.url,
          { format: c.format, subtitles: c.subtitles, auto_subs: c.auto_subs },
          t.title,
          null
        );
      }
      onAdded();
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  // 直下路径（D4）：已有画质偏好时跳过解析，按偏好构造 selector 直接入队
  // （未解析拿不到标题，filename 留 null——后端 build_args 对 None 兜底 "video"；字幕跟默认设置）
  const quickDownload = async () => {
    const selector = selectorFromPreference(preference);
    if (!selector) return;
    setBusy(true);
    setErr(null);
    try {
      await api.addVideoTask(
        url.trim(),
        {
          format: selector,
          subtitles: defaultSubLangs.split(/[,，]/).map((s) => s.trim()).filter(Boolean),
          auto_subs: defaultAutoSubs,
        },
        null,
        null
      );
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
        {video === 'probing' && <div className="video-panel__loading">正在解析视频信息…</div>}
        {video && video !== 'probing' ? (
          <>
            <VideoInfoPanel
              info={video.info}
              ffmpegAvailable={ffmpegAvailable}
              defaultSubLangs={defaultSubLangs}
              defaultAutoSubs={defaultAutoSubs}
              preference={preference}
              onConfirm={confirmVideo}
              onCancel={() => setVideo(null)}
              busy={busy}
            />
            {/* 面板态也要展示错误（confirmVideo 批量入队失败时给用户反馈） */}
            {err && <div className="error">{err}</div>}
          </>
        ) : (
          <>
            <label>URL</label>
            <input
              autoFocus
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="https://example.com/file.zip"
              onKeyDown={(e) => e.key === 'Enter' && !busy && submit()}
            />
            {isVideoUrl && (
              <div className="video-hint">
                <span>检测到视频链接</span>
                {selectorFromPreference(preference) && (
                  <button className="btn btn--sm btn--primary" disabled={busy} onClick={quickDownload}>
                    直接下载
                  </button>
                )}
                <button className="btn btn--sm" disabled={busy} onClick={probe}>
                  {busy ? '解析中…' : '解析视频'}
                </button>
              </div>
            )}
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
          </>
        )}
      </div>
    </div>
  );
}
