import { useEffect, useRef, useState } from 'react';
import { api } from '../api';
import type { ManagerConfig, VideoInfo, VideoMeta } from '../types';
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
  // 已自动解析过的 URL：面板「返回」后不重复触发；解析失败不循环重试（手动按钮仍在）
  const lastAutoProbedRef = useRef('');

  // 视频链接自动解析：URL 成为视频链接且处于表单态时，防抖后直接进解析。
  // 防抖避免打字中途的无效请求；URL 改变后可再次自动触发
  useEffect(() => {
    const u = url.trim();
    if (!looksLikeVideoUrl(u) || video !== null || u === lastAutoProbedRef.current) return;
    const t = setTimeout(() => {
      lastAutoProbedRef.current = u;
      probe();
    }, 500);
    return () => clearTimeout(t);
    // probe 为当次渲染闭包（读取最新 url），无需进依赖
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [url, video]);

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
    remember: boolean;
  }) => {
    if (!video || video === 'probing') return; // 仅面板态可达；顺带收窄类型
    setBusy(true);
    setErr(null);
    try {
      const info = video.info;
      const targets = c.entries ?? [{ url: url.trim(), title: info.title }];
      for (const t of targets) {
        // 单视频：probe 已取回完整元数据；播放列表条目（flat-playlist）只有
        // 标题可用，其余字段 null。TaskRow 的 video_meta?.title 分支由此激活
        const meta: VideoMeta = c.entries
          ? { title: t.title, duration_sec: null, thumbnail: null, uploader: null, webpage_url: null }
          : {
              title: info.title,
              duration_sec: info.duration_sec,
              thumbnail: info.thumbnail,
              uploader: info.uploader,
              webpage_url: info.webpage_url,
            };
        await api.addVideoTask(
          t.url,
          { format: c.format, subtitles: c.subtitles, auto_subs: c.auto_subs },
          t.title,
          meta
        );
      }
      onAdded();
      // D4「记住此选择」：当前画质/字幕偏好写回配置。preference 为 null
      // （config 未加载）时无基准可合并 HTTP 四字段，跳过整个记住逻辑；
      // 失败静默降级——任务已入队，主操作不受影响
      if (c.remember && preference) {
        await api
          .updateConfig({
            ...preference,
            video_max_height: c.audioOnly ? null : c.maxHeight,
            video_audio_only: c.audioOnly,
            video_sub_langs: c.subtitles.join(','),
            video_auto_subs: c.auto_subs,
          })
          .catch(() => {});
      }
    } catch (e) {
      setErr(String(e));
    } finally {
      setBusy(false);
    }
  };

  // 直下路径（D4）：已有画质偏好时跳过解析，按偏好构造 selector 直接入队
  // （未解析拿不到标题，filename 留 null——后端按 yt-dlp 标题模板命名；字幕跟默认设置）
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
        <div className="modal__body">
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
    </div>
  );
}
