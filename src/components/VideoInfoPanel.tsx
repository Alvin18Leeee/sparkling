import { useMemo, useState } from 'react';
import type { FormatEntry, ManagerConfig, PlaylistEntry, VideoInfo } from '../types';
import { fmtBytes, fmtDuration, httpsUpgrade } from '../types';

/** 画质档位（UI 选择粒度；selector 是 yt-dlp -f 模板，跨视频稳定） */
interface QualityOption {
  id: string;
  label: string;
  selector: string;
  needsMerge: boolean;
}

/** 从格式表聚合可选画质档位（height 降序 + 仅音频）。
 *  needsMerge 标记分离流合并档位（画质档位选择器总是走 bestvideo+bestaudio
 *  模板），仅用于档位文案——无 ffmpeg 时提示"需 ffmpeg，缺失"。 */
function qualityOptions(formats: FormatEntry[]): QualityOption[] {
  const heights = [...new Set(formats.filter((f) => f.height).map((f) => f.height!))]
    .sort((a, b) => b - a)
    .map<QualityOption>((h) => ({
      id: `h${h}`,
      label: `${h}p`,
      selector: `bv*[height<=${h}]+ba/b[height<=${h}]`,
      needsMerge: true, // 分离流合并（档位选择总是走 bestvideo+bestaudio 模板）
    }));
  const audio: QualityOption = {
    id: 'audio',
    label: '仅音频（m4a）',
    selector: 'ba/b',
    needsMerge: false,
  };
  return [...heights, audio];
}

/** 播放列表的固定画质档位：--flat-playlist 不展开逐条格式（逐条 probe 太慢），
 *  用选择器模板让 yt-dlp 下载时对每条视频自动选最接近的格式（跨视频稳定） */
const PLAYLIST_QUALITIES: QualityOption[] = [
  { id: 'auto', label: '自动最佳', selector: 'bv*+ba/b', needsMerge: true },
  { id: 'h2160', label: '2160p（4K）', selector: 'bv*[height<=2160]+ba/b[height<=2160]', needsMerge: true },
  { id: 'h1440', label: '1440p', selector: 'bv*[height<=1440]+ba/b[height<=1440]', needsMerge: true },
  { id: 'h1080', label: '1080p', selector: 'bv*[height<=1080]+ba/b[height<=1080]', needsMerge: true },
  { id: 'h720', label: '720p', selector: 'bv*[height<=720]+ba/b[height<=720]', needsMerge: true },
  { id: 'h480', label: '480p', selector: 'bv*[height<=480]+ba/b[height<=480]', needsMerge: true },
  { id: 'audio', label: '仅音频（m4a）', selector: 'ba/b', needsMerge: false },
];

/** 偏好 → 播放列表预选档位：就近向上匹配标准档位（height<=h 语义下
 *  向上匹配即覆盖偏好），无偏好=自动最佳 */
function playlistPreferredId(cfg: ManagerConfig | null): string {
  if (!cfg) return 'auto';
  if (cfg.video_audio_only) return 'audio';
  const h = cfg.video_max_height;
  if (h == null) return 'auto';
  const fit = [480, 720, 1080, 1440, 2160].find((v) => v >= h);
  return fit ? `h${fit}` : 'auto';
}

export default function VideoInfoPanel({
  info,
  ffmpegAvailable,
  defaultSubLangs,
  defaultAutoSubs,
  preference,
  onConfirm,
  onCancel,
  busy,
}: {
  info: VideoInfo;
  ffmpegAvailable: boolean;
  defaultSubLangs: string;
  defaultAutoSubs: boolean;
  preference: ManagerConfig | null;
  onConfirm: (c: {
    format: string;
    subtitles: string[];
    auto_subs: boolean;
    entries: PlaylistEntry[] | null;
    audioOnly: boolean;
    maxHeight: number | null;
    remember: boolean;
  }) => void;
  onCancel: () => void;
  busy: boolean;
}) {
  const isPlaylist = info.playlist != null && info.playlist.length > 0;
  // 单视频：从格式表聚合档位（默认最高）；播放列表：固定档位（偏好预选）
  const options = useMemo(
    () => (isPlaylist ? PLAYLIST_QUALITIES : qualityOptions(info.formats)),
    [isPlaylist, info.formats]
  );
  const [quality, setQuality] = useState(() =>
    isPlaylist ? playlistPreferredId(preference) : (options[0]?.id ?? '')
  );
  const [subLangs, setSubLangs] = useState(defaultSubLangs);
  const [autoSubs, setAutoSubs] = useState(defaultAutoSubs);
  // D4：选择记住为下次默认（画质 + 字幕），默认勾选
  const [remember, setRemember] = useState(true);
  const [selected, setSelected] = useState<Set<number>>(() =>
    new Set((info.playlist ?? []).map((_, i) => i))
  );
  const selectedOpt = options.find((o) => o.id === quality);

  const confirm = () => {
    const langs = subLangs.split(/[,，]/).map((s) => s.trim()).filter(Boolean);
    onConfirm({
      format: selectedOpt?.selector ?? 'bv*+ba/b',
      subtitles: langs,
      auto_subs: autoSubs,
      entries: isPlaylist ? (info.playlist ?? []).filter((_, i) => selected.has(i)) : null,
      audioOnly: selectedOpt?.id === 'audio',
      maxHeight: selectedOpt?.id.startsWith('h') ? Number(selectedOpt.id.slice(1)) : null,
      remember,
    });
  };

  return (
    <div className="video-panel">
      <div className="video-panel__head">
        {info.thumbnail && (
          <img
            className="video-panel__thumb"
            src={httpsUpgrade(info.thumbnail)}
            // B 站等图床 Referer 防盗链：webview 图片请求携带页面 Referer 会 403；
            // no-referrer 不发 Referer，图床放行空 Referer（实测 200 vs 403）
            referrerPolicy="no-referrer"
            alt=""
          />
        )}
        <div className="video-panel__meta">
          <div className="video-panel__title" title={info.title}>{info.title}</div>
          <div className="video-panel__sub">
            {info.uploader && <span>{info.uploader} · </span>}
            <span>{fmtDuration(info.duration_sec)}</span>
            {isPlaylist && <span> · 共 {info.playlist!.length} 集</span>}
          </div>
        </div>
      </div>

      {isPlaylist && (
        <div className="video-panel__list-wrap">
          {/* 全选栏在滚动区外——留白恒定（不随滚动变化），天然固定在顶部 */}
          <label className="video-panel__selectall">
            <input
              type="checkbox"
              checked={selected.size === info.playlist!.length}
              onChange={(ev) =>
                setSelected(
                  ev.target.checked
                    ? new Set((info.playlist ?? []).map((_, i) => i))
                    : new Set()
                )
              }
            />
            全选（{selected.size}/{info.playlist!.length}）
          </label>
          <div className="video-panel__list">
            {(info.playlist ?? []).map((e, i) => (
              <label key={i} className="video-panel__entry">
                <input
                  type="checkbox"
                  checked={selected.has(i)}
                  onChange={(ev) => {
                    const next = new Set(selected);
                    if (ev.target.checked) next.add(i); else next.delete(i);
                    setSelected(next);
                  }}
                />
                <span className="video-panel__entry-title" title={e.title}>{e.title}</span>
                <span className="video-panel__entry-dur">{fmtDuration(e.duration_sec)}</span>
              </label>
            ))}
          </div>
        </div>
      )}

      <label>画质{isPlaylist ? '（应用于全部已选条目）' : ''}</label>
      <select value={quality} onChange={(e) => setQuality(e.target.value)}>
        {options.map((o) => (
          <option key={o.id} value={o.id}>
            {o.label}{o.needsMerge && !ffmpegAvailable ? '（需 ffmpeg，缺失）' : ''}
          </option>
        ))}
      </select>
      {!isPlaylist && (
        /* 格式详情（信息性；播放列表 flat 模式无逐条格式表） */
        <details className="video-panel__formats">
          <summary>可用格式（{info.formats.length}）</summary>
          <table>
            <tbody>
              {info.formats.map((f) => (
                <tr key={f.format_id}>
                  <td>{f.format_id}</td>
                  <td>{f.height ? `${f.height}p${f.fps ? `/${Math.round(f.fps)}` : ''}` : '音频'}</td>
                  <td>{f.ext}</td>
                  <td>{f.vcodec === 'none' ? '—' : f.vcodec}</td>
                  <td>{f.filesize != null ? fmtBytes(f.filesize) : fmtBytes(f.tbr ? f.tbr * 1024 / 8 : null)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </details>
      )}

      <label>字幕语言（逗号分隔，留空不下字幕）</label>
      <input value={subLangs} onChange={(e) => setSubLangs(e.target.value)} placeholder="zh-Hans,en" />
      <label className="checkbox">
        <input type="checkbox" checked={autoSubs} onChange={(e) => setAutoSubs(e.target.checked)} />
        包含自动生成字幕（CC）
      </label>
      <label className="checkbox">
        <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
        记住此选择（作为下次默认）
      </label>

      <div className="modal-actions">
        <button className="btn" disabled={busy} onClick={onCancel}>返回</button>
        <button className="btn btn--primary" disabled={busy || (isPlaylist && selected.size === 0)} onClick={confirm}>
          {busy ? '添加中…' : '下载'}
        </button>
      </div>
    </div>
  );
}
