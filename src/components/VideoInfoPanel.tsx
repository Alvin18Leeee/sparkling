import { useMemo, useState } from 'react';
import type { FormatEntry, ManagerConfig, PlaylistEntry, VideoInfo } from '../types';
import { fmtBytes, fmtDuration, selectorFromPreference } from '../types';

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
  const options = useMemo(() => qualityOptions(info.formats), [info.formats]);
  const [quality, setQuality] = useState(options[0]?.id ?? '');
  const [subLangs, setSubLangs] = useState(defaultSubLangs);
  const [autoSubs, setAutoSubs] = useState(defaultAutoSubs);
  // D4：选择记住为下次默认（画质 + 字幕），默认勾选
  const [remember, setRemember] = useState(true);
  const [selected, setSelected] = useState<Set<number>>(() =>
    new Set((info.playlist ?? []).map((_, i) => i))
  );
  const isPlaylist = info.playlist != null && info.playlist.length > 0;
  const selectedOpt = options.find((o) => o.id === quality);
  // 播放列表：probe 对列表固定不返回 formats（画质档位表恒空），下载格式
  // 改按用户画质偏好推导，而非 quality 状态（其仅对单视频的格式表有意义）
  const playlistFormat = selectorFromPreference(preference) ?? 'bv*+ba/b';

  const confirm = () => {
    const langs = subLangs.split(/[,，]/).map((s) => s.trim()).filter(Boolean);
    onConfirm({
      format: isPlaylist ? playlistFormat : (selectedOpt?.selector ?? 'bv*+ba/b'),
      subtitles: langs,
      auto_subs: autoSubs,
      entries: isPlaylist ? (info.playlist ?? []).filter((_, i) => selected.has(i)) : null,
      audioOnly: isPlaylist ? playlistFormat === 'ba/b' : selectedOpt?.id === 'audio',
      maxHeight: isPlaylist
        ? playlistFormat === 'ba/b' ? null : (preference?.video_max_height ?? null)
        : selectedOpt?.id.startsWith('h') ? Number(selectedOpt.id.slice(1)) : null,
      remember,
    });
  };

  return (
    <div className="video-panel">
      <div className="video-panel__head">
        {info.thumbnail && <img className="video-panel__thumb" src={info.thumbnail} alt="" />}
        <div className="video-panel__meta">
          <div className="video-panel__title" title={info.title}>{info.title}</div>
          <div className="video-panel__sub">
            {info.uploader && <span>{info.uploader} · </span>}
            <span>{fmtDuration(info.duration_sec)}</span>
            {isPlaylist && <span> · 共 {info.playlist!.length} 集（已选 {selected.size}）</span>}
          </div>
        </div>
      </div>

      {isPlaylist && (
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
      )}

      {!isPlaylist && (
        <>
          <label>画质</label>
          <select value={quality} onChange={(e) => setQuality(e.target.value)}>
            {options.map((o) => (
              <option key={o.id} value={o.id}>
                {o.label}{o.needsMerge && !ffmpegAvailable ? '（需 ffmpeg，缺失）' : ''}
              </option>
            ))}
          </select>
          {/* 格式详情（信息性） */}
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
        </>
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
