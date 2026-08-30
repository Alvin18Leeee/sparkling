import { useEffect, useState } from 'react';
import { api } from '../api';
import type { ManagerConfig, YtdlpStatus } from '../types';

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
  // 视频偏好（新增视频任务与"直接下载"的默认画质/字幕来源）
  const [maxHeight, setMaxHeight] = useState(config?.video_max_height ?? 1080);
  const [audioOnly, setAudioOnly] = useState(config?.video_audio_only ?? false);
  const [subLangs, setSubLangs] = useState(config?.video_sub_langs ?? 'zh-Hans,en');
  const [autoSubs, setAutoSubs] = useState(config?.video_auto_subs ?? false);
  // 组件与 cookie 区
  const [ytdlp, setYtdlp] = useState<YtdlpStatus | null>(null);
  const [updating, setUpdating] = useState(false);
  const [cookieBrowser, setCookieBrowser] = useState('edge');
  // 失败走 .error 染色（与保存失败同款视觉），成功用中性 settings-msg
  const [cookieMsg, setCookieMsg] = useState<{ text: string; isErr: boolean } | null>(null);

  useEffect(() => {
    api.getYtdlpStatus().then(setYtdlp).catch(() => {});
  }, []);

  const doUpdate = async () => {
    setUpdating(true);
    setCookieMsg(null);
    try {
      setYtdlp(await api.updateYtdlp());
      // 首次 bundled→app-data 转换后需重启才切到新版本；app-data 同版本重下
      // 则下次 spawn 即生效——前端无法区分，统一提示
      setCookieMsg({ text: 'yt-dlp 已更新，如版本未刷新请重启应用', isErr: false });
    } catch (e) {
      setCookieMsg({ text: String(e), isErr: true });
    } finally {
      setUpdating(false);
    }
  };
  const doImportCookies = async () => {
    setUpdating(true);
    setCookieMsg(null);
    try {
      await api.importCookies(cookieBrowser);
      // cookie 路径在应用启动时判定（VideoEngine 构造），导入后需重启才接入
      setCookieMsg({ text: 'Cookie 已导入，重启应用后生效', isErr: false });
    } catch (e) {
      setCookieMsg({ text: String(e), isErr: true });
    } finally {
      setUpdating(false);
    }
  };
  const doClearCookies = async () => {
    // 后端删文件忽略错误恒返回 Ok——清除语义上"没有 cookie"即目标态
    await api.clearCookies().catch(() => {});
    setCookieMsg({ text: 'Cookie 已清除', isErr: false });
  };

  const save = async () => {
    const cfg: ManagerConfig = {
      max_concurrent: Math.max(1, Math.min(10, maxConcurrent)),
      auto_resume_on_start: autoResume,
      global_speed_limit: limitKb > 0 ? limitKb * 1024 : null,
      default_segments: Math.max(1, Math.min(64, defaultSegments)),
      // 互斥口径：仅音频时 max_height 存 null（selectorFromPreference 与播放列表
      // 确认都依赖此语义）。cookie_file 由后端管理，前端不提交——后端 serde
      // default 接 null，cookie 存在性即生效（见 Task 9），配置层不存路径
      video_max_height: audioOnly ? null : maxHeight,
      video_audio_only: audioOnly,
      video_sub_langs: subLangs,
      video_auto_subs: autoSubs,
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
        <div className="modal__body">
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

        <h3>视频下载</h3>
        <label>默认画质（最高分辨率）</label>
        <select value={audioOnly ? 'audio' : String(maxHeight)}
          onChange={(e) => {
            if (e.target.value === 'audio') setAudioOnly(true);
            else { setAudioOnly(false); setMaxHeight(Number(e.target.value)); }
          }}>
          <option value="2160">2160p（4K）</option>
          <option value="1440">1440p</option>
          <option value="1080">1080p</option>
          <option value="720">720p</option>
          <option value="480">480p</option>
          <option value="audio">仅音频</option>
        </select>
        <label>字幕语言（逗号分隔，留空不下字幕）</label>
        <input value={subLangs} onChange={(e) => setSubLangs(e.target.value)} />
        <label className="checkbox">
          <input type="checkbox" checked={autoSubs}
            onChange={(e) => setAutoSubs(e.target.checked)} />
          默认包含自动生成字幕（CC）
        </label>

        <h3>组件与 Cookie</h3>
        <div className="settings-row">
          <span>yt-dlp {ytdlp?.version ?? '…'}</span>
          <button className="btn btn--sm" disabled={updating} onClick={doUpdate}>
            {updating ? '处理中…' : '检查更新'}
          </button>
        </div>
        <div className="settings-row">
          <select value={cookieBrowser} onChange={(e) => setCookieBrowser(e.target.value)}>
            <option value="edge">Edge</option>
            <option value="chrome">Chrome</option>
            <option value="firefox">Firefox</option>
          </select>
          <button className="btn btn--sm" disabled={updating} onClick={doImportCookies}>导入 Cookie</button>
          <button className="btn btn--sm" onClick={doClearCookies}>清除</button>
        </div>
        <div className="settings-note">Cookie 文件保存在本机应用数据目录，仅用于视频解析下载；清除即删除文件。导入 Cookie 可解锁登录内容与会员画质。</div>
        {cookieMsg && (
          <div className={cookieMsg.isErr ? 'error' : 'settings-msg'}>{cookieMsg.text}</div>
        )}
        {err && <div className="error">{err}</div>}
        <div className="modal-actions">
          <button className="btn" onClick={onClose}>取消</button>
          <button className="btn btn--primary" onClick={save}>保存</button>
        </div>
        </div>
      </div>
    </div>
  );
}
