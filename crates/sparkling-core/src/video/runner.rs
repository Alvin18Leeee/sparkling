//! yt-dlp 进程调用抽象：生产 TokioChildRunner（spawn 真 exe）与测试 FakeRunner。
//! VideoEngine 依赖本 trait 而非直接 spawn——CI 无需真二进制即可全量单测。
use crate::{Result, SparklingError};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// 主动杀进程的原因（区别于进程自身退出）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillReason {
    Pause,
    Cancel,
}

/// 一次运行的退出结果。killed 优先于 code 判定（被杀进程 code 无意义）
#[derive(Debug, Clone)]
pub struct RunResult {
    pub killed: Option<KillReason>,
    pub code: Option<i32>,
    /// stderr 末尾若干 KB（错误摘要提取用）
    pub stderr_tail: String,
}

pub struct RunHandle {
    pub done: JoinHandle<RunResult>,
    kill_tx: mpsc::UnboundedSender<KillReason>,
}

impl RunHandle {
    pub fn kill(&self, reason: KillReason) {
        let _ = self.kill_tx.send(reason);
    }
    pub async fn wait(self) -> RunResult {
        self.done.await.unwrap_or(RunResult {
            killed: None,
            code: None,
            stderr_tail: "runner 任务异常退出".into(),
        })
    }
}

#[async_trait]
pub trait YtDlpRunner: Send + Sync {
    // for<'a> 显式 HRTB：async_trait 会把省略的 &str 生命周期固化为方法参数
    // 生命周期（'lifeN: 'async_trait），导致返回的 future 非 'static、无法 tokio::spawn
    async fn start(
        &self,
        args: Vec<String>,
        on_line: Box<dyn for<'a> FnMut(&'a str) + Send + 'static>,
    ) -> Result<RunHandle>;
}

/// 生产实现：spawn 真 yt-dlp 进程
pub struct TokioChildRunner {
    pub bin: PathBuf,
}

/// stderr 保留末尾 max 字节（按 char 边界安全截断）
fn tail_utf8(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let start = s.len() - max;
    let mut i = start;
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    s[i..].to_string()
}

#[async_trait]
impl YtDlpRunner for TokioChildRunner {
    async fn start(
        &self,
        args: Vec<String>,
        mut on_line: Box<dyn for<'a> FnMut(&'a str) + Send + 'static>,
    ) -> Result<RunHandle> {
        let mut cmd = tokio::process::Command::new(&self.bin);
        cmd.args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            // 中文 Windows：无控制台的管道 stdout 会让 Python 选 GBK 编码，
            // yt-dlp 输出含非 ASCII 行（目标文件名等）时报 [Errno 22] 退出 120
            // （真机验收 BV13ZKdzCEnf 复现）。UTF-8 模式（PEP 540）强制全 IO 走 UTF-8
            .env("PYTHONUTF8", "1")
            // abort/泄漏兜底：句柄 drop 即杀进程
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            // CREATE_NO_WINDOW（tokio Command 在 Windows 提供固有 creation_flags）
            cmd.creation_flags(0x0800_0000);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| SparklingError::Other(format!("启动 yt-dlp 失败: {e}")))?;
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (kill_tx, mut kill_rx) = mpsc::unbounded_channel::<KillReason>();
        let done = tokio::spawn(async move {
            let mut child = child;
            let mut killed = None;
            // stderr 有损读取：子进程在无控制台管道下可能以 GBK 编码输出
            // （中文 Windows 默认 ANSI 代码页），严格 UTF-8 解码会提前 Err 丢数据
            let stderr_task = tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut buf = Vec::new();
                let mut stderr = stderr;
                let _ = stderr.read_to_end(&mut buf).await;
                String::from_utf8_lossy(&buf).into_owned()
            });
            {
                // stdout 按字节读行 + 有损解码：严格 UTF-8 的 lines() 遇 GBK 字节
                // 返回 Err，若据此 break 会丢弃读端——子进程继续写管道在 Windows 上
                // 得到 EINVAL（WriteFile），yt-dlp 报 [Errno 22] 退出 120
                // （真机验收 BV13ZKdzCEnf 复现）。有损解码永不断管。
                use tokio::io::{AsyncBufReadExt, BufReader};
                let mut reader = BufReader::new(stdout);
                let mut raw = Vec::new();
                loop {
                    raw.clear();
                    tokio::select! {
                        r = reader.read_until(b'\n', &mut raw) => match r {
                            Ok(0) => break, // EOF
                            Ok(_) => {
                                let line = String::from_utf8_lossy(&raw);
                                on_line(line.trim_end());
                            }
                            Err(_) => break,
                        },
                        r = kill_rx.recv() => {
                            killed = r;
                            let _ = child.kill().await;
                            break;
                        }
                    }
                }
            }
            let code = child.wait().await.ok().and_then(|s| s.code());
            let stderr_tail = stderr_task.await.unwrap_or_default();
            RunResult {
                killed,
                code,
                stderr_tail: tail_utf8(&stderr_tail, 4096),
            }
        });
        Ok(RunHandle { done, kill_tx })
    }
}

/// 测试用脚本步骤
pub enum ScriptStep {
    Lines(&'static [&'static str]),
    Delay(std::time::Duration),
    Exit(i32),
}

/// 测试用 Runner：start 弹出一个脚本按步回放
#[derive(Default)]
pub struct FakeRunner {
    pub scripts: Mutex<VecDeque<Vec<ScriptStep>>>,
    pub calls: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl YtDlpRunner for FakeRunner {
    async fn start(
        &self,
        args: Vec<String>,
        mut on_line: Box<dyn for<'a> FnMut(&'a str) + Send + 'static>,
    ) -> Result<RunHandle> {
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        self.calls.lock().unwrap().push(args);
        let (kill_tx, mut kill_rx) = mpsc::unbounded_channel::<KillReason>();
        let done = tokio::spawn(async move {
            let mut killed: Option<KillReason> = None;
            for step in script {
                match step {
                    ScriptStep::Lines(lines) => {
                        for l in lines {
                            if killed.is_some() {
                                break;
                            }
                            on_line(l);
                        }
                    }
                    ScriptStep::Delay(d) => {
                        tokio::select! {
                            _ = tokio::time::sleep(d) => {}
                            r = kill_rx.recv() => killed = r,
                        }
                    }
                    ScriptStep::Exit(code) => {
                        let k = kill_rx.try_recv().ok().or(killed);
                        return RunResult {
                            killed: k,
                            code: if k.is_some() { None } else { Some(code) },
                            stderr_tail: String::new(),
                        };
                    }
                }
                if killed.is_some() {
                    break;
                }
            }
            RunResult {
                killed,
                code: None,
                stderr_tail: String::new(),
            }
        });
        Ok(RunHandle { done, kill_tx })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn recv_all(rx: &mut mpsc::UnboundedReceiver<String>) -> Vec<String> {
        let mut out = vec![];
        while let Ok(l) = rx.try_recv() {
            out.push(l);
        }
        out
    }

    #[tokio::test]
    async fn fake_runs_script_and_exits() {
        let r = FakeRunner::default();
        r.scripts.lock().unwrap().push_back(vec![
            ScriptStep::Lines(&["SPARKLING|100|200|200|50"]),
            ScriptStep::Exit(0),
        ]);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let h = r
            .start(
                vec!["-J".into()],
                Box::new(move |l| {
                    let _ = tx.send(l.to_string());
                }),
            )
            .await
            .unwrap();
        let res = h.wait().await;
        assert_eq!(res.code, Some(0));
        assert!(res.killed.is_none());
        assert_eq!(
            recv_all(&mut rx),
            vec!["SPARKLING|100|200|200|50".to_string()]
        );
        assert_eq!(r.calls.lock().unwrap()[0], vec!["-J".to_string()]);
    }

    #[tokio::test]
    async fn fake_kill_during_delay_reports_killed() {
        let r = FakeRunner::default();
        r.scripts.lock().unwrap().push_back(vec![
            ScriptStep::Lines(&["SPARKLING|1|10|10|1"]),
            ScriptStep::Delay(Duration::from_secs(60)),
            ScriptStep::Exit(0),
        ]);
        let h = r.start(vec![], Box::new(|_| {})).await.unwrap();
        h.kill(KillReason::Pause);
        let res = h.wait().await;
        assert_eq!(res.killed, Some(KillReason::Pause));
        assert_eq!(res.code, None);
    }

    #[tokio::test]
    async fn fake_exit_code_propagates() {
        let r = FakeRunner::default();
        r.scripts
            .lock()
            .unwrap()
            .push_back(vec![ScriptStep::Exit(2)]);
        let h = r.start(vec![], Box::new(|_| {})).await.unwrap();
        let res = h.wait().await;
        assert_eq!(res.code, Some(2));
    }
}
