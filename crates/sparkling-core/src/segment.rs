use serde::{Deserialize, Serialize};

/// 一个下载分片。`end` 为含端点偏移。
/// 不变量：`downloaded` 表示从 `start` 起已连续写入的字节数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Segment {
    pub index: usize,
    pub start: u64,
    pub end: u64,
    pub downloaded: u64,
}

impl Segment {
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn remaining(&self) -> u64 {
        self.len() - self.downloaded
    }
    /// 下一次 Range 请求的起始偏移
    pub fn next_offset(&self) -> u64 {
        self.start + self.downloaded
    }
}

/// 把 `total` 字节尽量均匀切成 `n` 段。total==0 返回空。
/// total < n 时实际段数 = total（每段至少 1 字节）。n==0 视为 1。
pub fn split(total: u64, n: u32) -> Vec<Segment> {
    if total == 0 {
        return Vec::new();
    }
    let n = (n.max(1) as u64).min(total);
    let base = total / n;
    let rem = total % n;
    let mut segs = Vec::with_capacity(n as usize);
    let mut start = 0u64;
    for i in 0..n {
        let len = base + if i < rem { 1 } else { 0 };
        segs.push(Segment {
            index: i as usize,
            start,
            end: start + len - 1,
            downloaded: 0,
        });
        start += len;
    }
    segs
}

/// 动态偷段：把 `from` 的剩余部分右半切出来作为新段（新 worker 接手）。
/// 剩余 < 2 字节时返回 None（不值得切）。
pub fn take_over(from: &mut Segment, new_index: usize) -> Option<Segment> {
    let rem = from.remaining();
    if rem < 2 {
        return None;
    }
    let half = rem / 2;
    let new_start = from.next_offset() + half;
    let stolen = Segment {
        index: new_index,
        start: new_start,
        end: from.end,
        downloaded: 0,
    };
    from.end = new_start - 1;
    Some(stolen)
}
