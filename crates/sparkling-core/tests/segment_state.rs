use sparkling_core::segment::{split, take_over, Segment};

#[test]
fn split_normal() {
    let segs = split(100, 8);
    assert_eq!(segs.len(), 8);
    // 100 = 8*12 + 4，前 4 段 13 字节
    let lens: Vec<u64> = segs.iter().map(|s| s.len()).collect();
    assert_eq!(lens, vec![13, 13, 13, 13, 12, 12, 12, 12]);
    assert_eq!(segs[0].start, 0);
    assert_eq!(segs[0].end, 12);
    assert_eq!(segs[7].end, 99);
    // 段间无缝衔接
    for w in segs.windows(2) {
        assert_eq!(w[0].end + 1, w[1].start);
    }
    for s in &segs {
        assert_eq!(s.downloaded, 0);
    }
}

#[test]
fn split_fewer_bytes_than_segments() {
    let segs = split(3, 8);
    assert_eq!(segs.len(), 3);
    assert!(segs.iter().all(|s| s.len() == 1));
}

#[test]
fn split_exact() {
    let segs = split(8, 8);
    assert_eq!(segs.len(), 8);
    assert!(segs.iter().all(|s| s.len() == 1));
}

#[test]
fn split_zero_and_guard() {
    assert!(split(0, 8).is_empty());
    let segs = split(100, 0); // n=0 视为 1
    assert_eq!(segs.len(), 1);
    assert_eq!(segs[0].len(), 100);
}

#[test]
fn split_huge() {
    let segs = split(u64::MAX, 8);
    assert_eq!(segs.len(), 8);
    assert_eq!(segs[0].start, 0);
    // 含端点语义：sum == u64::MAX，但末段 end == u64::MAX - 1（end+1 才是总字节）
    assert_eq!(segs[7].end, u64::MAX - 1);
    let total: u128 = segs.iter().map(|s| s.len() as u128).sum();
    assert_eq!(total, u64::MAX as u128);
}

#[test]
fn take_over_splits_remaining_in_half() {
    let mut seg = Segment { index: 0, start: 0, end: 99, downloaded: 10 };
    // 剩余 [10,99] 共 90 字节，右半 [55,99]
    let stolen = take_over(&mut seg, 8).unwrap();
    assert_eq!(stolen.index, 8);
    assert_eq!(stolen.start, 55);
    assert_eq!(stolen.end, 99);
    assert_eq!(stolen.downloaded, 0);
    assert_eq!(seg.end, 54);
    assert_eq!(seg.downloaded, 10);
    // 两段剩余之和不变
    assert_eq!(seg.remaining() + stolen.remaining(), 90);
}

#[test]
fn take_over_refuses_tiny_remaining() {
    // end=10 含端点 → len=11：downloaded=11 即剩余 0，downloaded=10 即剩余 1
    let mut seg = Segment { index: 0, start: 0, end: 10, downloaded: 11 }; // 剩余 0
    assert!(take_over(&mut seg, 1).is_none());
    let mut seg = Segment { index: 0, start: 0, end: 10, downloaded: 10 }; // 剩余 1
    assert!(take_over(&mut seg, 1).is_none());
}
