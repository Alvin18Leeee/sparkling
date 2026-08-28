use crate::task::TaskState;
use crate::{Result, SparklingError};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::path::Path;

/// 任务持久化记录（tasks 表的一行）
#[derive(Debug, Clone, Serialize)]
pub struct TaskRecord {
    pub id: String,
    pub url: String,
    pub state: TaskState,
    pub save_dir: String,
    pub filename: Option<String>,
    pub segments: u32,
    pub max_speed: Option<u64>,
    pub total_size: Option<u64>,
    pub downloaded: u64,
    pub error: Option<String>,
    pub created_at: i64,
}

pub struct TaskStore {
    conn: Connection,
}

impl TaskStore {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .map_err(|e| SparklingError::Other(format!("打开数据库失败: {e}")))?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()
            .map_err(|e| SparklingError::Other(format!("创建内存库失败: {e}")))?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS tasks (
                id          TEXT PRIMARY KEY,
                url         TEXT NOT NULL,
                state       TEXT NOT NULL,
                save_dir    TEXT NOT NULL,
                filename    TEXT,
                segments    INTEGER NOT NULL,
                max_speed   INTEGER,
                total_size  INTEGER,
                downloaded  INTEGER NOT NULL DEFAULT 0,
                error       TEXT,
                created_at  INTEGER NOT NULL
            );",
        )
        .map_err(|e| SparklingError::Other(format!("初始化表失败: {e}")))?;
        Ok(Self { conn })
    }

    pub fn insert(&self, r: &TaskRecord) -> Result<()> {
        self.conn
            .execute(
                "INSERT INTO tasks (id, url, state, save_dir, filename, segments, max_speed,
                                    total_size, downloaded, error, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    r.id, r.url, r.state.as_str(), r.save_dir, r.filename, r.segments,
                    r.max_speed, r.total_size, r.downloaded, r.error, r.created_at
                ],
            )
            .map_err(|e| SparklingError::Other(format!("插入失败: {e}")))?;
        Ok(())
    }

    fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<TaskRecord> {
        Ok(TaskRecord {
            id: row.get(0)?,
            url: row.get(1)?,
            state: TaskState::from_str(&row.get::<_, String>(2)?).unwrap_or(TaskState::Failed),
            save_dir: row.get(3)?,
            filename: row.get(4)?,
            segments: row.get::<_, u32>(5)?,
            max_speed: row.get(6)?,
            total_size: row.get(7)?,
            downloaded: row.get(8)?,
            error: row.get(9)?,
            created_at: row.get(10)?,
        })
    }

    pub fn get(&self, id: &str) -> Result<Option<TaskRecord>> {
        self.conn
            .query_row("SELECT * FROM tasks WHERE id = ?1", params![id], Self::row_to_record)
            .optional()
            .map_err(|e| SparklingError::Other(format!("查询失败: {e}")))
    }

    pub fn get_all(&self) -> Result<Vec<TaskRecord>> {
        let mut stmt = self
            .conn
            .prepare("SELECT * FROM tasks ORDER BY created_at DESC")
            .map_err(|e| SparklingError::Other(format!("查询失败: {e}")))?;
        let rows = stmt
            .query_map([], Self::row_to_record)
            .map_err(|e| SparklingError::Other(format!("查询失败: {e}")))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(|e| SparklingError::Other(format!("读取行失败: {e}")))?);
        }
        Ok(out)
    }

    pub fn update_state(&self, id: &str, state: TaskState, error: Option<&str>) -> Result<()> {
        self.conn
            .execute(
                "UPDATE tasks SET state = ?2, error = ?3 WHERE id = ?1",
                params![id, state.as_str(), error],
            )
            .map_err(|e| SparklingError::Other(format!("更新状态失败: {e}")))?;
        Ok(())
    }

    pub fn update_progress(&self, id: &str, downloaded: u64, total: u64) -> Result<()> {
        self.conn
            .execute(
                "UPDATE tasks SET downloaded = ?2, total_size = ?3 WHERE id = ?1",
                params![id, downloaded, total],
            )
            .map_err(|e| SparklingError::Other(format!("更新进度失败: {e}")))?;
        Ok(())
    }

    pub fn delete(&self, id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM tasks WHERE id = ?1", params![id])
            .map_err(|e| SparklingError::Other(format!("删除失败: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &str) -> TaskRecord {
        TaskRecord {
            id: id.into(),
            url: "http://example.com/a.bin".into(),
            state: TaskState::Queued,
            save_dir: "/tmp".into(),
            filename: Some("a.bin".into()),
            segments: 8,
            max_speed: None,
            total_size: None,
            downloaded: 0,
            error: None,
            created_at: 1700000000,
        }
    }

    #[test]
    fn insert_get_all_roundtrip() {
        let store = TaskStore::open_in_memory().unwrap();
        store.insert(&rec("t1")).unwrap();
        store.insert(&rec("t2")).unwrap();
        let all = store.get_all().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(store.get("t1").unwrap().unwrap().url, "http://example.com/a.bin");
        assert!(store.get("missing").unwrap().is_none());
    }

    #[test]
    fn update_state_and_progress() {
        let store = TaskStore::open_in_memory().unwrap();
        store.insert(&rec("t1")).unwrap();
        store.update_state("t1", TaskState::Failed, Some("网络错误")).unwrap();
        store.update_progress("t1", 12345, 67890).unwrap();
        let r = store.get("t1").unwrap().unwrap();
        assert_eq!(r.state, TaskState::Failed);
        assert_eq!(r.error.as_deref(), Some("网络错误"));
        assert_eq!(r.downloaded, 12345);
        assert_eq!(r.total_size, Some(67890));
        // 更新不存在的行不报错（幂等）
        store.update_state("nope", TaskState::Queued, None).unwrap();
    }

    #[test]
    fn delete_removes() {
        let store = TaskStore::open_in_memory().unwrap();
        store.insert(&rec("t1")).unwrap();
        store.delete("t1").unwrap();
        assert!(store.get("t1").unwrap().is_none());
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("tasks.db");
        {
            let store = TaskStore::open(&db).unwrap();
            store.insert(&rec("t1")).unwrap();
        }
        let store2 = TaskStore::open(&db).unwrap();
        assert_eq!(store2.get_all().unwrap().len(), 1);
    }
}
