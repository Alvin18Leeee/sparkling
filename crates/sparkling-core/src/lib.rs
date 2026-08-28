pub mod error;
pub mod task;

pub use error::{Result, SparklingError};
pub use task::{TaskId, TaskSpec, TaskState};
