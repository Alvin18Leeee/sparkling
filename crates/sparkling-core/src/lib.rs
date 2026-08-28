pub mod error;
pub mod segment;
pub mod task;

pub use error::{Result, SparklingError};
pub use segment::{split, take_over, Segment};
pub use task::{TaskId, TaskSpec, TaskState};
