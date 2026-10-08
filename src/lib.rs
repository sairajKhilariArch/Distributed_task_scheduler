use serde::{Serialize, Deserialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Running,
    Completed { result: String },
    Failed { error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub payload: String,
    pub status: TaskStatus,
    pub assigned_worker: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Task {
    pub fn new(payload: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            payload,
            status: TaskStatus::Pending,
            assigned_worker: None,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubmitTaskRequest {
    pub payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterWorkerRequest {
    pub worker_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskResultRequest {
    pub worker_id: String,
    pub status: TaskStatus,
}
