use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRecord {
    pub id: Uuid,
    pub category: String,
    pub created_at: DateTime<Utc>,
    pub payload: Value,
}
