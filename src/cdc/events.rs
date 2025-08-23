use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChangeOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub database: String,
    pub table: String,
    pub operation: ChangeOperation,
    pub before: Option<HashMap<String, serde_json::Value>>,
    pub after: Option<HashMap<String, serde_json::Value>>,
    pub primary_key: HashMap<String, serde_json::Value>,
    pub transaction_id: Option<String>,
}

impl ChangeEvent {
    pub fn new(
        database: String,
        table: String,
        operation: ChangeOperation,
        before: Option<HashMap<String, serde_json::Value>>,
        after: Option<HashMap<String, serde_json::Value>>,
        primary_key: HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            database,
            table,
            operation,
            before,
            after,
            primary_key,
            transaction_id: None,
        }
    }

    pub fn topic_name(&self) -> String {
        format!("cdc.{}.{}", self.database, self.table)
    }
}
