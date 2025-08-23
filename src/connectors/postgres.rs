use crate::cdc::events::{ChangeEvent, ChangeOperation};
use crate::config::DatabaseConfig;
use anyhow::Result;
use sqlx::{PgPool, Row};
use std::collections::HashMap;

pub struct PostgresConnector {
    pool: PgPool,
    config: DatabaseConfig,
}

impl PostgresConnector {
    pub async fn new(config: DatabaseConfig) -> Result<Self> {
        let pool = PgPool::connect(&config.connection_string).await?;
        Ok(Self { pool, config })
    }

    pub async fn setup_replication_slot(&self, slot: &str) -> Result<()> {
        let q = format!("SELECT pg_create_logical_replication_slot('{}', 'wal2json')", slot);
        sqlx::query(&q).execute(&self.pool).await.ok();
        Ok(())
    }

    pub async fn read_wal(&self, slot: &str, tx: &tokio::sync::mpsc::Sender<ChangeEvent>) -> Result<()> {
        let q = format!("SELECT data FROM pg_logical_slot_get_changes('{}', NULL, NULL)", slot);
        let rows = sqlx::query(&q).fetch_all(&self.pool).await?;
        for row in rows {
            let data: String = row.get("data");
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&data) {
                if let Some(event) = self.parse_wal(&json) {
                    tx.send(event).await.ok();
                }
            }
        }
        Ok(())
    }

    fn parse_wal(&self, json: &serde_json::Value) -> Option<ChangeEvent> {
        let change = json.get("change")?;
        let kind = change.get("kind")?.as_str()?;
        let table = change.get("table")?.as_str()?;
        
        if !self.config.tables.contains(&table.to_string()) {
            return None;
        }

        let op = match kind {
            "insert" => ChangeOperation::Insert,
            "update" => ChangeOperation::Update, 
            "delete" => ChangeOperation::Delete,
            _ => return None,
        };

        let mut pk = HashMap::new();
        let after = change.get("columnvalues")
            .and_then(|v| serde_json::from_value::<HashMap<String, serde_json::Value>>(v.clone()).ok());
        let before = change.get("oldkeys")
            .and_then(|v| serde_json::from_value::<HashMap<String, serde_json::Value>>(v.clone()).ok());

        if let Some(ref cols) = after {
            if let Some((k, v)) = cols.iter().next() {
                pk.insert(k.clone(), v.clone());
            }
        }

        Some(ChangeEvent::new(
            self.config.name.clone(),
            table.to_string(),
            op,
            before,
            after,
            pk,
        ))
    }
}
