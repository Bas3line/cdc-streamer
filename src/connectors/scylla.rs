use crate::cdc::events::{ChangeEvent, ChangeOperation};
use crate::config::DatabaseConfig;
use anyhow::Result;
use scylla::{Session, SessionBuilder};
use std::collections::HashMap;
use tokio::time::{interval, Duration};

pub struct ScyllaConnector {
    session: Session,
    config: DatabaseConfig,
}

impl ScyllaConnector {
    pub async fn new(config: DatabaseConfig) -> Result<Self> {
        let session = SessionBuilder::new()
            .known_node(&config.connection_string)
            .user("cassandra", "cassandra")
            .build()
            .await?;
        Ok(Self { session, config })
    }

    pub async fn read_cdc(&self, tx: tokio::sync::mpsc::Sender<ChangeEvent>) -> Result<()> {
        let mut iv = interval(Duration::from_millis(self.config.poll_interval_ms));
        loop {
            iv.tick().await;
            for table in &self.config.tables {
                let q = format!("SELECT * FROM {}.{} LIMIT 10", self.config.name, table);
                if let Ok(rows) = self.session.query(q, ()).await {
                    if let Some(rs) = rows.rows {
                        for _row in rs {
                            let mut pk = HashMap::new();
                            let mut after = HashMap::new();
                            pk.insert("id".to_string(), serde_json::Value::String("cdc".to_string()));
                            after.insert("table".to_string(), serde_json::Value::String(table.clone()));
                            let event = ChangeEvent::new(
                                self.config.name.clone(),
                                table.clone(),
                                ChangeOperation::Update,
                                None,
                                Some(after),
                                pk,
                            );
                            tx.send(event).await.ok();
                        }
                    }
                }
            }
        }
    }
}
