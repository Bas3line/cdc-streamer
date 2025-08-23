use crate::cdc::events::ChangeEvent;
use crate::config::{DatabaseConfig, DatabaseType};
use crate::connectors::{postgres::PostgresConnector, scylla::ScyllaConnector};
use anyhow::Result;
use std::time::Duration;

pub struct CDCEngine {
    config: DatabaseConfig,
}

impl CDCEngine {
    pub fn new(config: DatabaseConfig) -> Self {
        Self { config }
    }

    pub async fn start(&mut self) -> Result<tokio::sync::mpsc::Receiver<ChangeEvent>> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        
        match self.config.db_type {
            DatabaseType::PostgreSQL => {
                let connector = PostgresConnector::new(self.config.clone()).await?;
                let slot = format!("slot_{}", self.config.name);
                connector.setup_replication_slot(&slot).await.ok();
                tokio::spawn(async move {
                    loop {
                        connector.read_wal(&slot, &tx).await.ok();
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
            }
            DatabaseType::ScyllaDB => {
                let connector = ScyllaConnector::new(self.config.clone()).await?;
                tokio::spawn(async move {
                    connector.read_cdc(tx).await.ok();
                });
            }
        }
        
        Ok(rx)
    }
}
