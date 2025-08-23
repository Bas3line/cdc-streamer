use crate::cdc::events::ChangeEvent;
use crate::config::RedisConfig;
use redis::AsyncCommands;
use anyhow::Result;
use tracing::{info, error};
use redis::streams::StreamMaxlen;

pub struct RedisStreamer {
    client: redis::Client,
    config: RedisConfig,
}

impl RedisStreamer {
    pub fn new(config: RedisConfig) -> Result<Self> {
        let client = redis::Client::open(config.url.as_str())?;
        Ok(Self { client, config })
    }

    pub async fn stream_changes(&self, mut rx: tokio::sync::mpsc::Receiver<ChangeEvent>) {
        info!("Starting Redis streaming...");
        
        let mut conn = match self.client.get_async_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to connect to Redis: {}", e);
                return;
            }
        };

        while let Some(event) = rx.recv().await {
            if let Err(e) = self.publish_event(&mut conn, &event).await {
                error!("Failed to publish to Redis: {}", e);
            }
        }
    }

    async fn publish_event(
        &self,
        conn: &mut redis::aio::Connection,
        event: &ChangeEvent,
    ) -> Result<()> {
        let stream_name = format!("{}.{}", self.config.stream_prefix, event.topic_name());
        let payload = serde_json::to_string(event)?;

        let _: String = conn
            .xadd(
                &stream_name,
                "*",
                &[("data", payload), ("event_id", event.id.to_string())],
            )
            .await?;

        if let Some(max_len) = self.config.max_len {
            let _: i32 = conn
                .xtrim(&stream_name, StreamMaxlen::Approx(max_len))
                .await?;
        }

        info!("Published event {} to Redis stream {}", event.id, stream_name);
        Ok(())
    }
}
