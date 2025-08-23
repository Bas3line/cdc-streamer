use crate::cdc::events::ChangeEvent;
use crate::config::KafkaConfig;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use anyhow::Result;
use tracing::{info, error};
use std::time::Duration;

pub struct KafkaStreamer {
    producer: FutureProducer,
    config: KafkaConfig,
}

impl KafkaStreamer {
    pub fn new(config: KafkaConfig) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self { producer, config })
    }

    pub async fn stream_changes(&self, mut rx: tokio::sync::mpsc::Receiver<ChangeEvent>) {
        info!("Starting Kafka streaming...");
        
        while let Some(event) = rx.recv().await {
            if let Err(e) = self.publish_event(&event).await {
                error!("Failed to publish to Kafka: {}", e);
            }
        }
    }

    async fn publish_event(&self, event: &ChangeEvent) -> Result<()> {
        let topic = format!("{}.{}", self.config.topic_prefix, event.topic_name());
        let payload = serde_json::to_string(event)?;
        let key = event.id.to_string();

        let record = FutureRecord::to(&topic)
            .key(&key)
            .payload(&payload);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => {
                info!("Published event {} to topic {}", event.id, topic);
                Ok(())
            }
            Err((e, _)) => {
                error!("Failed to publish event: {:?}", e);
                Err(anyhow::anyhow!("Kafka publish failed: {:?}", e))
            }
        }
    }
}
