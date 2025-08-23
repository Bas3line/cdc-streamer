use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub active_db: Option<String>,
    pub databases: Vec<DatabaseConfig>,
    pub streaming: StreamingConfig,
    pub metrics: MetricsConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatabaseConfig {
    pub name: String,
    pub db_type: DatabaseType,
    pub connection_string: String,
    pub tables: Vec<String>,
    pub poll_interval_ms: u64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub enum DatabaseType {
    PostgreSQL,
    ScyllaDB,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StreamingConfig {
    pub kafka: Option<KafkaConfig>,
    pub redis: Option<RedisConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub topic_prefix: String,
    pub batch_size: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RedisConfig {
    pub url: String,
    pub stream_prefix: String,
    pub max_len: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub port: u16,
}

impl Config {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str(&content)?)
    }
}
