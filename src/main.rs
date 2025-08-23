use clap::Parser;
use tracing_subscriber::fmt::init;
use anyhow::Result;
use rust_cdc_streamer::{config::Config, cdc::engine::CDCEngine, streaming::kafka::KafkaStreamer};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "config/config.yaml")]
    config: String,
    #[arg(long)]
    only: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init();
    
    let args = Args::parse();
    let config = Config::from_file(&args.config)?;
    
    println!("Starting CDC Streamer");
    
    let db_choice = args.only.unwrap_or_else(|| {
        config.active_db.clone().unwrap_or("PostgreSQL".to_string())
    }).to_lowercase();

    let mut handles = vec![];
    
    for db_config in config.databases {
        let should_run = match &db_config.db_type {
            rust_cdc_streamer::config::DatabaseType::PostgreSQL => {
                db_choice == "psql" || db_choice == "postgresql"
            }
            rust_cdc_streamer::config::DatabaseType::ScyllaDB => {
                db_choice == "scylla" || db_choice == "scylladb"
            }
        };

        if should_run {
            let mut engine = CDCEngine::new(db_config);
            if let Ok(rx) = engine.start().await {
                if let Some(kafka_config) = &config.streaming.kafka {
                    let streamer = KafkaStreamer::new(kafka_config.clone())?;
                    let handle = tokio::spawn(async move {
                        streamer.stream_changes(rx).await;
                    });
                    handles.push(handle);
                }
            }
            break;
        }
    }
    
    for handle in handles {
        handle.await?;
    }
    
    Ok(())
}
