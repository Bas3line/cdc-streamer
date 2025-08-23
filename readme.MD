# Rust CDC Streamer
High-performance Change Data Capture (CDC) tool built in Rust. Streams real-time database changes from PostgreSQL and ScyllaDB to Kafka and Redis for analytics, monitoring, and downstream processing.

![Rust](https://img.shields.io/badge/Rust-000000?style=for-the-badge\&logo=rust\&logoColor=white)
![Tokio](https://img.shields.io/badge/Tokio-222222?style=for-the-badge\&logo=tokio\&logoColor=white)
![SQLx](https://img.shields.io/badge/SQLx-2E86C1?style=for-the-badge\&logo=postgresql\&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge\&logo=apachekafka\&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-D82C20?style=for-the-badge\&logo=redis\&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge\&logo=postgresql\&logoColor=white)
![ScyllaDB](https://img.shields.io/badge/ScyllaDB-53D1F6?style=for-the-badge\&logo=scylladb\&logoColor=000)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge\&logo=prometheus\&logoColor=white)
![Clap](https://img.shields.io/badge/Clap-3B3B3B?style=for-the-badge\&logo=rust\&logoColor=white)
![Tracing](https://img.shields.io/badge/Tracing-555555?style=for-the-badge\&logo=rust\&logoColor=white)


## Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │  Rust CDC       │    │     Kafka       │
│ (Logical Repl.) ├────┤   Streamer      ├────┤   (Topics)      │
└─────────────────┘    │                 │    └─────────────────┘
                       │                 │    
┌─────────────────┐    │   - WAL Parser  │    ┌─────────────────┐
│    ScyllaDB     │    │   - CDC Engine  │    │      Redis      │
│  (CDC Logs)     ├────┤   - Streaming   ├────┤   (Streams)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘

```

## Clone the repo and configure databases in `config/config.yaml`.

Start services with Docker Compose:
```
docker-compose -f docker/docker-compose.yml up -d
```

Run the streamer:
```
cargo run -- --only psql    # For PostgreSQL
cargo run -- --only scylla  # For ScyllaDB
```