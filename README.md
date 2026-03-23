# Real-Time Fraud Detection Streaming Platform

A production-grade streaming pipeline that detects fraudulent transactions in real time using Apache Kafka, Spark Structured Streaming, and Delta Lake.

## Architecture
```
Transaction Generator → Apache Kafka → Spark Structured Streaming → Fraud Detection Engine → Delta Lake
```

## Fraud Detection Rules

| Rule | Description |
|------|-------------|
| High Amount | Flags transactions exceeding $1,000 |
| Velocity Fraud | Flags users with more than 5 transactions in 5 minutes |
| Geographic Anomaly | Flags transactions from high-risk countries |

## Tech Stack

- **Apache Kafka** (Redpanda) — real-time message streaming
- **Spark Structured Streaming** — stream processing engine
- **Delta Lake** — ACID-compliant storage with time travel
- **Python / PySpark** — transaction generator and fraud rules
- **Docker** — containerized Kafka setup

## Project Structure
```
fraud-detection-streaming-platform/
├── src/
│   ├── generator/          # Transaction generator + Kafka producer
│   ├── streaming/          # Spark consumer + Delta Lake writer
│   └── detection/          # Fraud detection rules
├── docker/                 # Docker Compose for Kafka
└── tests/                  # Unit tests
```

## How to Run

### Prerequisites
- Python 3.10+
- Apache Spark 3.4.1
- Docker

### Setup
```bash
# Clone the repo
git clone https://github.com/minnu-et/fraud-detection-streaming-platform.git
cd fraud-detection-streaming-platform

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start Kafka
sudo service docker start
docker-compose -f docker/docker-compose.yml up -d
docker exec fraud-kafka rpk topic create transactions
```

### Run the Pipeline

**Terminal 1 — Start Spark Streaming:**
```bash
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
python -m src.streaming.spark_consumer
```

**Terminal 2 — Start Transaction Generator:**
```bash
python -m src.generator.run_generator
```

## Key Design Decisions

**Why Kafka?** Decouples the generator from Spark, handles backpressure, and enables message replay via offsets. Transactions are partitioned by `user_id` to preserve ordering for stateful fraud detection.

**Why Delta Lake?** ACID transactions prevent partial writes, time travel enables audit trails (critical for financial compliance), and schema enforcement prevents bad data entering the pipeline.

**Why Spark Structured Streaming?** Unified batch/stream API, native Delta Lake integration, and windowed aggregations for velocity detection.
