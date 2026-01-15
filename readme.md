# Real Time Clickstream Anomaly Detection

## Project Structure

```
RealTime-Clickstream-Anomaly/
├── backend/                  # FastAPI backend (Python)
│   ├── main.py               # FastAPI app entrypoint
│   ├── src/routes/           # API endpoints (realtime, batch)
│   └── ...
├── clickstream-frontend/     # Angular dashboard frontend
│   ├── src/app/              # Angular app code
│   └── ...
├── pipelines/                # PySpark batch & streaming jobs
│   ├── src/ingest_pipeline_batch/
│   ├── src/streaming_pipeline/
│   └── ...
├── data_producers/           # Rust & Python data generators
│   ├── clickstream_events/   # Rust real-time event producer
│   └── orders_transaction_history/ # Python batch generator
├── minio_data/               # MinIO S3-compatible data lake
├── docker-compose.yml        # Full stack orchestration
└── ...
```

## System Architecture

### Core Services
- **Zookeeper**: Kafka cluster coordination (2181)
- **Kafka**: Message broker (9092/29092)
- **MinIO**: S3-compatible object storage (9000/9001)
- **Spark**: Batch & streaming analytics (master/worker)
- **Backend API**: FastAPI (Python, port 8000)
- **Frontend**: Angular (port 4200)

### Data Flow Overview

```
User/Simulated Events
   ↓
Clickstream Producer (Rust) ──→ Kafka ──→ Spark Streaming Pipeline ──→ MinIO (Gold/Silver Tables)
   ↓
Orders History Generator (Python) ──→ Parquet ──→ MinIO
   ↓
Spark Batch Pipeline ──→ MinIO Gold Tables
   ↓
FastAPI Backend ──→ Angular Frontend (REST/WebSocket)
```

---

## Backend API (FastAPI)

**Base URL:** `http://localhost:8000/api`

### Real-time Endpoints (`/realtime`)
- `GET /realtime/anomalies` — List detected anomalies (from gold table)
- `GET /realtime/devices` — Device stats (from gold table)
- `GET /realtime/trending` — Trending page sections (from gold table)
- `GET /realtime/sessions` — Session stats (from gold table)
- `WebSocket /realtime/ws/anomalies` — Live anomaly stream
- `WebSocket /realtime/ws/devices` — Live device stats
- `WebSocket /realtime/ws/trending` — Live trending pages
- `WebSocket /realtime/ws/sessions` — Live session stats

### Batch Endpoints (`/batch`)
- `GET /batch/monthly-sales` — Monthly sales summary
- `GET /batch/country-stats` — Top countries by order count
- `GET /batch/hourly-peaks` — Hourly activity peaks

---

## Frontend (Angular Dashboard)

- Modern Angular app for real-time monitoring and analytics
- Connects to backend REST and WebSocket endpoints
- Live charts for anomalies, device stats, trending pages, and sessions

### Development

```bash
cd clickstream-frontend
ng serve
# Open http://localhost:4200/
```

For build/test details, see `clickstream-frontend/README.md`.

---

## Data Pipelines (PySpark)

### Streaming Pipeline
- Consumes Kafka `clickstream_events` topic
- Writes to MinIO (S3) gold tables:
  - `processed_data` (anomalies)
  - `device_stats`
  - `trending_pages`
  - `session_stats`
- Publishes to Kafka topics for real-time dashboard

### Batch Pipeline
- Processes historical orders Parquet data
- Aggregates to gold tables:
  - `monthly_sales`
  - `country_stats`
  - `hourly_peaks`

---

## Data Producers

### Clickstream Events Producer (Rust)
- Real-time user event simulation (configurable via env)
- Publishes to Kafka `clickstream_events`

### Orders Transaction History Generator (Python)
- Generates synthetic historical orders as Parquet
- Used for batch pipeline and MinIO initialization

---

## Quick Start

### Prerequisites
- Docker & Docker Compose

### Run All Services

```bash
docker compose up -d --build
```

---

## Troubleshooting & Tips

- **Kafka**: Ensure Zookeeper is up before Kafka. Check topic health in Docker logs.
- **MinIO**: Access console at [http://localhost:9001](http://localhost:9001) (user/pass: minioadmin)
- **Backend**: If API fails, check S3/MinIO env vars and FastAPI logs.
- **Frontend**: If charts don't update, verify backend is reachable at `http://localhost:8000/api`.
- **Spark**: For pipeline errors, check logs in `spark-master`, `batch-job`, or `realtime-pipeline` containers.

---

## Example Events

### Clickstream Event (JSON)
```json
{
  "event_timestamp": "2024-12-03T10:30:45.123Z",
  "session_id": "session_0",
  "user_id": "user_5432",
  "ip_address": "192.168.1.42",
  "country": "US",
  "click_sequence": 1,
  "page_category": "ELECTRONICS",
  "product_code": "PROD5231",
  "action_type": "VIEW_PRODUCT",
  "device_type": "MOBILE",
  "page_section": "MIDDLE_GRID"
}
```

### Transaction Record (Parquet)
| order_id | user_id | total_amount_usd | order_status | payment_method | shipping_country |
|----------|---------|------------------|--------------|----------------|------------------|
| uuid-1   | user_42 | 125.50           | COMPLETED    | CREDIT_CARD    | US               |

---

## References
- See `backend/README.md`, `pipelines/README.md`, and `clickstream-frontend/README.md` for more details on each component.
- Events per thread: 1,000 (configurable via `MAX_EVENTS`)
- Kafka broker: `kafka:29092` (internal network)
- Kafka topic: `clickstream_events`

#### Orders Transaction History Generator (Python)
Generates synthetic historical transaction data for the past 2 years with:
- **Transaction Details**: Order ID, user ID, session ID, timestamp, total amount
- **Order Info**: Items count, payment method (credit card, PayPal, gift card, bank transfer)
- **Order Status**: Completed, failed, returned, pending (weighted by payment method)
- **Shipping**: Destination country

**Configuration**:
- Dataset size: 50,000 rows
- Output format: Parquet file
- Realistic temporal patterns (seasonal weights, hourly patterns)

## Quick Start

### Prerequisites
- Docker
- Docker Compose

### Running the Application

1. **Start all services**:
   ```bash
   docker-compose up
   ```
   
   This will:
   - Start Zookeeper
   - Start Kafka with the `clickstream_events` topic
   - Build and run the clickstream events producer
   - Generate initial transaction history data

2. **View logs**:
   ```bash
   # All services
   docker-compose logs -f
   
   # Specific service (e.g., clickstream producer)
   docker-compose logs -f clickstream-producer
   ```

3. **Stop the application**:
   ```bash
   docker-compose down
   ```

### Configuration

You can customize the producers by modifying environment variables in `docker-compose.yml`:

**Clickstream Producer**:
- `KAFKA_BROKER`: Kafka broker address (default: `kafka:29092`)
- `KAFKA_TOPIC`: Kafka topic name (default: `clickstream_events`)
- `NUM_THREADS`: Number of producer threads (default: `1`)
- `MAX_EVENTS`: Events to generate per thread (default: `1000`)
- `RUST_LOG`: Log level (default: `info`)

**Orders Transaction History**:
- `OUTPUT_PATH`: Where to save the parquet file (default: `Orders_Transaction_History.parquet`)

### Data Flow

```
Clickstream Events:
  Real-time User Actions → Rust Producer → Kafka Topic → (downstream consumers)

Transaction History:
  Synthetic Historical Data → Python Generator → Parquet File
```

## Event Examples

### Clickstream Event (JSON)
```json
{
  "event_timestamp": "2024-12-03T10:30:45.123Z",
  "session_id": "session_0",
  "user_id": "user_5432",
  "ip_address": "192.168.1.42",
  "country": "US",
  "click_sequence": 1,
  "page_category": "ELECTRONICS",
  "product_code": "PROD5231",
  "action_type": "VIEW_PRODUCT",
  "device_type": "MOBILE",
  "page_section": "MIDDLE_GRID"
}
```

### Transaction Record (Parquet)
| order_id | user_id | total_amount_usd | order_status | payment_method | shipping_country |
|----------|---------|------------------|--------------|----------------|------------------|
| uuid-1   | user_42 | 125.50           | COMPLETED    | CREDIT_CARD    | US               |

## Development

### Building Clickstream Producer Locally
```bash
cd data_producers/clickstream_events
cargo build --release
```

### Testing Transaction History Generator Locally
```bash
cd data_producers/orders_transaction_history
python3 src/historic_dataset_generation.py
```

## Troubleshooting

- **Kafka connection issues**: Ensure Zookeeper is running before Kafka
- **Producer not starting**: Check that Kafka broker is accessible at the configured address
- **Out of memory**: Reduce `NUM_THREADS` or `MAX_EVENTS` values
