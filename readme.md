# Real Time Clickstream Anomaly Detection

## Project Structure

```
RealTime-Clickstream-Anomaly/
├── data_producers/
│   ├── clickstream_events/        # Real-time data
│   │   ├── src/
│   │   │   ├── main.rs            
│   │   │   └── handle_parquet.rs 
│   │   ├── Cargo.toml             
│   │   └── Dockerfile
│   │
│   └── orders_transaction_history/ # Historical data
│       ├── src/
│       │   └── historic_dataset_generation.py 
│       └── Dockerfile             
├── docker-compose.yml            
└── readme.md                      
```
## System Architecture

### Infrastructure Services
- **Zookeeper**: Manages Kafka cluster coordination (port 2181)
- **Kafka**: Distributed message broker (port 9092)
  - Topic: `clickstream_events` (1 partition, 1 replica)
  - Data persistence enabled

### Data Producers

#### Clickstream Events Producer (Rust)
Generates real-time user interaction events with the following attributes:
- **Event Data**: Timestamp, session ID, user ID, IP address, country
- **User Behavior**: Click sequence, action type (view, add to cart, purchase, etc.), page category
- **Device Info**: Device type (mobile, desktop, tablet), page section clicked
- **Product Info**: Product code, page category (apparel, electronics, home goods, etc.)

**Configuration**:
- Threads: 1 (configurable via `NUM_THREADS`)
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
