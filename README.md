# Real-Time Hospital Beds Monitoring Pipeline

A real-time streaming pipeline simulating IoT telemetry from 1000 hospital beds, with anomaly injection for critical clinical events. Built to explore end-to-end stream processing with Kafka, PyFlink, and ClickHouse — with full observability via Prometheus and Grafana.

> **Note:** Data is synthetically generated. This project was built as a technical proof-of-concept to validate pipeline architecture and toolchain integration. A production-grade successor using real IoT sensor data (OpenAQ), Kafka Schema Registry, Flink CEP, and ClickHouse materialized views is in progress.

---

## Architecture

```
Python Producer
(1000 beds, every 500ms)
(1% anomaly injection rate)
        │
        ▼
  Kafka Topic
 (hospital_vitals)
  3 partitions
        │
        ▼
    PyFlink Job
(5-min tumbling windows
 per bed_id)
        │
        ├──────────────────┐
        ▼                  ▼
vitals_raw           vitals_agg
(MergeTree)     (ReplacingMergeTree)
        │                  │
        └────────┬─────────┘
                 ▼
             Grafana
        (ClickHouse dashboards)

Prometheus ◄─── PyFlink metrics (port 9249)
                ClickHouse metrics (port 9363)
     │
     ▼
  Grafana
(pipeline health dashboard)
```

---

## Stack

| Component | Image | Role |
|---|---|---|
| **Python Producer** | custom | Generates synthetic vitals + anomaly events, publishes to Kafka every 500ms |
| **Apache Kafka** | `confluentinc/cp-kafka:7.5.0` | Message broker, 3-partition topic, idempotent producer config |
| **Zookeeper** | `confluentinc/cp-zookeeper:7.5.0` | Kafka coordination |
| **PyFlink** | custom | Consumes Kafka topic, runs 5-min windowed aggregations, writes to ClickHouse |
| **ClickHouse** | `clickhouse/clickhouse-server:latest` | OLAP storage — raw events (`MergeTree`) + aggregates (`ReplacingMergeTree`) |
| **Prometheus** | `prom/prometheus:latest` | Scrapes PyFlink (9249) and ClickHouse (9363) |
| **Grafana** | `grafana/grafana:10.4.0` | Two provisioned dashboards — ClickHouse datasource + Prometheus datasource |

---

## Data Schema

Each event published to `hospital_vitals`:

```json
{
  "timestamp":          "2024-11-01 14:23:01.500",
  "bed_id":             "BED_00742",
  "oxygen_sat":         97.8,
  "body_temp":          37.1,
  "heart_rate":         78,
  "blood_pressure_sys": 121,
  "glucose":            104.3,
  "respiration_rate":   16
}
```

**Throughput:** 1000 beds × 2 events/sec ≈ 2,000 events/sec

### Anomaly Injection

The producer injects critical clinical events at a **1% probability per reading**:

| Anomaly | Field affected | Injected range |
|---|---|---|
| Hypoxia | `oxygen_sat` | 80–89% |
| Fever | `body_temp` | 39–41°C |
| Tachycardia | `heart_rate` | 150–200 bpm |

This produces a realistic signal distribution in ClickHouse that can be queried for outlier analysis.

---

## ClickHouse Schema

Defined in `clickhouse-init/init.sql`, auto-applied on container start.

```sql
CREATE TABLE IF NOT EXISTS hospital.vitals_raw (
    bed_id              String,
    heart_rate          Int32,
    oxygen_sat          Float64,
    body_temp           Float64,
    blood_pressure_sys  Int32,
    glucose             Float64,
    respiration_rate    Int32,
    recorded_at         String
) ENGINE = MergeTree()
ORDER BY (recorded_at, bed_id);

CREATE TABLE IF NOT EXISTS hospital.vitals_agg (
    bed_id          String,
    avg_heart_rate  Float64,
    avg_oxygen_sat  Float64,
    avg_body_temp   Float64,
    avg_bp_sys      Float64,
    avg_glucose     Float64,
    avg_resp_rate   Float64,
    record_count    UInt32,
    window_start    String,
    window_end      String
) ENGINE = ReplacingMergeTree()
ORDER BY (window_start, bed_id);
```

`vitals_agg` uses `ReplacingMergeTree` so that reprocessed or late-arriving windows overwrite previous entries for the same `(window_start, bed_id)` key rather than duplicating rows.

---

## Grafana Dashboards

Provisioned automatically from `grafana-provisioning/` on container start. No manual setup required.

### Hospital Beds — `hospital_beds.json`

<!-- Add screenshot here -->

### Pipeline Monitor — `pipeline_monitor.json`

<!-- Add screenshot here -->

---

## Running Locally

### Prerequisites

- Docker + Docker Compose
- Ports `9092`, `2181`, `8123`, `9000`, `9090`, `3000`, `9249`, `9363` available locally

### Start everything

```bash
git clone <repo-url>
cd real-time-hospital-beds-pipeline
docker-compose up -d
```

PyFlink waits for Kafka and ClickHouse to be reachable before starting the job.

### Verify services

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 (admin / admin) |
| Prometheus | http://localhost:9090 |
| ClickHouse HTTP | http://localhost:8123 |
| PyFlink metrics | http://localhost:9249/metrics |
| ClickHouse metrics | http://localhost:9363/metrics |

### Check data is flowing

```bash
# Sample raw events from Kafka
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic hospital_vitals \
  --from-beginning \
  --max-messages 5

# Count rows in ClickHouse
curl "http://localhost:8123/?query=SELECT%20count()%20FROM%20hospital.vitals_raw" \
  -u user:password

# Check anomalies recorded
curl "http://localhost:8123/?query=SELECT%20count()%20FROM%20hospital.vitals_raw%20WHERE%20oxygen_sat%20%3C%2090" \
  -u user:password
```

### Tear down

```bash
docker-compose down -v
```

---

## Project Structure

```
├── producer/
│   ├── producer.py          # Synthetic vitals generator with anomaly injection
│   ├── Dockerfile
│   └── requirements.txt
├── pyflink-job/
│   ├── consumer.py          # PyFlink job: Kafka → windowed aggregations → ClickHouse
│   ├── flink-conf.yaml
│   ├── Dockerfile
│   └── requirements.txt
├── clickhouse-init/
│   ├── init.sql             # Table definitions (auto-applied on container start)
│   └── prometheus.xml       # ClickHouse Prometheus metrics config
├── grafana-provisioning/
│   ├── dashboards/
│   │   ├── dashboard-provider.yml
│   │   ├── hospital_beds.json
│   │   └── pipeline_monitor.json
│   └── data-sources/
│       └── datasources.yaml
├── prometheus.yml
└── docker-compose.yml
```

---

## Known Limitations & What's Next

| Limitation | Next Version |
|---|---|
| Synthetic data — anomalies are random, not time-correlated | Real OpenAQ sensor data with realistic failure modes |
| `recorded_at` stored as `String` — no native DateTime filtering | Migrate to `DateTime64` for proper time-based partitioning and range queries |
| No Kafka Schema Registry — plain JSON messages | Avro + Confluent Schema Registry for schema evolution |
| No watermarks or late event handling in Flink | Watermarks with configurable allowed lateness, side outputs for late records |
| No Flink CEP — only aggregations | CEP: sustained anomaly detection (e.g. 3 consecutive hypoxia readings) |
| ClickHouse has no materialized views or OLAP queries | `AggregatingMergeTree`, materialized views, `GROUP BY ROLLUP` |
| Single Kafka broker | 3-broker cluster with replication factor 2 |

---

## What I Learned

- PyFlink DataStream API: windowed aggregations, Kafka source/sink connector configuration
- ClickHouse engine selection: `MergeTree` vs `ReplacingMergeTree` and when each applies
- Prometheus scrape config across heterogeneous exporters on different ports
- Grafana provisioning-as-code: datasources and dashboards loaded from config files
- Docker Compose readiness patterns: polling loops for dependent service startup