# Foundry Pulse: High-Throughput Telemetry & Anomaly Ingestion Engine

High-throughput observability API gateway built in Go with TimescaleDB that ingests unstructured time-series metrics from upstream services (`foundry-router` and `foundry-crucible`).

## Architecture
- **Non-blocking Ingestion**: Accepts metrics over HTTP POST `/ingest`, parses and pushes into an internal buffered ring channel (`BUFFER_SIZE=10000`), returning `202 Accepted` in sub-millisecond times.
- **Worker Pool & Micro-Batching**: 8 concurrent background workers batch-flush events into TimescaleDB chunks every 500ms or 100 items.
- **TimescaleDB Hypertable**: Automatically partitions data by 7-day chunks, with 3-day compression policy and 30-day retention policies.
- **Fail-Safe In-Memory Ring Buffer**: Continues accepting telemetry even during database migrations or temporary downtime.

## API Usage
```bash
curl -X POST http://localhost:8082/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "source": "macr-gateway",
    "event_type": "request_routed",
    "metrics": {"token_count": 480, "latency_ms": 32.1},
    "metadata": {"route": "local_ollama", "status": "200"}
  }'
```
