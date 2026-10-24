# cipher-log

High-throughput telemetry ingestion engine and cryptographic Merkle audit ledger.

Written in pure Go. Buffers and flushes unstructured telemetry streams into an embedded SQLite WAL database and chains every event into an append-only SHA-256 Merkle tree for zero-trust integrity verification.

---

## Technical Specifications

* **Buffered Ingestion**: Accepts metrics over HTTP and Unix domain socket (`/tmp/cipher/log.sock`), routing events through an in-memory ring buffer (`capacity: 10,000`).
* **Cryptographic Verification**: Chained SHA-256 Merkle tree leaves. Any historical data modification produces an invalid root hash.
* **Storage Engine**: Embedded SQLite operating in Write-Ahead Logging (`WAL`) mode with automatic checkpointing.

---

## Build

```bash
go build -o cipher-log .
```

---

## Usage

```bash
./cipher-log --port 8082 --socket /tmp/cipher/log.sock --sqlite-path ./telemetry.db
```

### Endpoints

#### `POST /ingest`

Ingests structured telemetry events.

Request:
```json
{
  "source": "cipher-route",
  "event_type": "request_routed",
  "metrics": {
    "token_count": 480,
    "latency_ms": 32.1
  },
  "metadata": {
    "route": "local_ollama",
    "status": "200"
  }
}
```

#### `GET /verify-audit-chain`

Cryptographically validates the integrity of the Merkle tree.

Response:
```json
{
  "status": "VERIFIED_INTEGRITY",
  "leaf_count": 42,
  "tampered": false,
  "cryptographic_proof": "SHA-256 Merkle Chain Integrity Verified Untampered"
}
```

---

## License

MIT License. Copyright (c) 2026 Shlok Tiwari.
