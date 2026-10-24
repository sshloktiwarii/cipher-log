# 📜 cipher-log — Cryptographic Merkle Audit Ledger & Telemetry Engine

[![Language](https://img.shields.io/badge/Language-Go%201.24-00ADD8?style=flat-square&logo=go)](https://go.dev/)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=flat-square)](#)
[![Zero-Dependency](https://img.shields.io/badge/Zero--Dependency-Pure%20Go%20%2B%20SQLite%20WAL-blue?style=flat-square)](#)

A high-throughput, non-blocking telemetry ingestion and cryptographic audit ledger. Chains every AI execution, routing decision, and security violation into an append-only **SHA-256 Merkle tree**, providing mathematical proof of audit integrity with zero external database dependencies (backed by embedded SQLite WAL).

---

### 💡 For Non-Techies: What does this do?
> Imagine a digital black-box flight recorder for everything AI does on your computer.
>
> Every time the AI runs a command, checks a file, or routes a message:
> - **It records it instantly**: In less than 1 millisecond.
> - **It locks it with digital cryptography**: Each log entry is cryptographically linked to the previous one in a "Merkle chain". If anyone tries to secretly alter, delete, or tamper with the history, the cryptographic proof breaks immediately.
> - **Mathematical proof**: You can verify your system has never been tampered with at any time.

---

## Cryptographic Guarantees

| Invariant | Implementation | Guarantee |
| :--- | :--- | :--- |
| **Tamper Detection** | Sequential SHA-256 Merkle Hashing | Any historical modification invalidates the cryptographic root. |
| **High Throughput** | In-memory buffered ring channel | Absorbs $10,000+$ logs/sec without blocking upstream callers. |
| **Embedded Storage** | Pure Go SQLite with Write-Ahead Logging (WAL) | Zero external database servers required. |
| **Zero Host Exposure** | Unix Domain Socket (`/tmp/cipher/log.sock`) | Inter-service IPC runs over isolated socket. |

---

## Quickstart

### Build Standalone Binary
```bash
go build -o cipher-log .
```

### Run Standalone
```bash
./cipher-log --port 8082 --socket /tmp/cipher/log.sock --sqlite-path ./telemetry.db
```

### Verify Audit Chain Integrity
```bash
curl http://localhost:8082/verify-audit-chain
```
*Response:*
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
MIT © [Shlok Tiwari](https://github.com/sshloktiwarii)
