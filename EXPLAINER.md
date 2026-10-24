# cipher-log: The Plain-English Guide

## What does this do?

When you give an AI tool permission to read files, run tests, or modify code, you need a reliable record of exactly what happened.

If something goes wrong (such as an unexpected file change or a network timeout), you need to know:
* What action did the AI take?
* Exactly when did it happen?
* Did anyone tamper with or delete the logs to hide an error?

`cipher-log` is a tamper-evident digital record book:
* **Instant recording:** It accepts and logs events in less than 1 millisecond without slowing down your computer.
* **Cryptographic chain:** Every log entry is cryptographically linked to the entry before it using SHA-256 math (a Merkle tree). If anyone edits an old entry, the chain breaks immediately.
* **Zero external servers:** It stores records inside a local, high-speed SQLite database on your hard drive. No cloud servers required.

---

## What happens if I don't use this?

Without a tamper-evident log, troubleshooting an autonomous agent failure is guesswork. Standard text log files can be modified, truncated, or accidentally deleted by runaway scripts, leaving no evidence of why an issue occurred.

---

## Frequently Asked Questions

#### Does this send my activity logs to the cloud?
No. All log records are stored exclusively on your local hard drive in `telemetry.db`.

#### How do I verify the logs have not been tampered with?
You can query the verification endpoint at any time:
```bash
curl http://localhost:8082/verify-audit-chain
```
If the records are intact, it returns `"status": "VERIFIED_INTEGRITY"` and `"tampered": false`.

#### How do I run it?
In the full Cipher system, `cipher appliance` starts `cipher-log` automatically. To run it alone:
```bash
./cipher-log --port 8082 --sqlite-path ./telemetry.db
```
