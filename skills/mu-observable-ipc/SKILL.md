---
name: mu-observable-ipc
description: "Build zero-copy MUOB wire frames, Apache Arrow OTEL/regression batches, Apache Fory RPCs, W3C TraceContext headers, and secret redaction with mu-observable-ipc."
---

# `mu-observable-ipc` Telemetry Framing & Wire Contracts

Use these guidelines when emitting or consuming telemetry and control-plane RPCs with `mu-observable-ipc`:

## 1. `MUOB` Wire Framing & Payloads

- **Telemetry Batches (Apache Arrow IPC)**:
  - Use `arrow_otel` builders for structured logs (`v_logs`), W3C distributed spans (`v_traces`), and hardware utilization metrics (`v_metrics` including `arch`, `cpu_cluster`, `vlen_bits`, `memlock_used_bytes`, `memlock_limit_bytes`).
  - Use `arrow_regression` builders for CI test case runs (`v_tests`) and hardware-stratified benchmark samples (`v_benchmarks`).
- **Control-Plane RPCs (Apache Fory)**:
  - Use `control` types for low-overhead RPC messages over `MUOB` framed connections.

## 2. Mandatory Edge Secret Redaction

- Always scrub log bodies, span attributes, and diagnostic strings through `DeterministicSecretRedactor` (`SecretRedactor` trait) **before** persisting to local ring-buffer WALs or serializing Arrow IPC batches.
- Secrets are replaced deterministically with `REDACTED_PLACEHOLDER` so downstream analytical queries never ingest bearer tokens, webhook secrets, or private keys.

## 3. W3C `TraceContext` Propagation

- Propagate `trace_id` (32-hex) and `span_id` (16-hex) via `trace_context` helpers across HTTP headers (`traceparent`), WebSocket frames, and IPC boundaries so cross-service trace waterfalls remain contiguous.
