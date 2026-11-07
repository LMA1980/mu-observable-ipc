# mu-observable-ipc

<p align="center">
  <a href="https://hook.lma1980.duckdns.org/jobs">
    <img src="https://hook.lma1980.duckdns.org/cards/LMA1980/mu-observable-ipc/card.svg" alt="mu-observable-ipc Factory Scorecard" width="520"/>
  </a>
</p>

<p align="center">
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/mu-observable-ipc/status.svg" alt="CI Status"/></a>
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/mu-observable-ipc/score.svg" alt="Quality Score"/></a>
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/mu-observable-ipc/coverage.svg" alt="Coverage"/></a>
  <a href="https://hook.lma1980.duckdns.org/jobs"><img src="https://hook.lma1980.duckdns.org/badges/LMA1980/mu-observable-ipc/benchmark.svg" alt="Benchmark"/></a>
</p>

---

Shared wire contracts, zero-copy Apache Arrow telemetry schemas, OpenTelemetry (OTEL) span/metric/log envelopes, and Apache Fory control-plane RPC protocols for the **MuObserve** distributed observability and regression analytics platform.

## Architectural Scope

- **24-Byte Zero-Copy Framing (`IpcHeader`)**: `#[repr(C, align(8))]` frame header (`magic = *b"MUOB"`) for multiplexed mTLS streams between `MuObserve-collector`, `MuObserve`, `ActionRunner-controller`, and `MuConDe-controller`.
- **Apache Arrow Columnar Telemetry Schemas**:
  - `otel_log_schema`: Structured logs with W3C `trace_id`, `span_id`, severity, service/node SAN attribution, and redacted attributes.
  - `otel_span_schema`: Distributed trace spans (`trace_id`, `span_id`, `parent_span_id`, duration, status, resource attributes).
  - `otel_metric_schema`: Hardware and service metrics (CPU, RSS, `RLIMIT_MEMLOCK` headroom, SpacemiT K3 `X100`/`A100` cluster counters, inference `tok/s`).
  - `test_case_run_schema`: Granular per-test execution records (`test_name`, `crate_name`, `status`, `duration_ms`, rolling flakiness index).
  - `benchmark_run_schema`: Hardware-stratified per-benchmark statistical records (`ns_per_iter`, `mad_ns`, `p50`/`p95`/`p99`, `arch`, `cpu_cluster`, `vlen_bits`, throughput).
- **Apache Fory Control-Plane RPC Envelopes**:
  - Centralized `ActionRunner-controller` job history, branch snapshot, and submodule snapshot CRUD contracts.
  - Hardware-aware `RegressionGateRequest` / `RegressionGateReport` and `CanarySloAssertionRequest` / `CanarySloAssertionResponse`.
- **W3C `traceparent` & Secret Redaction Traits**: Compile-time trait contracts for deterministic secret scrubbing prior to Edge WAL persistence.

## License

Licensed under `LGPL-3.0-only WITH LGPL-3.0-linking-exception`. See [`LICENSE`](LICENSE) for details.
