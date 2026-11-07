//! `mu-observable-ipc`: Shared wire contracts, Apache Arrow schemas, W3C trace context,
//! deterministic secret redaction, and Apache Fory RPC envelopes for MuObserve.

pub mod arrow_otel;
pub mod arrow_regression;
pub mod control;
pub mod error;
pub mod framing;
pub mod redact;
pub mod trace_context;

/// Unified re-export module for all Apache Arrow telemetry and regression schemas and codecs.
pub mod arrow_schemas {
    pub use crate::arrow_otel::*;
    pub use crate::arrow_regression::*;
}

pub use arrow_otel::{
    decode_log_batch, decode_metric_batch, decode_span_batch, encode_log_batch,
    encode_metric_batch, encode_span_batch, log_entries_to_record_batch,
    metric_entries_to_record_batch, otel_log_schema, otel_metric_schema, otel_span_schema,
    record_batch_to_log_entries, record_batch_to_metric_entries, record_batch_to_span_entries,
    span_entries_to_record_batch, OtelLogEntry, OtelMetricEntry, OtelSpanEntry,
};
pub use arrow_regression::{
    benchmark_run_schema, benchmark_runs_to_record_batch, decode_benchmark_batch,
    decode_benchmark_run_batch, decode_test_case_batch, decode_test_run_batch,
    encode_benchmark_batch, encode_benchmark_run_batch, encode_test_case_batch,
    encode_test_run_batch, record_batch_to_benchmark_runs, record_batch_to_test_runs,
    test_case_run_schema, test_runs_to_record_batch, BenchmarkRunEntry, TestCaseRunEntry,
};
pub use control::{
    create_fory_registry, create_fory_runtime, decode_control_envelope, encode_control_envelope,
    shared_fory_registry, BenchmarkRegressionItem, CanarySloAssertionRequest,
    CanarySloAssertionResponse, CentralBranchSnapshot, CentralJobRecord, CentralSubmoduleSnapshot,
    ControlRpcEnvelope, ForyObject, QueryBranchSnapshotsRequest, QueryBranchSnapshotsResponse,
    QueryJobsHistoryRequest, QueryJobsHistoryResponse, QuerySubmoduleSnapshotsRequest,
    QuerySubmoduleSnapshotsResponse, RegressionGateReport, RegressionGateRequest,
    RegressionVerdict, TestRegressionItem, UpsertJobBatchRequest, UpsertJobRecordRequest,
    UpsertJobRecordResponse, WalBatchAck, FORY_ID_BENCHMARK_REGRESSION_ITEM,
    FORY_ID_CANARY_SLO_REQ, FORY_ID_CANARY_SLO_RESP, FORY_ID_CENTRAL_BRANCH_SNAPSHOT,
    FORY_ID_CENTRAL_JOB_RECORD, FORY_ID_CENTRAL_SUBMODULE_SNAPSHOT, FORY_ID_CONTROL_RPC_ENVELOPE,
    FORY_ID_QUERY_BRANCH_SNAPSHOTS_REQ, FORY_ID_QUERY_BRANCH_SNAPSHOTS_RESP,
    FORY_ID_QUERY_JOBS_HISTORY_REQ, FORY_ID_QUERY_JOBS_HISTORY_RESP,
    FORY_ID_QUERY_SUBMODULE_SNAPSHOTS_REQ, FORY_ID_QUERY_SUBMODULE_SNAPSHOTS_RESP,
    FORY_ID_REGRESSION_GATE_REPORT, FORY_ID_REGRESSION_GATE_REQ, FORY_ID_REGRESSION_VERDICT,
    FORY_ID_TEST_REGRESSION_ITEM, FORY_ID_UPSERT_JOB_BATCH_REQ, FORY_ID_UPSERT_JOB_RECORD_REQ,
    FORY_ID_UPSERT_JOB_RECORD_RESP, FORY_ID_WAL_BATCH_ACK,
};
pub use error::IpcError;
pub use framing::{
    read_frame, write_frame, IpcHeader, CURRENT_VERSION, FLAG_COMPRESSED, FLAG_END_OF_STREAM,
    FLAG_HIGH_PRIORITY, HEADER_SIZE, MAGIC, MAX_FRAME_SIZE, MSG_TYPE_ARROW_BENCH_RUNS,
    MSG_TYPE_ARROW_OTEL_LOGS, MSG_TYPE_ARROW_OTEL_METRICS, MSG_TYPE_ARROW_OTEL_SPANS,
    MSG_TYPE_ARROW_TEST_RUNS, MSG_TYPE_FORY_CONTROL, MSG_TYPE_WAL_ACK,
};
pub use redact::{DeterministicSecretRedactor, SecretRedactor, REDACTED_PLACEHOLDER};
pub use trace_context::{
    encode_hex, parse_span_id_hex, parse_trace_id_hex, TraceContext, TRACE_FLAG_SAMPLED,
    W3C_TRACEPARENT_VERSION,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_constants() {
        assert_eq!(&MAGIC, b"MUOB");
        assert_eq!(CURRENT_VERSION, 1);
        assert_eq!(HEADER_SIZE, 24);
    }
}
