use mu_observable_ipc::{
    decode_benchmark_run_batch, decode_control_envelope, decode_log_batch, decode_metric_batch,
    decode_span_batch, decode_test_run_batch, encode_benchmark_run_batch, encode_control_envelope,
    encode_log_batch, encode_metric_batch, encode_span_batch, encode_test_run_batch, read_frame,
    write_frame, BenchmarkRegressionItem, BenchmarkRunEntry, CanarySloAssertionRequest,
    CanarySloAssertionResponse, CentralBranchSnapshot, CentralJobRecord, CentralSubmoduleSnapshot,
    ControlRpcEnvelope, DeterministicSecretRedactor, IpcHeader, OtelLogEntry, OtelMetricEntry,
    OtelSpanEntry, QueryBranchSnapshotsRequest, QueryBranchSnapshotsResponse,
    QueryJobsHistoryRequest, QueryJobsHistoryResponse, QuerySubmoduleSnapshotsRequest,
    QuerySubmoduleSnapshotsResponse, RegressionGateReport, RegressionGateRequest,
    RegressionVerdict, SecretRedactor, TestCaseRunEntry, TestRegressionItem, TraceContext,
    UpsertJobBatchRequest, UpsertJobRecordRequest, UpsertJobRecordResponse, WalBatchAck,
    FLAG_COMPRESSED, FLAG_HIGH_PRIORITY, MSG_TYPE_ARROW_BENCH_RUNS, MSG_TYPE_ARROW_OTEL_LOGS,
    MSG_TYPE_ARROW_OTEL_METRICS, MSG_TYPE_ARROW_OTEL_SPANS, MSG_TYPE_ARROW_TEST_RUNS,
    MSG_TYPE_FORY_CONTROL, MSG_TYPE_WAL_ACK,
};

#[tokio::test]
async fn test_multiplexed_arrow_and_fory_stream_roundtrip() {
    let (mut writer, mut reader) = tokio::io::duplex(64 * 1024);
    let redactor = DeterministicSecretRedactor::new();

    // 1. Root W3C TraceContext + child span
    let root_ctx = TraceContext::from_seed(0x0102_0304_0506_0708, 0x1112_1314_1516_1718, 42, true);
    let child_ctx = root_ctx.next_deterministic_child(1);
    let parsed_ctx =
        TraceContext::parse_traceparent(&child_ctx.to_traceparent()).expect("parse traceparent");
    assert_eq!(parsed_ctx, child_ctx);

    // 2. OtelLogEntry batch over MUOB frame
    let mut log_entry = OtelLogEntry {
        timestamp_unix_nano: 1_700_000_100_000_000_000,
        trace_id: child_ctx.trace_id_hex(),
        span_id: child_ctx.span_id_hex(),
        severity_number: 17,
        severity_text: "ERROR".to_string(),
        service_name: "action-runner-worker".to_string(),
        node_id: "worker-riscv64".to_string(),
        body: "Webhook auth secret=super_secret_token_999 failed".to_string(),
        attributes_json:
            r#"{"authorization":"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.payload"}"#.to_string(),
        job_id: Some("job-500".to_string()),
        stage_id: Some("test-check".to_string()),
    };
    log_entry.redact_in_place(&redactor);
    assert!(!log_entry.body.contains("super_secret_token_999"));
    assert!(!log_entry.attributes_json.contains("eyJ0eXAiOiJKV1Qi"));

    let log_bytes = encode_log_batch(std::slice::from_ref(&log_entry)).expect("encode logs");
    let log_hdr = IpcHeader::new(
        MSG_TYPE_ARROW_OTEL_LOGS,
        log_bytes.len() as u32,
        FLAG_HIGH_PRIORITY,
        1,
    );
    write_frame(&mut writer, &log_hdr, &log_bytes)
        .await
        .expect("write log frame");

    // 3. OtelSpanEntry batch over MUOB frame
    let mut span_entry = OtelSpanEntry {
        trace_id: child_ctx.trace_id_hex(),
        span_id: child_ctx.span_id_hex(),
        parent_span_id: Some(root_ctx.span_id_hex()),
        service_name: "action-runner-worker".to_string(),
        node_id: "worker-riscv64".to_string(),
        span_name: "stage.execute".to_string(),
        span_kind: "INTERNAL".to_string(),
        start_time_unix_nano: 1_700_000_100_000_000_000,
        end_time_unix_nano: 1_700_000_102_500_000_000,
        duration_ns: 2_500_000_000,
        status_code: "ERROR".to_string(),
        status_message: Some("token=ghp_1234567890abcdef".to_string()),
        attributes_json: r#"{"node":"worker-riscv64"}"#.to_string(),
    };
    span_entry.redact_in_place(&redactor);
    assert_eq!(
        span_entry.status_message.as_deref(),
        Some("token=[REDACTED:MUOBSERVE]")
    );

    let span_bytes = encode_span_batch(std::slice::from_ref(&span_entry)).expect("encode spans");
    let span_hdr = IpcHeader::new(MSG_TYPE_ARROW_OTEL_SPANS, span_bytes.len() as u32, 0, 2);
    write_frame(&mut writer, &span_hdr, &span_bytes)
        .await
        .expect("write span frame");

    // 4. OtelMetricEntry batch over MUOB frame
    let metric_entry = OtelMetricEntry {
        timestamp_unix_nano: 1_700_000_103_000_000_000,
        service_name: "k3-manager".to_string(),
        node_id: "worker-riscv64".to_string(),
        metric_name: "k3.memlock.used_bytes".to_string(),
        metric_unit: "By".to_string(),
        value_f64: 1_073_741_824.0,
        arch: "riscv64gc".to_string(),
        cpu_cluster: Some("A100".to_string()),
        vlen_bits: Some(1024),
        memlock_used_bytes: Some(1_073_741_824),
        memlock_limit_bytes: Some(30_064_771_072),
        trace_id: Some(root_ctx.trace_id_hex()),
        span_id: Some(root_ctx.span_id_hex()),
        attributes_json: "{}".to_string(),
    };
    let metric_bytes =
        encode_metric_batch(std::slice::from_ref(&metric_entry)).expect("encode metrics");
    let metric_hdr = IpcHeader::new(MSG_TYPE_ARROW_OTEL_METRICS, metric_bytes.len() as u32, 0, 3);
    write_frame(&mut writer, &metric_hdr, &metric_bytes)
        .await
        .expect("write metric frame");

    // 5. TestCaseRunEntry & BenchmarkRunEntry batches over MUOB frames
    let test_run = TestCaseRunEntry {
        job_id: "job-500".to_string(),
        repo_key: "lma1980/mu-observable-ipc".to_string(),
        branch_key: "feat/wave1-mu-observable-ipc".to_string(),
        commit_sha: "0123456789abcdef".to_string(),
        crate_name: "mu-observable-ipc".to_string(),
        test_binary: "ipc_roundtrip".to_string(),
        test_name: "test_multiplexed_arrow_and_fory_stream_roundtrip".to_string(),
        status: "ok".to_string(),
        duration_ms: 3.25,
        failure_summary: None,
        timestamp_unix_nano: 1_700_000_104_000_000_000,
        node_id: "node-a".to_string(),
        arch: "x86_64".to_string(),
    };
    let test_bytes =
        encode_test_run_batch(std::slice::from_ref(&test_run)).expect("encode test runs");
    let test_hdr = IpcHeader::new(
        MSG_TYPE_ARROW_TEST_RUNS,
        test_bytes.len() as u32,
        FLAG_HIGH_PRIORITY,
        4,
    );
    write_frame(&mut writer, &test_hdr, &test_bytes)
        .await
        .expect("write test frame");

    let bench_run = BenchmarkRunEntry {
        job_id: "job-500".to_string(),
        repo_key: "lma1980/k3-kernels-rvv".to_string(),
        branch_key: "main".to_string(),
        commit_sha: "0123456789abcdef".to_string(),
        crate_name: "k3-kernels-rvv".to_string(),
        bench_name: "vmadot_i8_8x16".to_string(),
        ns_per_iter: 950.0,
        std_dev_ns: 12.0,
        mad_ns: 8.5,
        p50_ns: 948.0,
        p95_ns: 965.0,
        p99_ns: 975.0,
        throughput_value: Some(128.0),
        throughput_unit: Some("GOPS".to_string()),
        arch: "riscv64gc".to_string(),
        cpu_cluster: "A100".to_string(),
        vlen_bits: 1024,
        timestamp_unix_nano: 1_700_000_105_000_000_000,
        node_id: "worker-riscv64".to_string(),
    };
    let bench_bytes =
        encode_benchmark_run_batch(std::slice::from_ref(&bench_run)).expect("encode bench runs");
    let bench_hdr = IpcHeader::new(
        MSG_TYPE_ARROW_BENCH_RUNS,
        bench_bytes.len() as u32,
        FLAG_COMPRESSED | FLAG_HIGH_PRIORITY,
        5,
    );
    write_frame(&mut writer, &bench_hdr, &bench_bytes)
        .await
        .expect("write bench frame");

    // 6. ControlRpcEnvelope (Fory) over MUOB frame
    let ack = ControlRpcEnvelope::WalAck(WalBatchAck {
        sequence: 5,
        accepted_rows: 1,
        ok: true,
        error_message: None,
    });
    let ack_bytes = encode_control_envelope(&ack).expect("encode ack");
    let ack_hdr = IpcHeader::new(MSG_TYPE_WAL_ACK, ack_bytes.len() as u32, 0, 6);
    write_frame(&mut writer, &ack_hdr, &ack_bytes)
        .await
        .expect("write ack frame");

    // Read all 6 frames back in order and verify payloads
    let (h1, p1) = read_frame(&mut reader).await.expect("read frame 1");
    assert_eq!(h1.msg_type, MSG_TYPE_ARROW_OTEL_LOGS);
    assert_eq!(decode_log_batch(&p1).expect("decode logs"), vec![log_entry]);

    let (h2, p2) = read_frame(&mut reader).await.expect("read frame 2");
    assert_eq!(h2.msg_type, MSG_TYPE_ARROW_OTEL_SPANS);
    assert_eq!(
        decode_span_batch(&p2).expect("decode spans"),
        vec![span_entry]
    );

    let (h3, p3) = read_frame(&mut reader).await.expect("read frame 3");
    assert_eq!(h3.msg_type, MSG_TYPE_ARROW_OTEL_METRICS);
    assert_eq!(
        decode_metric_batch(&p3).expect("decode metrics"),
        vec![metric_entry]
    );

    let (h4, p4) = read_frame(&mut reader).await.expect("read frame 4");
    assert_eq!(h4.msg_type, MSG_TYPE_ARROW_TEST_RUNS);
    assert_eq!(
        decode_test_run_batch(&p4).expect("decode test runs"),
        vec![test_run]
    );

    let (h5, p5) = read_frame(&mut reader).await.expect("read frame 5");
    assert_eq!(h5.msg_type, MSG_TYPE_ARROW_BENCH_RUNS);
    assert_eq!(
        decode_benchmark_run_batch(&p5).expect("decode bench runs"),
        vec![bench_run]
    );

    let (h6, p6) = read_frame(&mut reader).await.expect("read frame 6");
    assert_eq!(h6.msg_type, MSG_TYPE_WAL_ACK);
    assert_eq!(decode_control_envelope(&p6).expect("decode ack"), ack);
}

#[test]
fn test_all_control_envelope_variants_roundtrip() {
    let envelopes = vec![
        ControlRpcEnvelope::Noop,
        ControlRpcEnvelope::UpsertJobRecordReq(UpsertJobRecordRequest {
            request_id: "r1".to_string(),
            record: CentralJobRecord {
                job_id: "j1".to_string(),
                repo_key: "lma1980/muobserve".to_string(),
                ..Default::default()
            },
            branch_snapshot: Some(CentralBranchSnapshot {
                repo_key: "lma1980/muobserve".to_string(),
                branch_key: "main".to_string(),
                ..Default::default()
            }),
            submodule_snapshots: vec![CentralSubmoduleSnapshot {
                parent_repo_key: "lma1980/k3".to_string(),
                branch_key: "main".to_string(),
                submodule_key: "k3-core".to_string(),
                ..Default::default()
            }],
        }),
        ControlRpcEnvelope::UpsertJobRecordResp(UpsertJobRecordResponse {
            request_id: "r1".to_string(),
            accepted: true,
            upserted_count: 1,
            error_message: None,
        }),
        ControlRpcEnvelope::UpsertJobBatchReq(UpsertJobBatchRequest {
            request_id: "r2".to_string(),
            records: vec![CentralJobRecord::default()],
            branch_snapshots: vec![CentralBranchSnapshot::default()],
            submodule_snapshots: vec![CentralSubmoduleSnapshot::default()],
        }),
        ControlRpcEnvelope::QueryJobsHistoryReq(QueryJobsHistoryRequest {
            request_id: "r3".to_string(),
            repo_key: Some("lma1980/muobserve".to_string()),
            branch_key: Some("main".to_string()),
            job_id: None,
            limit: 25,
        }),
        ControlRpcEnvelope::QueryJobsHistoryResp(QueryJobsHistoryResponse {
            request_id: "r3".to_string(),
            records: vec![CentralJobRecord::default()],
        }),
        ControlRpcEnvelope::QueryBranchSnapshotsReq(QueryBranchSnapshotsRequest {
            request_id: "r4".to_string(),
            repo_key: Some("lma1980/muobserve".to_string()),
            branch_key: None,
        }),
        ControlRpcEnvelope::QueryBranchSnapshotsResp(QueryBranchSnapshotsResponse {
            request_id: "r4".to_string(),
            snapshots: vec![CentralBranchSnapshot::default()],
        }),
        ControlRpcEnvelope::QuerySubmoduleSnapshotsReq(QuerySubmoduleSnapshotsRequest {
            request_id: "r5".to_string(),
            parent_repo_key: Some("lma1980/k3".to_string()),
            submodule_key: Some("k3-core".to_string()),
            branch_key: Some("main".to_string()),
        }),
        ControlRpcEnvelope::QuerySubmoduleSnapshotsResp(QuerySubmoduleSnapshotsResponse {
            request_id: "r5".to_string(),
            snapshots: vec![CentralSubmoduleSnapshot::default()],
        }),
        ControlRpcEnvelope::RegressionGateReq(RegressionGateRequest {
            request_id: "r6".to_string(),
            repo_key: "lma1980/k3-kernels-rvv".to_string(),
            candidate_commit: "abc1234".to_string(),
            baseline_branch: "main".to_string(),
            max_slowdown_pct: 5.0,
            max_flakiness_pct: 1.0,
        }),
        ControlRpcEnvelope::RegressionGateResp(RegressionGateReport {
            request_id: "r6".to_string(),
            repo_key: "lma1980/k3-kernels-rvv".to_string(),
            candidate_commit: "abc1234".to_string(),
            baseline_branch: "main".to_string(),
            verdict: RegressionVerdict::Warn,
            regressed_benchmarks: vec![BenchmarkRegressionItem::default()],
            flaky_or_failed_tests: vec![TestRegressionItem::default()],
            summary: "warn".to_string(),
        }),
        ControlRpcEnvelope::CanarySloReq(CanarySloAssertionRequest {
            request_id: "r7".to_string(),
            service_name: "muobserve-collector".to_string(),
            candidate_digest: "sha256:beef".to_string(),
            node_id: "node-a".to_string(),
            bake_window_secs: 120,
            max_error_logs: 0,
            max_p99_latency_ms: 50.0,
        }),
        ControlRpcEnvelope::CanarySloResp(CanarySloAssertionResponse {
            request_id: "r7".to_string(),
            healthy: true,
            error_log_count: 0,
            p99_latency_ms: 4.2,
            taint_recommended: false,
            reason: "SLO satisfied".to_string(),
        }),
    ];

    for env in envelopes {
        let hdr = IpcHeader::new(MSG_TYPE_FORY_CONTROL, 0, 0, 1);
        assert_eq!(hdr.msg_type, MSG_TYPE_FORY_CONTROL);
        let bytes = encode_control_envelope(&env).expect("encode envelope");
        let decoded = decode_control_envelope(&bytes).expect("decode envelope");
        assert_eq!(decoded, env);
    }

    let redactor = DeterministicSecretRedactor::new();
    assert_eq!(
        redactor.redact("api_key=abc123xyz"),
        "api_key=[REDACTED:MUOBSERVE]"
    );
}
