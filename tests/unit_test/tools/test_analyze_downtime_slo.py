from argparse import Namespace

from tools.analyze_downtime_slo import analyze_request, build_summary, migration_windows


def _args():
    return Namespace(
        pre_migration_tpot_window=8,
        ttft_slo_ms=1000.0,
        tpot_slo_ms=25.0,
        strict_ttft_slo_ms=500.0,
        strict_tpot_slo_ms=14.5,
        loose_ttft_slo_ms=2000.0,
        loose_tpot_slo_ms=50.0,
    )


def _trace():
    return {
        "request_id": "request-1",
        "request_start_wall_time_ms": 1000.0,
        "token_timestamps_ms": [100.0, 120.0, 300.0, 320.0],
        "token_intervals_ms": [20.0, 180.0, 20.0],
        "strict_per_token_trace": True,
        "ttft_ms": 100.0,
        "e2e_latency_ms": 320.0,
        "finished": True,
        "aborted": False,
        "timeout": False,
    }


def test_server_and_visible_downtime_are_computed_separately():
    events = [
        {"event": "migration_start", "request_id": "request-1", "timestamp_ms": 1130.0},
        {"event": "migration_suspend", "request_id": "request-1", "timestamp_ms": 1170.0},
        {"event": "migration_resume", "request_id": "request-1", "timestamp_ms": 1280.0},
        {"event": "migration_end", "request_id": "request-1", "timestamp_ms": 1290.0},
    ]
    windows = migration_windows(events)["request-1"]

    row = analyze_request(_trace(), windows, _args())
    summary = build_summary([row], len(events), _args())

    assert row["server_downtime_ms"] == 110.0
    assert row["visible_downtime_ms"] == 180.0
    assert row["migration_internal_time_ms"] == 160.0
    assert summary["server_downtime_ms"]["mean"] == 110.0
    assert summary["server_downtime_per_request_ms"]["mean"] == 110.0
    assert summary["server_downtime_computable_migration_count"] == 1
    assert summary["server_downtime_computable_request_count"] == 1


def test_server_downtime_is_unavailable_without_resume_event():
    events = [
        {"event": "migration_start", "request_id": "request-1", "timestamp_ms": 1130.0},
        {"event": "migration_suspend", "request_id": "request-1", "timestamp_ms": 1170.0},
        {"event": "migration_end", "request_id": "request-1", "timestamp_ms": 1290.0},
    ]
    row = analyze_request(_trace(), migration_windows(events)["request-1"], _args())

    assert row["server_downtime_ms"] is None
    assert "final_suspend_begin/dst_resume" in row["server_downtime_unavailable_reason"]
