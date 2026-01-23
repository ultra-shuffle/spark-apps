#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path


def _to_int(value, default=0):
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(value)
    except Exception:
        return default


def _get(dct, *keys, default=None):
    cur = dct
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def parse_eventlog(path: Path) -> dict:
    app_start_ts = None
    app_end_ts = None
    app_id = None
    app_name = None

    task_count = 0
    task_failed = 0

    exec_run_time_ms = 0
    jvm_gc_time_ms = 0

    shuffle_write_bytes = 0
    shuffle_write_records = 0
    shuffle_write_time_ns = 0

    shuffle_read_remote_bytes = 0
    shuffle_read_local_bytes = 0
    shuffle_read_records = 0
    shuffle_read_fetch_wait_ms = 0

    stages = {}

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except json.JSONDecodeError:
                continue

            etype = evt.get("Event")
            if etype == "SparkListenerApplicationStart":
                app_start_ts = _to_int(evt.get("Timestamp"), app_start_ts)
                app_id = evt.get("App ID") or app_id
                app_name = evt.get("App Name") or app_name
            elif etype == "SparkListenerApplicationEnd":
                app_end_ts = _to_int(evt.get("Timestamp"), app_end_ts)
            elif etype == "SparkListenerStageSubmitted":
                info = evt.get("Stage Info") or {}
                sid = _to_int(info.get("Stage ID"), None)
                if sid is None:
                    continue
                stages.setdefault(sid, {})
                stages[sid]["name"] = info.get("Stage Name") or stages[sid].get("name")
                stages[sid]["submission_time_ms"] = _to_int(
                    info.get("Submission Time"), stages[sid].get("submission_time_ms")
                )
            elif etype == "SparkListenerStageCompleted":
                info = evt.get("Stage Info") or {}
                sid = _to_int(info.get("Stage ID"), None)
                if sid is None:
                    continue
                stages.setdefault(sid, {})
                stages[sid]["name"] = info.get("Stage Name") or stages[sid].get("name")
                stages[sid]["completion_time_ms"] = _to_int(
                    info.get("Completion Time"), stages[sid].get("completion_time_ms")
                )
                stages[sid]["num_tasks"] = _to_int(
                    info.get("Number of Tasks"), stages[sid].get("num_tasks")
                )
            elif etype == "SparkListenerTaskEnd":
                task_count += 1
                reason = evt.get("Task End Reason") or {}
                if isinstance(reason, dict) and reason.get("Reason") not in (None, "Success"):
                    task_failed += 1

                metrics = evt.get("Task Metrics") or {}
                exec_run_time_ms += _to_int(metrics.get("Executor Run Time"), 0)
                jvm_gc_time_ms += _to_int(metrics.get("JVM GC Time"), 0)

                sw = metrics.get("Shuffle Write Metrics") or {}
                shuffle_write_bytes += _to_int(sw.get("Shuffle Bytes Written"), 0)
                shuffle_write_records += _to_int(sw.get("Shuffle Records Written"), 0)
                shuffle_write_time_ns += _to_int(sw.get("Shuffle Write Time"), 0)

                sr = metrics.get("Shuffle Read Metrics") or {}
                shuffle_read_remote_bytes += _to_int(sr.get("Remote Bytes Read"), 0)
                shuffle_read_local_bytes += _to_int(sr.get("Local Bytes Read"), 0)
                shuffle_read_records += _to_int(sr.get("Records Read"), 0)
                shuffle_read_fetch_wait_ms += _to_int(sr.get("Fetch Wait Time"), 0)

    stage_rows = []
    for sid, info in sorted(stages.items(), key=lambda x: x[0]):
        sub = info.get("submission_time_ms")
        comp = info.get("completion_time_ms")
        dur = None
        if isinstance(sub, int) and isinstance(comp, int) and comp >= sub:
            dur = comp - sub
        stage_rows.append(
            {
                "stage_id": sid,
                "name": info.get("name"),
                "submission_time_ms": sub,
                "completion_time_ms": comp,
                "duration_ms": dur,
                "num_tasks": info.get("num_tasks"),
            }
        )

    app_duration_ms = None
    if isinstance(app_start_ts, int) and isinstance(app_end_ts, int) and app_end_ts >= app_start_ts:
        app_duration_ms = app_end_ts - app_start_ts

    return {
        "app_id": app_id,
        "app_name": app_name,
        "app_start_ts_ms": app_start_ts,
        "app_end_ts_ms": app_end_ts,
        "app_duration_ms": app_duration_ms,
        "tasks_total": task_count,
        "tasks_failed": task_failed,
        "executor_run_time_ms_sum": exec_run_time_ms,
        "jvm_gc_time_ms_sum": jvm_gc_time_ms,
        "shuffle_write_bytes_sum": shuffle_write_bytes,
        "shuffle_write_records_sum": shuffle_write_records,
        "shuffle_write_time_ns_sum": shuffle_write_time_ns,
        "shuffle_read_remote_bytes_sum": shuffle_read_remote_bytes,
        "shuffle_read_local_bytes_sum": shuffle_read_local_bytes,
        "shuffle_read_bytes_sum": shuffle_read_remote_bytes + shuffle_read_local_bytes,
        "shuffle_read_records_sum": shuffle_read_records,
        "shuffle_read_fetch_wait_ms_sum": shuffle_read_fetch_wait_ms,
        "stages": stage_rows,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("eventlog", type=Path)
    ap.add_argument("--pretty", action="store_true")
    args = ap.parse_args()

    if not args.eventlog.is_file():
        print(f"ERROR: not a file: {args.eventlog}", file=sys.stderr)
        return 2

    summary = parse_eventlog(args.eventlog)
    if args.pretty:
        json.dump(summary, sys.stdout, indent=2, sort_keys=True)
    else:
        json.dump(summary, sys.stdout, separators=(",", ":"), sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

