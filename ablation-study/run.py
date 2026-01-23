#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class Variant:
    name: str
    scache_conf_dir: Path
    spark_conf_overrides: Dict[str, str]
    notes: str = ""


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _ts() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.localtime())


def _run(
    cmd: List[str],
    *,
    cwd: Path,
    env: Dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    check: bool,
) -> Tuple[int, float]:
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()
    with stdout_path.open("wb") as out, stderr_path.open("wb") as err:
        p = subprocess.Popen(cmd, cwd=str(cwd), env=env, stdout=out, stderr=err)
        rc = p.wait()
    elapsed_s = time.time() - start
    if check and rc != 0:
        raise RuntimeError(f"command failed (rc={rc}): {cmd}")
    return rc, elapsed_s


def _spark_submit_extra_args(overrides: Dict[str, str]) -> str:
    args: List[str] = []
    for k, v in overrides.items():
        args += ["--conf", f"{k}={v}"]
    return " ".join(shlex.quote(a) for a in args)


def _find_single_eventlog(eventlog_dir: Path) -> Optional[Path]:
    if not eventlog_dir.is_dir():
        return None
    candidates = [p for p in eventlog_dir.iterdir() if p.is_file()]
    # Prefer completed logs (not *.inprogress).
    finished = [p for p in candidates if not p.name.endswith(".inprogress")]
    if len(finished) == 1:
        return finished[0]
    if len(finished) > 1:
        return max(finished, key=lambda p: p.stat().st_mtime)
    inprogress = [p for p in candidates if p.name.endswith(".inprogress")]
    if len(inprogress) == 1:
        return inprogress[0]
    if len(inprogress) > 1:
        return max(inprogress, key=lambda p: p.stat().st_mtime)
    return None


def _parse_eventlog(tools_dir: Path, eventlog_path: Path, out_json: Path) -> Optional[dict]:
    cmd = [sys.executable, str(tools_dir / "parse_eventlog.py"), str(eventlog_path)]
    try:
        res = subprocess.run(cmd, check=True, capture_output=True, text=True)
    except Exception:
        return None
    try:
        summary = json.loads(res.stdout)
    except Exception:
        return None
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def _write_csv_row(csv_path: Path, header: List[str], row: Dict[str, object]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    exists = csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in header})


def _variants(root: Path) -> Dict[str, Variant]:
    base = root / "ablation-study" / "conf" / "scache-multinode"
    return {
        "ultrashuffle-full": Variant(
            name="ultrashuffle-full",
            scache_conf_dir=base / "ultrashuffle-full",
            spark_conf_overrides={},
            notes="Pool slices + partition homes + remote caching + shared CXL pool.",
        ),
        "no-partition-homes": Variant(
            name="no-partition-homes",
            scache_conf_dir=base / "no-partition-homes",
            spark_conf_overrides={},
            notes="Requires optional SCache patch to take effect (otherwise same as full).",
        ),
        "no-remote-cache": Variant(
            name="no-remote-cache",
            scache_conf_dir=base / "no-remote-cache",
            spark_conf_overrides={},
            notes="Disables caching for non-local blocks (remote=DISK_ONLY).",
        ),
        "service-mediated-fetch": Variant(
            name="service-mediated-fetch",
            scache_conf_dir=base / "service-mediated-fetch",
            spark_conf_overrides={},
            notes="Disables shared CXL pool; uses client-to-client fetch.",
        ),
        "per-block-files": Variant(
            name="per-block-files",
            scache_conf_dir=base / "per-block-files",
            # NOTE: Spark no-local-files mode currently expects the pool-slice upload path.
            # For per-block IPC files, run in sidecar mode (keep Spark shuffle files).
            spark_conf_overrides={
                "spark.scache.shuffle.noLocalFiles": "false",
            },
            notes="Per-block IPC files; runs Spark in sidecar mode (noLocalFiles=false).",
        ),
    }


def _rewrite_kv_conf(src: Path, dst: Path, updates: Dict[str, str]) -> None:
    """
    Update a simple key=value config file (HOCON-style) without parsing full HOCON.

    - Preserves comments/unknown lines.
    - Rewrites all occurrences of updated keys.
    - Appends missing keys at the end.
    """

    key_re = re.compile(r"^(\s*)([A-Za-z0-9_.-]+)(\s*=\s*)(.*?)(\s*)$")

    def is_comment(line: str) -> bool:
        stripped = line.lstrip()
        return stripped.startswith("#") or stripped.startswith("//")

    lines = src.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)
    out: List[str] = []

    for line in lines:
        if is_comment(line):
            out.append(line)
            continue
        m = key_re.match(line.rstrip("\n"))
        if not m:
            out.append(line)
            continue
        key = m.group(2)
        if key in updates:
            out.append(f"{m.group(1)}{key}{m.group(3)}{updates[key]}{m.group(5)}\n")
        else:
            out.append(line)

    existing_keys = set()
    for line in out:
        if is_comment(line):
            continue
        m = key_re.match(line.rstrip("\n"))
        if m:
            existing_keys.add(m.group(2))

    for k, v in updates.items():
        if k not in existing_keys:
            out.append(f"{k}={v}\n")

    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text("".join(out), encoding="utf-8")


def run_ablation(args: argparse.Namespace) -> int:
    root = _repo_root()
    tools_dir = root / "ablation-study" / "tools"

    variants_all = _variants(root)
    requested = args.variants or list(variants_all.keys())
    variants: List[Variant] = []
    for name in requested:
        v = variants_all.get(name)
        if v is None:
            print(f"ERROR: unknown variant: {name}", file=sys.stderr)
            return 2
        variants.append(v)

    results_root = (root / "ablation-study" / "results" / _ts()) if args.out is None else Path(args.out)
    results_root.mkdir(parents=True, exist_ok=True)

    csv_path = results_root / "ablation.csv"
    csv_header = [
        "variant",
        "repeat",
        "exit_code",
        "submit_elapsed_s",
        "app_duration_ms",
        "shuffle_write_bytes",
        "shuffle_read_bytes",
        "eventlog",
        "notes",
    ]

    start_script = root / "start-standalone-multinode.sh"
    stop_script = root / "stop-standalone-multinode.sh"
    submit_script = root / "submit-groupbytest-mn.sh"

    if not start_script.is_file() or not stop_script.is_file() or not submit_script.is_file():
        print("ERROR: expected standalone multinode scripts not found at repo root", file=sys.stderr)
        return 2

    workload_args = args.workload_args

    for variant in variants:
        variant_dir = results_root / "ablation" / variant.name
        variant_dir.mkdir(parents=True, exist_ok=True)

        if args.restart_cluster:
            env = dict(os.environ)
            env["SCACHE_CONF_OVERRIDE_DIR"] = str(variant.scache_conf_dir)

            _run(
                [str(stop_script)],
                cwd=root,
                env=env,
                stdout_path=variant_dir / "cluster-stop.stdout.log",
                stderr_path=variant_dir / "cluster-stop.stderr.log",
                check=False,
            )
            _run(
                [str(start_script)],
                cwd=root,
                env=env,
                stdout_path=variant_dir / "cluster-start.stdout.log",
                stderr_path=variant_dir / "cluster-start.stderr.log",
                check=True,
            )

        for rep in range(args.repeats):
            run_dir = variant_dir / f"run-{rep:03d}"
            run_dir.mkdir(parents=True, exist_ok=True)

            eventlog_dir = run_dir / "spark-events"
            eventlog_dir.mkdir(parents=True, exist_ok=True)
            spark_overrides = {
                "spark.app.name": f"ablation-{variant.name}",
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": f"file://{eventlog_dir}",
                "spark.eventLog.compress": "false",
            }
            spark_overrides.update(variant.spark_conf_overrides)

            env = dict(os.environ)
            env["SPARK_SUBMIT_EXTRA_ARGS"] = _spark_submit_extra_args(spark_overrides)

            submit_cmd = [str(submit_script)] + workload_args
            rc, elapsed_s = _run(
                submit_cmd,
                cwd=root,
                env=env,
                stdout_path=run_dir / "submit.stdout.log",
                stderr_path=run_dir / "submit.stderr.log",
                check=False,
            )

            eventlog_path = _find_single_eventlog(eventlog_dir)
            parsed = None
            if eventlog_path is not None:
                parsed = _parse_eventlog(tools_dir, eventlog_path, run_dir / "eventlog.summary.json")

            meta = {
                "variant": variant.name,
                "repeat": rep,
                "submit_cmd": submit_cmd,
                "spark_submit_extra_args": env.get("SPARK_SUBMIT_EXTRA_ARGS", ""),
                "scache_conf_dir": str(variant.scache_conf_dir),
                "restart_cluster": bool(args.restart_cluster),
                "exit_code": rc,
                "submit_elapsed_s": elapsed_s,
                "eventlog": str(eventlog_path) if eventlog_path else None,
                "eventlog_summary": parsed,
                "notes": variant.notes,
            }
            (run_dir / "run.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            _write_csv_row(
                csv_path,
                csv_header,
                {
                    "variant": variant.name,
                    "repeat": rep,
                    "exit_code": rc,
                    "submit_elapsed_s": f"{elapsed_s:.3f}",
                    "app_duration_ms": (parsed or {}).get("app_duration_ms", ""),
                    "shuffle_write_bytes": (parsed or {}).get("shuffle_write_bytes_sum", ""),
                    "shuffle_read_bytes": (parsed or {}).get("shuffle_read_bytes_sum", ""),
                    "eventlog": str(eventlog_path) if eventlog_path else "",
                    "notes": variant.notes,
                },
            )

    return 0


def run_sensitivity(args: argparse.Namespace) -> int:
    root = _repo_root()
    tools_dir = root / "ablation-study" / "tools"

    base_variant = _variants(root)["ultrashuffle-full"]
    base_conf = base_variant.scache_conf_dir / "scache.conf"
    base_slaves = base_variant.scache_conf_dir / "slaves"
    if not base_conf.is_file():
        print(f"ERROR: base scache.conf not found: {base_conf}", file=sys.stderr)
        return 2

    results_root = (root / "ablation-study" / "results" / _ts()) if args.out is None else Path(args.out)
    results_root.mkdir(parents=True, exist_ok=True)

    start_script = root / "start-standalone-multinode.sh"
    stop_script = root / "stop-standalone-multinode.sh"
    submit_script = root / "submit-groupbytest-mn.sh"

    if not start_script.is_file() or not stop_script.is_file() or not submit_script.is_file():
        print("ERROR: expected standalone multinode scripts not found at repo root", file=sys.stderr)
        return 2

    sweep_root = results_root / f"sensitivity-{args.sweep}"
    sweep_root.mkdir(parents=True, exist_ok=True)

    csv_path = sweep_root / "sensitivity.csv"
    csv_header = [
        "sweep",
        "value",
        "repeat",
        "exit_code",
        "submit_elapsed_s",
        "app_duration_ms",
        "shuffle_write_bytes",
        "shuffle_read_bytes",
        "eventlog",
    ]

    workload_args_base = list(args.workload_args)

    for value in args.values:
        value_label = str(value)

        updates: Dict[str, str] = {}
        workload_args = workload_args_base

        if args.sweep == "cxl-capacity":
            # Interpret "value" as a typesafe-config size string: e.g., 512m, 1g.
            updates["scache.memory.offHeap.size"] = value_label
            updates["scache.storage.cxl.shared.pool.size"] = value_label
        elif args.sweep == "align":
            # Interpret "value" as bytes alignment: e.g., 4096, 65536.
            updates["scache.daemon.ipc.pool.align"] = value_label
            updates["scache.storage.cxl.shared.pool.align"] = value_label
        elif args.sweep == "working-set-fit":
            # Interpret "value" as GroupByTest numKVPairs (changes working set size).
            workload_args = workload_args_base.copy()
            workload_args[1] = value_label
        else:
            print(f"ERROR: unsupported sweep: {args.sweep}", file=sys.stderr)
            return 2

        # Prepare config dir (only for config-changing sweeps).
        conf_dir = base_variant.scache_conf_dir
        if updates:
            conf_dir = sweep_root / "generated-conf" / value_label
            conf_dir.mkdir(parents=True, exist_ok=True)
            _rewrite_kv_conf(base_conf, conf_dir / "scache.conf", updates)
            if base_slaves.is_file():
                (conf_dir / "slaves").write_text(
                    base_slaves.read_text(encoding="utf-8"),
                    encoding="utf-8",
                )

        if args.restart_cluster:
            env = dict(os.environ)
            env["SCACHE_CONF_OVERRIDE_DIR"] = str(conf_dir)

            _run(
                [str(stop_script)],
                cwd=root,
                env=env,
                stdout_path=sweep_root / f"cluster-stop.{value_label}.stdout.log",
                stderr_path=sweep_root / f"cluster-stop.{value_label}.stderr.log",
                check=False,
            )
            _run(
                [str(start_script)],
                cwd=root,
                env=env,
                stdout_path=sweep_root / f"cluster-start.{value_label}.stdout.log",
                stderr_path=sweep_root / f"cluster-start.{value_label}.stderr.log",
                check=True,
            )

        for rep in range(args.repeats):
            run_dir = sweep_root / "runs" / value_label / f"run-{rep:03d}"
            run_dir.mkdir(parents=True, exist_ok=True)

            eventlog_dir = run_dir / "spark-events"
            eventlog_dir.mkdir(parents=True, exist_ok=True)
            spark_overrides = {
                "spark.app.name": f"sensitivity-{args.sweep}-{value_label}",
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": f"file://{eventlog_dir}",
                "spark.eventLog.compress": "false",
            }

            env = dict(os.environ)
            env["SPARK_SUBMIT_EXTRA_ARGS"] = _spark_submit_extra_args(spark_overrides)

            submit_cmd = [str(submit_script)] + workload_args
            rc, elapsed_s = _run(
                submit_cmd,
                cwd=root,
                env=env,
                stdout_path=run_dir / "submit.stdout.log",
                stderr_path=run_dir / "submit.stderr.log",
                check=False,
            )

            eventlog_path = _find_single_eventlog(eventlog_dir)
            parsed = None
            if eventlog_path is not None:
                parsed = _parse_eventlog(tools_dir, eventlog_path, run_dir / "eventlog.summary.json")

            meta = {
                "sweep": args.sweep,
                "value": value_label,
                "repeat": rep,
                "submit_cmd": submit_cmd,
                "spark_submit_extra_args": env.get("SPARK_SUBMIT_EXTRA_ARGS", ""),
                "scache_conf_dir": str(conf_dir),
                "scache_conf_updates": updates,
                "exit_code": rc,
                "submit_elapsed_s": elapsed_s,
                "eventlog": str(eventlog_path) if eventlog_path else None,
                "eventlog_summary": parsed,
            }
            (run_dir / "run.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            _write_csv_row(
                csv_path,
                csv_header,
                {
                    "sweep": args.sweep,
                    "value": value_label,
                    "repeat": rep,
                    "exit_code": rc,
                    "submit_elapsed_s": f"{elapsed_s:.3f}",
                    "app_duration_ms": (parsed or {}).get("app_duration_ms", ""),
                    "shuffle_write_bytes": (parsed or {}).get("shuffle_write_bytes_sum", ""),
                    "shuffle_read_bytes": (parsed or {}).get("shuffle_read_bytes_sum", ""),
                    "eventlog": str(eventlog_path) if eventlog_path else "",
                },
            )

    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="UltraShuffle ablation/sensitivity runner (multinode simulation).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    ab = sub.add_parser("ablation", help="Run ablation variants.")
    ab.add_argument("--out", type=str, default=None, help="Results directory (default: timestamped under ablation-study/results/).")
    ab.add_argument("--repeats", type=int, default=3)
    ab.add_argument("--no-restart-cluster", dest="restart_cluster", action="store_false")
    ab.add_argument("--variants", nargs="*", default=None, help="Subset of variants to run.")
    ab.add_argument(
        "--workload-args",
        nargs=4,
        default=["32", "200000", "1024", "32"],
        metavar=("numMappers", "numKVPairs", "valSize", "numReducers"),
        help="Args for org.apache.spark.examples.GroupByTest (passed to submit-groupbytest-mn.sh).",
    )
    ab.set_defaults(restart_cluster=True)

    se = sub.add_parser("sensitivity", help="Run sensitivity sweeps.")
    se.add_argument("--out", type=str, default=None, help="Results directory (default: timestamped under ablation-study/results/).")
    se.add_argument("--repeats", type=int, default=3)
    se.add_argument("--no-restart-cluster", dest="restart_cluster", action="store_false")
    se.add_argument("--sweep", choices=["cxl-capacity", "align", "working-set-fit"], required=True)
    se.add_argument(
        "--values",
        nargs="+",
        required=True,
        help="Sweep values (e.g., 512m 1g for cxl-capacity; 4096 65536 for align; 100000 200000 for working-set-fit).",
    )
    se.add_argument(
        "--workload-args",
        nargs=4,
        default=["32", "200000", "1024", "32"],
        metavar=("numMappers", "numKVPairs", "valSize", "numReducers"),
        help="Args for org.apache.spark.examples.GroupByTest (passed to submit-groupbytest-mn.sh).",
    )
    se.set_defaults(restart_cluster=True)

    args = ap.parse_args()
    if args.cmd == "ablation":
        return run_ablation(args)
    if args.cmd == "sensitivity":
        return run_sensitivity(args)
    ap.error("unreachable")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
