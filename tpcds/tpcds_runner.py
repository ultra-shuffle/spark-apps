#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Optional


TPCDS_TABLES = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
]


@dataclass(frozen=True)
class QueryResult:
    query: str
    iteration: int
    elapsed_ms: int
    rows: Optional[int]
    ok: bool
    error: Optional[str]


def _normalize_base_uri(base_uri: str) -> str:
    base_uri = base_uri.strip()
    if not base_uri:
        raise ValueError("base_uri is empty")
    return base_uri.rstrip("/")


def _list_sql_files(query_dir: Path) -> list[Path]:
    if not query_dir.exists():
        raise FileNotFoundError(f"query dir not found: {query_dir}")
    files = sorted([p for p in query_dir.rglob("*.sql") if p.is_file()])
    return files


def _query_sort_key(p: Path):
    name = p.stem
    m = re.match(r"^[qQ](\d+)(.*)$", name)
    if m:
        return (0, int(m.group(1)), m.group(2), name)
    m = re.match(r"^(\d+)(.*)$", name)
    if m:
        return (1, int(m.group(1)), m.group(2), name)
    return (2, name)


def _pick_queries(files: list[Path], queries: str) -> list[Path]:
    wanted = []
    tokens = [t.strip() for t in queries.split(",") if t.strip()]
    if not tokens:
        return files

    by_stem = {p.stem: p for p in files}

    def resolve(tok: str) -> Optional[Path]:
        if tok in by_stem:
            return by_stem[tok]
        if re.fullmatch(r"\d+", tok):
            for candidate in (f"q{tok}", f"Q{tok}", tok):
                if candidate in by_stem:
                    return by_stem[candidate]
        m = re.fullmatch(r"[qQ](\d+)", tok)
        if m:
            n = m.group(1)
            for candidate in (f"q{n}", f"Q{n}", n):
                if candidate in by_stem:
                    return by_stem[candidate]
        for stem, p in by_stem.items():
            if stem.startswith(tok):
                return p
        return None

    missing = []
    for tok in tokens:
        p = resolve(tok)
        if p is None:
            missing.append(tok)
        else:
            wanted.append(p)

    if missing:
        available = ", ".join([p.stem for p in sorted(files, key=_query_sort_key)[:20]])
        raise ValueError(
            f"Could not resolve queries: {', '.join(missing)}. "
            f"Available (first 20): {available}"
        )

    # Keep provided order, de-dup.
    seen = set()
    out = []
    for p in wanted:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def _read_sql(p: Path) -> str:
    text = p.read_text(encoding="utf-8")
    text = text.strip()
    text = re.sub(r";\s*$", "", text)
    if not text:
        raise ValueError(f"empty sql file: {p}")
    return text


def _register_views(spark, base_uri: str, fmt: str, table_filter: Optional[str]) -> list[str]:
    base_uri = _normalize_base_uri(base_uri)
    pattern = re.compile(table_filter) if table_filter else None
    registered = []

    for table in TPCDS_TABLES:
        if pattern and not pattern.search(table):
            continue
        table_path = f"{base_uri}/{table}"
        spark.sql(
            f"CREATE OR REPLACE TEMP VIEW {table} "
            f"USING {fmt} "
            f"OPTIONS (path '{table_path}')"
        )
        registered.append(table)

    return registered


def _write_results(out_dir: Path, results: list[QueryResult]):
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "results.json"
    csv_path = out_dir / "results.csv"

    with json_path.open("w", encoding="utf-8") as f:
        json.dump([asdict(r) for r in results], f, indent=2, sort_keys=True)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["query", "iteration", "elapsed_ms", "rows", "ok", "error"])
        for r in results:
            w.writerow([r.query, r.iteration, r.elapsed_ms, r.rows, r.ok, r.error or ""])


def _write_run_info(out_dir: Path, info: dict):
    out_dir.mkdir(parents=True, exist_ok=True)
    with (out_dir / "run-info.json").open("w", encoding="utf-8") as f:
        json.dump(info, f, indent=2, sort_keys=True)


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Run TPC-DS queries on Spark (PySpark).")
    p.add_argument("--mode", choices=["prepare", "run"], required=True)
    p.add_argument("--base-uri", required=True, help="Base URI containing table subdirs (e.g. hdfs://.../sf=1/parquet)")
    p.add_argument("--format", default="parquet", choices=["parquet", "orc"])
    p.add_argument("--query-dir", required=True)
    p.add_argument("--queries", default="", help="Comma-separated query list (e.g. q1,q2 or 1,2). Empty = all.")
    p.add_argument("--out-dir", required=True, help="Local output directory on the driver")
    p.add_argument("--iterations", type=int, default=int(os.environ.get("TPCDS_ITERATIONS", "1")))
    p.add_argument("--table-filter", default=os.environ.get("TPCDS_TABLE_FILTER", ""))
    args = p.parse_args(argv)

    try:
        from pyspark.sql import SparkSession  # type: ignore
    except Exception as e:
        print(f"ERROR: PySpark is required (run via spark-submit). {e}", file=sys.stderr)
        return 2

    out_dir = Path(args.out_dir)
    query_dir = Path(args.query_dir)

    files = _list_sql_files(query_dir)
    if not files:
        print(
            f"ERROR: No .sql query files found under {query_dir}. "
            "Place TPC-DS queries under ./tpcds/queries (see ./tpcds/README.md).",
            file=sys.stderr,
        )
        return 2

    files = sorted(files, key=_query_sort_key)
    try:
        picked = _pick_queries(files, args.queries)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2

    spark = SparkSession.builder.appName("tpcds-runner").getOrCreate()

    table_filter = args.table_filter.strip() or None
    registered = _register_views(spark, args.base_uri, args.format, table_filter)

    info = {
        "appId": spark.sparkContext.applicationId,
        "sparkVersion": spark.version,
        "master": spark.sparkContext.master,
        "mode": args.mode,
        "baseUri": _normalize_base_uri(args.base_uri),
        "format": args.format,
        "queryDir": str(query_dir),
        "queries": [p.stem for p in picked],
        "iterations": args.iterations,
        "registeredTables": registered,
    }
    _write_run_info(out_dir, info)

    if args.mode == "prepare":
        print(f"Prepared temp views: {len(registered)} tables")
        print(f"Run info written to: {out_dir / 'run-info.json'}")
        spark.stop()
        return 0

    if args.iterations < 1:
        print("ERROR: --iterations must be >= 1", file=sys.stderr)
        spark.stop()
        return 2

    results: list[QueryResult] = []

    for qfile in picked:
        qname = qfile.stem
        sql_text = _read_sql(qfile)

        for it in range(1, args.iterations + 1):
            start = time.perf_counter()
            ok = True
            err = None
            rows = None
            try:
                df = spark.sql(sql_text)
                # Trigger execution in the JVM without collecting rows to Python.
                rows = int(df._jdf.rdd().count())
            except Exception as e:
                ok = False
                err = str(e)
            elapsed_ms = int((time.perf_counter() - start) * 1000)

            results.append(
                QueryResult(
                    query=qname,
                    iteration=it,
                    elapsed_ms=elapsed_ms,
                    rows=rows,
                    ok=ok,
                    error=err,
                )
            )

            status = "OK" if ok else "FAIL"
            print(f"{qname} iter={it} {status} {elapsed_ms}ms rows={rows}")
            sys.stdout.flush()

            if not ok:
                break

    _write_results(out_dir, results)
    print(f"Results written to: {out_dir / 'results.csv'}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

