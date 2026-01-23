#!/usr/bin/env python3
import argparse
import csv
import math
import re
import statistics
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def _safe_float(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({k: "" if row.get(k) is None else row.get(k) for k in fieldnames})


def _duration_ms(row: Dict[str, str]) -> Optional[float]:
    app_ms = _safe_float(row.get("app_duration_ms"))
    if app_ms is not None and app_ms > 0:
        return app_ms
    submit_s = _safe_float(row.get("submit_elapsed_s"))
    if submit_s is not None and submit_s > 0:
        return submit_s * 1000.0
    return None


def _filter_rows(rows: Iterable[Dict[str, str]], *, include_failed: bool) -> List[Dict[str, str]]:
    kept: List[Dict[str, str]] = []
    for row in rows:
        exit_code = (row.get("exit_code") or "").strip()
        if not include_failed and exit_code not in ("", "0"):
            continue
        if _duration_ms(row) is None:
            continue
        kept.append(row)
    return kept


def _summarize_exit_codes(rows: Iterable[Dict[str, str]]) -> str:
    counts: Dict[str, int] = {}
    for row in rows:
        code = (row.get("exit_code") or "").strip() or "<empty>"
        counts[code] = counts.get(code, 0) + 1
    parts = [f"{k}={v}" for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]
    return ", ".join(parts)


@dataclass(frozen=True)
class Stats:
    n: int
    mean: float
    stdev: float

    @property
    def stderr(self) -> float:
        if self.n <= 1:
            return 0.0
        return self.stdev / math.sqrt(self.n)


def _stats(values: Sequence[float]) -> Stats:
    if not values:
        return Stats(n=0, mean=float("nan"), stdev=float("nan"))
    if len(values) == 1:
        return Stats(n=1, mean=values[0], stdev=0.0)
    return Stats(n=len(values), mean=statistics.mean(values), stdev=statistics.stdev(values))


def _parse_size_bytes(value: str) -> float:
    s = value.strip().lower()
    if not s:
        raise ValueError("empty size")

    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([a-z]+)?", s)
    if not m:
        raise ValueError(f"invalid size: {value}")

    number = float(m.group(1))
    unit = (m.group(2) or "b").strip()

    factors = {
        "b": 1,
        "bytes": 1,
        "k": 1024,
        "kb": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "t": 1024**4,
        "tb": 1024**4,
    }
    if unit not in factors:
        raise ValueError(f"unknown unit: {unit}")
    return number * factors[unit]


def _human_bytes(num_bytes: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(num_bytes)
    idx = 0
    while idx + 1 < len(units) and value >= 1024.0:
        value /= 1024.0
        idx += 1
    if units[idx] == "B":
        return f"{int(value)}{units[idx]}"
    if value >= 10:
        return f"{value:.0f}{units[idx]}"
    return f"{value:.1f}{units[idx]}"


def _ensure_matplotlib():
    try:
        import matplotlib  # noqa: F401
    except Exception as e:
        raise RuntimeError(
            "matplotlib is required for plotting. "
            "On this machine it should already be installed."
        ) from e


def _save(fig, out_base: Path) -> None:
    out_base.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_base.with_suffix(".pdf"))
    fig.savefig(out_base.with_suffix(".png"), dpi=200)


def plot_ablation(ablation_csv: Path, out_dir: Path, baseline: str, *, include_failed: bool) -> None:
    _ensure_matplotlib()
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows_all = _read_csv(ablation_csv)
    rows = _filter_rows(rows_all, include_failed=include_failed)

    by_variant: Dict[str, List[float]] = {}
    for row in rows:
        variant = (row.get("variant") or "").strip()
        ms = _duration_ms(row)
        if not variant or ms is None:
            continue
        by_variant.setdefault(variant, []).append(ms)

    if not by_variant:
        summary = _summarize_exit_codes(rows_all)
        hint = "" if include_failed else " (try --include-failed to plot submit time anyway)"
        raise RuntimeError(
            f"no plottable rows found in {ablation_csv}{hint}; exit_code counts: {summary}"
        )

    label_map = {
        "ultrashuffle-full": "UltraShuffle",
        "per-block-files": "Per-block IPC files",
        "no-partition-homes": "Random placement",
        "no-remote-cache": "No remote cache",
        "service-mediated-fetch": "Service fetch",
    }
    order = [
        "ultrashuffle-full",
        "per-block-files",
        "no-partition-homes",
        "no-remote-cache",
        "service-mediated-fetch",
    ]
    variants = [v for v in order if v in by_variant] + [v for v in sorted(by_variant.keys()) if v not in order]

    stats_by_variant = {v: _stats(by_variant[v]) for v in variants}
    baseline_use = baseline
    if baseline_use not in stats_by_variant:
        baseline_use = min(stats_by_variant.keys(), key=lambda v: stats_by_variant[v].mean)
        print(
            f"WARNING: baseline {baseline!r} missing from filtered rows in {ablation_csv}; "
            f"using {baseline_use!r} instead. "
            f"(include_failed={include_failed}; exit_code counts: {_summarize_exit_codes(rows_all)})",
            file=sys.stderr,
        )
    baseline_ms = stats_by_variant[baseline_use].mean

    # 1) Absolute runtime (seconds).
    fig, ax = plt.subplots(figsize=(8.0, 3.4))
    xs = list(range(len(variants)))
    means_s = [stats_by_variant[v].mean / 1000.0 for v in variants]
    errs_s = [stats_by_variant[v].stdev / 1000.0 for v in variants]
    ax.bar(xs, means_s, yerr=errs_s, capsize=3, color="#4C78A8")
    ax.set_ylabel("Runtime (s)")
    ax.set_xticks(xs, [label_map.get(v, v) for v in variants], rotation=20, ha="right")
    ax.grid(axis="y", linestyle=":", linewidth=0.8)
    title_suffix = " (incl. failed runs)" if include_failed else ""
    ax.set_title(f"UltraShuffle ablation (mean ± std){title_suffix}")
    out_base = out_dir / "ablation-runtime"
    _save(fig, out_base)
    _write_csv(
        out_base.with_suffix(".csv"),
        fieldnames=[
            "x",
            "variant",
            "label",
            "n",
            "mean_ms",
            "stdev_ms",
            "stderr_ms",
            "mean_s",
            "stdev_s",
            "stderr_s",
            "include_failed",
        ],
        rows=[
            {
                "x": x,
                "variant": v,
                "label": label_map.get(v, v),
                "n": stats_by_variant[v].n,
                "mean_ms": stats_by_variant[v].mean,
                "stdev_ms": stats_by_variant[v].stdev,
                "stderr_ms": stats_by_variant[v].stderr,
                "mean_s": stats_by_variant[v].mean / 1000.0,
                "stdev_s": stats_by_variant[v].stdev / 1000.0,
                "stderr_s": stats_by_variant[v].stderr / 1000.0,
                "include_failed": include_failed,
            }
            for x, v in zip(xs, variants)
        ],
    )
    plt.close(fig)

    # 2) Normalized runtime (× baseline).
    fig, ax = plt.subplots(figsize=(8.0, 3.4))
    means_norm = [stats_by_variant[v].mean / baseline_ms for v in variants]
    errs_norm = [stats_by_variant[v].stdev / baseline_ms for v in variants]
    ax.bar(xs, means_norm, yerr=errs_norm, capsize=3, color="#F58518")
    ax.axhline(1.0, color="black", linewidth=0.8)
    ax.set_ylabel(f"Normalized runtime (× {label_map.get(baseline_use, baseline_use)})")
    ax.set_xticks(xs, [label_map.get(v, v) for v in variants], rotation=20, ha="right")
    ax.grid(axis="y", linestyle=":", linewidth=0.8)
    ax.set_title(f"UltraShuffle ablation (normalized, mean ± std){title_suffix}")
    out_base = out_dir / "ablation-normalized"
    _save(fig, out_base)
    _write_csv(
        out_base.with_suffix(".csv"),
        fieldnames=[
            "x",
            "variant",
            "label",
            "n",
            "mean_ms",
            "stdev_ms",
            "stderr_ms",
            "mean_s",
            "stdev_s",
            "stderr_s",
            "mean_norm",
            "stdev_norm",
            "stderr_norm",
            "baseline_variant",
            "baseline_label",
            "baseline_mean_ms",
            "baseline_mean_s",
            "include_failed",
        ],
        rows=[
            {
                "x": x,
                "variant": v,
                "label": label_map.get(v, v),
                "n": stats_by_variant[v].n,
                "mean_ms": stats_by_variant[v].mean,
                "stdev_ms": stats_by_variant[v].stdev,
                "stderr_ms": stats_by_variant[v].stderr,
                "mean_s": stats_by_variant[v].mean / 1000.0,
                "stdev_s": stats_by_variant[v].stdev / 1000.0,
                "stderr_s": stats_by_variant[v].stderr / 1000.0,
                "mean_norm": stats_by_variant[v].mean / baseline_ms,
                "stdev_norm": stats_by_variant[v].stdev / baseline_ms,
                "stderr_norm": stats_by_variant[v].stderr / baseline_ms,
                "baseline_variant": baseline_use,
                "baseline_label": label_map.get(baseline_use, baseline_use),
                "baseline_mean_ms": baseline_ms,
                "baseline_mean_s": baseline_ms / 1000.0,
                "include_failed": include_failed,
            }
            for x, v in zip(xs, variants)
        ],
    )
    plt.close(fig)


def plot_sensitivity(sensitivity_csv: Path, out_dir: Path, *, include_failed: bool) -> None:
    _ensure_matplotlib()
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows_all = _read_csv(sensitivity_csv)
    rows = _filter_rows(rows_all, include_failed=include_failed)
    if not rows:
        summary = _summarize_exit_codes(rows_all)
        hint = "" if include_failed else " (try --include-failed to plot submit time anyway)"
        raise RuntimeError(
            f"no plottable rows found in {sensitivity_csv}{hint}; exit_code counts: {summary}"
        )

    sweep = (rows[0].get("sweep") or "").strip() or "sweep"

    # Group: value -> durations.
    by_value: Dict[str, List[float]] = {}
    for row in rows:
        value = (row.get("value") or "").strip()
        ms = _duration_ms(row)
        if not value or ms is None:
            continue
        by_value.setdefault(value, []).append(ms)

    def key_fn(v: str) -> float:
        if sweep == "cxl-capacity":
            return _parse_size_bytes(v)
        return float(_safe_float(v) or 0.0)

    values_sorted = sorted(by_value.keys(), key=key_fn)
    xs: List[float] = []
    xlabels: List[str] = []
    for v in values_sorted:
        if sweep == "cxl-capacity":
            b = _parse_size_bytes(v)
            xs.append(b / (1024.0**3))
            xlabels.append(_human_bytes(b))
        else:
            xs.append(float(_safe_float(v) or 0.0))
            xlabels.append(v)

    ys_s = []
    yerr_s = []
    stats_by_value: Dict[str, Stats] = {}
    for v in values_sorted:
        st = _stats(by_value[v])
        stats_by_value[v] = st
        ys_s.append(st.mean / 1000.0)
        yerr_s.append(st.stdev / 1000.0)

    fig, ax = plt.subplots(figsize=(7.2, 3.4))
    ax.errorbar(xs, ys_s, yerr=yerr_s, marker="o", linewidth=1.5, capsize=3, color="#54A24B")
    ax.grid(axis="y", linestyle=":", linewidth=0.8)

    if sweep == "cxl-capacity":
        ax.set_xlabel("CXL/shared-pool capacity (GiB)")
    elif sweep == "align":
        ax.set_xlabel("Block alignment (bytes)")
        try:
            ax.set_xscale("log", base=2)
        except TypeError:
            ax.set_xscale("log")
    elif sweep == "working-set-fit":
        ax.set_xlabel("Working set size (numKVPairs)")
    else:
        ax.set_xlabel("Value")

    ax.set_ylabel("Runtime (s)")
    if sweep in ("cxl-capacity",):
        ax.set_xticks(xs, xlabels, rotation=20, ha="right")
    title_suffix = " (incl. failed runs)" if include_failed else ""
    ax.set_title(f"Sensitivity: {sweep} (mean ± std){title_suffix}")
    out_base = out_dir / f"sensitivity-{sweep}"
    _save(fig, out_base)
    _write_csv(
        out_base.with_suffix(".csv"),
        fieldnames=[
            "sweep",
            "value",
            "x",
            "x_label",
            "n",
            "mean_ms",
            "stdev_ms",
            "stderr_ms",
            "mean_s",
            "stdev_s",
            "stderr_s",
            "include_failed",
        ],
        rows=[
            {
                "sweep": sweep,
                "value": v,
                "x": x,
                "x_label": xl,
                "n": stats_by_value[v].n,
                "mean_ms": stats_by_value[v].mean,
                "stdev_ms": stats_by_value[v].stdev,
                "stderr_ms": stats_by_value[v].stderr,
                "mean_s": stats_by_value[v].mean / 1000.0,
                "stdev_s": stats_by_value[v].stdev / 1000.0,
                "stderr_s": stats_by_value[v].stderr / 1000.0,
                "include_failed": include_failed,
            }
            for v, x, xl in zip(values_sorted, xs, xlabels)
        ],
    )
    plt.close(fig)


def plot_from_results_dir(results_dir: Path, out_dir: Optional[Path], baseline: str, *, include_failed: bool) -> None:
    if out_dir is None:
        out_dir = results_dir / "plots"

    ablation_csv = results_dir / "ablation.csv"
    if ablation_csv.is_file():
        plot_ablation(ablation_csv, out_dir, baseline=baseline, include_failed=include_failed)

    for p in sorted(results_dir.iterdir()):
        if not p.is_dir() or not p.name.startswith("sensitivity-"):
            continue
        sensitivity_csv = p / "sensitivity.csv"
        if sensitivity_csv.is_file():
            plot_sensitivity(sensitivity_csv, out_dir, include_failed=include_failed)


def main() -> int:
    ap = argparse.ArgumentParser(description="Plot UltraShuffle ablation-study results (CSV -> PDF/PNG).")
    ap.add_argument("--results-dir", type=Path, required=True, help="Directory containing ablation.csv and/or sensitivity-*/.")
    ap.add_argument("--out-dir", type=Path, default=None, help="Output directory (default: <results-dir>/plots).")
    ap.add_argument("--baseline", type=str, default="ultrashuffle-full", help="Baseline variant for normalized ablation plot.")
    ap.add_argument(
        "--include-failed",
        action="store_true",
        help="Include non-zero exit_code runs (plots submit_elapsed_s when app_duration_ms is missing).",
    )
    args = ap.parse_args()

    if not args.results_dir.is_dir():
        print(f"ERROR: not a directory: {args.results_dir}", file=sys.stderr)
        return 2

    plot_from_results_dir(args.results_dir, args.out_dir, baseline=args.baseline, include_failed=args.include_failed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
