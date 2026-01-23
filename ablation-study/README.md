# UltraShuffle ablation study (Spark + SCache)

This directory contains a **reproducible experiment harness** for the paper’s evaluation section:

- Ablations:
  - **(i) Pool slices** vs **per-block files**
  - **(ii) partition homes** vs **random placement**
  - **(iii) caching** for **non-local** blocks
  - **(iv) shared CXL pool** vs **service-mediated fetch**
- Sensitivity sweeps:
  - CXL capacity
  - block size/alignment
  - fraction of working set fitting in CXL memory

The harness targets this repo’s **standalone multinode simulation** (`start-standalone-multinode.sh`)
and uses `org.apache.spark.examples.GroupByTest` as a shuffle-heavy workload.

## Layout

- `ablation-study/conf/`: SCache config variants (used via `SCACHE_CONF_OVERRIDE_DIR=...`)
- `ablation-study/patches/`: optional patches (for knobs not yet exposed by config)
- `ablation-study/tools/parse_eventlog.py`: parses Spark event logs into a JSON summary
- `ablation-study/run.py`: ablation + sensitivity runner (writes CSV + per-run logs)

## Prerequisites

- `SPARK_HOME` points to the customized Spark distribution (or `env.sh` can auto-detect it).
- SCache is installed at `$SCACHE_HOME` (default `/home/yxz/SCache`) and startable by
  `start-standalone-multinode.sh`.
- Spark is configured to enable SCache (see `conf/spark-defaults.conf`).

## Run ablations

Runs all variants (3 repeats each) and writes results under `ablation-study/results/<timestamp>/`:

```bash
python3 ablation-study/run.py ablation
```

Run a subset:

```bash
python3 ablation-study/run.py ablation --variants ultrashuffle-full no-remote-cache service-mediated-fetch
```

Tune the workload (GroupByTest positional args):

```bash
python3 ablation-study/run.py ablation --workload-args 32 200000 1024 32
```

Outputs:
- `ablation-study/results/<ts>/ablation.csv`
- `ablation-study/results/<ts>/ablation/<variant>/run-*/run.json`
- `ablation-study/results/<ts>/ablation/<variant>/run-*/eventlog.summary.json`

Note: Spark requires `spark.eventLog.dir` to exist as a directory; `ablation-study/run.py`
creates per-run eventlog directories automatically.

## Run sensitivity sweeps

CXL capacity sweep (updates both `scache.memory.offHeap.size` and `scache.storage.cxl.shared.pool.size`):

```bash
python3 ablation-study/run.py sensitivity --sweep cxl-capacity --values 512m 1g 2g
```

Alignment sweep (updates both pool alignments):

```bash
python3 ablation-study/run.py sensitivity --sweep align --values 4096 65536 2097152
```

Working-set-fit sweep (varies `numKVPairs` while keeping config fixed):

```bash
python3 ablation-study/run.py sensitivity --sweep working-set-fit --values 50000 100000 200000
```

Outputs:
- `ablation-study/results/<ts>/sensitivity-<sweep>/sensitivity.csv`
- `ablation-study/results/<ts>/sensitivity-<sweep>/runs/<value>/run-*/run.json`

## Mapping paper knobs → current configs

- **Pool slices vs per-block files**
  - pool slices: `ablation-study/conf/scache-multinode/ultrashuffle-full/scache.conf`
    (`scache.daemon.ipc.backend=pool`)
  - per-block files: `ablation-study/conf/scache-multinode/per-block-files/scache.conf`
    (`scache.daemon.ipc.backend=files`)
  - Note: the current Spark no-local-files fast path expects pool-slice upload; therefore the
    per-block-files variant runs with `spark.scache.shuffle.noLocalFiles=false` (sidecar mode).
- **Partition homes vs random placement**
  - Config keys are present in `ultrashuffle-full` vs `no-partition-homes` as
    `scache.shuffle.reducePlacement=homes|random`.
  - This requires applying `ablation-study/patches/scache-partition-homes.patch` to `$SCACHE_HOME`
    (and rebuilding SCache) to take effect.
- **Caching for non-local blocks**
  - cached: `scache.daemon.putBlock.storageLevel.remote=OFF_HEAP` (in full config)
  - disabled: `scache.daemon.putBlock.storageLevel.remote=DISK_ONLY` (in `no-remote-cache`)
- **Shared CXL pool vs service-mediated fetch**
  - shared pool: `scache.storage.cxl.shared.enabled=true` + `scache.storage.network.enabled=false`
  - service fetch: `scache.storage.cxl.shared.enabled=false` + `scache.storage.network.enabled=true`

## Event log parser

```bash
python3 ablation-study/tools/parse_eventlog.py /path/to/spark-events/<eventlog>
```

## Plotting

Given a results directory produced by `ablation-study/run.py`, generate `PDF`/`PNG` plots under
`<results-dir>/plots/` (and matching `*.csv` files with the plotted mean/std data):

```bash
python3 ablation-study/plot.py --results-dir ablation-study/results/<timestamp>
```
