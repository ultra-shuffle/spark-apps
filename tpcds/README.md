# TPC-DS (Spark SQL)

This repo includes a small TPC-DS harness for Spark SQL:

- Public sources: `tpcds-kit/` (git submodule, see `tpcds-kit/EULA.txt`)
- Query generator wrapper: `tpcds/gen-queries.sh` (calls `tpcds-kit/tools/dsqgen`)
- Query runner wrapper: `run-tpcds.sh` (submits `tpcds/tpcds_runner.py` via `spark-submit`)

## Quick start

Prereqs:

- Java + Spark (`SPARK_HOME` set)
- `python3` (used to split `dsqgen` output and by the runner)
- A prepared dataset in Parquet/ORC (see “Dataset layout”)

Generate queries + run:

```bash
cd /path/to/spark-apps
git submodule update --init --recursive
cd tpcds-kit/tools && make OS=LINUX
cd /path/to/spark-apps

./start-standalone.sh

TPCDS_SCALE=1 TPCDS_CLEAN=1 ./tpcds/gen-queries.sh

TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet \
TPCDS_FORMAT=parquet \
./run-tpcds.sh
```

## Query generation

The wrapper defaults to `TPCDS_DIALECT=spark` and uses `tpcds/dialects/*.tpl` to provide a
Spark-friendly LIMIT syntax and mandatory `_BEGIN/_END` markers.

Common knobs:

- `TPCDS_SCALE=1` (TPC-DS scale in GB; used for query parameterization)
- `TPCDS_DIALECT=spark` (recommended for Spark SQL)

Outputs:

- `tpcds/queries/q1.sql .. q99.sql`

Notes:

- Generated `.sql` files are ignored by default (`.gitignore`) to avoid licensing/policy issues.
- If you see `open of distributions failed: tpcds.idx`, ensure `tpcds-kit/tools` was built and
  you are using `tpcds/gen-queries.sh` (it passes `-DISTRIBUTIONS .../tpcds.idx`).

## Dataset layout

`run-tpcds.sh` expects a dataset where each table is stored under:

```
<TPCDS_BASE_URI>/
  call_center/
  catalog_page/
  catalog_returns/
  ...
  store_sales/
  ...
```

and each `<table>/` directory is written as Parquet or ORC.

## Data generation (optional)

If you don’t already have a dataset, the vendored kit includes `dsdgen` for generating raw
pipe-delimited `.dat` files.

Build tools (generates `dsdgen`, `dsqgen`, and `tpcds.idx`):

```bash
cd /path/to/spark-apps/tpcds-kit/tools
make OS=LINUX
```

Generate raw data (example SF=1GB):

```bash
cd /path/to/spark-apps/tpcds-kit/tools
mkdir -p /mnt/tpcds/sf=1/raw
./dsdgen -SCALE 1 -DIR /mnt/tpcds/sf=1/raw -FORCE
```

If you run `dsdgen` outside `tpcds-kit/tools`, pass `-DISTRIBUTIONS /path/to/tpcds.idx`.

Then convert `.dat` to Parquet/ORC and write each table to `<TPCDS_BASE_URI>/<table>/`.
The canonical schema is in `tpcds-kit/tools/tpcds.sql`.

## Running

HDFS-backed dataset (typical for multi-node runs):

```bash
SPARK_MASTER=spark://spark-master:7077 \
TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet \
TPCDS_FORMAT=parquet \
./run-tpcds.sh
```

Local filesystem dataset (single machine only):

```bash
TPCDS_FS=local \
TPCDS_DATA_DIR=/absolute/path/to/tpcds/sf=1/parquet \
TPCDS_FORMAT=parquet \
./run-tpcds.sh
```

## Disk usage planning

TPC-DS can use disk for **(a)** the dataset itself and **(b)** Spark local scratch (shuffle/spill) under `SPARK_LOCAL_DIRS`.

Rough sizing rules of thumb (per **scale factor** `SF`, in decimal GB):

- Raw `.dat` from `dsdgen`: ~`1 × SF` GB
- Parquet/ORC (Snappy/ZSTD): typically `0.3–0.8 × SF` GB (worst-case: ~`1 × SF` GB)
- Peak Spark scratch (`SPARK_LOCAL_DIRS`) during query execution: often `0.5–2 × dataset` (can be higher if memory is tight and heavy spilling occurs)

Conservative “don’t fill the disk” budgets if everything lives on one filesystem:

- **Dataset already prepared (Parquet/ORC):** plan **~`3 × SF` GB** free (dataset + scratch)
- **Generate raw + convert + run (keep both raw and Parquet/ORC):** plan **~`5 × SF` GB** free

If you store the dataset in HDFS with replication `R`, multiply the **dataset** portion by `R` (scratch is still local on each worker).

Quick sanity checks:

```bash
du -sh /path/to/tpcds/sf=1/*            # dataset footprint
du -sh /path/to/spark-local             # SPARK_LOCAL_DIRS footprint
df -h /path/to                          # free space
```

Select queries:

```bash
TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet \
TPCDS_QUERIES="1,2,3,14" \
./run-tpcds.sh
```

Tune Spark resources:

```bash
TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet \
TPCDS_EXECUTOR_CORES=8 TPCDS_CORES_MAX=32 \
TPCDS_EXECUTOR_MEMORY=16g TPCDS_DRIVER_MEMORY=8g \
./run-tpcds.sh
```

Run with an alternate Spark config (example: vanilla Spark with `spark.scache.enable=false`):

```bash
SPARK_CONF_DIR=/path/to/other/spark/conf \
TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet \
./run-tpcds.sh
```

Append Spark SQL configs (bash array; each entry becomes a `--conf k=v`):

```bash
TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet \
TPCDS_SPARK_CONF=("spark.sql.shuffle.partitions=1024" "spark.sql.adaptive.enabled=false") \
./run-tpcds.sh
```

Repeat each query multiple times (runner env):

```bash
TPCDS_ITERATIONS=3 \
TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet \
./run-tpcds.sh
```

Outputs are written under `logs/tpcds/<run-id>/` on the **driver** node:

- `run-info.json` (Spark appId, master, query list, etc.)
- `results.csv` / `results.json`
