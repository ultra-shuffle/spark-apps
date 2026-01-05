# spark-apps

Small helper repo for running Apache Spark (standalone mode) plus a few local test jobs/configs.

## What’s here

- **Standalone cluster scripts**: start/stop Spark master + worker(s).
- **Spark configs** in `conf/` (defaults, env, logging, metrics, scheduler).
- **Example job**: `groupbytest.py` with `submit-groupbytest.sh`.
- **HiBench** benchmark suite vendored under `HiBench-7.1.1/`.

## Prerequisites

- Java (for Spark)
- A Spark distribution available on the machine (e.g. `SPARK_HOME` set), or installed Spark on `PATH`

## Quick start (standalone)

1. Review environment and Spark settings:

   - `env.sh`
   - `conf/spark-env.sh`
   - `conf/spark-defaults.conf`

2. Start the standalone cluster:

   ```bash
   ./start-standalone.sh
   ```

3. Submit the example job:

   ```bash
   ./submit-groupbytest.sh
   ```

4. Stop the cluster:

   ```bash
   ./stop-standalone.sh
   ```

## 4-node (simulated) + NUMA/CXL-memory experiment

There are two useful modes:

1) **Single-host / multi-executor** (quick sanity / baseline): `./start-standalone.sh` + `NUM_EXECUTORS=...`.
2) **Multi-host simulation (Spark sees N hosts)**: `./start-standalone-multinode.sh` starts **N Spark workers**
   on distinct loopback IPs (e.g. `127.0.0.2..`) and **N ScacheClients** (one per simulated node).

Example: **4 hosts**, Spark workers pinned to NUMA node **0**, ScacheClients pinned to NUMA node **1**:

```bash
NUM_NODES=4 \
SPARK_WORKER_NUMACTL_OPTS="--cpunodebind=0 --membind=0" \
SCACHE_CLIENT_SCRIPT_OPTS="--cpu-node 1 --mem-node 1" \
WORKER_CORES=8 WORKER_MEMORY=16g \
./start-standalone-multinode.sh

# One executor per host: set WORKER_CORES == EXECUTOR_CORES and NUM_EXECUTORS == NUM_NODES.
NUM_EXECUTORS=4 EXECUTOR_CORES=8 \
EXECUTOR_MEMORY=8G DRIVER_MEMORY=8G \
./submit-groupbytest.sh 32 800000 1024 32
```

Notes:

- Override host list with `SPARK_SIM_HOSTS="127.0.0.2 127.0.0.3 127.0.0.4 127.0.0.5"`.
- `conf/scache-multinode/scache.conf` defaults to `scache.daemon.ipc.backend=files` to avoid sharing a pool allocator across multiple ScacheClients.
- Stop with `./stop-standalone-multinode.sh`.

## Logs and runtime state

- Spark logs: `logs/`
- Spark event logs: `logs/spark-events/`
- PID/state files: `run/`
- Spark local directories: `tmp/spark-local/`

## HiBench

The `HiBench-7.1.1/` directory contains the HiBench benchmark suite.

Typical flow is:

- Configure HiBench under `HiBench-7.1.1/conf/`
- Use scripts under `HiBench-7.1.1/bin/` to build and run workloads

### Quick run (minimal)

This repo includes a thin wrapper that generates a local override config and runs a single benchmark:

- Minimal smoke test (Spark-only): `./run-hibench.sh` (defaults to `micro/sleep` on `spark`)
- Example with data (no HDFS required): `HADOOP_HOME=/path/to/hadoop ./run-hibench.sh micro/wordcount spark`
- HDFS mode (requires HDFS): `HADOOP_HOME=/path/to/hadoop HIBENCH_FS=hdfs HDFS_MASTER=hdfs://localhost:8020 ./run-hibench.sh micro/wordcount spark`

Useful knobs:

- Control Spark resources via `HIBENCH_SPARK_EXECUTOR_MEMORY`, `HIBENCH_SPARK_EXECUTOR_CORES`, `HIBENCH_SPARK_CORES_MAX`, `HIBENCH_SPARK_DRIVER_MEMORY`.
- If you want to run against a different Spark config (e.g. vanilla Spark with `spark.scache.enable=false`), export `SPARK_CONF_DIR=/path/to/other/conf` before running; `env.sh` will respect it.

The wrapper writes `HiBench-7.1.1/conf/zzz-spark-apps.conf` (ignored by git) to set:

- `hibench.spark.master` (defaults to this repo’s standalone master `spark://localhost:17077`)
- `hibench.spark.home` (defaults to `SPARK_HOME`)
- `hibench.hadoop.home` (from `HADOOP_HOME`)
- `hibench.hdfs.master` / `hibench.hdfs.data.dir`:
  - default: `file:///` + `${repo}/tmp/hibench-data` (local filesystem, no HDFS daemon)
  - HDFS mode: set `HIBENCH_FS=hdfs` + `HDFS_MASTER=...`

Refer to `HiBench-7.1.1/README.md` for HiBench-specific instructions.

## TPC-DS

This repo includes a small TPC-DS runner (Spark SQL) plus a query generator wrapper:

- Wrapper: `run-tpcds.sh`
- Docs: `tpcds/README.md`
- Sources: `tpcds-kit/` (git submodule) + `tpcds/gen-queries.sh`

Quick start:

```bash
git submodule update --init --recursive
cd tpcds-kit/tools && make OS=LINUX
cd /path/to/spark-apps
./start-standalone.sh
TPCDS_SCALE=1 TPCDS_CLEAN=1 ./tpcds/gen-queries.sh
TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet TPCDS_FORMAT=parquet ./run-tpcds.sh
```
