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

The wrapper writes `HiBench-7.1.1/conf/zzz-spark-apps.conf` (ignored by git) to set:

- `hibench.spark.master` (defaults to this repo’s standalone master `spark://localhost:17077`)
- `hibench.spark.home` (defaults to `SPARK_HOME`)
- `hibench.hadoop.home` (from `HADOOP_HOME`)
- `hibench.hdfs.master` / `hibench.hdfs.data.dir`:
  - default: `file:///` + `${repo}/tmp/hibench-data` (local filesystem, no HDFS daemon)
  - HDFS mode: set `HIBENCH_FS=hdfs` + `HDFS_MASTER=...`

Refer to `HiBench-7.1.1/README.md` for HiBench-specific instructions.
