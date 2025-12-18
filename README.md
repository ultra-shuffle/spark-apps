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

Refer to `HiBench-7.1.1/README.md` for HiBench-specific instructions.
