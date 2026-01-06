#!/usr/bin/env bash
set -euo pipefail

# This script is intended to be executed, not sourced.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "ERROR: Do not source this script. Run: ./submit-groupbytest.sh" >&2
  return 1 2>/dev/null || exit 1
fi

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load workspace Spark env (SPARK_HOME, SPARK_CONF_DIR, SPARK_LOG_DIR)
# shellcheck disable=SC1091
source "${_root_dir}/env.sh"

# Clear leaked vars from a prior sourced run (see conf/spark-env.sh marker).
if [[ "${SPARK_APPS_ENV_MARKER:-}" == "1" ]]; then
  for _v in ${SPARK_APPS_ENV_VARS:-""}; do
    unset "${_v}" || true
  done
  unset SPARK_APPS_ENV_MARKER SPARK_APPS_ENV_VARS || true
fi

# Load standalone daemon settings (SPARK_MASTER_HOST/PORT, etc.)
if [[ -f "${SPARK_CONF_DIR}/spark-env.sh" ]]; then
  # shellcheck disable=SC1090
  source "${SPARK_CONF_DIR}/spark-env.sh"
fi

: "${SPARK_HOME:?SPARK_HOME is not set (check env.sh)}"

master_host="${SPARK_MASTER_HOST:-localhost}"
master_port="${SPARK_MASTER_PORT:-17077}"
master_url="spark://${master_host}:${master_port}"

# Resource defaults (override by exporting these vars before running the script)
DRIVER_MEMORY="${DRIVER_MEMORY:-16G}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-16G}"
EXECUTOR_CORES="${EXECUTOR_CORES:-8}"
NUM_EXECUTORS="${NUM_EXECUTORS:-}"

_cores_max_was_set=0
if [[ -n "${CORES_MAX+x}" ]]; then
  _cores_max_was_set=1
fi

CORES_MAX="${CORES_MAX:-${EXECUTOR_CORES}}"
if [[ -n "${NUM_EXECUTORS}" && "${_cores_max_was_set}" == "0" ]]; then
  if ! [[ "${NUM_EXECUTORS}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: NUM_EXECUTORS must be an integer, got: ${NUM_EXECUTORS}" >&2
    exit 1
  fi
  if ! [[ "${EXECUTOR_CORES}" =~ ^[0-9]+$ ]]; then
    echo "ERROR: EXECUTOR_CORES must be an integer, got: ${EXECUTOR_CORES}" >&2
    exit 1
  fi
  CORES_MAX="$((NUM_EXECUTORS * EXECUTOR_CORES))"
fi

MEMORY_FRACTION="${MEMORY_FRACTION:-0.8}"

EXAMPLES_CLASS="${EXAMPLES_CLASS:-org.apache.spark.examples.GroupByTest}"

# Prefer explicit override; otherwise try common Spark layouts.
if [[ -n "${EXAMPLES_JAR:-}" ]]; then
  examples_jar="${EXAMPLES_JAR}"
elif [[ -f "${SPARK_HOME}/examples/target/scala-2.13/jars/spark-examples_2.13-3.5.7.jar" ]]; then
  examples_jar="${SPARK_HOME}/examples/target/scala-2.13/jars/spark-examples_2.13-3.5.7.jar"
elif [[ -f "${SPARK_HOME}/examples/jars/spark-examples_2.13-3.5.7.jar" ]]; then
  examples_jar="${SPARK_HOME}/examples/jars/spark-examples_2.13-3.5.7.jar"
else
  echo "ERROR: Spark examples JAR not found." >&2
  echo "Tried:" >&2
  echo "- \$EXAMPLES_JAR" >&2
  echo "- ${SPARK_HOME}/examples/target/scala-2.13/jars/spark-examples_2.13-3.5.7.jar" >&2
  echo "- ${SPARK_HOME}/examples/jars/spark-examples_2.13-3.5.7.jar" >&2
  exit 1
fi

extra_submit_args=()
if [[ -n "${SPARK_SUBMIT_EXTRA_ARGS:-}" ]]; then
  # Word-split is intentional to allow passing multiple args via a single env var.
  # Example:
  #   SPARK_SUBMIT_EXTRA_ARGS="--conf spark.scache.enable=false" ./submit-groupbytest.sh ...
  # shellcheck disable=SC2206
  extra_submit_args=(${SPARK_SUBMIT_EXTRA_ARGS})
fi

"${SPARK_HOME}/bin/spark-submit" \
  --class "${EXAMPLES_CLASS}" \
  --master "${master_url}" \
  --deploy-mode client \
  --driver-memory "${DRIVER_MEMORY}" \
  --executor-memory "${EXECUTOR_MEMORY}" \
  --executor-cores "${EXECUTOR_CORES}" \
  --conf "spark.cores.max=${CORES_MAX}" \
  --conf "spark.memory.fraction=${MEMORY_FRACTION}" \
  "${extra_submit_args[@]}" \
  "${examples_jar}" \
  "$@"

# Example (matches the old 1.6-style positional args):
#   ./submit-groupbytest.sh 10 800000 1024 10
#
# Override resources:
#   DRIVER_MEMORY=32G EXECUTOR_MEMORY=32G EXECUTOR_CORES=8 CORES_MAX=8 ./submit-groupbytest.sh 10 800000 1024 10
#
# Override jar/class if needed:
#   EXAMPLES_JAR=/path/to/spark-examples.jar EXAMPLES_CLASS=org.apache.spark.examples.GroupByTest ./submit-groupbytest.sh 10 800000 1024 10
