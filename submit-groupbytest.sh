#!/usr/bin/env bash
set -euo pipefail

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load workspace Spark env (SPARK_HOME, SPARK_CONF_DIR, SPARK_LOG_DIR)
# shellcheck disable=SC1091
source "${_root_dir}/env.sh"

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
DRIVER_MEMORY="${DRIVER_MEMORY:-4G}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-4G}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
CORES_MAX="${CORES_MAX:-${EXECUTOR_CORES}}"
MEMORY_FRACTION="${MEMORY_FRACTION:-0.3}"

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

"${SPARK_HOME}/bin/spark-submit" \
  --class "${EXAMPLES_CLASS}" \
  --master "${master_url}" \
  --deploy-mode client \
  --driver-memory "${DRIVER_MEMORY}" \
  --executor-memory "${EXECUTOR_MEMORY}" \
  --executor-cores "${EXECUTOR_CORES}" \
  --conf "spark.cores.max=${CORES_MAX}" \
  --conf "spark.memory.fraction=${MEMORY_FRACTION}" \
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
