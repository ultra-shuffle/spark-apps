#!/usr/bin/env bash
set -euo pipefail

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load workspace Spark env (SPARK_HOME, SPARK_CONF_DIR, SPARK_LOG_DIR)
# shellcheck disable=SC1091
source "${_root_dir}/env.sh"

# Load standalone daemon settings (ports, work dirs, etc.)
if [[ -f "${SPARK_CONF_DIR}/spark-env.sh" ]]; then
  # shellcheck disable=SC1090
  source "${SPARK_CONF_DIR}/spark-env.sh"
fi

: "${SPARK_HOME:?SPARK_HOME is not set (check env.sh)}"

mkdir -p "${SPARK_LOG_DIR:-${_root_dir}/logs}" \
         "${_root_dir}/run" \
         "${_root_dir}/work" \
         "${_root_dir}/tmp/spark-local" \
         "${_root_dir}/logs/spark-events"

# Optional: start SCache (local). This calls the scripts under SCACHE_HOME/sbin.
# Enable by exporting ENABLE_SCACHE=1.
ENABLE_SCACHE="${ENABLE_SCACHE:-1}"
SCACHE_HOME="${SCACHE_HOME:-/home/yxz/SCache}"
_scache_marker="${_root_dir}/run/scache.started"

if [[ "${ENABLE_SCACHE}" == "1" ]]; then
  if [[ -x "${SCACHE_HOME}/sbin/start-scache.sh" ]]; then
    if [[ -f "${_scache_marker}" ]]; then
      echo "SCache marker exists; assuming already started: ${_scache_marker}"
    else
      echo "Starting SCache via ${SCACHE_HOME}/sbin/start-scache.sh"
      "${SCACHE_HOME}/sbin/start-scache.sh"
      touch "${_scache_marker}"
    fi
  else
    echo "ERROR: ENABLE_SCACHE=1 but missing executable: ${SCACHE_HOME}/sbin/start-scache.sh" >&2
    exit 1
  fi
fi

master_host="${SPARK_MASTER_HOST:-localhost}"
master_port="${SPARK_MASTER_PORT:-17077}"
master_url="spark://${master_host}:${master_port}"

"${SPARK_HOME}/sbin/start-master.sh"

# Start a single local worker (no SSH) so it inherits this environment.
"${SPARK_HOME}/sbin/start-worker.sh" --webui-port "${SPARK_WORKER_WEBUI_PORT:-18081}" "${master_url}"

echo "Started Spark standalone"
echo "- Master URL: ${master_url}"
echo "- Master UI : http://${master_host}:${SPARK_MASTER_WEBUI_PORT:-18080}"
echo "- Worker UI : http://${master_host}:${SPARK_WORKER_WEBUI_PORT:-18081}"

if [[ "${ENABLE_SCACHE}" == "1" ]]; then
  echo "- SCache     : started (marker ${_scache_marker})"
fi
