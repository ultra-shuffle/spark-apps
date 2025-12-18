#!/usr/bin/env bash
set -euo pipefail

# This script is intended to be executed, not sourced.
# Sourcing would pollute the current shell with exported vars and make later runs
# appear to "inherit" values.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "ERROR: Do not source this script. Run: ./start-standalone.sh" >&2
  return 1 2>/dev/null || exit 1
fi

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load workspace Spark env (SPARK_HOME, SPARK_CONF_DIR, SPARK_LOG_DIR)
# shellcheck disable=SC1091
source "${_root_dir}/env.sh"

# If a previous *sourced* run left Spark vars in the parent shell, clear them so
# this run picks up the current defaults from conf/spark-env.sh.
if [[ "${SPARK_APPS_ENV_MARKER:-}" == "1" ]]; then
  for _v in ${SPARK_APPS_ENV_VARS:-""}; do
    unset "${_v}" || true
  done
  unset SPARK_APPS_ENV_MARKER SPARK_APPS_ENV_VARS || true
fi

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

# Ensure History Server UI port doesn't collide with master UI port.
# Respect an explicitly set SPARK_HISTORY_OPTS; otherwise set a sane default.
if [[ -z "${SPARK_HISTORY_OPTS:-}" ]]; then
  history_ui_port="${SPARK_HISTORY_UI_PORT:-18082}"
  export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=${history_ui_port}"
else
  history_ui_port="${SPARK_HISTORY_UI_PORT:-18082}"
fi

"${SPARK_HOME}/sbin/start-master.sh"

# Start a single local worker (no SSH) so it inherits this environment.
"${SPARK_HOME}/sbin/start-worker.sh" --webui-port "${SPARK_WORKER_WEBUI_PORT:-18081}" "${master_url}"

# Start History Server (reads spark.history.fs.logDirectory from conf/spark-defaults.conf).
"${SPARK_HOME}/sbin/start-history-server.sh"

echo "Started Spark standalone"
echo "- Master URL: ${master_url}"
echo "- Master UI : http://${master_host}:${SPARK_MASTER_WEBUI_PORT:-18080}"
echo "- Worker UI : http://${master_host}:${SPARK_WORKER_WEBUI_PORT:-18081}"
echo "- History UI: http://${master_host}:${history_ui_port}"

if [[ "${ENABLE_SCACHE}" == "1" ]]; then
  echo "- SCache     : started (marker ${_scache_marker})"
fi
