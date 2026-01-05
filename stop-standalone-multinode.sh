#!/usr/bin/env bash
set -euo pipefail

# This script is intended to be executed, not sourced.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "ERROR: Do not source this script. Run: ./stop-standalone-multinode.sh" >&2
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

# Load standalone daemon settings (SPARK_PID_DIR, ports, etc.)
if [[ -f "${SPARK_CONF_DIR}/spark-env.sh" ]]; then
  # shellcheck disable=SC1090
  source "${SPARK_CONF_DIR}/spark-env.sh"
fi

: "${SPARK_HOME:?SPARK_HOME is not set (check env.sh)}"

_state_file="${_root_dir}/run/standalone-multinode.state"
NUM_NODES="${NUM_NODES:-4}"
SPARK_IDENT_STRING="${SPARK_IDENT_STRING:-spark-apps-mn}"

if [[ -f "${_state_file}" ]]; then
  # shellcheck disable=SC1090
  source "${_state_file}" || true
fi

export SPARK_IDENT_STRING
export SPARK_WORKER_INSTANCES="${NUM_NODES}"

"${SPARK_HOME}/sbin/stop-worker.sh" >/dev/null 2>&1 || true
"${SPARK_HOME}/sbin/stop-master.sh" >/dev/null 2>&1 || true
"${SPARK_HOME}/sbin/stop-history-server.sh" >/dev/null 2>&1 || true

# Stop SCache multinode daemons if present.
ENABLE_SCACHE="${ENABLE_SCACHE:-1}"
SCACHE_HOME="${SCACHE_HOME:-/home/yxz/SCache}"
_scache_pid_root="${_root_dir}/run/scache-multinode"
_scache_log_root="${_root_dir}/logs/scache-multinode"

if [[ "${ENABLE_SCACHE}" == "1" && -x "${SCACHE_HOME}/sbin/stop-client.sh" && -x "${SCACHE_HOME}/sbin/stop-master.sh" ]]; then
  i=1
  while [[ "${i}" -le "${NUM_NODES}" ]]; do
    if [[ -d "${_scache_pid_root}/node-${i}" ]]; then
      SCACHE_PID_DIR="${_scache_pid_root}/node-${i}" \
      SCACHE_LOG_DIR="${_scache_log_root}/node-${i}" \
      "${SCACHE_HOME}/sbin/stop-client.sh" || true
    fi
    i=$((i + 1))
  done
  if [[ -d "${_scache_pid_root}/master" ]]; then
    SCACHE_PID_DIR="${_scache_pid_root}/master" \
    SCACHE_LOG_DIR="${_scache_log_root}/master" \
    "${SCACHE_HOME}/sbin/stop-master.sh" || true
  fi
fi

echo "Stopped Spark standalone (multinode simulation) (master/workers/history)."
echo "- SPARK_IDENT_STRING=${SPARK_IDENT_STRING}"
echo "- NUM_NODES=${NUM_NODES}"
