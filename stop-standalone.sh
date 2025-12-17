#!/usr/bin/env bash
set -euo pipefail

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load workspace Spark env (SPARK_HOME, SPARK_CONF_DIR, SPARK_LOG_DIR)
# shellcheck disable=SC1091
source "${_root_dir}/env.sh"

# Load standalone daemon settings (SPARK_PID_DIR, ports, etc.)
if [[ -f "${SPARK_CONF_DIR}/spark-env.sh" ]]; then
  # shellcheck disable=SC1090
  source "${SPARK_CONF_DIR}/spark-env.sh"
fi

: "${SPARK_HOME:?SPARK_HOME is not set (check env.sh)}"

# Optional: stop SCache if this workspace started it.
SCACHE_HOME="${SCACHE_HOME:-/home/yxz/SCache}"
_scache_marker="${_root_dir}/run/scache.started"

# Stop local worker/master using pid files in SPARK_PID_DIR (set in conf/spark-env.sh).
# This avoids stopping other users' Spark daemons.

"${SPARK_HOME}/sbin/stop-worker.sh" >/dev/null 2>&1 || true
"${SPARK_HOME}/sbin/stop-master.sh" >/dev/null 2>&1 || true

if [[ -f "${_scache_marker}" ]]; then
  if [[ -x "${SCACHE_HOME}/sbin/stop-scache.sh" ]]; then
    echo "Stopping SCache via ${SCACHE_HOME}/sbin/stop-scache.sh"
    "${SCACHE_HOME}/sbin/stop-scache.sh" || true
  else
    echo "WARNING: SCache marker exists but missing executable: ${SCACHE_HOME}/sbin/stop-scache.sh" >&2
  fi
  rm -f "${_scache_marker}" || true
fi

# Helpful status output
pid_dir="${SPARK_PID_DIR:-/tmp}"
if [[ -d "${pid_dir}" ]]; then
  if compgen -G "${pid_dir}/spark-*.pid" >/dev/null; then
    echo "Remaining Spark pid files in ${pid_dir}:"
    ls -1 "${pid_dir}"/spark-*.pid || true
  else
    echo "No Spark pid files found in ${pid_dir}."
  fi
fi

echo "Stopped Spark standalone (master/worker)."
