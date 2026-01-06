#!/usr/bin/env bash
set -euo pipefail

# This script is intended to be executed, not sourced.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "ERROR: Do not source this script. Run: ./start-standalone-multinode.sh" >&2
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

NUM_NODES="${NUM_NODES:-4}"
SPARK_IDENT_STRING="${SPARK_IDENT_STRING:-spark-apps-mn}"
export SPARK_IDENT_STRING

SPARK_SIM_HOST_BASE="${SPARK_SIM_HOST_BASE:-127.0.0.}"
SPARK_SIM_HOST_START="${SPARK_SIM_HOST_START:-2}"
SPARK_SIM_HOSTS="${SPARK_SIM_HOSTS:-}"

WORKER_PORT_BASE="${WORKER_PORT_BASE:-17078}"
WORKER_WEBUI_PORT_BASE="${WORKER_WEBUI_PORT_BASE:-18081}"

WORKER_CORES="${WORKER_CORES:-8}"
WORKER_MEMORY="${WORKER_MEMORY:-16g}"

master_host="${SPARK_MASTER_HOST:-localhost}"
master_port="${SPARK_MASTER_PORT:-17077}"
master_url="spark://${master_host}:${master_port}"

# Optional: bind each worker (and thus its executors) to specific NUMA nodes.
SPARK_WORKER_CPU_NODE="${SPARK_WORKER_CPU_NODE:-}"
SPARK_WORKER_MEM_NODE="${SPARK_WORKER_MEM_NODE:-${SPARK_WORKER_NUMA_NODE:-}}"
SPARK_WORKER_NUMACTL_OPTS="${SPARK_WORKER_NUMACTL_OPTS:-}"

_worker_cmd_prefix=()
if [[ -n "${SPARK_WORKER_NUMACTL_OPTS// /}" ]]; then
  command -v numactl >/dev/null 2>&1 || {
    echo "ERROR: SPARK_WORKER_NUMACTL_OPTS set but numactl is not installed" >&2
    exit 1
  }
  # shellcheck disable=SC2206
  _worker_cmd_prefix=(numactl ${SPARK_WORKER_NUMACTL_OPTS})
else
  _numa_args=()
  if [[ -n "${SPARK_WORKER_CPU_NODE// /}" ]]; then
    _numa_args+=(--cpunodebind="${SPARK_WORKER_CPU_NODE}")
  fi
  if [[ -n "${SPARK_WORKER_MEM_NODE// /}" ]]; then
    _numa_args+=(--membind="${SPARK_WORKER_MEM_NODE}")
  fi
  if [[ ${#_numa_args[@]} -gt 0 ]]; then
    command -v numactl >/dev/null 2>&1 || {
      echo "ERROR: SPARK_WORKER_CPU_NODE/SPARK_WORKER_MEM_NODE set but numactl is not installed" >&2
      exit 1
    }
    _worker_cmd_prefix=(numactl "${_numa_args[@]}")
  fi
fi

# Optional: start SCache (local multinode). Enable by exporting ENABLE_SCACHE=1.
ENABLE_SCACHE="${ENABLE_SCACHE:-1}"
SCACHE_HOME="${SCACHE_HOME:-/home/yxz/SCache}"
SCACHE_MASTER_HOST="${SCACHE_MASTER_HOST:-127.0.0.1}"
SCACHE_MASTER_PORT="${SCACHE_MASTER_PORT:-16388}"
SCACHE_CLIENT_PORT="${SCACHE_CLIENT_PORT:-15678}"

_scache_pid_root="${_root_dir}/run/scache-multinode"
_scache_log_root="${_root_dir}/logs/scache-multinode"
_scache_state_file="${_root_dir}/run/standalone-multinode.state"

{
  echo "SPARK_IDENT_STRING=${SPARK_IDENT_STRING}"
  echo "NUM_NODES=${NUM_NODES}"
  echo "ENABLE_SCACHE=${ENABLE_SCACHE}"
  echo "SCACHE_HOME=${SCACHE_HOME}"
} > "${_scache_state_file}"

sync_scache_conf() {
  local conf_dir="${SCACHE_CONF_OVERRIDE_DIR:-${_root_dir}/conf/scache-multinode}"
  local conf_src="${conf_dir}/scache.conf"
  local slaves_src="${conf_dir}/slaves"
  local conf_dst="${SCACHE_HOME}/conf/scache.conf"
  local slaves_dst="${SCACHE_HOME}/conf/slaves"

  if [[ ! -f "${conf_src}" ]]; then
    echo "ERROR: SCache conf override missing: ${conf_src}" >&2
    return 1
  fi

  mkdir -p "${SCACHE_HOME}/conf"

  if [[ -e "${conf_dst}" && ! -L "${conf_dst}" && ! -e "${conf_dst}.orig" ]]; then
    cp -f "${conf_dst}" "${conf_dst}.orig"
  fi
  ln -sf "${conf_src}" "${conf_dst}"
  echo "SCache conf override: ${conf_dst} -> ${conf_src}"

  if [[ -f "${slaves_src}" ]]; then
    if [[ -e "${slaves_dst}" && ! -L "${slaves_dst}" && ! -e "${slaves_dst}.orig" ]]; then
      cp -f "${slaves_dst}" "${slaves_dst}.orig"
    fi
    ln -sf "${slaves_src}" "${slaves_dst}"
    echo "SCache slaves override: ${slaves_dst} -> ${slaves_src}"
  fi
}

start_scache_multinode() {
  [[ "${ENABLE_SCACHE}" == "1" ]] || return 0

  if [[ ! -x "${SCACHE_HOME}/sbin/start-master.sh" || ! -x "${SCACHE_HOME}/sbin/start-client.sh" ]]; then
    echo "ERROR: ENABLE_SCACHE=1 but missing SCache scripts under ${SCACHE_HOME}/sbin" >&2
    exit 1
  fi

  sync_scache_conf

  mkdir -p "${_scache_pid_root}/master" "${_scache_log_root}/master"

  # Start ScacheMaster once.
  SCACHE_PID_DIR="${_scache_pid_root}/master" \
  SCACHE_LOG_DIR="${_scache_log_root}/master" \
  "${SCACHE_HOME}/sbin/start-master.sh"

  # Start one ScacheClient per simulated node (local processes bound to distinct loopback IPs).
  local i=1
  local host
  local hosts=()
  if [[ -n "${SPARK_SIM_HOSTS// /}" ]]; then
    # shellcheck disable=SC2206
    hosts=(${SPARK_SIM_HOSTS})
  else
    while [[ "${i}" -le "${NUM_NODES}" ]]; do
      host="${SPARK_SIM_HOST_BASE}$((SPARK_SIM_HOST_START + i - 1))"
      hosts+=("${host}")
      i=$((i + 1))
    done
  fi

  i=1
  for host in "${hosts[@]}"; do
    mkdir -p "${_scache_pid_root}/node-${i}" "${_scache_log_root}/node-${i}"
    SCACHE_LOCAL_IP="${host}" \
    SCACHE_PID_DIR="${_scache_pid_root}/node-${i}" \
    SCACHE_LOG_DIR="${_scache_log_root}/node-${i}" \
    "${SCACHE_HOME}/sbin/start-client.sh" \
      ${SCACHE_CLIENT_SCRIPT_OPTS:-} \
      -- \
      --ip "${host}" \
      --port "${SCACHE_CLIENT_PORT}" \
      --master "${SCACHE_MASTER_HOST}" \
      --masterPort "${SCACHE_MASTER_PORT}" &
    i=$((i + 1))
  done

  wait

  echo "SCache multinode started (${NUM_NODES} clients)"
  echo "- Master : ${SCACHE_MASTER_HOST}:${SCACHE_MASTER_PORT}"
  i=1
  for host in "${hosts[@]}"; do
    echo "- Client${i}: ${host}:${SCACHE_CLIENT_PORT} (pid dir ${_scache_pid_root}/node-${i})"
    i=$((i + 1))
  done
}

start_scache_multinode

# Ensure History Server UI port doesn't collide with master/worker UI ports.
history_ui_port="${SPARK_HISTORY_UI_PORT:-18082}"
master_ui_port="${SPARK_MASTER_WEBUI_PORT:-18080}"
worker_ui_min="${WORKER_WEBUI_PORT_BASE}"
worker_ui_max="$((WORKER_WEBUI_PORT_BASE + NUM_NODES - 1))"

if [[ "${history_ui_port}" -eq "${master_ui_port}" ]] || \
   ([[ "${history_ui_port}" -ge "${worker_ui_min}" ]] && [[ "${history_ui_port}" -le "${worker_ui_max}" ]]); then
  history_ui_port="$((worker_ui_max + 1))"
  if [[ "${history_ui_port}" -eq "${master_ui_port}" ]]; then
    history_ui_port="$((history_ui_port + 1))"
  fi
fi

if [[ -z "${SPARK_HISTORY_OPTS:-}" ]]; then
  export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=${history_ui_port}"
fi

"${SPARK_HOME}/sbin/start-master.sh"

# Start N local workers bound to distinct loopback IPs, so Spark treats them as N hosts.
CLASS="org.apache.spark.deploy.worker.Worker"

hosts=()
if [[ -n "${SPARK_SIM_HOSTS// /}" ]]; then
  # shellcheck disable=SC2206
  hosts=(${SPARK_SIM_HOSTS})
else
  i=1
  while [[ "${i}" -le "${NUM_NODES}" ]]; do
    hosts+=("${SPARK_SIM_HOST_BASE}$((SPARK_SIM_HOST_START + i - 1))")
    i=$((i + 1))
  done
fi

if [[ "${#hosts[@]}" -ne "${NUM_NODES}" ]]; then
  echo "ERROR: NUM_NODES=${NUM_NODES} but got ${#hosts[@]} hosts from SPARK_SIM_HOSTS/BASE+START" >&2
  exit 1
fi

i=1
for host in "${hosts[@]}"; do
  worker_port="$((WORKER_PORT_BASE + i - 1))"
  worker_ui_port="$((WORKER_WEBUI_PORT_BASE + i - 1))"
  worker_dir="${_root_dir}/work/worker-${i}"
  local_dirs="${_root_dir}/tmp/spark-local/worker-${i}"
  mkdir -p "${worker_dir}" "${local_dirs}"

  if [[ ${#_worker_cmd_prefix[@]} -gt 0 ]]; then
    echo "Starting worker ${i} on ${host} under: ${_worker_cmd_prefix[*]}"
  else
    echo "Starting worker ${i} on ${host}"
  fi

  SPARK_LOCAL_IP="${host}" \
  SCACHE_LOCAL_IP="${host}" \
  SPARK_WORKER_CORES="${WORKER_CORES}" \
  SPARK_WORKER_MEMORY="${WORKER_MEMORY}" \
  SPARK_WORKER_DIR="${worker_dir}" \
  SPARK_LOCAL_DIRS="${local_dirs}" \
  "${_worker_cmd_prefix[@]}" "${SPARK_HOME}/sbin/spark-daemon.sh" start "${CLASS}" "${i}" \
    --webui-port "${worker_ui_port}" \
    --port "${worker_port}" \
    "${master_url}"

  i=$((i + 1))
done

# Start History Server (reads spark.history.fs.logDirectory from conf/spark-defaults.conf).
"${SPARK_HOME}/sbin/start-history-server.sh"

echo "Started Spark standalone (multinode simulation)"
echo "- Master URL: ${master_url}"
echo "- Master UI : http://${master_host}:${SPARK_MASTER_WEBUI_PORT:-18080}"
echo "- History UI: http://${master_host}:${history_ui_port}"
echo "- Workers   : ${NUM_NODES} (SPARK_IDENT_STRING=${SPARK_IDENT_STRING})"
i=1
for host in "${hosts[@]}"; do
  echo "  - Worker${i}: ${host} (ui : http://${host}:$((WORKER_WEBUI_PORT_BASE + i - 1)))"
  i=$((i + 1))
done
