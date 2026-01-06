#!/usr/bin/env bash
set -euo pipefail

# Submit GroupByTest in the standalone "multinode simulation" environment.
#
# This pins the Spark driver (and driver-side SCache daemon) to a loopback IP that has a
# ScacheClient listening (started by ./start-standalone-multinode.sh).
#
# Usage:
#   ./submit-groupbytest-mn.sh [numMappers] [numKVPairs] [valSize] [numReducers]
#
# Resource overrides (env):
#   DRIVER_MEMORY, EXECUTOR_MEMORY, EXECUTOR_CORES, NUM_EXECUTORS, CORES_MAX
# Driver bind override (env):
#   DRIVER_HOST (default: 127.0.0.2)

# This script is intended to be executed, not sourced.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "ERROR: Do not source this script. Run: ./submit-groupbytest-mn.sh" >&2
  return 1 2>/dev/null || exit 1
fi

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

state_file="${_root_dir}/run/standalone-multinode.state"
num_nodes=""
if [[ -f "${state_file}" ]]; then
  # shellcheck disable=SC1090
  source "${state_file}"
  num_nodes="${NUM_NODES:-}"
fi

driver_host="${DRIVER_HOST:-127.0.0.2}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-${driver_host}}"
export SCACHE_LOCAL_IP="${SCACHE_LOCAL_IP:-${driver_host}}"

DRIVER_MEMORY="${DRIVER_MEMORY:-16G}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-16G}"
EXECUTOR_CORES="${EXECUTOR_CORES:-8}"
NUM_EXECUTORS="${NUM_EXECUTORS:-${num_nodes:-4}}"
CORES_MAX="${CORES_MAX:-$((NUM_EXECUTORS * EXECUTOR_CORES))}"

export DRIVER_MEMORY EXECUTOR_MEMORY EXECUTOR_CORES NUM_EXECUTORS CORES_MAX

if [[ $# -eq 0 ]]; then
  # Defaults chosen to exercise shuffle without immediately OOM'ing in a 4x16G executor setup.
  set -- 32 200000 1024 32
fi

exec "${_root_dir}/submit-groupbytest.sh" "$@"

