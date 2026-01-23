#!/usr/bin/env bash
set -euo pipefail

# Run HiBench benchmarks in the standalone "multinode simulation" environment.
#
# This pins the Spark driver (and driver-side SCache daemon) to a loopback IP that has a
# ScacheClient listening (started by ./start-standalone-multinode.sh).
#
# Usage:
#   ./run-hibench-mn.sh [BENCHMARK] [FRAMEWORK] [--no-prepare|--only-prepare|--only-run]
#
# Shortcuts for common HiBench workloads:
#   ./run-hibench-mn.sh repartition        # micro/repartition spark
#   ./run-hibench-mn.sh sort               # micro/sort spark
#   ./run-hibench-mn.sh terasort           # micro/terasort spark
#   ./run-hibench-mn.sh wordcount          # micro/wordcount spark
#
# Resource overrides (env):
#   DRIVER_MEMORY, EXECUTOR_MEMORY, EXECUTOR_CORES, NUM_EXECUTORS, CORES_MAX
# Driver bind override (env):
#   DRIVER_HOST (default: 127.0.0.2)
#
# All other options from run-hibench.sh are supported (--scale, --fs, etc.)

# This script is intended to be executed, not sourced.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "ERROR: Do not source this script. Run: ./run-hibench-mn.sh" >&2
  return 1 2>/dev/null || exit 1
fi

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Load state from multinode startup (NUM_NODES, etc.)
state_file="${_root_dir}/run/standalone-multinode.state"
num_nodes=""
if [[ -f "${state_file}" ]]; then
  # shellcheck disable=SC1090
  source "${state_file}"
  num_nodes="${NUM_NODES:-}"
fi

# Pin driver to a loopback IP used by multinode simulation.
driver_host="${DRIVER_HOST:-127.0.0.2}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-${driver_host}}"
export SCACHE_LOCAL_IP="${SCACHE_LOCAL_IP:-${driver_host}}"

# Resource defaults (override by exporting these vars before running the script).
DRIVER_MEMORY="${DRIVER_MEMORY:-16G}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-16G}"
EXECUTOR_CORES="${EXECUTOR_CORES:-8}"
NUM_EXECUTORS="${NUM_EXECUTORS:-${num_nodes:-4}}"
CORES_MAX="${CORES_MAX:-$((NUM_EXECUTORS * EXECUTOR_CORES))}"

# Export these so run-hibench.sh picks them up.
export HIBENCH_SPARK_DRIVER_MEMORY="${HIBENCH_SPARK_DRIVER_MEMORY:-${DRIVER_MEMORY}}"
export HIBENCH_SPARK_EXECUTOR_MEMORY="${HIBENCH_SPARK_EXECUTOR_MEMORY:-${EXECUTOR_MEMORY}}"
export HIBENCH_SPARK_EXECUTOR_CORES="${HIBENCH_SPARK_EXECUTOR_CORES:-${EXECUTOR_CORES}}"
export HIBENCH_SPARK_CORES_MAX="${HIBENCH_SPARK_CORES_MAX:-${CORES_MAX}}"

# Convenience: expand short benchmark names to full paths.
_expand_benchmark() {
  local bench="$1"
  case "${bench}" in
    repartition)   echo "micro/repartition" ;;
    sort)          echo "micro/sort" ;;
    terasort)      echo "micro/terasort" ;;
    wordcount)     echo "micro/wordcount" ;;
    dfsioe)        echo "micro/dfsioe" ;;
    sleep)         echo "micro/sleep" ;;
    # Pass through anything else as-is.
    *)             echo "${bench}" ;;
  esac
}

# Parse arguments: look for the benchmark arg (first positional) to expand.
args=()
benchmark_set=0
for arg in "$@"; do
  if [[ "${arg}" != -* && "${benchmark_set}" == "0" ]]; then
    # First positional arg is the benchmark name.
    args+=("$(_expand_benchmark "${arg}")")
    benchmark_set=1
  else
    args+=("${arg}")
  fi
done

# If no benchmark specified, default to repartition for multinode testing.
if [[ "${benchmark_set}" == "0" ]]; then
  args=("micro/repartition" "spark" "${args[@]}")
fi

echo "=============================================="
echo "HiBench Multinode Mode"
echo "=============================================="
echo "Driver host      : ${driver_host}"
echo "SPARK_LOCAL_IP   : ${SPARK_LOCAL_IP}"
echo "SCACHE_LOCAL_IP  : ${SCACHE_LOCAL_IP}"
echo "Driver memory    : ${HIBENCH_SPARK_DRIVER_MEMORY}"
echo "Executor memory  : ${HIBENCH_SPARK_EXECUTOR_MEMORY}"
echo "Executor cores   : ${HIBENCH_SPARK_EXECUTOR_CORES}"
echo "Total cores      : ${HIBENCH_SPARK_CORES_MAX}"
echo "Num nodes        : ${NUM_EXECUTORS}"
echo "=============================================="

exec "${_root_dir}/run-hibench.sh" "${args[@]}"
