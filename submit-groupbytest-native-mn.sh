#!/usr/bin/env bash
set -euo pipefail

# Submit GroupByTest in the standalone "multinode simulation" environment with native Spark shuffle
# (SCache disabled).
#
# Usage:
#   ./submit-groupbytest-native-mn.sh [numMappers] [numKVPairs] [valSize] [numReducers]
#
# Resource overrides (env):
#   DRIVER_MEMORY, EXECUTOR_MEMORY, EXECUTOR_CORES, NUM_EXECUTORS, CORES_MAX

# This script is intended to be executed, not sourced.
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  echo "ERROR: Do not source this script. Run: ./submit-groupbytest-native-mn.sh" >&2
  return 1 2>/dev/null || exit 1
fi

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

_extra_args=()
if [[ -n "${SPARK_SUBMIT_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  _extra_args=(${SPARK_SUBMIT_EXTRA_ARGS})
fi

_extra_args+=(
  --conf spark.scache.enable=false
  --conf spark.scache.shuffle.noLocalFiles=false
  --conf spark.shuffle.useOldFetchProtocol=false
)

export SPARK_SUBMIT_EXTRA_ARGS="${_extra_args[*]}"

exec "${_root_dir}/submit-groupbytest-mn.sh" "$@"
