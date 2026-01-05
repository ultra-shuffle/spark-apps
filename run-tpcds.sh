#!/usr/bin/env bash
set -euo pipefail

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  ./run-tpcds.sh [--no-prepare|--only-prepare|--only-run]

Required:
  - TPC-DS query .sql files under ./tpcds/queries (see ./tpcds/README.md)
  - A prepared TPC-DS dataset where each table is stored under:
      <TPCDS_BASE_URI>/<tableName>/
    in Parquet or ORC.

Common env:
  # Where Spark is:
  SPARK_HOME=/path/to/spark

  # Which Spark master to use (default: this repo's standalone master):
  SPARK_MASTER=spark://host:port

  # Dataset location (choose one):
  TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet
  # or (local):
  TPCDS_FS=local
  TPCDS_DATA_DIR=/absolute/path/to/tpcds/sf=1/parquet

  # Dataset format:
  TPCDS_FORMAT=parquet|orc   (default: parquet)

  # Query selection (optional):
  # Examples: "q1,q2,q3" or "1,2,3"
  TPCDS_QUERIES=

  # Spark resources (optional):
  TPCDS_DRIVER_MEMORY=4g
  TPCDS_EXECUTOR_MEMORY=8g
  TPCDS_EXECUTOR_CORES=4
  TPCDS_CORES_MAX=16

  # Extra Spark properties appended to spark-submit (repeatable):
  # Example: TPCDS_SPARK_CONF=("spark.sql.shuffle.partitions=1024" "spark.sql.adaptive.enabled=false")
  TPCDS_SPARK_CONF=()

Examples:
  # Run against local standalone (Spark+SCache simulation):
  TPCDS_BASE_URI=hdfs://namenode:8020/user/$USER/tpcds/sf=1/parquet \
  ./run-tpcds.sh

  # Run a subset of queries:
  TPCDS_QUERIES="1,2,3" ./run-tpcds.sh
EOF
}

do_prepare="${PREPARE:-1}"
do_run="${RUN:-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --no-prepare)
      do_prepare=0
      shift
      ;;
    --no-run)
      do_run=0
      shift
      ;;
    --only-prepare)
      do_prepare=1
      do_run=0
      shift
      ;;
    --only-run)
      do_prepare=0
      do_run=1
      shift
      ;;
    *)
      die "Unknown option: $1"
      ;;
  esac
done

# Load workspace Spark env (SPARK_HOME, SPARK_CONF_DIR, SPARK_LOG_DIR)
# shellcheck disable=SC1091
source "${_root_dir}/env.sh"

# Load standalone daemon settings (ports, work dirs, etc.)
if [[ -f "${SPARK_CONF_DIR}/spark-env.sh" ]]; then
  # shellcheck disable=SC1090
  source "${SPARK_CONF_DIR}/spark-env.sh"
fi

: "${SPARK_HOME:?SPARK_HOME is not set (check env.sh)}"
[[ -x "${SPARK_HOME}/bin/spark-submit" ]] || die "Spark executable not found: ${SPARK_HOME}/bin/spark-submit"

master_host="${SPARK_MASTER_HOST:-localhost}"
master_port="${SPARK_MASTER_PORT:-17077}"
spark_master="${SPARK_MASTER:-spark://${master_host}:${master_port}}"

fs_mode="${TPCDS_FS:-}"
format="${TPCDS_FORMAT:-parquet}"
queries="${TPCDS_QUERIES:-}"

case "${format}" in
  parquet|orc) ;;
  *) die "Unsupported TPCDS_FORMAT: ${format} (expected parquet|orc)" ;;
esac

base_uri="${TPCDS_BASE_URI:-}"
if [[ -z "${base_uri// /}" ]]; then
  if [[ "${fs_mode:-}" == "local" ]]; then
    data_dir="${TPCDS_DATA_DIR:-${_root_dir}/tmp/tpcds-data/sf=1/${format}}"
    [[ "${data_dir}" == /* ]] || die "TPCDS_DATA_DIR must be an absolute path in local mode: ${data_dir}"
    base_uri="file://${data_dir}"
  else
    die "Set TPCDS_BASE_URI=... (or set TPCDS_FS=local + TPCDS_DATA_DIR=...)"
  fi
fi

query_dir="${TPCDS_QUERY_DIR:-${_root_dir}/tpcds/queries}"
runner="${_root_dir}/tpcds/tpcds_runner.py"
[[ -f "${runner}" ]] || die "Runner script not found: ${runner}"

mkdir -p "${_root_dir}/logs/tpcds"
run_id="${TPCDS_RUN_ID:-$(date +%Y%m%d-%H%M%S)}"
out_dir="${TPCDS_OUT_DIR:-${_root_dir}/logs/tpcds/${run_id}}"

driver_memory="${TPCDS_DRIVER_MEMORY:-4g}"
executor_memory="${TPCDS_EXECUTOR_MEMORY:-8g}"
executor_cores="${TPCDS_EXECUTOR_CORES:-4}"
cores_max="${TPCDS_CORES_MAX:-}"

extra_confs=()
if [[ -n "${TPCDS_SPARK_CONF[*]-}" ]]; then
  # shellcheck disable=SC2206
  extra_confs=(${TPCDS_SPARK_CONF[@]})
fi

submit_args=(
  --master "${spark_master}"
  --deploy-mode client
  --driver-memory "${driver_memory}"
  --executor-memory "${executor_memory}"
  --executor-cores "${executor_cores}"
)

if [[ -n "${cores_max// /}" ]]; then
  submit_args+=(--conf "spark.cores.max=${cores_max}")
fi

for c in "${extra_confs[@]}"; do
  submit_args+=(--conf "${c}")
done

runner_args=(
  --base-uri "${base_uri}"
  --format "${format}"
  --query-dir "${query_dir}"
  --out-dir "${out_dir}"
)
if [[ -n "${queries// /}" ]]; then
  runner_args+=(--queries "${queries}")
fi

echo "TPC-DS:"
echo "- Spark master: ${spark_master}"
echo "- Base URI    : ${base_uri}"
echo "- Format      : ${format}"
echo "- Query dir   : ${query_dir}"
echo "- Out dir     : ${out_dir}"
if [[ -n "${queries// /}" ]]; then
  echo "- Queries     : ${queries}"
fi

if [[ "${do_prepare}" == "1" ]]; then
  echo "Preparing (registering temp views)..."
  "${SPARK_HOME}/bin/spark-submit" "${submit_args[@]}" "${runner}" "${runner_args[@]}" --mode prepare
fi

if [[ "${do_run}" == "1" ]]; then
  echo "Running queries..."
  "${SPARK_HOME}/bin/spark-submit" "${submit_args[@]}" "${runner}" "${runner_args[@]}" --mode run
fi
