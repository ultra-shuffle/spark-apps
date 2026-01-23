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
  ./run-hibench.sh [BENCHMARK] [FRAMEWORK] [--no-prepare|--only-prepare|--only-run]

Defaults:
  BENCHMARK = micro/sleep
  FRAMEWORK = spark

Required env:
  HADOOP_HOME=/path/to/hadoop

Optional env (common):
  # Filesystem mode for HiBench inputs/outputs:
  #   local (default): uses file:// and MapReduce LocalJobRunner (no HDFS daemon needed)
  #   hdfs           : uses hdfs://... and a running HDFS cluster
  HIBENCH_FS=local|hdfs

  # If HIBENCH_FS=local:
  HIBENCH_DATA_DIR=/absolute/path/for/hibench-data

  # If HIBENCH_FS=hdfs:
  HDFS_MASTER=hdfs://localhost:8020

  HIBENCH_SCALE_PROFILE=tiny|small|large|huge|gigantic|bigdata
  HIBENCH_SPARK_EXECUTOR_MEMORY=1g
  HIBENCH_SPARK_EXECUTOR_CORES=1
  # Standalone-only: total cores for the application (typically numExecutors * executorCores)
  HIBENCH_SPARK_CORES_MAX=4
  HIBENCH_SPARK_DRIVER_MEMORY=1g
  # Extra Spark properties appended verbatim to zzz-spark-apps.conf (optional).
  # Example:
  #   HIBENCH_SPARK_EXTRA_CONF=$'spark.sql.shuffle.partitions 1024\nspark.shuffle.compress false'
  HIBENCH_SPARK_EXTRA_CONF=

Examples:
  # Local FS mode (no HDFS):
  HADOOP_HOME=/opt/hadoop ./run-hibench.sh micro/wordcount spark

  # HDFS mode (requires HDFS cluster):
  HADOOP_HOME=/opt/hadoop HIBENCH_FS=hdfs HDFS_MASTER=hdfs://localhost:8020 ./run-hibench.sh micro/wordcount spark
EOF
}

benchmark="${BENCHMARK:-micro/sleep}"
framework="${FRAMEWORK:-spark}"
do_prepare="${PREPARE:-1}"
do_run="${RUN:-1}"
fs_mode="${HIBENCH_FS:-local}"

positional=()
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
    --hadoop-home)
      [[ $# -ge 2 ]] || die "--hadoop-home requires a value"
      export HADOOP_HOME="$2"
      shift 2
      ;;
    --hdfs-master)
      [[ $# -ge 2 ]] || die "--hdfs-master requires a value"
      export HDFS_MASTER="$2"
      shift 2
      ;;
    --fs)
      [[ $# -ge 2 ]] || die "--fs requires a value (local|hdfs)"
      fs_mode="$2"
      shift 2
      ;;
    --data-dir)
      [[ $# -ge 2 ]] || die "--data-dir requires a value"
      export HIBENCH_DATA_DIR="$2"
      shift 2
      ;;
    --spark-master)
      [[ $# -ge 2 ]] || die "--spark-master requires a value"
      export SPARK_MASTER="$2"
      shift 2
      ;;
    --scale)
      [[ $# -ge 2 ]] || die "--scale requires a value"
      export HIBENCH_SCALE_PROFILE="$2"
      shift 2
      ;;
    --executor-memory)
      [[ $# -ge 2 ]] || die "--executor-memory requires a value"
      export HIBENCH_SPARK_EXECUTOR_MEMORY="$2"
      shift 2
      ;;
    --executor-cores)
      [[ $# -ge 2 ]] || die "--executor-cores requires a value"
      export HIBENCH_SPARK_EXECUTOR_CORES="$2"
      shift 2
      ;;
    --cores-max)
      [[ $# -ge 2 ]] || die "--cores-max requires a value"
      export HIBENCH_SPARK_CORES_MAX="$2"
      shift 2
      ;;
    --driver-memory)
      [[ $# -ge 2 ]] || die "--driver-memory requires a value"
      export HIBENCH_SPARK_DRIVER_MEMORY="$2"
      shift 2
      ;;
    --)
      shift
      positional+=("$@")
      break
      ;;
    -*)
      die "Unknown option: $1"
      ;;
    *)
      positional+=("$1")
      shift
      ;;
  esac
done

if [[ ${#positional[@]} -ge 1 ]]; then
  benchmark="${positional[0]}"
fi
if [[ ${#positional[@]} -ge 2 ]]; then
  framework="${positional[1]}"
fi
if [[ ${#positional[@]} -ge 3 ]]; then
  die "Too many positional args: ${positional[*]}"
fi

benchmark="${benchmark//./\/}"

HIBENCH_HOME="${HIBENCH_HOME:-${_root_dir}/HiBench-7.1.1}"
[[ -d "${HIBENCH_HOME}" ]] || die "HIBENCH_HOME not found: ${HIBENCH_HOME}"

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

if [[ -z "${HADOOP_HOME:-}" && -x "/home/yxz/hadoop/bin/hadoop" ]]; then
  export HADOOP_HOME="/home/yxz/hadoop"
fi

[[ -n "${HADOOP_HOME:-}" ]] || die "HADOOP_HOME is not set (use --hadoop-home or export HADOOP_HOME)"
[[ -x "${HADOOP_HOME}/bin/hadoop" ]] || die "Hadoop executable not found: ${HADOOP_HOME}/bin/hadoop"

# Find Hadoop MapReduce examples JAR (contains TeraSort classes required by HiBench).
# Prefer the compatible version (Hadoop 3.3.4 for Spark 3.5) from lib/ if available.
hadoop_examples_jar="${HIBENCH_HADOOP_EXAMPLES_JAR:-}"
if [[ -z "${hadoop_examples_jar}" ]]; then
  # Try common locations - prefer compatible version in lib/ first
  for candidate in \
    "${_root_dir}/lib/hadoop-mapreduce-examples-3.3.4.jar" \
    "${_root_dir}/lib/hadoop-mapreduce-examples"*.jar \
    "${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples"*.jar \
    /home/yxz/hadoop/hadoop-dist/target/hadoop-*/share/hadoop/mapreduce/hadoop-mapreduce-examples*.jar; do
    if [[ -f "${candidate}" && "${candidate}" != *-sources.jar && "${candidate}" != *-javadoc.jar && "${candidate}" != *-test*.jar ]]; then
      hadoop_examples_jar="${candidate}"
      break
    fi
  done
fi

if ! command -v python3 >/dev/null 2>&1; then
  die "python3 is required by HiBench (HiBench-7.1.1/bin/functions/load_config.py)"
fi

master_host="${SPARK_MASTER_HOST:-localhost}"
master_port="${SPARK_MASTER_PORT:-17077}"
spark_master="${SPARK_MASTER:-spark://${master_host}:${master_port}}"

scale_profile="${HIBENCH_SCALE_PROFILE:-tiny}"
map_parallelism="${HIBENCH_MAP_PARALLELISM:-2}"
shuffle_parallelism="${HIBENCH_SHUFFLE_PARALLELISM:-2}"

executor_memory="${HIBENCH_SPARK_EXECUTOR_MEMORY:-1g}"
executor_cores="${HIBENCH_SPARK_EXECUTOR_CORES:-}"
driver_memory="${HIBENCH_SPARK_DRIVER_MEMORY:-1g}"
cores_max="${HIBENCH_SPARK_CORES_MAX:-}"
spark_extra_conf="${HIBENCH_SPARK_EXTRA_CONF:-}"

hibench_masters_hostnames="${HIBENCH_MASTERS_HOSTNAMES:-}"
hibench_slaves_hostnames="${HIBENCH_SLAVES_HOSTNAMES:-}"

case "${fs_mode}" in
  local|hdfs) ;;
  *)
    die "Unsupported HIBENCH_FS (or --fs): ${fs_mode} (expected: local|hdfs)"
    ;;
esac

if [[ "${fs_mode}" == "local" ]]; then
  hdfs_master="file:///"
  data_dir="${HIBENCH_DATA_DIR:-${_root_dir}/tmp/hibench-data}"
  if [[ "${data_dir}" != /* ]]; then
    die "HIBENCH_DATA_DIR must be an absolute path in local mode: ${data_dir}"
  fi

  data_dir_uri="file://${data_dir}"

  hadoop_conf_dir="${_root_dir}/tmp/hibench-hadoop-conf"
  mkdir -p "${hadoop_conf_dir}"

  cat >"${hadoop_conf_dir}/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>file:///</value>
  </property>
</configuration>
EOF

  cat >"${hadoop_conf_dir}/mapred-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>local</value>
  </property>
</configuration>
EOF
else
  hdfs_master="${HDFS_MASTER:-hdfs://localhost:8020}"
  data_dir_uri='${hibench.hdfs.master}/user/$USER/HiBench'
  hadoop_conf_dir="${HADOOP_CONF_DIR:-${HADOOP_HOME}/etc/hadoop}"
fi

hibench_hosts_conf=""
if [[ -n "${hibench_masters_hostnames// /}" || -n "${hibench_slaves_hostnames// /}" ]]; then
  if [[ -z "${hibench_masters_hostnames// /}" ]]; then
    # Best-effort default: derive master hostname from spark://host:port
    hibench_masters_hostnames="$(echo "${spark_master}" | sed -E 's#^spark://([^:/]+).*#\\1#')"
  fi
  if [[ -z "${hibench_slaves_hostnames// /}" ]]; then
    # If not provided, rely on HiBench probing or set to master only.
    hibench_slaves_hostnames="${hibench_masters_hostnames}"
  fi
  hibench_hosts_conf=$(
    cat <<EOF

# HiBench monitoring hosts (optional).
hibench.masters.hostnames      ${hibench_masters_hostnames}
hibench.slaves.hostnames       ${hibench_slaves_hostnames}
EOF
  )
elif [[ "${fs_mode}" == "local" ]]; then
  # Single-node convenience: avoid SSH prompts for monitoring.
  hibench_hosts_conf=$(
    cat <<'EOF'

# HiBench monitoring hosts (local FS mode).
hibench.masters.hostnames      localhost
hibench.slaves.hostnames       localhost
EOF
  )
fi

override_conf="${HIBENCH_HOME}/conf/zzz-spark-apps.conf"

spark_executor_cores_conf=""
if [[ -n "${executor_cores// /}" ]]; then
  spark_executor_cores_conf=$(
    cat <<EOF

spark.executor.cores          ${executor_cores}
EOF
  )
fi

spark_cores_max_conf=""
if [[ -n "${cores_max// /}" ]]; then
  spark_cores_max_conf=$(
    cat <<EOF

spark.cores.max               ${cores_max}
EOF
  )
fi

spark_extra_conf_block=""
if [[ -n "${spark_extra_conf// /}" ]]; then
  spark_extra_conf_block=$(
    cat <<EOF

# Extra Spark properties (from HIBENCH_SPARK_EXTRA_CONF).
${spark_extra_conf}
EOF
  )
fi

# Hadoop examples JAR (for TeraSort classes used by HiBench)
spark_jars_conf=""
if [[ -n "${hadoop_examples_jar// /}" && -f "${hadoop_examples_jar}" ]]; then
  spark_jars_conf=$(
    cat <<EOF

# Hadoop MapReduce examples JAR (TeraInputFormat, etc.)
spark.jars                    ${hadoop_examples_jar}
EOF
  )
  echo "Using Hadoop examples JAR: ${hadoop_examples_jar}"
fi

cat >"${override_conf}" <<EOF
# Auto-generated by ${_root_dir}/run-hibench.sh
# This file is loaded last (zzz-*) so it can override defaults in hibench.conf.

# Data scale profile.
hibench.scale.profile                ${scale_profile}
hibench.default.map.parallelism      ${map_parallelism}
hibench.default.shuffle.parallelism  ${shuffle_parallelism}

# Hadoop / HDFS
hibench.hadoop.home            ${HADOOP_HOME}
hibench.hadoop.executable      ${HADOOP_HOME}/bin/hadoop
hibench.hadoop.configure.dir   ${hadoop_conf_dir}
hibench.hdfs.master            ${hdfs_master}

# Root FS path for HiBench data.
hibench.hdfs.data.dir          ${data_dir_uri}

# Spark (standalone)
hibench.spark.home             \$SPARK_HOME
hibench.spark.master           ${spark_master}
${hibench_hosts_conf}

spark.executor.memory          ${executor_memory}
${spark_executor_cores_conf}
spark.driver.memory            ${driver_memory}
${spark_cores_max_conf}
${spark_jars_conf}
${spark_extra_conf_block}
EOF

prepare_script="${HIBENCH_HOME}/bin/workloads/${benchmark}/prepare/prepare.sh"
run_script="${HIBENCH_HOME}/bin/workloads/${benchmark}/${framework}/run.sh"

[[ -f "${prepare_script}" ]] || die "Prepare script not found: ${prepare_script}"
[[ -f "${run_script}" ]] || die "Run script not found: ${run_script}"

echo "HiBench:"
echo "- Home        : ${HIBENCH_HOME}"
echo "- Override conf: ${override_conf}"
echo "- Benchmark   : ${benchmark}"
echo "- Framework   : ${framework}"
echo "- Spark master: ${spark_master}"
echo "- FS mode     : ${fs_mode}"
echo "- FS master   : ${hdfs_master}"
echo "- Data dir    : ${data_dir_uri}"

if [[ "${do_prepare}" == "1" ]]; then
  echo "Running prepare: ${prepare_script}"
  "${prepare_script}"
fi

if [[ "${do_run}" == "1" ]]; then
  echo "Running workload: ${run_script}"
  "${run_script}"
fi
