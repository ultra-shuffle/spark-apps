#!/usr/bin/env bash

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Prefer an explicitly exported SPARK_HOME; otherwise use common locations.
if [[ -z "${SPARK_HOME:-}" ]]; then
	if [[ -d "/home/yxz/spark-3.5" ]]; then
		export SPARK_HOME="/home/yxz/spark-3.5"
	elif [[ -d "/mnt/spark" ]]; then
		export SPARK_HOME="/mnt/spark"
	else
		export SPARK_HOME="/home/yxz/spark-3.5"
	fi
fi

# Point Spark at this project's configuration directory.
export SPARK_CONF_DIR="${_root_dir}/conf"

# Store daemon logs under this project.
export SPARK_LOG_DIR="${_root_dir}/logs"