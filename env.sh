#!/usr/bin/env bash

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Prefer an explicitly exported SPARK_HOME; otherwise try a few common locations.
if [[ -z "${SPARK_HOME:-}" ]]; then
	for candidate in "${_root_dir}/../spark-"* "/mnt/spark"; do
		if [[ -d "${candidate}" && -x "${candidate}/bin/spark-submit" ]]; then
			export SPARK_HOME="${candidate}"
			break
		fi
	done
fi

: "${SPARK_HOME:?SPARK_HOME is not set; export SPARK_HOME=/path/to/spark}"

# Point Spark at this project's configuration directory by default.
# If the user already set SPARK_CONF_DIR (e.g. to compare against a vanilla Spark config),
# respect it.
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-${_root_dir}/conf}"

# Store daemon logs under this project by default.
export SPARK_LOG_DIR="${SPARK_LOG_DIR:-${_root_dir}/logs}"
