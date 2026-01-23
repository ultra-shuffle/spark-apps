#!/usr/bin/env bash
set -euo pipefail

# Pack spark-apps, Spark, SCache, and dependencies for deployment to a remote machine.
#
# Usage:
#   ./pack-for-remote.sh [output_dir]
#   ./pack-for-remote.sh --deploy user@host [remote_base_dir]
#
# Examples:
#   # Create tarball in current directory
#   ./pack-for-remote.sh
#
#   # Create tarball in specific directory
#   ./pack-for-remote.sh /tmp/deploy
#
#   # Pack and deploy to remote host
#   ./pack-for-remote.sh --deploy yxz@10.0.16.103
#   ./pack-for-remote.sh --deploy yxz@10.0.16.103 /home/yxz

_root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck disable=SC1091
source "${_root_dir}/env.sh"

SPARK_HOME="${SPARK_HOME:-/home/yxz/spark-3.5}"
SCACHE_HOME="${SCACHE_HOME:-/home/yxz/SCache}"
HIBENCH_HOME="${HIBENCH_HOME:-${_root_dir}/HiBench-7.1.1}"

# Output settings
PACK_NAME="spark-hibench-deploy"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
TARBALL_NAME="${PACK_NAME}-${TIMESTAMP}.tar.gz"

deploy_mode=0
remote_host=""
remote_base="/home/yxz"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deploy)
      deploy_mode=1
      shift
      if [[ $# -ge 1 && "$1" != -* ]]; then
        remote_host="$1"
        shift
      fi
      if [[ $# -ge 1 && "$1" != -* ]]; then
        remote_base="$1"
        shift
      fi
      ;;
    -h|--help)
      head -20 "$0" | tail -18
      exit 0
      ;;
    *)
      output_dir="$1"
      shift
      ;;
  esac
done

output_dir="${output_dir:-${_root_dir}}"
mkdir -p "${output_dir}"

staging_dir="${output_dir}/.pack-staging-$$"
trap 'rm -rf "${staging_dir}"' EXIT
mkdir -p "${staging_dir}"

echo "=============================================="
echo "Packing for Remote Deployment"
echo "=============================================="
echo "Spark home   : ${SPARK_HOME}"
echo "SCache home  : ${SCACHE_HOME}"
echo "HiBench home : ${HIBENCH_HOME}"
echo "Output dir   : ${output_dir}"
echo "=============================================="

# --- 1. spark-apps (scripts, conf, lib) ---
echo "Packing spark-apps..."
mkdir -p "${staging_dir}/spark-apps"

# Copy scripts
for f in "${_root_dir}"/*.sh "${_root_dir}"/*.scala "${_root_dir}"/*.py; do
  [[ -f "$f" ]] && cp "$f" "${staging_dir}/spark-apps/"
done

# Copy conf
cp -r "${_root_dir}/conf" "${staging_dir}/spark-apps/"

# Copy lib (hadoop examples jar)
if [[ -d "${_root_dir}/lib" ]]; then
  cp -r "${_root_dir}/lib" "${staging_dir}/spark-apps/"
fi

# --- 2. HiBench (only essential parts) ---
echo "Packing HiBench..."
mkdir -p "${staging_dir}/spark-apps/HiBench-7.1.1"

# Copy HiBench bin, conf, and essential workload scripts
cp -r "${HIBENCH_HOME}/bin" "${staging_dir}/spark-apps/HiBench-7.1.1/"
cp -r "${HIBENCH_HOME}/conf" "${staging_dir}/spark-apps/HiBench-7.1.1/"

# Copy sparkbench assembly JAR (the built artifact)
mkdir -p "${staging_dir}/spark-apps/HiBench-7.1.1/sparkbench/assembly/target"
if [[ -f "${HIBENCH_HOME}/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar" ]]; then
  cp "${HIBENCH_HOME}/sparkbench/assembly/target/sparkbench-assembly-7.1.1-dist.jar" \
     "${staging_dir}/spark-apps/HiBench-7.1.1/sparkbench/assembly/target/"
else
  echo "WARNING: HiBench sparkbench assembly JAR not found!"
fi

# --- 3. Spark distribution (minimal: bin, jars, conf, sbin) ---
echo "Packing Spark..."
mkdir -p "${staging_dir}/spark-3.5"

for subdir in bin jars conf sbin python; do
  if [[ -d "${SPARK_HOME}/${subdir}" ]]; then
    cp -r "${SPARK_HOME}/${subdir}" "${staging_dir}/spark-3.5/"
  fi
done

# Copy essential files from root
for f in README.md LICENSE NOTICE RELEASE; do
  [[ -f "${SPARK_HOME}/${f}" ]] && cp "${SPARK_HOME}/${f}" "${staging_dir}/spark-3.5/" || true
done

# --- 4. SCache (if exists) ---
if [[ -d "${SCACHE_HOME}" ]]; then
  echo "Packing SCache..."
  mkdir -p "${staging_dir}/SCache"
  
  # Copy essential parts
  for subdir in bin sbin conf scripts lib target/scala-2.12; do
    src="${SCACHE_HOME}/${subdir}"
    if [[ -d "${src}" ]]; then
      mkdir -p "${staging_dir}/SCache/$(dirname "${subdir}")"
      cp -r "${src}" "${staging_dir}/SCache/${subdir}"
    fi
  done
  
  # Copy JAR files from target
  if [[ -d "${SCACHE_HOME}/target/scala-2.12" ]]; then
    mkdir -p "${staging_dir}/SCache/target/scala-2.12"
    cp "${SCACHE_HOME}/target/scala-2.12"/*.jar "${staging_dir}/SCache/target/scala-2.12/" 2>/dev/null || true
  fi

  # Copy native libraries if present
  if [[ -d "${SCACHE_HOME}/native" ]]; then
    cp -r "${SCACHE_HOME}/native" "${staging_dir}/SCache/"
  fi
fi

# --- 5. Create setup script for remote ---
cat > "${staging_dir}/setup-remote.sh" << 'SETUP_EOF'
#!/usr/bin/env bash
set -euo pipefail

# Run this on the remote machine after extracting the tarball.
# Usage: ./setup-remote.sh [base_dir]
#   base_dir defaults to $HOME

base_dir="${1:-$HOME}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Setting up in: ${base_dir}"

# If script_dir == base_dir, files are already in place (extracted directly to target)
if [[ "${script_dir}" == "${base_dir}" ]]; then
  echo "Files already in target directory, skipping move."
else
  # Move directories to base
  for dir in spark-apps spark-3.5 SCache; do
    if [[ -d "${script_dir}/${dir}" ]]; then
      target="${base_dir}/${dir}"
      if [[ -d "${target}" ]]; then
        echo "WARNING: ${target} already exists, backing up to ${target}.bak"
        mv "${target}" "${target}.bak.$(date +%s)"
      fi
      mv "${script_dir}/${dir}" "${target}"
      echo "Installed: ${target}"
    fi
  done
fi

# Create necessary directories
mkdir -p "${base_dir}/spark-apps/logs" \
         "${base_dir}/spark-apps/run" \
         "${base_dir}/spark-apps/work" \
         "${base_dir}/spark-apps/tmp"

# Update env.sh if needed
env_file="${base_dir}/spark-apps/env.sh"
if [[ -f "${env_file}" ]]; then
  # Update paths to use the new base directory
  sed -i "s|/home/yxz|${base_dir}|g" "${env_file}"
fi

echo ""
echo "=============================================="
echo "Setup complete!"
echo "=============================================="
echo ""
echo "Next steps:"
echo "  1. Ensure HADOOP_HOME is set or Hadoop is installed"
echo "  2. cd ${base_dir}/spark-apps"
echo "  3. Start Spark standalone cluster:"
echo "     ./start-standalone-multinode.sh"
echo "  4. Run HiBench benchmarks:"
echo "     ./run-hibench-mn.sh repartition spark"
echo ""
SETUP_EOF
chmod +x "${staging_dir}/setup-remote.sh"

# --- 6. Create tarball ---
echo "Creating tarball..."
tarball_path="${output_dir}/${TARBALL_NAME}"
(cd "${staging_dir}" && tar czf "${tarball_path}" .)

# Calculate size
tarball_size=$(du -h "${tarball_path}" | cut -f1)
echo ""
echo "=============================================="
echo "Tarball created: ${tarball_path}"
echo "Size: ${tarball_size}"
echo "=============================================="

# --- 7. Deploy if requested ---
if [[ "${deploy_mode}" == "1" ]]; then
  if [[ -z "${remote_host}" ]]; then
    echo "ERROR: --deploy requires a remote host (e.g., --deploy user@host)"
    exit 1
  fi
  
  echo ""
  echo "Deploying to ${remote_host}:${remote_base}..."
  
  # Copy tarball
  scp "${tarball_path}" "${remote_host}:/tmp/${TARBALL_NAME}"
  
  # Extract and setup on remote
  ssh "${remote_host}" bash -s "${remote_base}" "${TARBALL_NAME}" << 'REMOTE_EOF'
    base_dir="$1"
    tarball="$2"
    mkdir -p "${base_dir}"
    cd "${base_dir}"
    tar xzf "/tmp/${tarball}"
    ./setup-remote.sh "${base_dir}"
    rm -f "/tmp/${tarball}"
REMOTE_EOF
  
  echo ""
  echo "Deployment complete!"
  echo "SSH to ${remote_host} and run:"
  echo "  cd ${remote_base}/spark-apps && ./run-hibench-mn.sh repartition spark"
fi

echo ""
echo "To manually deploy:"
echo "  scp ${tarball_path} user@remote:/tmp/"
echo "  ssh user@remote 'cd /home/yxz && tar xzf /tmp/${TARBALL_NAME} && ./setup-remote.sh'"
