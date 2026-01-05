#!/usr/bin/env bash
set -euo pipefail

_this_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_root_dir="$(cd "${_this_dir}/.." && pwd)"

die() {
  echo "ERROR: $*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  ./tpcds/gen-queries.sh

Generates TPC-DS query .sql files from the vendored `tpcds-kit` templates using `dsqgen`.

Requires:
  - `tpcds-kit` submodule checked out
  - `dsqgen` built (see `tpcds-kit/README.md`)

Env:
  TPCDS_KIT_DIR=./tpcds-kit
  TPCDS_DIALECT=spark
  TPCDS_SCALE=1
  TPCDS_QUERY_OUT_DIR=./tpcds/queries
  TPCDS_CLEAN=0|1   (default: 0) remove existing q*.sql first

Example:
  TPCDS_DIALECT=spark TPCDS_SCALE=1 ./tpcds/gen-queries.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown arg: $1"
      ;;
  esac
done

kit_dir="${TPCDS_KIT_DIR:-${_root_dir}/tpcds-kit}"
dsqgen="${kit_dir}/tools/dsqgen"
templates_dir="${kit_dir}/query_templates"
templates_lst="${templates_dir}/templates.lst"
distributions="${TPCDS_DISTRIBUTIONS:-${kit_dir}/tools/tpcds.idx}"

dialect="${TPCDS_DIALECT:-spark}"
scale="${TPCDS_SCALE:-1}"
out_dir="${TPCDS_QUERY_OUT_DIR:-${_this_dir}/queries}"
clean="${TPCDS_CLEAN:-0}"

relpath() {
  local from="$1"
  local to="$2"

  if command -v realpath >/dev/null 2>&1; then
    realpath --relative-to "${from}" "${to}"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$from" "$to" <<'PY'
import os
import sys

print(os.path.relpath(sys.argv[2], sys.argv[1]))
PY
    return 0
  fi

  die "Need 'realpath' or 'python3' to compute a relative path for the dialect template"
}

[[ -d "${kit_dir}" ]] || die "TPCDS_KIT_DIR not found: ${kit_dir} (did you init submodules?)"
[[ -x "${dsqgen}" ]] || die "dsqgen not found/executable: ${dsqgen} (build it: cd ${kit_dir}/tools && make OS=LINUX)"
[[ -f "${templates_lst}" ]] || die "templates.lst not found: ${templates_lst}"
[[ -f "${distributions}" ]] || die "tpcds.idx not found: ${distributions} (build tools to generate it)"
command -v python3 >/dev/null 2>&1 || die "python3 is required to split dsqgen output into per-query .sql files"

# If this repo provides a dialect wrapper (tpcds/dialects/*.tpl), prefer it.
# This fixes dsqgen runtime errors when the upstream dialect templates don't
# define the mandatory _BEGIN/_END substitutions.
dialect_tpl="${TPCDS_DIALECT_TPL:-${_this_dir}/dialects/${dialect}.tpl}"
if [[ -f "${dialect_tpl}" ]]; then
  dialect_rel="$(relpath "${templates_dir}" "${dialect_tpl}")"
  dialect="${dialect_rel%.tpl}"
fi

mkdir -p "${out_dir}"
if [[ "${clean}" == "1" ]]; then
  rm -f "${out_dir}"/q*.sql || true
fi

tmpdir="$(mktemp -d "${_root_dir}/tmp/tpcds-dsqgen.XXXXXX")"
trap 'rm -rf "${tmpdir}"' EXIT

echo "Generating TPC-DS queries via dsqgen"
echo "- Dialect : ${dialect}"
echo "- Scale   : ${scale}"
echo "- Out dir : ${out_dir}"

"${dsqgen}" \
  -DIRECTORY "${templates_dir}" \
  -INPUT "${templates_lst}" \
  -DISTRIBUTIONS "${distributions}" \
  -VERBOSE N \
  -QUALIFY Y \
  -SCALE "${scale}" \
  -DIALECT "${dialect}" \
  -OUTPUT_DIR "${tmpdir}"

shopt -s nullglob
stream_files=("${tmpdir}"/*.sql)
if [[ ${#stream_files[@]} -eq 0 ]]; then
  die "dsqgen produced no .sql files under ${tmpdir}"
fi
if [[ ${#stream_files[@]} -ne 1 ]]; then
  die "dsqgen produced ${#stream_files[@]} stream files; expected 1 (set STREAMS=1)"
fi

stream_file="${stream_files[0]}"

count="$(
  python3 - "${stream_file}" "${out_dir}" <<'PY'
import pathlib
import sys

stream_path = pathlib.Path(sys.argv[1])
out_dir = pathlib.Path(sys.argv[2])

text = stream_path.read_text(encoding="utf-8", errors="replace")
lines = text.splitlines(keepends=True)

blocks = []
current = None

def is_begin(line: str) -> bool:
    return line.lstrip().startswith("--BEGIN")

def is_end(line: str) -> bool:
    return line.lstrip().startswith("--END")

for line in lines:
    if current is None:
        if is_begin(line):
            current = [line]
        continue

    current.append(line)
    if is_end(line):
        blocks.append(current)
        current = None

if current is not None:
    raise SystemExit("ERROR: Unterminated query block (missing --END)")
if not blocks:
    raise SystemExit(
        "ERROR: No query blocks found. Use a dialect template that defines _BEGIN/_END "
        "markers (this repo's default TPCDS_DIALECT=spark does)."
    )

out_dir.mkdir(parents=True, exist_ok=True)

for i, block in enumerate(blocks, start=1):
    out_path = out_dir / f"q{i}.sql"
    out_path.write_text("".join(block).strip() + "\n", encoding="utf-8")

print(len(blocks))
PY
)"

echo "Generated ${count} query files under: ${out_dir}"
