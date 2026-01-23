# spark-apps agent instructions

This repository is a **benchmarking harness** for a customized Apache Spark fork integrated with
**SCache** (shuffle over shared memory), used as a prototype towards a **CXL-compatible**
shuffle/runtime. Since physical CXL hardware is not currently available, experiments often use
**cross-NUMA memory** as a performance simulation and for functional verification.

## Repo intent / guardrails

- Keep changes **minimal, experimental, and reproducible**.
- Prefer **new code under `ablation-study/`** over modifying existing top-level scripts unless
  necessary for correctness.
- Avoid adding heavyweight dependencies; use **bash + Python (stdlib)** where possible.
- Do not introduce network dependencies (downloads, external services) into experiment runners.

## Experiment scripts conventions

- Bash scripts: use `#!/usr/bin/env bash` + `set -euo pipefail`; avoid being sourced.
- Use `env.sh` to discover `SPARK_HOME` and default `SPARK_CONF_DIR`.
- Write outputs (stdout/stderr, parsed metrics) under `ablation-study/results/` and keep logs
  grouped by variant/run for later plotting.
- Prefer passing Spark knobs via `spark-submit --conf ...` and SCache knobs via
  `SCACHE_CONF_OVERRIDE_DIR=...` (then restart standalone daemons when required).

