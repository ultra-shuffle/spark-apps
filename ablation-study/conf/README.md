# Ablation-study configs

This folder contains **SCache config variants** intended to be used via:

`SCACHE_CONF_OVERRIDE_DIR=/path/to/variant ./start-standalone-multinode.sh`

Each variant is a directory containing `scache.conf` (and optionally `slaves`).

Notes:
- Spark-side knobs (e.g., `spark.scache.shuffle.noLocalFiles`) are controlled via `spark-submit`
  `--conf ...` in the ablation-study runner scripts (not via these files).
- Some variants (e.g., `no-partition-homes`) may require small optional patches in upstream repos;
  see `ablation-study/patches/`.

