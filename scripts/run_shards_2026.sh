#!/usr/bin/env bash
set -u

MAX_PARALLEL="${MAX_PARALLEL:-20}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p archives/rup-sharded-2026 logs/shards-2026

running=0
for jenis in 1 2 3 4 5; do
  for sumber in Penyedia Swakelola; do
    for dana in APBN APBNP APBD APBDP PHLN PNBP BLUD GABUNGAN LAINNYA; do
      shard="y2026-j${jenis}-${sumber}-${dana}"
      python3 inaproc_pg_pipeline.py seed-listing-file \
        --output "archives/rup-sharded-2026/${shard}.jsonl" \
        --max-pages 999999 \
        --page-size 100 \
        --timeout 45 \
        --tahun 2026 \
        --jenis-klpd "${jenis}" \
        --sumber "${sumber}" \
        --sumber-dana "${dana}" \
        --truncate > "logs/shards-2026/${shard}.log" 2>&1 &

      running=$((running + 1))
      if [ "$running" -ge "$MAX_PARALLEL" ]; then
        wait -n || true
        running=$((running - 1))
      fi
    done
  done
done

wait
