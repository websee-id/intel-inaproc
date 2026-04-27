# Intel INAPROC Scraper

High-throughput INAPROC RUP scraper using raw Streamlit WebSocket access, with a PostgreSQL pipeline for multi-worker detail scraping.

## Setup

```bash
python3 -m pip install -r requirements.txt
python3 -m playwright install chromium
export DATABASE_URL='postgres://...'
```

Keep `DATABASE_URL` in the environment. Do not commit database credentials.

## PostgreSQL Pipeline

```bash
python3 inaproc_pg_pipeline.py init-db

python3 inaproc_pg_pipeline.py seed-listing-file \
  --output archives/rup-full/listing.jsonl \
  --max-pages 42244 \
  --page-size 100 \
  --truncate

python3 inaproc_pg_pipeline.py prepare-listing-copy \
  --input archives/rup-full/listing.jsonl \
  --output archives/rup-full/listing-copy.csv

python3 inaproc_pg_pipeline.py bulk-load-listing \
  --input archives/rup-full/listing-copy.csv

python3 inaproc_pg_pipeline.py daily-listing \
  --max-pages 300 \
  --page-size 100 \
  --archive-dir archives/rup-incremental

python3 inaproc_pg_pipeline.py detail-worker \
  --worker-id worker-01 \
  --limit 1000 \
  --concurrency 5

python3 inaproc_pg_pipeline.py report-queue
```

Run multiple `detail-worker` processes with different `--worker-id` values. PostgreSQL uses `FOR UPDATE SKIP LOCKED` so workers do not claim the same queue rows.

## Diagnostics

```bash
python3 inaproc_pipeline.py ws-probe --page-size 100
python3 inaproc_pipeline.py browser-probe
```

The browser probe is a fallback diagnostic only. The scraper path uses the raw WebSocket.

## Tests

```bash
python3 -m py_compile inaproc_ws_scraper.py inaproc_pipeline.py inaproc_pg_pipeline.py
python3 -m unittest tests/test_inaproc_ws_scraper.py tests/test_inaproc_pipeline.py tests/test_inaproc_pg_pipeline.py
```
