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

python3 inaproc_pg_pipeline.py daily-listing \
  --max-pages 100 \
  --page-size 100 \
  --resume \
  --archive-dir archives/rup

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
