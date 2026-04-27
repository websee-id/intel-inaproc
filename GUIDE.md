# Production Run Guide

This guide describes the recommended PostgreSQL-only workflow:

1. seed all current RUP listing data once;
2. keep listing updates running every few hours;
3. run multiple detail workers continuously;
4. monitor and repair the queue when needed.

The scraper uses raw Streamlit WebSocket access for INAPROC listing/detail pages. PostgreSQL is the source of truth. Detail workers use `FOR UPDATE SKIP LOCKED`, so many worker processes can run in parallel without claiming the same job.

## 1. Server Setup

```bash
git clone https://github.com/websee-id/intel-inaproc.git
cd intel-inaproc

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 -m playwright install chromium
```

Set PostgreSQL through the environment. Do not commit credentials.

```bash
export DATABASE_URL='postgres://USER:PASSWORD@HOST:PORT/DB?sslmode=require'
```

Initialize schema once:

```bash
python3 inaproc_pg_pipeline.py init-db
```

## 2. Initial Full Seed

Run this once to seed the main listing table and fill the detail queue.

```bash
python3 inaproc_pg_pipeline.py daily-listing \
  --max-pages 42244 \
  --page-size 100 \
  --archive-dir archives/rup-full-seed \
  --rate-delay 0.2 \
  --timeout 20
```

Notes:

- Current observed size is about `42,244` pages at `100` rows/page, around `4.22M` listing rows.
- `--archive-dir` writes JSONL page archives for audit/replay.
- `--rate-delay 0.2` is a conservative delay between processed pages.
- If the full seed stops, resume from the checkpoint:

```bash
python3 inaproc_pg_pipeline.py daily-listing \
  --max-pages 42244 \
  --page-size 100 \
  --resume \
  --archive-dir archives/rup-full-seed \
  --rate-delay 0.2 \
  --timeout 20
```

The listing scraper still navigates Streamlit pages sequentially. For this public WebSocket path, large page jumps are not random access.

## 3. Continuous Listing Updates

After the initial full seed, run smaller update sweeps every few hours.

Recommended every 3-6 hours:

```bash
python3 inaproc_pg_pipeline.py daily-listing \
  --max-pages 300 \
  --page-size 100 \
  --resume \
  --archive-dir archives/rup-incremental \
  --rate-delay 0.2 \
  --timeout 20
```

If you want each scheduled run to always start from page 1 instead of continuing the checkpoint, omit `--resume`:

```bash
python3 inaproc_pg_pipeline.py daily-listing \
  --max-pages 300 \
  --page-size 100 \
  --archive-dir archives/rup-incremental \
  --rate-delay 0.2 \
  --timeout 20
```

Practical recommendation:

- Full seed: use `--resume`.
- Recurring freshness scan: start from page 1 unless you specifically want a rolling sweep.
- Run a deeper rolling sweep daily or weekly if you need stronger backfill guarantees.

## 4. Continuous Detail Workers

Run multiple worker processes. Each process can also run multiple concurrent WebSocket detail fetches.

```bash
python3 inaproc_pg_pipeline.py detail-worker \
  --worker-id worker-01 \
  --limit 1000 \
  --concurrency 5 \
  --timeout 20

python3 inaproc_pg_pipeline.py detail-worker \
  --worker-id worker-02 \
  --limit 1000 \
  --concurrency 5 \
  --timeout 20
```

Start conservative:

- 2-4 worker processes;
- `--concurrency 5` each;
- increase only after observing error rate and database load.

For a large full-detail backfill, repeat workers continuously using systemd, supervisor, Docker, or a process manager. Each worker exits after processing up to `--limit` jobs, so the process manager should restart it.

## 5. Monitoring

Check queue state:

```bash
python3 inaproc_pg_pipeline.py report-queue --sample-limit 20
```

Important statuses:

- `pending`: waiting to be processed;
- `processing`: claimed by a worker;
- `done`: detail fetched successfully;
- `failed`: detail failed and can be retried.

Check table counts directly:

```sql
select count(*) from rup_listing;
select status, count(*) from rup_detail_queue group by status order by status;
select count(*) from rup_detail;
select status, count(*) from scrape_runs group by status order by status;
```

## 6. Retry And Recovery

Requeue failed jobs that have not exceeded the attempt limit:

```bash
python3 inaproc_pg_pipeline.py requeue-failed --max-attempts 5
```

Reset jobs stuck in `processing` after a worker crash:

```bash
python3 inaproc_pg_pipeline.py reset-stale-processing \
  --older-than 2026-04-27T00:00:00+00:00
```

Use a timestamp older than the maximum expected worker runtime.

## 7. Cron Example

Example crontab:

```cron
DATABASE_URL=postgres://USER:PASSWORD@HOST:PORT/DB?sslmode=require
APP_DIR=/opt/intel-inaproc

0 */4 * * * cd $APP_DIR && . .venv/bin/activate && python3 inaproc_pg_pipeline.py daily-listing --max-pages 300 --page-size 100 --archive-dir archives/rup-incremental --rate-delay 0.2 --timeout 20 >> logs/listing.log 2>&1
*/30 * * * * cd $APP_DIR && . .venv/bin/activate && python3 inaproc_pg_pipeline.py requeue-failed --max-attempts 5 >> logs/requeue.log 2>&1
*/15 * * * * cd $APP_DIR && . .venv/bin/activate && python3 inaproc_pg_pipeline.py report-queue --sample-limit 5 >> logs/queue.log 2>&1
```

For workers, prefer systemd or supervisor over cron.

## 8. systemd Worker Example

`/etc/systemd/system/inaproc-worker@.service`:

```ini
[Unit]
Description=INAPROC detail worker %i
After=network-online.target

[Service]
WorkingDirectory=/opt/intel-inaproc
Environment=DATABASE_URL=postgres://USER:PASSWORD@HOST:PORT/DB?sslmode=require
ExecStart=/opt/intel-inaproc/.venv/bin/python /opt/intel-inaproc/inaproc_pg_pipeline.py detail-worker --worker-id worker-%i --limit 1000 --concurrency 5 --timeout 20
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable several workers:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now inaproc-worker@01
sudo systemctl enable --now inaproc-worker@02
sudo systemctl enable --now inaproc-worker@03
sudo systemctl enable --now inaproc-worker@04
```

## 9. Health Checks

WebSocket probe:

```bash
python3 inaproc_pipeline.py ws-probe --page-size 100 --timeout 20
```

Browser fallback probe:

```bash
python3 inaproc_pipeline.py browser-probe
```

If `ws-probe` fails but `browser-probe` works, Streamlit widget IDs or page hash may have changed and the WebSocket scraper needs updating.

## 10. Recommended Operating Pattern

Initial launch:

1. `init-db`;
2. full seed listing;
3. start 2-4 detail workers;
4. monitor queue and error rate;
5. increase workers gradually.

Ongoing:

1. listing scan every 3-6 hours;
2. detail workers always running;
3. requeue failed jobs every 30-60 minutes;
4. reset stale processing jobs if workers crash;
5. run `ws-probe` daily or before large jobs.
