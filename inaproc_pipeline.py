#!/usr/bin/env python3
import argparse
import asyncio
import hashlib
import json
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from inaproc_ws_scraper import scrape_detail, scrape_listing_pages


IGNORED_HASH_FIELDS = {"page", "total_pages", "No."}


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def connect_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("pragma journal_mode = wal")
    conn.execute("pragma synchronous = normal")
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        create table if not exists rup_listing (
          kode_rup text primary key,
          nama_instansi text,
          nama_satuan_kerja text,
          tahun_anggaran text,
          cara_pengadaan text,
          metode_pengadaan text,
          jenis_pengadaan text,
          nama_paket text,
          sumber_dana text,
          produk_dalam_negeri text,
          total_nilai text,
          row_json text not null,
          row_hash text not null,
          first_seen_at text not null,
          last_seen_at text not null,
          last_changed_at text,
          source_page integer,
          scrape_run_id text
        );

        create table if not exists rup_detail_queue (
          kode_rup text primary key,
          sumber text,
          reason text not null,
          status text not null default 'pending',
          attempts integer not null default 0,
          last_error text,
          created_at text not null,
          updated_at text not null
        );

        create table if not exists rup_detail (
          kode_rup text primary key,
          status text not null,
          detail_json text,
          sumber_dana_json text,
          error text,
          fetched_at text not null
        );

        create table if not exists rup_listing_anomalies (
          id integer primary key autoincrement,
          reason text not null,
          payload_json text not null,
          scrape_run_id text,
          detected_at text not null
        );

        create table if not exists scrape_runs (
          run_id text primary key,
          job_name text not null,
          started_at text not null,
          finished_at text,
          status text not null,
          stats_json text
        );

        create table if not exists checkpoints (
          job_name text primary key,
          state_json text not null,
          updated_at text not null
        );

        create index if not exists idx_detail_queue_status on rup_detail_queue(status, updated_at);
        """
    )
    _ensure_column(conn, "rup_detail_queue", "sumber", "text")
    conn.commit()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    columns = {row["name"] for row in conn.execute(f"pragma table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"alter table {table} add column {column} {column_type}")


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def normalized_row(row: dict[str, Any]) -> dict[str, str]:
    return {key: _clean(value) for key, value in row.items()}


def row_hash(row: dict[str, Any]) -> str:
    normalized = {
        key: value
        for key, value in normalized_row(row).items()
        if key not in IGNORED_HASH_FIELDS
    }
    payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def listing_page_range(max_pages: int, start_page: int = 1, end_page: int | None = None) -> list[int]:
    if max_pages < 1:
        return []
    if start_page < 1:
        raise ValueError("start_page must be >= 1")
    natural_end = start_page + max_pages - 1
    final_page = min(natural_end, end_page) if end_page else natural_end
    if final_page < start_page:
        return []
    return list(range(start_page, final_page + 1))


def archive_listing_page(archive_dir: Path, run_id: str, page_result: dict[str, Any]) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    page = int(page_result.get("page") or 0)
    path = archive_dir / f"{run_id}-listing-page-{page:06d}.jsonl"
    lines = []
    if page_result.get("status") != "ok":
        payload = {"run_id": run_id, "archived_at": utc_now(), **page_result}
        lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    else:
        for row in page_result.get("rows", []):
            payload = {
                "run_id": run_id,
                "archived_at": utc_now(),
                "page": page_result.get("page"),
                "total_pages": page_result.get("total_pages"),
                **row,
            }
            lines.append(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return path


def _enqueue_detail(conn: sqlite3.Connection, kode_rup: str, sumber: str, reason: str, now: str) -> None:
    conn.execute(
        """
        insert into rup_detail_queue (kode_rup, sumber, reason, status, attempts, created_at, updated_at)
        values (?, ?, ?, 'pending', 0, ?, ?)
        on conflict(kode_rup) do update set
          sumber = excluded.sumber,
          reason = excluded.reason,
          status = 'pending',
          updated_at = excluded.updated_at
        """,
        (kode_rup, sumber, reason, now, now),
    )


def _record_anomaly(conn: sqlite3.Connection, row: dict[str, Any], reason: str, run_id: str, now: str) -> None:
    conn.execute(
        """
        insert into rup_listing_anomalies (reason, payload_json, scrape_run_id, detected_at)
        values (?, ?, ?, ?)
        """,
        (reason, json.dumps(row, ensure_ascii=False, sort_keys=True), run_id, now),
    )


def upsert_listing_row(conn: sqlite3.Connection, row: dict[str, Any], run_id: str) -> str:
    normalized = normalized_row(row)
    kode_rup = normalized.get("Kode RUP", "")
    now = utc_now()
    if not kode_rup:
        _record_anomaly(conn, normalized, "missing_kode_rup", run_id, now)
        conn.commit()
        return "anomaly"
    sumber = normalized.get("Cara Pengadaan", "")

    digest = row_hash(normalized)
    existing = conn.execute(
        "select row_hash from rup_listing where kode_rup = ?",
        (kode_rup,),
    ).fetchone()
    row_json = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    values = (
        kode_rup,
        normalized.get("Nama Instansi", ""),
        normalized.get("Nama Satuan Kerja", ""),
        normalized.get("Tahun Anggaran", ""),
        normalized.get("Cara Pengadaan", ""),
        normalized.get("Metode Pengadaan", ""),
        normalized.get("Jenis Pengadaan", ""),
        normalized.get("Nama Paket", ""),
        normalized.get("Sumber Dana", ""),
        normalized.get("Produk Dalam Negeri", ""),
        normalized.get("Total Nilai (Rp)", ""),
        row_json,
        digest,
        now,
        now,
        now if existing is None or existing["row_hash"] != digest else None,
        int(normalized.get("page") or 0),
        run_id,
    )
    if existing is None:
        conn.execute(
            """
            insert into rup_listing (
              kode_rup, nama_instansi, nama_satuan_kerja, tahun_anggaran,
              cara_pengadaan, metode_pengadaan, jenis_pengadaan, nama_paket,
              sumber_dana, produk_dalam_negeri, total_nilai, row_json, row_hash,
              first_seen_at, last_seen_at, last_changed_at, source_page, scrape_run_id
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            values,
        )
        _enqueue_detail(conn, kode_rup, sumber, "new", now)
        conn.commit()
        return "new"

    if existing["row_hash"] == digest:
        conn.execute(
            """
            update rup_listing
            set last_seen_at = ?, source_page = ?, scrape_run_id = ?
            where kode_rup = ?
            """,
            (now, int(normalized.get("page") or 0), run_id, kode_rup),
        )
        conn.commit()
        return "same"

    conn.execute(
        """
        update rup_listing
        set nama_instansi = ?, nama_satuan_kerja = ?, tahun_anggaran = ?,
            cara_pengadaan = ?, metode_pengadaan = ?, jenis_pengadaan = ?,
            nama_paket = ?, sumber_dana = ?, produk_dalam_negeri = ?,
            total_nilai = ?, row_json = ?, row_hash = ?, last_seen_at = ?,
            last_changed_at = ?, source_page = ?, scrape_run_id = ?
        where kode_rup = ?
        """,
        (
            normalized.get("Nama Instansi", ""),
            normalized.get("Nama Satuan Kerja", ""),
            normalized.get("Tahun Anggaran", ""),
            normalized.get("Cara Pengadaan", ""),
            normalized.get("Metode Pengadaan", ""),
            normalized.get("Jenis Pengadaan", ""),
            normalized.get("Nama Paket", ""),
            normalized.get("Sumber Dana", ""),
            normalized.get("Produk Dalam Negeri", ""),
            normalized.get("Total Nilai (Rp)", ""),
            row_json,
            digest,
            now,
            now,
            int(normalized.get("page") or 0),
            run_id,
            kode_rup,
        ),
    )
    _enqueue_detail(conn, kode_rup, sumber, "changed", now)
    conn.commit()
    return "changed"


def dequeue_detail_jobs(conn: sqlite3.Connection, limit: int) -> list[dict[str, str]]:
    rows = conn.execute(
        """
        select kode_rup, sumber from rup_detail_queue
        where status = 'pending'
        order by updated_at asc
        limit ?
        """,
        (limit,),
    ).fetchall()
    jobs = [{"kode_rup": row["kode_rup"], "sumber": row["sumber"] or ""} for row in rows]
    now = utc_now()
    conn.executemany(
        """
        update rup_detail_queue
        set status = 'processing', attempts = attempts + 1, updated_at = ?
        where kode_rup = ?
        """,
        [(now, job["kode_rup"]) for job in jobs],
    )
    conn.commit()
    return jobs


def mark_detail_result(conn: sqlite3.Connection, kode_rup: str, result: dict[str, Any]) -> None:
    now = utc_now()
    status = result.get("status", "ok")
    error = result.get("error")
    conn.execute(
        """
        insert into rup_detail (kode_rup, status, detail_json, sumber_dana_json, error, fetched_at)
        values (?, ?, ?, ?, ?, ?)
        on conflict(kode_rup) do update set
          status = excluded.status,
          detail_json = excluded.detail_json,
          sumber_dana_json = excluded.sumber_dana_json,
          error = excluded.error,
          fetched_at = excluded.fetched_at
        """,
        (
            kode_rup,
            status,
            json.dumps(result.get("detail", {}), ensure_ascii=False, sort_keys=True),
            json.dumps(result.get("sumber_dana", []), ensure_ascii=False, sort_keys=True),
            error,
            now,
        ),
    )
    conn.execute(
        """
        update rup_detail_queue
        set status = ?, last_error = ?, updated_at = ?
        where kode_rup = ?
        """,
        ("done" if status == "ok" else "failed", error, now, kode_rup),
    )
    conn.commit()


def set_checkpoint(conn: sqlite3.Connection, job_name: str, state: dict[str, Any]) -> None:
    conn.execute(
        """
        insert into checkpoints (job_name, state_json, updated_at)
        values (?, ?, ?)
        on conflict(job_name) do update set
          state_json = excluded.state_json,
          updated_at = excluded.updated_at
        """,
        (job_name, json.dumps(state, ensure_ascii=False, sort_keys=True), utc_now()),
    )
    conn.commit()


def get_checkpoint(conn: sqlite3.Connection, job_name: str) -> dict[str, Any] | None:
    row = conn.execute("select state_json from checkpoints where job_name = ?", (job_name,)).fetchone()
    return json.loads(row["state_json"]) if row else None


def requeue_failed_details(conn: sqlite3.Connection, max_attempts: int = 5) -> int:
    now = utc_now()
    cur = conn.execute(
        """
        update rup_detail_queue
        set status = 'pending', last_error = null, updated_at = ?
        where status = 'failed' and attempts < ?
        """,
        (now, max_attempts),
    )
    conn.commit()
    return cur.rowcount


def reset_stale_processing(conn: sqlite3.Connection, older_than: str) -> int:
    now = utc_now()
    cur = conn.execute(
        """
        update rup_detail_queue
        set status = 'pending', last_error = 'reset stale processing job', updated_at = ?
        where status = 'processing' and updated_at < ?
        """,
        (now, older_than),
    )
    conn.commit()
    return cur.rowcount


def queue_report(conn: sqlite3.Connection, sample_limit: int = 10) -> dict[str, Any]:
    by_status = {
        row["status"]: row["count"]
        for row in conn.execute(
            """
            select status, count(*) as count
            from rup_detail_queue
            group by status
            order by status
            """
        ).fetchall()
    }
    failed_samples = [
        dict(row)
        for row in conn.execute(
            """
            select kode_rup, sumber, attempts, last_error, updated_at
            from rup_detail_queue
            where status = 'failed'
            order by updated_at desc
            limit ?
            """,
            (sample_limit,),
        ).fetchall()
    ]
    return {"by_status": by_status, "failed_samples": failed_samples}


def start_run(conn: sqlite3.Connection, job_name: str) -> str:
    run_id = str(uuid4())
    conn.execute(
        "insert into scrape_runs (run_id, job_name, started_at, status) values (?, ?, ?, 'running')",
        (run_id, job_name, utc_now()),
    )
    conn.commit()
    return run_id


def finish_run(conn: sqlite3.Connection, run_id: str, status: str, stats: dict[str, Any]) -> None:
    conn.execute(
        """
        update scrape_runs
        set finished_at = ?, status = ?, stats_json = ?
        where run_id = ?
        """,
        (utc_now(), status, json.dumps(stats, ensure_ascii=False, sort_keys=True), run_id),
    )
    conn.commit()


async def browser_fallback_probe(
    page_size: int = 100,
    timeout_ms: int = 45000,
    stealth: bool = True,
) -> dict[str, Any]:
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        return {"status": "unavailable", "error": f"playwright is not installed: {exc}"}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
            ),
            locale="id-ID",
            timezone_id="Asia/Jakarta",
            viewport={"width": 1366, "height": 768},
            extra_http_headers={
                "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )
        if stealth:
            await context.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['id-ID', 'id', 'en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                window.chrome = window.chrome || { runtime: {} };
                """
            )
        page = await context.new_page()
        await page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "media", "font"}
            else route.continue_(),
        )
        try:
            await page.goto("https://data.inaproc.id/rup", wait_until="domcontentloaded", timeout=timeout_ms)
            await page.wait_for_selector("table", timeout=timeout_ms)
            rows = await page.locator("table tbody tr").count()
            text = await page.locator("body").inner_text(timeout=5000)
            has_pagination = "Halaman " in text
            return {
                "status": "ok" if has_pagination else "degraded",
                "rows": rows,
                "requested_page_size": page_size,
                "stealth": stealth,
                "has_pagination": has_pagination,
                "title": await page.title(),
                "body_sample": text[:500],
            }
        except Exception as exc:  # noqa: BLE001 - diagnostic fallback should report any browser failure.
            return {"status": "error", "error": str(exc)}
        finally:
            await context.close()
            await browser.close()


async def run_daily_listing(args: argparse.Namespace) -> dict[str, Any]:
    conn = connect_db(args.db)
    run_id = start_run(conn, "daily-listing")
    stats = {"new": 0, "changed": 0, "same": 0, "anomaly": 0, "errors": 0, "pages": 0, "rows": 0}
    unchanged_streak = 0
    status = "ok"
    try:
        start_page = args.start_page
        if args.resume:
            checkpoint = get_checkpoint(conn, "daily-listing")
            if checkpoint and int(checkpoint.get("last_page") or 0) >= start_page:
                start_page = int(checkpoint["last_page"]) + 1
        pages = listing_page_range(args.max_pages, start_page=start_page, end_page=args.end_page)
        if not pages:
            stats["skipped"] = True
            page_results = []
        else:
            for attempt in range(args.retries + 1):
                try:
                    page_results = await scrape_listing_pages(
                        len(pages),
                        timeout=args.timeout,
                        page_size=args.page_size,
                        start_page=pages[0],
                    )
                    break
                except Exception:
                    if attempt >= args.retries:
                        raise
                    await asyncio.sleep(min(2**attempt, 10))

        for page_result in page_results:
            if args.archive_dir:
                archive_listing_page(Path(args.archive_dir), run_id, page_result)
            stats["pages"] += 1
            if page_result.get("status") != "ok":
                stats["errors"] += 1
                if args.browser_fallback and page_result.get("page") == 1:
                    fallback = await browser_fallback_probe(args.page_size, stealth=not args.no_stealth)
                    Path(args.fallback_report).write_text(json.dumps(fallback, ensure_ascii=False, indent=2), encoding="utf-8")
                continue
            page_changed = 0
            for row in page_result["rows"]:
                record = {"page": page_result["page"], "total_pages": page_result["total_pages"], **row}
                outcome = upsert_listing_row(conn, record, run_id)
                stats[outcome] += 1
                stats["rows"] += 1
                if outcome in {"new", "changed", "anomaly"}:
                    page_changed += 1
            unchanged_streak = unchanged_streak + 1 if page_changed == 0 else 0
            set_checkpoint(
                conn,
                "daily-listing",
                {"last_page": page_result["page"], "run_id": run_id, "unchanged_streak": unchanged_streak},
            )
            if stats["pages"] >= args.min_pages and unchanged_streak >= args.stop_unchanged_pages:
                break
            if args.rate_delay:
                await asyncio.sleep(args.rate_delay)
    except Exception as exc:  # noqa: BLE001 - job runner records failure and optionally probes browser.
        status = "error"
        stats["error"] = str(exc)
        if args.browser_fallback:
            fallback = await browser_fallback_probe(args.page_size, stealth=not args.no_stealth)
            Path(args.fallback_report).write_text(json.dumps(fallback, ensure_ascii=False, indent=2), encoding="utf-8")
    finally:
        finish_run(conn, run_id, status, stats)
        conn.close()
    return {"run_id": run_id, "status": status, "stats": stats}


async def websocket_probe(args: argparse.Namespace) -> dict[str, Any]:
    try:
        pages = await scrape_listing_pages(1, timeout=args.timeout, page_size=args.page_size)
    except Exception as exc:  # noqa: BLE001 - diagnostic command should report any scraper failure.
        return {"status": "error", "error": str(exc)}
    first = pages[0] if pages else {"status": "error", "rows": []}
    return {
        "status": first.get("status", "error"),
        "page": first.get("page"),
        "total_pages": first.get("total_pages"),
        "rows": len(first.get("rows", [])),
        "page_size": args.page_size,
        "streamlit": {
            "page_script_hash": scrape_listing_pages.__globals__.get("PAGE_SCRIPT_HASH"),
            "next_page_widget_id": scrape_listing_pages.__globals__.get("NEXT_PAGE_WIDGET_ID"),
            "entry_per_page_widget_id": scrape_listing_pages.__globals__.get("ENTRY_PER_PAGE_WIDGET_ID"),
        },
    }


async def run_detail_worker(args: argparse.Namespace) -> dict[str, Any]:
    conn = connect_db(args.db)
    run_id = start_run(conn, "detail-worker")
    jobs = dequeue_detail_jobs(conn, args.limit)
    stats = {"done": 0, "failed": 0, "picked": len(jobs)}
    semaphore = asyncio.Semaphore(args.concurrency)

    async def run_one(job: dict[str, str]) -> tuple[str, dict[str, Any]]:
        kode = job["kode_rup"]
        sumber = job.get("sumber") or None
        async with semaphore:
            try:
                return kode, await scrape_detail(kode, timeout=args.timeout, sumber=sumber)
            except Exception as exc:  # noqa: BLE001 - worker records per-code failure.
                return kode, {"kode": kode, "status": "error", "error": str(exc)}

    for kode, result in await asyncio.gather(*(run_one(job) for job in jobs)):
        mark_detail_result(conn, kode, result)
        if result.get("status") == "ok":
            stats["done"] += 1
        else:
            stats["failed"] += 1
    finish_run(conn, run_id, "ok", stats)
    conn.close()
    return {"run_id": run_id, "status": "ok", "stats": stats}


def main() -> None:
    parser = argparse.ArgumentParser(description="INAPROC incremental scraper pipeline.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    init = sub.add_parser("init-db")
    init.add_argument("--db", default="inaproc.sqlite")

    daily = sub.add_parser("daily-listing")
    daily.add_argument("--db", default="inaproc.sqlite")
    daily.add_argument("--max-pages", type=int, default=200)
    daily.add_argument("--min-pages", type=int, default=20)
    daily.add_argument("--start-page", type=int, default=1)
    daily.add_argument("--end-page", type=int)
    daily.add_argument("--resume", action="store_true")
    daily.add_argument("--stop-unchanged-pages", type=int, default=50)
    daily.add_argument("--page-size", type=int, default=100, choices=[20, 50, 100])
    daily.add_argument("--timeout", type=float, default=20.0)
    daily.add_argument("--retries", type=int, default=2)
    daily.add_argument("--rate-delay", type=float, default=0.0)
    daily.add_argument("--archive-dir")
    daily.add_argument("--browser-fallback", action="store_true")
    daily.add_argument("--fallback-report", default="browser_fallback_report.json")
    daily.add_argument("--no-stealth", action="store_true")

    worker = sub.add_parser("detail-worker")
    worker.add_argument("--db", default="inaproc.sqlite")
    worker.add_argument("--limit", type=int, default=100)
    worker.add_argument("--concurrency", type=int, default=5)
    worker.add_argument("--timeout", type=float, default=20.0)

    probe = sub.add_parser("browser-probe")
    probe.add_argument("--page-size", type=int, default=100, choices=[20, 50, 100])
    probe.add_argument("--no-stealth", action="store_true")

    ws_probe = sub.add_parser("ws-probe")
    ws_probe.add_argument("--page-size", type=int, default=100, choices=[20, 50, 100])
    ws_probe.add_argument("--timeout", type=float, default=20.0)

    report = sub.add_parser("report-queue")
    report.add_argument("--db", default="inaproc.sqlite")
    report.add_argument("--sample-limit", type=int, default=10)

    requeue = sub.add_parser("requeue-failed")
    requeue.add_argument("--db", default="inaproc.sqlite")
    requeue.add_argument("--max-attempts", type=int, default=5)

    stale = sub.add_parser("reset-stale-processing")
    stale.add_argument("--db", default="inaproc.sqlite")
    stale.add_argument("--older-than", required=True, help="ISO timestamp threshold, e.g. 2026-04-27T00:00:00+00:00")

    args = parser.parse_args()
    if args.cmd == "init-db":
        conn = connect_db(args.db)
        conn.close()
        print(json.dumps({"status": "ok", "db": args.db}))
    elif args.cmd == "daily-listing":
        print(json.dumps(asyncio.run(run_daily_listing(args)), ensure_ascii=False, indent=2))
    elif args.cmd == "detail-worker":
        print(json.dumps(asyncio.run(run_detail_worker(args)), ensure_ascii=False, indent=2))
    elif args.cmd == "browser-probe":
        print(json.dumps(asyncio.run(browser_fallback_probe(args.page_size, stealth=not args.no_stealth)), ensure_ascii=False, indent=2))
    elif args.cmd == "ws-probe":
        print(json.dumps(asyncio.run(websocket_probe(args)), ensure_ascii=False, indent=2))
    elif args.cmd == "report-queue":
        conn = connect_db(args.db)
        print(json.dumps(queue_report(conn, sample_limit=args.sample_limit), ensure_ascii=False, indent=2))
        conn.close()
    elif args.cmd == "requeue-failed":
        conn = connect_db(args.db)
        print(json.dumps({"status": "ok", "requeued": requeue_failed_details(conn, args.max_attempts)}, ensure_ascii=False, indent=2))
        conn.close()
    elif args.cmd == "reset-stale-processing":
        conn = connect_db(args.db)
        print(json.dumps({"status": "ok", "reset": reset_stale_processing(conn, args.older_than)}, ensure_ascii=False, indent=2))
        conn.close()
    else:
        sys.exit(2)


if __name__ == "__main__":
    main()
