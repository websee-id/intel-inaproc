#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row

from inaproc_pipeline import (
    archive_listing_page,
    listing_page_range,
    normalized_row,
    row_hash,
    utc_now,
)
from inaproc_ws_scraper import iter_scrape_listing_pages, scrape_detail, scrape_listing_pages


LISTING_COPY_COLUMNS = [
    "kode_rup",
    "nama_instansi",
    "nama_satuan_kerja",
    "tahun_anggaran",
    "cara_pengadaan",
    "metode_pengadaan",
    "jenis_pengadaan",
    "nama_paket",
    "sumber_dana",
    "produk_dalam_negeri",
    "total_nilai",
    "row_json",
    "row_hash",
    "first_seen_at",
    "last_seen_at",
    "last_changed_at",
    "source_page",
    "scrape_run_id",
]


def build_dsn(value: str | None, env: dict[str, str] | None = None) -> str:
    env = env if env is not None else os.environ
    dsn = value or env.get("DATABASE_URL")
    if not dsn:
        raise ValueError("provide --dsn or set DATABASE_URL")
    return dsn


def connect_pg(dsn: str) -> psycopg.Connection:
    return psycopg.connect(dsn, row_factory=dict_row, autocommit=False)


def init_pg(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
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
              row_json jsonb not null,
              row_hash text not null,
              first_seen_at timestamptz not null,
              last_seen_at timestamptz not null,
              last_changed_at timestamptz,
              source_page integer,
              scrape_run_id text
            )
            """
        )
        cur.execute(
            """
            create table if not exists rup_detail_queue (
              kode_rup text primary key,
              sumber text,
              reason text not null,
              status text not null default 'pending',
              attempts integer not null default 0,
              last_error text,
              locked_by text,
              locked_at timestamptz,
              created_at timestamptz not null,
              updated_at timestamptz not null
            )
            """
        )
        cur.execute(
            """
            create table if not exists rup_detail (
              kode_rup text primary key,
              status text not null,
              detail_json jsonb,
              sumber_dana_json jsonb,
              error text,
              fetched_at timestamptz not null
            )
            """
        )
        cur.execute(
            """
            create table if not exists rup_listing_anomalies (
              id bigserial primary key,
              reason text not null,
              payload_json jsonb not null,
              scrape_run_id text,
              detected_at timestamptz not null
            )
            """
        )
        cur.execute(
            """
            create table if not exists scrape_runs (
              run_id text primary key,
              job_name text not null,
              started_at timestamptz not null,
              finished_at timestamptz,
              status text not null,
              stats_json jsonb
            )
            """
        )
        cur.execute(
            """
            create table if not exists checkpoints (
              job_name text primary key,
              state_json jsonb not null,
              updated_at timestamptz not null
            )
            """
        )
        cur.execute("create index if not exists idx_rup_detail_queue_status_updated on rup_detail_queue(status, updated_at)")
        cur.execute("create index if not exists idx_rup_detail_queue_locked on rup_detail_queue(locked_by, locked_at)")
        cur.execute("create index if not exists idx_rup_listing_source_page on rup_listing(source_page)")
    conn.commit()


def write_listing_seed_page(path: Path, page_result: dict[str, Any]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    if page_result.get("status") != "ok":
        return 0
    with path.open("a", encoding="utf-8") as f:
        count = 0
        for row in page_result.get("rows", []):
            record = {"page": page_result["page"], "total_pages": page_result["total_pages"], **row}
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
            count += 1
    return count


def iter_listing_seed_records(path: Path):
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def listing_seed_to_copy_rows(records, run_id: str):
    rows = []
    now = utc_now()
    for record in records:
        normalized = normalized_row(record)
        kode_rup = normalized.get("Kode RUP", "")
        if not kode_rup:
            continue
        row_json = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
        rows.append(
            (
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
                row_hash(normalized),
                now,
                now,
                now,
                int(normalized.get("page") or 0),
                run_id,
            )
        )
    return rows


def write_listing_seed_copy_csv(jsonl_path: Path, csv_path: Path, run_id: str) -> int:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for row in listing_seed_to_copy_rows(iter_listing_seed_records(jsonl_path), run_id):
            writer.writerow(row)
            count += 1
    return count


def bulk_load_listing_csv_pg(conn: psycopg.Connection, csv_path: Path) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("drop table if exists rup_listing_stage")
        cur.execute(
            """
            create temporary table rup_listing_stage (
              kode_rup text,
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
              row_json jsonb,
              row_hash text,
              first_seen_at timestamptz,
              last_seen_at timestamptz,
              last_changed_at timestamptz,
              source_page integer,
              scrape_run_id text
            ) on commit drop
            """
        )
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            with cur.copy(
                """
                copy rup_listing_stage (
                  kode_rup, nama_instansi, nama_satuan_kerja, tahun_anggaran,
                  cara_pengadaan, metode_pengadaan, jenis_pengadaan, nama_paket,
                  sumber_dana, produk_dalam_negeri, total_nilai, row_json, row_hash,
                  first_seen_at, last_seen_at, last_changed_at, source_page, scrape_run_id
                ) from stdin with (format csv)
                """
            ) as copy:
                for chunk in iter(lambda: f.read(1024 * 1024), ""):
                    copy.write(chunk)
        cur.execute("select count(*) as count from rup_listing_stage")
        staged = cur.fetchone()["count"]
        cur.execute(
            """
            insert into rup_listing (
              kode_rup, nama_instansi, nama_satuan_kerja, tahun_anggaran,
              cara_pengadaan, metode_pengadaan, jenis_pengadaan, nama_paket,
              sumber_dana, produk_dalam_negeri, total_nilai, row_json, row_hash,
              first_seen_at, last_seen_at, last_changed_at, source_page, scrape_run_id
            )
            select distinct on (kode_rup)
              kode_rup, nama_instansi, nama_satuan_kerja, tahun_anggaran,
              cara_pengadaan, metode_pengadaan, jenis_pengadaan, nama_paket,
              sumber_dana, produk_dalam_negeri, total_nilai, row_json, row_hash,
              first_seen_at, last_seen_at, last_changed_at, source_page, scrape_run_id
            from rup_listing_stage
            order by kode_rup, source_page desc
            on conflict (kode_rup) do update set
              nama_instansi = excluded.nama_instansi,
              nama_satuan_kerja = excluded.nama_satuan_kerja,
              tahun_anggaran = excluded.tahun_anggaran,
              cara_pengadaan = excluded.cara_pengadaan,
              metode_pengadaan = excluded.metode_pengadaan,
              jenis_pengadaan = excluded.jenis_pengadaan,
              nama_paket = excluded.nama_paket,
              sumber_dana = excluded.sumber_dana,
              produk_dalam_negeri = excluded.produk_dalam_negeri,
              total_nilai = excluded.total_nilai,
              row_json = excluded.row_json,
              row_hash = excluded.row_hash,
              last_seen_at = excluded.last_seen_at,
              last_changed_at = case
                when rup_listing.row_hash is distinct from excluded.row_hash then excluded.last_changed_at
                else rup_listing.last_changed_at
              end,
              source_page = excluded.source_page,
              scrape_run_id = excluded.scrape_run_id
            """
        )
        upserted = cur.rowcount
        cur.execute(
            """
            insert into rup_detail_queue (kode_rup, sumber, reason, status, attempts, created_at, updated_at)
            select s.kode_rup, s.cara_pengadaan, 'seed', 'pending', 0, %s, %s
            from rup_listing_stage s
            on conflict (kode_rup) do nothing
            """,
            (utc_now(), utc_now()),
        )
        queued = cur.rowcount
    conn.commit()
    return {"staged": staged, "upserted": upserted, "queued": queued}


def start_run(conn: psycopg.Connection, job_name: str) -> str:
    run_id = str(uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "insert into scrape_runs (run_id, job_name, started_at, status) values (%s, %s, %s, 'running')",
            (run_id, job_name, utc_now()),
        )
    conn.commit()
    return run_id


def finish_run(conn: psycopg.Connection, run_id: str, status: str, stats: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update scrape_runs
            set finished_at = %s, status = %s, stats_json = %s::jsonb
            where run_id = %s
            """,
            (utc_now(), status, json.dumps(stats, ensure_ascii=False), run_id),
        )
    conn.commit()


def _record_anomaly(conn: psycopg.Connection, row: dict[str, str], reason: str, run_id: str, now: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into rup_listing_anomalies (reason, payload_json, scrape_run_id, detected_at)
            values (%s, %s::jsonb, %s, %s)
            """,
            (reason, json.dumps(row, ensure_ascii=False, sort_keys=True), run_id, now),
        )


def upsert_listing_row_pg(conn: psycopg.Connection, row: dict[str, Any], run_id: str) -> str:
    normalized = normalized_row(row)
    kode_rup = normalized.get("Kode RUP", "")
    now = utc_now()
    if not kode_rup:
        _record_anomaly(conn, normalized, "missing_kode_rup", run_id, now)
        conn.commit()
        return "anomaly"

    digest = row_hash(normalized)
    row_json = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    sumber = normalized.get("Cara Pengadaan", "")
    with conn.cursor() as cur:
        cur.execute("select row_hash from rup_listing where kode_rup = %s", (kode_rup,))
        existing = cur.fetchone()
        if existing is None:
            cur.execute(
                """
                insert into rup_listing (
                  kode_rup, nama_instansi, nama_satuan_kerja, tahun_anggaran,
                  cara_pengadaan, metode_pengadaan, jenis_pengadaan, nama_paket,
                  sumber_dana, produk_dalam_negeri, total_nilai, row_json, row_hash,
                  first_seen_at, last_seen_at, last_changed_at, source_page, scrape_run_id
                ) values (
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s::jsonb, %s, %s, %s, %s, %s, %s
                )
                """,
                (
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
                    now,
                    int(normalized.get("page") or 0),
                    run_id,
                ),
            )
            _enqueue_detail_pg(cur, kode_rup, sumber, "new", now)
            conn.commit()
            return "new"

        if existing["row_hash"] == digest:
            cur.execute(
                """
                update rup_listing
                set last_seen_at = %s, source_page = %s, scrape_run_id = %s
                where kode_rup = %s
                """,
                (now, int(normalized.get("page") or 0), run_id, kode_rup),
            )
            conn.commit()
            return "same"

        cur.execute(
            """
            update rup_listing
            set nama_instansi = %s, nama_satuan_kerja = %s, tahun_anggaran = %s,
                cara_pengadaan = %s, metode_pengadaan = %s, jenis_pengadaan = %s,
                nama_paket = %s, sumber_dana = %s, produk_dalam_negeri = %s,
                total_nilai = %s, row_json = %s::jsonb, row_hash = %s, last_seen_at = %s,
                last_changed_at = %s, source_page = %s, scrape_run_id = %s
            where kode_rup = %s
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
        _enqueue_detail_pg(cur, kode_rup, sumber, "changed", now)
    conn.commit()
    return "changed"


def _enqueue_detail_pg(cur: psycopg.Cursor, kode_rup: str, sumber: str, reason: str, now: str) -> None:
    cur.execute(
        """
        insert into rup_detail_queue (kode_rup, sumber, reason, status, attempts, created_at, updated_at)
        values (%s, %s, %s, 'pending', 0, %s, %s)
        on conflict (kode_rup) do update set
          sumber = excluded.sumber,
          reason = excluded.reason,
          status = 'pending',
          locked_by = null,
          locked_at = null,
          updated_at = excluded.updated_at
        """,
        (kode_rup, sumber, reason, now, now),
    )


def detail_queue_claim_sql() -> str:
    return """
    with picked as (
      select kode_rup
      from rup_detail_queue
      where status = 'pending'
      order by updated_at asc
      limit %s
      for update skip locked
    )
    update rup_detail_queue q
    set status = 'processing',
        attempts = attempts + 1,
        locked_by = %s,
        locked_at = %s,
        updated_at = %s
    from picked
    where q.kode_rup = picked.kode_rup
    returning q.kode_rup, q.sumber
    """


def claim_detail_jobs_pg(conn: psycopg.Connection, limit: int, worker_id: str) -> list[dict[str, str]]:
    now = utc_now()
    with conn.cursor() as cur:
        cur.execute(detail_queue_claim_sql(), (limit, worker_id, now, now))
        rows = cur.fetchall()
    conn.commit()
    return [{"kode_rup": row["kode_rup"], "sumber": row.get("sumber") or ""} for row in rows]


def mark_detail_result_pg(conn: psycopg.Connection, kode_rup: str, result: dict[str, Any], worker_id: str) -> None:
    now = utc_now()
    status = result.get("status", "ok")
    error = result.get("error")
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into rup_detail (kode_rup, status, detail_json, sumber_dana_json, error, fetched_at)
            values (%s, %s, %s::jsonb, %s::jsonb, %s, %s)
            on conflict (kode_rup) do update set
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
        cur.execute(
            """
            update rup_detail_queue
            set status = %s, last_error = %s, locked_by = null, locked_at = null, updated_at = %s
            where kode_rup = %s and locked_by = %s
            """,
            ("done" if status == "ok" else "failed", error, now, kode_rup, worker_id),
        )
    conn.commit()


def queue_report_pg(conn: psycopg.Connection, sample_limit: int = 10) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("select status, count(*) as count from rup_detail_queue group by status order by status")
        by_status = {row["status"]: row["count"] for row in cur.fetchall()}
        cur.execute(
            """
            select kode_rup, sumber, attempts, last_error, locked_by, locked_at, updated_at
            from rup_detail_queue
            where status in ('failed', 'processing')
            order by updated_at desc
            limit %s
            """,
            (sample_limit,),
        )
        samples = cur.fetchall()
    return {"by_status": by_status, "samples": samples}


def requeue_failed_pg(conn: psycopg.Connection, max_attempts: int = 5) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            update rup_detail_queue
            set status = 'pending', last_error = null, locked_by = null, locked_at = null, updated_at = %s
            where status = 'failed' and attempts < %s
            """,
            (utc_now(), max_attempts),
        )
        count = cur.rowcount
    conn.commit()
    return count


def reset_stale_processing_pg(conn: psycopg.Connection, older_than: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            update rup_detail_queue
            set status = 'pending',
                last_error = 'reset stale processing job',
                locked_by = null,
                locked_at = null,
                updated_at = %s
            where status = 'processing' and locked_at < %s
            """,
            (utc_now(), older_than),
        )
        count = cur.rowcount
    conn.commit()
    return count


async def scrape_detail_with_retries(
    kode: str,
    sumber: str | None,
    timeout: float,
    retries: int,
    retry_delay: float,
    scraper=scrape_detail,
) -> dict[str, Any]:
    last_result: dict[str, Any] | None = None
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            result = await scraper(kode, timeout=timeout, sumber=sumber)
            if result.get("status") == "ok":
                return result
            last_result = result
        except Exception as exc:  # noqa: BLE001 - caller records final per-code failure.
            last_error = exc
        if attempt < retries:
            await asyncio.sleep(min(retry_delay * (2**attempt), 10))
    if last_result is not None:
        return last_result
    return {"kode": kode, "status": "error", "error": str(last_error)}


def set_checkpoint_pg(conn: psycopg.Connection, job_name: str, state: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into checkpoints (job_name, state_json, updated_at)
            values (%s, %s::jsonb, %s)
            on conflict (job_name) do update set
              state_json = excluded.state_json,
              updated_at = excluded.updated_at
            """,
            (job_name, json.dumps(state, ensure_ascii=False, sort_keys=True), utc_now()),
        )
    conn.commit()


def get_checkpoint_pg(conn: psycopg.Connection, job_name: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("select state_json from checkpoints where job_name = %s", (job_name,))
        row = cur.fetchone()
    return row["state_json"] if row else None


async def run_daily_listing_pg(args: argparse.Namespace) -> dict[str, Any]:
    dsn = build_dsn(args.dsn)
    conn = connect_pg(dsn)
    init_pg(conn)
    run_id = start_run(conn, "daily-listing")
    stats = {"new": 0, "changed": 0, "same": 0, "anomaly": 0, "errors": 0, "pages": 0, "rows": 0}
    status = "ok"
    try:
        start_page = args.start_page
        if args.resume:
            checkpoint = get_checkpoint_pg(conn, "daily-listing")
            if checkpoint and int(checkpoint.get("last_page") or 0) >= start_page:
                start_page = int(checkpoint["last_page"]) + 1
        pages = listing_page_range(args.max_pages, start_page=start_page, end_page=args.end_page)
        page_results = []
        if pages:
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
                continue
            for row in page_result["rows"]:
                record = {"page": page_result["page"], "total_pages": page_result["total_pages"], **row}
                outcome = upsert_listing_row_pg(conn, record, run_id)
                stats[outcome] += 1
                stats["rows"] += 1
            set_checkpoint_pg(conn, "daily-listing", {"last_page": page_result["page"], "run_id": run_id})
            if args.rate_delay:
                await asyncio.sleep(args.rate_delay)
    except Exception as exc:  # noqa: BLE001 - command records operational failures.
        status = "error"
        stats["error"] = str(exc)
    finally:
        finish_run(conn, run_id, status, stats)
        conn.close()
    return {"run_id": run_id, "status": status, "stats": stats}


async def run_detail_worker_pg(args: argparse.Namespace) -> dict[str, Any]:
    dsn = build_dsn(args.dsn)
    worker_id = args.worker_id or str(uuid4())
    conn = connect_pg(dsn)
    init_pg(conn)
    run_id = start_run(conn, "detail-worker")
    jobs = claim_detail_jobs_pg(conn, args.limit, worker_id)
    stats = {"done": 0, "failed": 0, "picked": len(jobs), "worker_id": worker_id}
    semaphore = asyncio.Semaphore(args.concurrency)

    async def run_one(job: dict[str, str]) -> tuple[str, dict[str, Any]]:
        kode = job["kode_rup"]
        sumber = job.get("sumber") or None
        async with semaphore:
            return kode, await scrape_detail_with_retries(
                kode,
                sumber=sumber,
                timeout=args.timeout,
                retries=args.retries,
                retry_delay=args.retry_delay,
            )

    for kode, result in await asyncio.gather(*(run_one(job) for job in jobs)):
        mark_detail_result_pg(conn, kode, result, worker_id)
        if result.get("status") == "ok":
            stats["done"] += 1
        else:
            stats["failed"] += 1
    finish_run(conn, run_id, "ok", stats)
    conn.close()
    return {"run_id": run_id, "status": "ok", "stats": stats}


async def run_seed_listing_file(args: argparse.Namespace) -> dict[str, Any]:
    output = Path(args.output)
    if args.truncate and output.exists():
        output.unlink()
    run_id = str(uuid4())
    stats = {"run_id": run_id, "status": "ok", "pages": 0, "rows": 0, "errors": 0, "output": str(output)}
    try:
        pages = listing_page_range(args.max_pages, start_page=args.start_page, end_page=args.end_page)
        if pages:
            async for page_result in iter_scrape_listing_pages(
                len(pages),
                timeout=args.timeout,
                page_size=args.page_size,
                start_page=pages[0],
            ):
                stats["pages"] += 1
                if page_result.get("status") != "ok":
                    stats["errors"] += 1
                    continue
                stats["rows"] += write_listing_seed_page(output, page_result)
                print(
                    json.dumps(
                        {
                            "event": "seed-page",
                            "page": page_result["page"],
                            "rows": len(page_result["rows"]),
                            "total_rows": stats["rows"],
                            "total_pages": page_result.get("total_pages"),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                if args.rate_delay:
                    await asyncio.sleep(args.rate_delay)
    except Exception as exc:  # noqa: BLE001 - command returns operational failure.
        stats["status"] = "error"
        stats["error"] = str(exc)
    return stats


def run_prepare_listing_copy(args: argparse.Namespace) -> dict[str, Any]:
    run_id = args.run_id or str(uuid4())
    rows = write_listing_seed_copy_csv(Path(args.input), Path(args.output), run_id)
    return {"status": "ok", "run_id": run_id, "rows": rows, "output": args.output}


def run_bulk_load_listing(args: argparse.Namespace) -> dict[str, Any]:
    dsn = build_dsn(args.dsn)
    conn = connect_pg(dsn)
    init_pg(conn)
    stats = bulk_load_listing_csv_pg(conn, Path(args.input))
    conn.close()
    return {"status": "ok", **stats}


def main() -> None:
    parser = argparse.ArgumentParser(description="INAPROC PostgreSQL scraper pipeline for multi-worker runs.")
    parser.add_argument("--dsn", help="PostgreSQL DSN. Prefer DATABASE_URL env instead of shell history.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db")

    daily = sub.add_parser("daily-listing")
    daily.add_argument("--max-pages", type=int, default=200)
    daily.add_argument("--start-page", type=int, default=1)
    daily.add_argument("--end-page", type=int)
    daily.add_argument("--resume", action="store_true")
    daily.add_argument("--page-size", type=int, default=100, choices=[20, 50, 100])
    daily.add_argument("--timeout", type=float, default=20.0)
    daily.add_argument("--retries", type=int, default=2)
    daily.add_argument("--rate-delay", type=float, default=0.0)
    daily.add_argument("--archive-dir")

    worker = sub.add_parser("detail-worker")
    worker.add_argument("--worker-id")
    worker.add_argument("--limit", type=int, default=500)
    worker.add_argument("--concurrency", type=int, default=5)
    worker.add_argument("--timeout", type=float, default=20.0)
    worker.add_argument("--retries", type=int, default=2)
    worker.add_argument("--retry-delay", type=float, default=1.0)

    report = sub.add_parser("report-queue")
    report.add_argument("--sample-limit", type=int, default=10)

    requeue = sub.add_parser("requeue-failed")
    requeue.add_argument("--max-attempts", type=int, default=5)

    stale = sub.add_parser("reset-stale-processing")
    stale.add_argument("--older-than", required=True)

    seed_file = sub.add_parser("seed-listing-file")
    seed_file.add_argument("--output", required=True)
    seed_file.add_argument("--max-pages", type=int, default=200)
    seed_file.add_argument("--start-page", type=int, default=1)
    seed_file.add_argument("--end-page", type=int)
    seed_file.add_argument("--page-size", type=int, default=100, choices=[20, 50, 100])
    seed_file.add_argument("--timeout", type=float, default=20.0)
    seed_file.add_argument("--rate-delay", type=float, default=0.0)
    seed_file.add_argument("--truncate", action="store_true")

    prepare_copy = sub.add_parser("prepare-listing-copy")
    prepare_copy.add_argument("--input", required=True)
    prepare_copy.add_argument("--output", required=True)
    prepare_copy.add_argument("--run-id")

    bulk_load = sub.add_parser("bulk-load-listing")
    bulk_load.add_argument("--input", required=True)

    args = parser.parse_args()
    if args.cmd == "init-db":
        dsn = build_dsn(args.dsn)
        conn = connect_pg(dsn)
        init_pg(conn)
        conn.close()
        print(json.dumps({"status": "ok", "db": "postgres"}))
    elif args.cmd == "daily-listing":
        print(json.dumps(asyncio.run(run_daily_listing_pg(args)), ensure_ascii=False, indent=2))
    elif args.cmd == "detail-worker":
        print(json.dumps(asyncio.run(run_detail_worker_pg(args)), ensure_ascii=False, indent=2))
    elif args.cmd == "report-queue":
        dsn = build_dsn(args.dsn)
        conn = connect_pg(dsn)
        print(json.dumps(queue_report_pg(conn, args.sample_limit), ensure_ascii=False, indent=2, default=str))
        conn.close()
    elif args.cmd == "requeue-failed":
        dsn = build_dsn(args.dsn)
        conn = connect_pg(dsn)
        print(json.dumps({"status": "ok", "requeued": requeue_failed_pg(conn, args.max_attempts)}, ensure_ascii=False, indent=2))
        conn.close()
    elif args.cmd == "reset-stale-processing":
        dsn = build_dsn(args.dsn)
        conn = connect_pg(dsn)
        print(json.dumps({"status": "ok", "reset": reset_stale_processing_pg(conn, args.older_than)}, ensure_ascii=False, indent=2))
        conn.close()
    elif args.cmd == "seed-listing-file":
        print(json.dumps(asyncio.run(run_seed_listing_file(args)), ensure_ascii=False, indent=2))
    elif args.cmd == "prepare-listing-copy":
        print(json.dumps(run_prepare_listing_copy(args), ensure_ascii=False, indent=2))
    elif args.cmd == "bulk-load-listing":
        print(json.dumps(run_bulk_load_listing(args), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
