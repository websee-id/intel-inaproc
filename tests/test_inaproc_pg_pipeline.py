import unittest

import asyncio
import tempfile
from pathlib import Path

from inaproc_pg_pipeline import (
    build_dsn,
    detail_queue_claim_sql,
    iter_listing_seed_records,
    listing_seed_to_copy_rows,
    scrape_detail_with_retries,
    write_listing_seed_page,
)


class InaprocPgPipelineTest(unittest.TestCase):
    def test_build_dsn_requires_explicit_or_environment_value(self):
        with self.assertRaises(ValueError):
            build_dsn(None, env={})

        self.assertEqual(
            build_dsn("postgres://user:pass@example/db", env={}),
            "postgres://user:pass@example/db",
        )
        self.assertEqual(
            build_dsn(None, env={"DATABASE_URL": "postgres://env/db"}),
            "postgres://env/db",
        )

    def test_detail_queue_claim_uses_skip_locked(self):
        sql = detail_queue_claim_sql()

        self.assertIn("for update skip locked", " ".join(sql.lower().split()))
        self.assertIn("locked_by", sql)
        self.assertIn("attempts = attempts + 1", sql)

    def test_scrape_detail_with_retries_retries_missing_status(self):
        calls = []

        async def scraper(kode, timeout, sumber=None):
            calls.append((kode, timeout, sumber))
            if len(calls) == 1:
                return {"kode": kode, "status": "missing"}
            return {"kode": kode, "status": "ok", "detail": {"Kode RUP": kode}, "sumber_dana": []}

        result = asyncio.run(
            scrape_detail_with_retries(
                "123",
                sumber="Penyedia",
                timeout=1,
                retries=1,
                retry_delay=0,
                scraper=scraper,
            )
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(calls), 2)

    def test_write_listing_seed_page_outputs_one_json_record_per_row(self):
        page = {
            "page": 2,
            "total_pages": 9,
            "status": "ok",
            "rows": [{"Kode RUP": "123", "Nama Paket": "Paket A", "Cara Pengadaan": "Penyedia"}],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "seed.jsonl"
            count = write_listing_seed_page(path, page)
            records = list(iter_listing_seed_records(path))

        self.assertEqual(count, 1)
        self.assertEqual(records[0]["page"], 2)
        self.assertEqual(records[0]["Kode RUP"], "123")

    def test_listing_seed_to_copy_rows_normalizes_and_skips_missing_kode(self):
        records = [
            {"page": 1, "Kode RUP": "123", "Nama Paket": "Paket A", "Cara Pengadaan": "Penyedia"},
            {"page": 1, "Kode RUP": "", "Nama Paket": "Bad"},
        ]

        rows = listing_seed_to_copy_rows(records, run_id="run-1")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "123")
        self.assertEqual(rows[0][7], "Paket A")
        self.assertEqual(rows[0][-1], "run-1")


if __name__ == "__main__":
    unittest.main()
