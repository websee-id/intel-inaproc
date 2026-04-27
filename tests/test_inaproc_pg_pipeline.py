import unittest

import asyncio

from inaproc_pg_pipeline import build_dsn, detail_queue_claim_sql, scrape_detail_with_retries


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


if __name__ == "__main__":
    unittest.main()
