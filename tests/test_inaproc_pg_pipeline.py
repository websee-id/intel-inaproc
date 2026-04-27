import unittest

from inaproc_pg_pipeline import build_dsn, detail_queue_claim_sql


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


if __name__ == "__main__":
    unittest.main()
