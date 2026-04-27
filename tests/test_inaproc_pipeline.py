import sqlite3
import tempfile
import unittest
from pathlib import Path

from inaproc_pipeline import (
    archive_listing_page,
    dequeue_detail_jobs,
    get_checkpoint,
    init_db,
    listing_page_range,
    queue_report,
    requeue_failed_details,
    reset_stale_processing,
    row_hash,
    set_checkpoint,
    upsert_listing_row,
)


ROW = {
    "page": 1,
    "total_pages": 42242,
    "No.": "1",
    "Nama Instansi": "KOTA DEPOK",
    "Nama Paket": "Belanja Alat Tulis Kantor",
    "Kode RUP": "63450085",
    "Cara Pengadaan": "Penyedia",
    "Total Nilai (Rp)": "Rp 350.000",
}


class InaprocPipelineTest(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        init_db(self.conn)

    def test_row_hash_ignores_page_metadata(self):
        a = row_hash(ROW)
        b = row_hash({**ROW, "page": 2, "total_pages": 50000, "No.": "101"})

        self.assertEqual(a, b)

    def test_upsert_new_row_indexes_code_and_enqueues_detail(self):
        result = upsert_listing_row(self.conn, ROW, run_id="run-1")

        self.assertEqual(result, "new")
        stored = self.conn.execute("select * from rup_listing where kode_rup = ?", ("63450085",)).fetchone()
        self.assertIsNotNone(stored)
        self.assertEqual(stored["nama_paket"], "Belanja Alat Tulis Kantor")
        jobs = dequeue_detail_jobs(self.conn, limit=10)
        self.assertEqual(jobs, [{"kode_rup": "63450085", "sumber": "Penyedia"}])

    def test_upsert_same_row_only_updates_last_seen(self):
        self.assertEqual(upsert_listing_row(self.conn, ROW, run_id="run-1"), "new")
        self.assertEqual(upsert_listing_row(self.conn, ROW, run_id="run-2"), "same")
        jobs = dequeue_detail_jobs(self.conn, limit=10)

        self.assertEqual(jobs, [{"kode_rup": "63450085", "sumber": "Penyedia"}])

    def test_upsert_changed_row_enqueues_changed_detail(self):
        self.assertEqual(upsert_listing_row(self.conn, ROW, run_id="run-1"), "new")
        jobs = dequeue_detail_jobs(self.conn, limit=10)
        self.assertEqual(jobs, [{"kode_rup": "63450085", "sumber": "Penyedia"}])

        changed = {**ROW, "Total Nilai (Rp)": "Rp 450.000"}
        self.assertEqual(upsert_listing_row(self.conn, changed, run_id="run-2"), "changed")
        jobs = dequeue_detail_jobs(self.conn, limit=10)
        self.assertEqual(jobs, [{"kode_rup": "63450085", "sumber": "Penyedia"}])

    def test_swakelola_row_enqueues_sumber_swakelola(self):
        row = {**ROW, "Kode RUP": "43017295", "Cara Pengadaan": "Swakelola"}

        self.assertEqual(upsert_listing_row(self.conn, row, run_id="run-1"), "new")
        jobs = dequeue_detail_jobs(self.conn, limit=10)

        self.assertEqual(jobs, [{"kode_rup": "43017295", "sumber": "Swakelola"}])

    def test_missing_kode_goes_to_anomalies(self):
        result = upsert_listing_row(self.conn, {**ROW, "Kode RUP": ""}, run_id="run-1")

        self.assertEqual(result, "anomaly")
        count = self.conn.execute("select count(*) from rup_listing_anomalies").fetchone()[0]
        self.assertEqual(count, 1)

    def test_checkpoint_round_trip(self):
        set_checkpoint(self.conn, "daily-listing", {"last_page": 10})

        self.assertEqual(get_checkpoint(self.conn, "daily-listing"), {"last_page": 10})

    def test_listing_page_range_supports_chunks_and_resume(self):
        self.assertEqual(listing_page_range(max_pages=3, start_page=5, end_page=9), [5, 6, 7])
        self.assertEqual(listing_page_range(max_pages=10, start_page=5, end_page=7), [5, 6, 7])

    def test_archive_listing_page_writes_jsonl_records(self):
        page_result = {"page": 2, "total_pages": 9, "status": "ok", "rows": [ROW]}
        with tempfile.TemporaryDirectory() as tmpdir:
            path = archive_listing_page(Path(tmpdir), "run-1", page_result)

            lines = path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), 1)
        self.assertIn('"run_id":"run-1"', lines[0])
        self.assertIn('"Kode RUP":"63450085"', lines[0])

    def test_requeue_failed_details_respects_attempt_limit(self):
        upsert_listing_row(self.conn, ROW, run_id="run-1")
        self.conn.execute(
            """
            update rup_detail_queue
            set status = 'failed', attempts = 2, last_error = 'timeout'
            where kode_rup = ?
            """,
            ("63450085",),
        )
        self.conn.commit()

        self.assertEqual(requeue_failed_details(self.conn, max_attempts=3), 1)
        row = self.conn.execute("select status, last_error from rup_detail_queue where kode_rup = ?", ("63450085",)).fetchone()
        self.assertEqual(row["status"], "pending")
        self.assertIsNone(row["last_error"])

    def test_queue_report_counts_statuses(self):
        upsert_listing_row(self.conn, ROW, run_id="run-1")
        self.conn.execute(
            "update rup_detail_queue set status = 'failed', attempts = 1, last_error = 'timeout' where kode_rup = ?",
            ("63450085",),
        )
        self.conn.commit()

        report = queue_report(self.conn)

        self.assertEqual(report["by_status"], {"failed": 1})
        self.assertEqual(report["failed_samples"][0]["kode_rup"], "63450085")

    def test_reset_stale_processing_requeues_old_jobs(self):
        upsert_listing_row(self.conn, ROW, run_id="run-1")
        self.conn.execute(
            """
            update rup_detail_queue
            set status = 'processing', updated_at = '2026-01-01T00:00:00+00:00'
            where kode_rup = ?
            """,
            ("63450085",),
        )
        self.conn.commit()

        self.assertEqual(reset_stale_processing(self.conn, older_than="2026-01-02T00:00:00+00:00"), 1)
        row = self.conn.execute("select status from rup_detail_queue where kode_rup = ?", ("63450085",)).fetchone()
        self.assertEqual(row["status"], "pending")


if __name__ == "__main__":
    unittest.main()
