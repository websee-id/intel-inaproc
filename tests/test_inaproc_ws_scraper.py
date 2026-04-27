import asyncio
import unittest

from inaproc_ws_scraper import (
    build_entry_per_page_message,
    build_rerun_message,
    extract_page_info_from_payload,
    extract_tables_from_payload,
    flatten_result,
    iter_scrape_listing_pages,
    parse_kode_lines,
    scrape_with_retries,
)


DETAIL_FRAME = b"""
\x00<div class='wrap-table wrap-table-main'><table border="1" class="dataframe">
  <thead>
    <tr><th>No.</th><th>Deskripsi</th><th>Detail</th></tr>
  </thead>
  <tbody>
    <tr><td>1</td><td>Kode RUP</td><td>64228258</td></tr>
    <tr><td>2</td><td>Nama Paket</td><td>Belanja Sewa Mess</td></tr>
  </tbody>
</table></div>
"""


FUND_FRAME = b"""
\x00<div class='wrap-table'><table border="1" class="dataframe">
  <thead>
    <tr><th>No.</th><th>Sumber Dana</th><th>T.A.</th><th>Pagu</th></tr>
  </thead>
  <tbody>
    <tr><td>1</td><td>APBD</td><td>2026</td><td>Rp 1.400.000</td></tr>
  </tbody>
</table></div>
"""


LIST_FRAME = b"""
<div class="custom-table-container"><table border="1" class="dataframe">
  <thead>
    <tr><th>No.</th><th>Nama Instansi</th><th>Nama Paket</th><th>Kode RUP</th></tr>
  </thead>
  <tbody>
    <tr><td>1</td><td>KAB. SUBANG</td><td>Paket A</td><td><a href="?kode=64713903&sumber=Penyedia">64713903</a></td></tr>
  </tbody>
</table></div>
"""


PAGE_FRAME = b"<p style='text-align: center'>Halaman 12 dari 211.208</p>"


class InaprocWsScraperTest(unittest.TestCase):
    def test_extracts_detail_and_fund_tables_from_streamlit_payloads(self):
        tables = extract_tables_from_payload(DETAIL_FRAME + FUND_FRAME)

        self.assertEqual(tables[0]["kind"], "detail")
        self.assertEqual(tables[0]["rows"][0]["Deskripsi"], "Kode RUP")
        self.assertEqual(tables[0]["rows"][0]["Detail"], "64228258")
        self.assertEqual(tables[0]["rows"][1]["Detail"], "Belanja Sewa Mess")

        self.assertEqual(tables[1]["kind"], "sumber_dana")
        self.assertEqual(tables[1]["rows"][0]["Sumber Dana"], "APBD")
        self.assertEqual(tables[1]["rows"][0]["Pagu"], "Rp 1.400.000")

    def test_extracts_listing_table_from_main_page_payload(self):
        tables = extract_tables_from_payload(LIST_FRAME)

        self.assertEqual(tables[0]["kind"], "listing")
        self.assertEqual(tables[0]["rows"][0]["Nama Instansi"], "KAB. SUBANG")
        self.assertEqual(tables[0]["rows"][0]["Kode RUP"], "64713903")

    def test_extracts_page_info_from_payload(self):
        self.assertEqual(extract_page_info_from_payload(PAGE_FRAME), {"page": 12, "total_pages": 211208})

    def test_builds_streamlit_rerun_message_for_kode_query(self):
        msg = build_rerun_message("64228258")

        self.assertIn(b"kode=64228258", msg)
        self.assertNotIn(b"sumber=", msg)
        self.assertIn(b"rup", msg)
        self.assertIn(b"https://data.inaproc.id/rup", msg)
        self.assertIn(b"en-US", msg)

    def test_builds_streamlit_rerun_message_with_sumber_query(self):
        msg = build_rerun_message("43017295", sumber="Swakelola")

        self.assertIn(b"kode=43017295&sumber=Swakelola", msg)

    def test_builds_entry_per_page_select_message(self):
        msg = build_entry_per_page_message(100)

        self.assertIn(b"$$ID-7ffea71d800559f7b6f8922dca5b713e-None", msg)
        self.assertIn(b"100", msg)
        self.assertIn(b"cf49de8dce0882063532bfe93fe34a29", msg)

    def test_parse_kode_lines_ignores_blanks_comments_and_duplicates(self):
        lines = ["64228258\n", "  # note\n", "\n", "64228259,extra\n", "64228258\n"]

        self.assertEqual(parse_kode_lines(lines), ["64228258", "64228259"])

    def test_flatten_result_keeps_detail_fields_and_first_fund_row(self):
        flat = flatten_result(
            {
                "kode": "64228258",
                "detail": {"Nama Paket": "Belanja", "Total Pagu": "Rp 1.400.000"},
                "sumber_dana": [{"Sumber Dana": "APBD", "MAK": "1.02", "Pagu": "Rp 1.400.000"}],
            }
        )

        self.assertEqual(flat["kode"], "64228258")
        self.assertEqual(flat["Nama Paket"], "Belanja")
        self.assertEqual(flat["sumber_dana_1_Sumber Dana"], "APBD")
        self.assertEqual(flat["sumber_dana_1_MAK"], "1.02")

    def test_scrape_with_retries_returns_error_record_after_failures(self):
        async def always_fail(kode, timeout):
            raise RuntimeError(f"boom {kode}")

        result = asyncio.run(scrape_with_retries("64228258", always_fail, retries=2, timeout=1))

        self.assertEqual(result["kode"], "64228258")
        self.assertEqual(result["status"], "error")
        self.assertIn("boom 64228258", result["error"])

    def test_iter_scrape_listing_pages_rejects_invalid_start_page(self):
        async def collect():
            return [page async for page in iter_scrape_listing_pages(pages=1, start_page=0)]

        with self.assertRaises(ValueError):
            asyncio.run(collect())


if __name__ == "__main__":
    unittest.main()
