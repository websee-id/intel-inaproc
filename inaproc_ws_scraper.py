#!/usr/bin/env python3
import argparse
import asyncio
import json
import re
import sys
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import websockets


STREAM_URL = "wss://data.inaproc.id/_stcore/stream"
APP_URL = "https://data.inaproc.id/rup"
PAGE_SCRIPT_HASH = "cf49de8dce0882063532bfe93fe34a29"
NEXT_PAGE_WIDGET_ID = "$$ID-ca8f81b90b63b495f6603fe5090838d2-None"
ENTRY_PER_PAGE_WIDGET_ID = "$$ID-7ffea71d800559f7b6f8922dca5b713e-None"
TAHUN_WIDGET_ID = "$$ID-865b4ac13685d129b5c95b48fca4990a-None"
JENIS_KLPD_WIDGET_ID = "$$ID-300b63c2241c35c8aeeee621c202f3f5-None"
CARA_PENGADAAN_WIDGET_ID = "$$ID-adc099fb56d35441d8fec9c51f2de628-None"
SUMBER_DANA_WIDGET_ID = "$$ID-532e3e12f0242b2b67367be2c65a17a6-None"
JENIS_KLPD_VALUES = {
    "1": "Kementerian",
    "2": "Lembaga",
    "3": "Provinsi",
    "4": "Kabupaten",
    "5": "Kota",
}
JENIS_KLPD_PREFIXES = {
    "1": ("KEMENTERIAN",),
    "2": ("LEMBAGA",),
    "3": ("PROVINSI",),
    "4": ("KAB.", "KABUPATEN"),
    "5": ("KOTA",),
}


def _varint(value: int) -> bytes:
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _field_varint(field_number: int, value: int) -> bytes:
    return _varint((field_number << 3) | 0) + _varint(value)


def _field_bytes(field_number: int, value: bytes | str) -> bytes:
    if isinstance(value, str):
        value = value.encode("utf-8")
    return _varint((field_number << 3) | 2) + _varint(len(value)) + value


def build_rerun_message(kode: str, page: str = "rup", sumber: str | None = None) -> bytes:
    """Build the minimal Streamlit BackMsg frame that asks the app to rerun.

    This mirrors the browser frame observed from the Streamlit frontend:
    top-level field 11 contains query string, page name, and page config.
    """
    page_state = b"".join(
        [
            _field_bytes(1, "UTC"),
            _field_varint(2, 0),
            _field_bytes(3, "en-US"),
            _field_bytes(4, APP_URL),
            _field_varint(5, 0),
            _field_bytes(6, "light"),
        ]
    )
    query = f"kode={kode}"
    if sumber:
        query = f"{query}&sumber={sumber}"
    rerun = b"".join(
        [
            _field_bytes(1, query),
            _field_bytes(2, b""),
            _field_bytes(3, b""),
            _field_bytes(4, page),
            _field_bytes(8, page_state),
        ]
    )
    return _field_bytes(11, rerun)


def _app_url(query_string: str = "") -> str:
    return f"{APP_URL}?{query_string}" if query_string else APP_URL


def build_listing_query(
    tahun: str | None = None,
    jenis_klpd: str | int | None = None,
    instansi: str | None = None,
    sumber: str | None = None,
    sumber_dana: str | None = None,
) -> str:
    params: dict[str, str] = {}
    if tahun:
        params["tahun"] = str(tahun)
    if jenis_klpd:
        params["jenis_klpd"] = str(jenis_klpd)
    if instansi:
        params["instansi"] = instansi
    if sumber:
        params["sumber"] = sumber
    if sumber_dana:
        params["sumber_dana"] = sumber_dana
    return urlencode(params)


def effective_listing_filters(
    tahun: str | None = None,
    jenis_klpd: str | int | None = None,
    instansi: str | None = None,
    sumber: str | None = None,
    sumber_dana: str | None = None,
) -> dict[str, str]:
    filters = {
        "tahun": str(tahun) if tahun else "",
        "jenis_klpd": str(jenis_klpd) if jenis_klpd else "",
        "instansi": instansi or "",
        "sumber": sumber or "",
        "sumber_dana": sumber_dana or "",
    }
    if filters["tahun"] == "2026":
        filters["tahun"] = ""
    return {key: value for key, value in filters.items() if value}


def build_main_page_message(query_string: str = "") -> bytes:
    return _field_bytes(
        11,
        b"".join(
            [
                _field_bytes(1, query_string),
                _field_bytes(2, b""),
                _field_bytes(3, b""),
                _field_bytes(4, "rup"),
                _field_bytes(8, _build_context_info(_app_url(query_string))),
            ]
        ),
    )


def build_button_click_message(widget_id: str, query_string: str = "") -> bytes:
    widget_state = _field_bytes(
        1,
        b"".join(
            [
                _field_bytes(1, widget_id),
                _field_varint(2, 1),
            ]
        ),
    )
    return _field_bytes(
        11,
        b"".join(
            [
                _field_bytes(1, query_string),
                _field_bytes(2, widget_state),
                _field_bytes(3, PAGE_SCRIPT_HASH),
                _field_bytes(4, b""),
                _field_bytes(5, b""),
                _field_bytes(8, _build_context_info(_app_url(query_string))),
            ]
        ),
    )


def build_entry_per_page_message(page_size: int, query_string: str = "") -> bytes:
    if page_size not in {20, 50, 100}:
        raise ValueError("page_size must be one of: 20, 50, 100")
    widget_state = _field_bytes(
        1,
        b"".join(
            [
                _field_bytes(1, ENTRY_PER_PAGE_WIDGET_ID),
                _field_bytes(6, str(page_size)),
            ]
        ),
    )
    return _field_bytes(
        11,
        b"".join(
            [
                _field_bytes(1, query_string),
                _field_bytes(2, widget_state),
                _field_bytes(3, PAGE_SCRIPT_HASH),
                _field_bytes(4, b""),
                _field_bytes(5, b""),
                _field_bytes(8, _build_context_info(_app_url(query_string))),
            ]
        ),
    )


def _select_state(widget_id: str, value: str) -> bytes:
    return _field_bytes(
        1,
        b"".join(
            [
                _field_bytes(1, widget_id),
                _field_bytes(6, value),
            ]
        ),
    )


def build_listing_filter_state_message(
    page_size: int = 100,
    tahun: str | None = None,
    jenis_klpd: str | int | None = None,
    sumber: str | None = None,
    sumber_dana: str | None = None,
) -> bytes:
    states = [_select_state(ENTRY_PER_PAGE_WIDGET_ID, str(page_size))]
    if tahun:
        states.append(_select_state(TAHUN_WIDGET_ID, str(tahun)))
    if jenis_klpd:
        states.append(_select_state(JENIS_KLPD_WIDGET_ID, JENIS_KLPD_VALUES[str(jenis_klpd)]))
    if sumber:
        states.append(_select_state(CARA_PENGADAAN_WIDGET_ID, sumber))
    if sumber_dana:
        states.append(_select_state(SUMBER_DANA_WIDGET_ID, sumber_dana))
    return _field_bytes(
        11,
        b"".join(
            [
                _field_bytes(1, ""),
                _field_bytes(2, b"".join(states)),
                _field_bytes(3, PAGE_SCRIPT_HASH),
                _field_bytes(4, b""),
                _field_bytes(5, b""),
                _field_bytes(8, _build_context_info()),
            ]
        ),
    )


def _build_context_info(url: str = APP_URL) -> bytes:
    return b"".join(
        [
            _field_bytes(1, "UTC"),
            _field_varint(2, 0),
            _field_bytes(3, "en-US"),
            _field_bytes(4, url),
            _field_varint(5, 0),
            _field_bytes(6, "light"),
        ]
    )


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._row = []
        elif tag in {"th", "td"} and self._row is not None:
            self._cell = []
        elif tag == "br" and self._cell is not None:
            self._cell.append(" ")

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"th", "td"} and self._cell is not None and self._row is not None:
            text = " ".join("".join(self._cell).split())
            self._row.append(unescape(text))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None


def _table_kind(headers: list[str]) -> str:
    if headers == ["No.", "Deskripsi", "Detail"]:
        return "detail"
    if "Kode RUP" in headers and "Nama Paket" in headers:
        return "listing"
    if "Sumber Dana" in headers:
        return "sumber_dana"
    return "table"


def _parse_table(html: str) -> dict[str, Any] | None:
    parser = _TableParser()
    parser.feed(html)
    if not parser.rows:
        return None
    headers = parser.rows[0]
    rows = []
    for row in parser.rows[1:]:
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        rows.append(dict(zip(headers, row[: len(headers)])))
    return {"kind": _table_kind(headers), "headers": headers, "rows": rows}


def extract_tables_from_payload(payload: bytes) -> list[dict[str, Any]]:
    text = payload.decode("utf-8", errors="ignore")
    tables = []
    for match in re.finditer(r"<table\b.*?</table>", text, flags=re.IGNORECASE | re.DOTALL):
        parsed = _parse_table(match.group(0))
        if parsed:
            tables.append(parsed)
    return tables


def extract_page_info_from_payload(payload: bytes) -> dict[str, int] | None:
    text = payload.decode("utf-8", errors="ignore")
    match = re.search(r"Halaman\s+([0-9.]+)\s+dari\s+([0-9.]+)", text)
    if not match:
        return None
    return {
        "page": int(match.group(1).replace(".", "")),
        "total_pages": int(match.group(2).replace(".", "")),
    }


def listing_row_matches_filters(row: dict[str, str], filters: dict[str, str]) -> bool:
    if filters.get("tahun") and row.get("Tahun Anggaran") != filters["tahun"]:
        return False
    if filters.get("sumber") and row.get("Cara Pengadaan") != filters["sumber"]:
        return False
    if filters.get("sumber_dana") and row.get("Sumber Dana") != filters["sumber_dana"]:
        return False
    jenis_klpd = filters.get("jenis_klpd")
    if jenis_klpd:
        nama_instansi = row.get("Nama Instansi", "").upper()
        if not nama_instansi.startswith(JENIS_KLPD_PREFIXES[jenis_klpd]):
            return False
    return True


def listing_rows_match_filters(rows: list[dict[str, str]], filters: dict[str, str]) -> bool:
    return bool(rows) and all(listing_row_matches_filters(row, filters) for row in rows)


def parse_kode_lines(lines: list[str]) -> list[str]:
    kodes = []
    seen = set()
    for line in lines:
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        value = value.split(",", 1)[0].strip()
        if value and value not in seen:
            seen.add(value)
            kodes.append(value)
    return kodes


def flatten_result(result: dict[str, Any]) -> dict[str, Any]:
    flat = {
        "kode": result.get("kode", ""),
        "status": result.get("status", "ok"),
    }
    flat.update(result.get("detail", {}))
    for idx, row in enumerate(result.get("sumber_dana", []), start=1):
        for key, value in row.items():
            flat[f"sumber_dana_{idx}_{key}"] = value
    if "error" in result:
        flat["error"] = result["error"]
    return flat


async def scrape_detail(kode: str, timeout: float = 20.0, sumber: str | None = None) -> dict[str, Any]:
    query = f"kode={kode}" + (f"&sumber={sumber}" if sumber else "")
    headers = {
        "Origin": "https://data.inaproc.id",
        "Referer": f"{APP_URL}?{query}",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    tables: list[dict[str, Any]] = []
    async with websockets.connect(
        STREAM_URL,
        additional_headers=headers,
        open_timeout=timeout,
        max_size=20_000_000,
    ) as ws:
        await ws.send(build_rerun_message(kode, sumber=sumber))
        while True:
            try:
                message = await asyncio.wait_for(ws.recv(), timeout=timeout)
            except TimeoutError:
                break
            if isinstance(message, str):
                payload = message.encode("utf-8")
            else:
                payload = message
            for table in extract_tables_from_payload(payload):
                if table["kind"] not in {t["kind"] for t in tables}:
                    tables.append(table)
            if {"detail", "sumber_dana"}.issubset({t["kind"] for t in tables}):
                break

    detail = next((t for t in tables if t["kind"] == "detail"), None)
    sumber_dana = next((t for t in tables if t["kind"] == "sumber_dana"), None)
    return {
        "kode": kode,
        "sumber": sumber,
        "status": "ok" if detail else "missing",
        "detail": {
            row["Deskripsi"]: row["Detail"]
            for row in (detail or {}).get("rows", [])
            if "Deskripsi" in row and "Detail" in row
        },
        "sumber_dana": (sumber_dana or {}).get("rows", []),
        "tables_found": [table["kind"] for table in tables],
    }


async def _read_listing_page(
    ws: Any,
    target_page: int,
    timeout: float,
    settle_seconds: float = 1.0,
    expected_rows: int | None = None,
    expected_filters: dict[str, str] | None = None,
) -> dict[str, Any]:
    deadline = asyncio.get_event_loop().time() + timeout
    last_listing: dict[str, Any] | None = None
    page_info: dict[str, int] | None = None
    matched_at: float | None = None

    while asyncio.get_event_loop().time() < deadline:
        now = asyncio.get_event_loop().time()
        remaining = max(0.1, deadline - now)
        recv_timeout = remaining
        if matched_at and last_listing:
            settle_remaining = settle_seconds - (now - matched_at)
            if settle_remaining <= 0:
                break
            recv_timeout = min(remaining, max(0.1, settle_remaining))
        try:
            message = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
        except TimeoutError:
            if matched_at and last_listing:
                break
            break
        payload = message.encode("utf-8") if isinstance(message, str) else message
        for table in extract_tables_from_payload(payload):
            if table["kind"] == "listing":
                last_listing = table
        info = extract_page_info_from_payload(payload)
        if info:
            page_info = info
            if info["page"] == target_page:
                matched_at = asyncio.get_event_loop().time()
        has_expected_rows = (
            expected_rows is None
            or last_listing is not None
            and len(last_listing.get("rows", [])) >= expected_rows
        )
        has_expected_filters = (
            not expected_filters
            or last_listing is not None
            and listing_rows_match_filters(last_listing.get("rows", []), expected_filters)
        )
        if matched_at and last_listing and has_expected_rows and has_expected_filters and asyncio.get_event_loop().time() - matched_at >= settle_seconds:
            break

    if not page_info or page_info["page"] != target_page or not last_listing:
        return {
            "page": target_page,
            "status": "error",
            "error": f"did not receive listing page {target_page}",
            "rows": [],
        }
    if expected_rows is not None and len(last_listing.get("rows", [])) < expected_rows:
        return {
            "page": target_page,
            "status": "error",
            "error": f"did not receive listing page {target_page} with {expected_rows} rows",
            "rows": [],
        }
    if expected_filters and not listing_rows_match_filters(last_listing.get("rows", []), expected_filters):
        return {
            "page": target_page,
            "status": "error",
            "error": f"did not receive listing page {target_page} matching filters",
            "rows": [],
        }

    return {
        "page": page_info["page"],
        "total_pages": page_info["total_pages"],
        "status": "ok",
        "rows": last_listing["rows"],
    }


async def scrape_listing_pages(
    pages: int = 1,
    timeout: float = 20.0,
    page_size: int = 100,
    start_page: int = 1,
    query_string: str = "",
    listing_filters: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    return [
        page_result
        async for page_result in iter_scrape_listing_pages(
            pages=pages,
            timeout=timeout,
            page_size=page_size,
            start_page=start_page,
            query_string=query_string,
            listing_filters=listing_filters,
        )
    ]


async def iter_scrape_listing_pages(
    pages: int = 1,
    timeout: float = 20.0,
    page_size: int = 100,
    start_page: int = 1,
    query_string: str = "",
    listing_filters: dict[str, str] | None = None,
):
    if pages < 1:
        return
    if start_page < 1:
        raise ValueError("start_page must be >= 1")

    headers = {
        "Origin": "https://data.inaproc.id",
        "Referer": _app_url(query_string),
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    async with websockets.connect(
        STREAM_URL,
        additional_headers=headers,
        open_timeout=timeout,
        max_size=50_000_000,
    ) as ws:
        end_page = start_page + pages - 1
        listing_filters = {key: value for key, value in (listing_filters or {}).items() if value}
        use_state_filters = bool(listing_filters.get("tahun"))
        initial_query = "" if use_state_filters else query_string
        await ws.send(build_main_page_message(initial_query))
        if page_size != 20:
            await _read_listing_page(ws, 1, timeout)
            if use_state_filters:
                await ws.send(build_listing_filter_state_message(page_size=page_size, **listing_filters))
            else:
                await ws.send(build_entry_per_page_message(page_size, query_string))
        elif use_state_filters:
            await _read_listing_page(ws, 1, timeout)
            await ws.send(build_listing_filter_state_message(page_size=page_size, **listing_filters))
        page_result = await _read_listing_page(
            ws,
            1,
            timeout,
            expected_rows=page_size if page_size != 20 else None,
            expected_filters=listing_filters if use_state_filters else None,
        )
        if start_page == 1:
            yield page_result
        for page in range(2, end_page + 1):
            await ws.send(build_button_click_message(NEXT_PAGE_WIDGET_ID, initial_query))
            page_result = await _read_listing_page(ws, page, timeout)
            if page >= start_page:
                yield page_result
            if page_result.get("status") != "ok":
                break


async def scrape_with_retries(
    kode: str,
    scraper=scrape_detail,
    retries: int = 2,
    timeout: float = 20.0,
) -> dict[str, Any]:
    last_error = None
    for attempt in range(retries + 1):
        try:
            return await scraper(kode, timeout)
        except Exception as exc:  # noqa: BLE001 - CLI scraper records per-code failures.
            last_error = exc
            if attempt < retries:
                await asyncio.sleep(min(2**attempt, 5))
    return {"kode": kode, "status": "error", "error": str(last_error)}


async def scrape_many(
    kodes: list[str],
    concurrency: int = 5,
    retries: int = 2,
    timeout: float = 20.0,
) -> list[dict[str, Any]]:
    semaphore = asyncio.Semaphore(concurrency)

    async def run_one(kode: str) -> dict[str, Any]:
        async with semaphore:
            return await scrape_with_retries(kode, retries=retries, timeout=timeout)

    return await asyncio.gather(*(run_one(kode) for kode in kodes))


def _load_kodes(args: argparse.Namespace) -> list[str]:
    kodes = []
    if args.kode:
        kodes.extend(args.kode)
    if args.input:
        kodes.extend(parse_kode_lines(Path(args.input).read_text(encoding="utf-8").splitlines()))
    if not kodes and not sys.stdin.isatty():
        kodes.extend(parse_kode_lines(sys.stdin.read().splitlines()))
    return parse_kode_lines(kodes)


def _write_results(results: list[dict[str, Any]], output: str | None, pretty: bool) -> None:
    lines = [json.dumps(flatten_result(result), ensure_ascii=False) for result in results]
    text = "\n".join(lines) + ("\n" if lines else "")
    if pretty and len(results) == 1:
        text = json.dumps(results[0], ensure_ascii=False, indent=2) + "\n"
    if output:
        Path(output).write_text(text, encoding="utf-8")
    else:
        print(text, end="")


def _write_listing_results(results: list[dict[str, Any]], output: str | None) -> None:
    lines = []
    for page_result in results:
        if page_result.get("status") != "ok":
            lines.append(json.dumps(page_result, ensure_ascii=False))
            continue
        for row in page_result["rows"]:
            record = {"page": page_result["page"], "total_pages": page_result["total_pages"], **row}
            lines.append(json.dumps(record, ensure_ascii=False))
    text = "\n".join(lines) + ("\n" if lines else "")
    if output:
        Path(output).write_text(text, encoding="utf-8")
    else:
        print(text, end="")


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape INAPROC RUP detail via raw Streamlit WebSocket.")
    parser.add_argument("kode", nargs="*", help="Kode RUP, e.g. 64228258")
    parser.add_argument("-i", "--input", help="File containing one kode per line, comments allowed with #")
    parser.add_argument("-o", "--output", help="Output JSONL path. Defaults to stdout.")
    parser.add_argument("-c", "--concurrency", type=int, default=5)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--pretty", action="store_true", help="Pretty-print a single result.")
    parser.add_argument("--list-pages", type=int, help="Scrape N listing pages from /rup instead of detail pages.")
    parser.add_argument("--page-size", type=int, default=100, choices=[20, 50, 100])
    parser.add_argument("--start-page", type=int, default=1, help="First listing page to emit when using --list-pages.")
    parser.add_argument("--tahun", help="Listing filter, e.g. 2026.")
    parser.add_argument("--jenis-klpd", choices=["1", "2", "3", "4", "5"], help="Listing filter: 1 Kementerian, 2 Lembaga, 3 Provinsi, 4 Kabupaten, 5 Kota.")
    parser.add_argument("--instansi", help="Listing filter by INAPROC instansi code, e.g. D108.")
    parser.add_argument("--sumber", choices=["Penyedia", "Swakelola"], help="Listing filter for Cara Pengadaan.")
    parser.add_argument("--sumber-dana", choices=["APBN", "APBNP", "APBD", "APBDP", "PHLN", "PNBP", "BLUD", "GABUNGAN", "LAINNYA"], help="Listing filter for Sumber Dana.")
    args = parser.parse_args()
    if args.list_pages:
        listing_filters = effective_listing_filters(
            tahun=args.tahun,
            jenis_klpd=args.jenis_klpd,
            instansi=args.instansi,
            sumber=args.sumber,
            sumber_dana=args.sumber_dana,
        )
        query_string = build_listing_query(
            tahun=listing_filters.get("tahun"),
            jenis_klpd=listing_filters.get("jenis_klpd"),
            instansi=listing_filters.get("instansi"),
            sumber=listing_filters.get("sumber"),
            sumber_dana=listing_filters.get("sumber_dana"),
        )
        results = asyncio.run(
            scrape_listing_pages(
                args.list_pages,
                timeout=args.timeout,
                page_size=args.page_size,
                start_page=args.start_page,
                query_string=query_string,
                listing_filters=listing_filters,
            )
        )
        _write_listing_results(results, args.output)
        return
    kodes = _load_kodes(args)
    if not kodes:
        parser.error("provide kode arguments, --input file, or stdin")
    results = asyncio.run(
        scrape_many(
            kodes,
            concurrency=max(1, args.concurrency),
            retries=max(0, args.retries),
            timeout=args.timeout,
        )
    )
    _write_results(results, args.output, args.pretty)


if __name__ == "__main__":
    main()
