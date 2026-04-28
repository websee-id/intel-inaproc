#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = ROOT / "archives" / "rup-attempts-2026"
LOG_DIR = ROOT / "logs" / "instansi-filter-shards"
CODE_FILES = [
    ROOT / "archives" / "rup-sharded-2026" / "j4-penyedia-apbd-instansi-codes.jsonl",
    ROOT / "archives" / "rup-sharded-2026" / "j4-penyedia-apbd-instansi-codes-701-999.jsonl",
]


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_codes() -> list[str]:
    codes: dict[str, bool] = {}
    for path in CODE_FILES:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("valid") and record.get("code"):
                codes[str(record["code"])] = True
    return sorted(codes)


def parse_final_log(path: Path) -> dict:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8", errors="ignore")
    idx = text.rfind('{\n  "run_id"')
    if idx == -1:
        return {}
    try:
        return json.loads(text[idx:])
    except json.JSONDecodeError:
        return {}


def parse_seed_progress(path: Path) -> tuple[int, int | None, int]:
    last_page = 0
    total_pages = None
    rows = 0
    if not path.exists():
        return last_page, total_pages, rows
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if '"event": "seed-page"' not in line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        last_page = max(last_page, int(event.get("page") or 0))
        if event.get("total_pages"):
            total_pages = int(event["total_pages"])
        rows += int(event.get("rows") or 0)
    return last_page, total_pages, rows


def final_logs(shard: str) -> list[dict]:
    finals = []
    for path in LOG_DIR.glob(f"{shard}-*.log"):
        final = parse_final_log(path)
        if final:
            finals.append(final)
    return finals


def shard_logs(shard: str) -> list[Path]:
    return sorted(LOG_DIR.glob(f"{shard}-*.log"), key=lambda path: path.stat().st_mtime)


def code_is_done(shard: str, max_attempts: int) -> tuple[bool, str]:
    logs = shard_logs(shard)
    completed_pages: set[int] = set()
    known_total_pages = None
    finals = []
    for path in logs:
        last_page, total_pages, _rows = parse_seed_progress(path)
        if total_pages:
            known_total_pages = max(known_total_pages or 0, total_pages)
        if last_page:
            completed_pages.update(range(1, last_page + 1))
        final = parse_final_log(path)
        if final:
            finals.append(final)
    for final in finals:
        if (
            final.get("status") == "ok"
            and int(final.get("errors") or 0) == 0
            and int(final.get("pages") or 0) > 0
            and int(final.get("rows") or 0) > 0
        ):
            return True, "completed"
    if known_total_pages and all(page in completed_pages for page in range(1, known_total_pages + 1)):
        return True, "completed"
    if len(finals) >= max_attempts:
        return True, "exhausted"
    return False, "pending"


def next_start_page(shard: str) -> int:
    last_page = 0
    for path in shard_logs(shard):
        page, _total_pages, _rows = parse_seed_progress(path)
        last_page = max(last_page, page)
    return last_page + 1


def launch(code: str, sumber: str, dana: str, timeout: float) -> tuple[subprocess.Popen, Path, Path, int]:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = utc_stamp()
    shard = f"y2026-j4-{sumber}-{dana}-{code}"
    start_page = next_start_page(shard)
    suffix = f"from-{start_page}-{stamp}" if start_page > 1 else stamp
    output = ARCHIVE_DIR / f"{shard}-{suffix}.jsonl"
    log_path = LOG_DIR / f"{shard}-{stamp}.log"
    log = log_path.open("w", encoding="utf-8")
    cmd = [
        "python3",
        "inaproc_pg_pipeline.py",
        "seed-listing-file",
        "--output",
        str(output.relative_to(ROOT)),
        "--max-pages",
        "999999",
        "--page-size",
        "100",
        "--timeout",
        str(timeout),
        "--start-page",
        str(start_page),
        "--tahun",
        "2026",
        "--jenis-klpd",
        "4",
        "--instansi",
        code,
        "--sumber",
        sumber,
        "--sumber-dana",
        dana,
        "--truncate",
    ]
    return subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT), output, log_path, start_page


def stop_child(proc: subprocess.Popen, grace_seconds: float = 5.0) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=grace_seconds)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def latest_activity(paths: list[Path], fallback: float) -> float:
    latest = fallback
    for path in paths:
        try:
            latest = max(latest, path.stat().st_mtime)
        except FileNotFoundError:
            continue
    return latest


def main() -> None:
    parser = argparse.ArgumentParser(description="Run j4 Kabupaten shards split by instansi code for one sumber/sumber_dana filter.")
    parser.add_argument("--sumber", required=True, choices=["Penyedia", "Swakelola"])
    parser.add_argument("--sumber-dana", required=True, choices=["APBN", "APBNP", "APBD", "APBDP", "PHLN", "PNBP", "BLUD", "GABUNGAN", "LAINNYA"])
    parser.add_argument("--max-parallel", type=int, default=int(os.environ.get("MAX_PARALLEL", "20")))
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--child-timeout", type=float, default=300.0, help="Kill a child only after this many seconds without output/log activity.")
    parser.add_argument("--timeout", type=float, default=45.0)
    parser.add_argument("--limit", type=int, help="Limit number of codes for testing.")
    args = parser.parse_args()

    codes = load_codes()
    if args.limit:
        codes = codes[: args.limit]
    status_counts = {"completed": 0, "exhausted": 0, "pending": 0}
    pending = []
    for code in codes:
        done, status = code_is_done(f"y2026-j4-{args.sumber}-{args.sumber_dana}-{code}", args.max_attempts)
        status_counts[status] += 1
        if not done:
            pending.append(code)
    children: dict[str, tuple[subprocess.Popen, float, Path, Path]] = {}
    print(
        json.dumps(
            {
                "event": "instansi-filter-manifest",
                "jobs": len(pending),
                "max_parallel": args.max_parallel,
                "max_attempts": args.max_attempts,
                "sumber": args.sumber,
                "sumber_dana": args.sumber_dana,
                **status_counts,
            }
        ),
        flush=True,
    )

    while pending or children:
        now = time.monotonic()
        for code, (proc, started_at, output_path, log_path) in list(children.items()):
            if proc.poll() is not None:
                children.pop(code)
                continue
            last_activity = latest_activity([output_path, log_path], started_at)
            if time.time() - last_activity >= args.child_timeout:
                stop_child(proc)
                children.pop(code)
                print(
                    json.dumps(
                        {
                            "event": "instansi-filter-child-timeout",
                            "code": code,
                            "sumber": args.sumber,
                            "sumber_dana": args.sumber_dana,
                            "child_timeout": args.child_timeout,
                            "idle_seconds": int(time.time() - last_activity),
                        }
                    ),
                    flush=True,
                )
        while pending and len(children) < args.max_parallel:
            code = pending.pop(0)
            proc, output_path, log_path, start_page = launch(code, args.sumber, args.sumber_dana, args.timeout)
            children[code] = (proc, time.time(), output_path, log_path)
            print(json.dumps({"event": "instansi-filter-start", "code": code, "start_page": start_page, "sumber": args.sumber, "sumber_dana": args.sumber_dana}), flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
