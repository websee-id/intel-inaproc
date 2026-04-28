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


def final_logs(shard: str) -> list[dict]:
    finals = []
    for path in LOG_DIR.glob(f"{shard}-*.log"):
        final = parse_final_log(path)
        if final:
            finals.append(final)
    return finals


def code_is_done(shard: str, max_attempts: int) -> tuple[bool, str]:
    finals = final_logs(shard)
    for final in finals:
        if (
            final.get("status") == "ok"
            and int(final.get("errors") or 0) == 0
            and int(final.get("pages") or 0) > 0
            and int(final.get("rows") or 0) > 0
        ):
            return True, "completed"
    if len(finals) >= max_attempts:
        return True, "exhausted"
    return False, "pending"


def launch(code: str, sumber: str, dana: str, timeout: float) -> subprocess.Popen:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = utc_stamp()
    shard = f"y2026-j4-{sumber}-{dana}-{code}"
    output = ARCHIVE_DIR / f"{shard}-{stamp}.jsonl"
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
    return subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run j4 Kabupaten shards split by instansi code for one sumber/sumber_dana filter.")
    parser.add_argument("--sumber", required=True, choices=["Penyedia", "Swakelola"])
    parser.add_argument("--sumber-dana", required=True, choices=["APBN", "APBNP", "APBD", "APBDP", "PHLN", "PNBP", "BLUD", "GABUNGAN", "LAINNYA"])
    parser.add_argument("--max-parallel", type=int, default=int(os.environ.get("MAX_PARALLEL", "20")))
    parser.add_argument("--max-attempts", type=int, default=3)
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
    children: dict[str, subprocess.Popen] = {}
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
        for code, proc in list(children.items()):
            if proc.poll() is not None:
                children.pop(code)
        while pending and len(children) < args.max_parallel:
            code = pending.pop(0)
            children[code] = launch(code, args.sumber, args.sumber_dana, args.timeout)
            print(json.dumps({"event": "instansi-filter-start", "code": code, "sumber": args.sumber, "sumber_dana": args.sumber_dana}), flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
