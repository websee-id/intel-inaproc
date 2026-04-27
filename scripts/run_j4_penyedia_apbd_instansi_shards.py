#!/usr/bin/env python3
import json
import os
import subprocess
import time
from pathlib import Path


MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "60"))
ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = ROOT / "archives" / "rup-sharded-2026"
LOG_DIR = ROOT / "logs" / "shards-2026-instansi"
CODE_FILES = [
    ARCHIVE_DIR / "j4-penyedia-apbd-instansi-codes.jsonl",
    ARCHIVE_DIR / "j4-penyedia-apbd-instansi-codes-701-999.jsonl",
]


def load_codes() -> list[dict[str, str]]:
    by_code: dict[str, dict[str, str]] = {}
    for path in CODE_FILES:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("valid"):
                by_code[record["code"]] = record
    return [by_code[code] for code in sorted(by_code)]


def has_final_log(shard: str) -> bool:
    path = LOG_DIR / f"{shard}.log"
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8", errors="ignore")
    return '"run_id"' in text and '"status"' in text and '"pages"' in text


def launch(record: dict[str, str]) -> subprocess.Popen:
    code = record["code"]
    shard = f"y2026-j4-Penyedia-APBD-{code}"
    output = ARCHIVE_DIR / f"{shard}.jsonl"
    log_path = LOG_DIR / f"{shard}.log"
    if output.exists():
        output.unlink()
    log = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(
        [
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
            "45",
            "--tahun",
            "2026",
            "--jenis-klpd",
            "4",
            "--instansi",
            code,
            "--sumber",
            "Penyedia",
            "--sumber-dana",
            "APBD",
            "--truncate",
        ],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
    )


def main() -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    records = load_codes()
    children: dict[str, subprocess.Popen] = {}
    pending = {f"y2026-j4-Penyedia-APBD-{record['code']}": record for record in records}

    while True:
        for shard, proc in list(children.items()):
            if proc.poll() is not None:
                children.pop(shard)

        unfinished = {shard: record for shard, record in pending.items() if not has_final_log(shard)}
        if not unfinished and not children:
            break

        slots = max(0, MAX_PARALLEL - len(children))
        for shard, record in list(unfinished.items()):
            if slots <= 0:
                break
            if shard in children:
                continue
            children[shard] = launch(record)
            slots -= 1

        time.sleep(5)


if __name__ == "__main__":
    main()
