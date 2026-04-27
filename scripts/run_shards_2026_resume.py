#!/usr/bin/env python3
import os
import subprocess
import time
from pathlib import Path


MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "40"))
ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = ROOT / "archives" / "rup-sharded-2026"
LOG_DIR = ROOT / "logs" / "shards-2026"
FUNDS = ["APBN", "APBNP", "APBD", "APBDP", "PHLN", "PNBP", "BLUD", "GABUNGAN", "LAINNYA"]
SOURCES = ["Penyedia", "Swakelola"]


def all_shards() -> list[tuple[str, str, str, str]]:
    shards = []
    for jenis in ["1", "2", "3", "4", "5"]:
        for sumber in SOURCES:
            for dana in FUNDS:
                name = f"y2026-j{jenis}-{sumber}-{dana}"
                shards.append((name, jenis, sumber, dana))
    return shards


def running_shards() -> set[str]:
    result = subprocess.run(
        ["pgrep", "-af", "python3 inaproc_pg_pipeline.py seed-listing-file"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    running = set()
    for line in result.stdout.splitlines():
        marker = "--output archives/rup-sharded-2026/"
        if marker not in line:
            continue
        shard = line.split(marker, 1)[1].split(".jsonl", 1)[0]
        running.add(shard)
    return running


def shard_has_final_log(shard: str) -> bool:
    log_path = LOG_DIR / f"{shard}.log"
    if not log_path.exists():
        return False
    text = log_path.read_text(encoding="utf-8", errors="ignore")
    return '"run_id"' in text and '"status"' in text and '"pages"' in text


def launch(shard: str, jenis: str, sumber: str, dana: str) -> subprocess.Popen:
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
            jenis,
            "--sumber",
            sumber,
            "--sumber-dana",
            dana,
            "--truncate",
        ],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
    )


def main() -> None:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    pending = all_shards()
    children: dict[str, subprocess.Popen] = {}

    while True:
        for shard, proc in list(children.items()):
            if proc.poll() is not None:
                children.pop(shard)

        running = running_shards() | set(children)
        unfinished = [(name, jenis, sumber, dana) for name, jenis, sumber, dana in pending if not shard_has_final_log(name)]
        if not unfinished and not running:
            break

        slots = max(0, MAX_PARALLEL - len(running))
        for name, jenis, sumber, dana in unfinished:
            if slots <= 0:
                break
            if name in running:
                continue
            children[name] = launch(name, jenis, sumber, dana)
            running.add(name)
            slots -= 1

        time.sleep(5)


if __name__ == "__main__":
    main()
