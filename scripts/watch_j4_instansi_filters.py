#!/usr/bin/env python3
import argparse
import json
import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ATTEMPT_DIR = ROOT / "archives" / "rup-attempts-2026"
LOG_DIR = ROOT / "logs" / "instansi-filter-shards"
WATCH_LOG = LOG_DIR / "watchdog.log"
FILTERS = [("Swakelola", "APBD"), ("Penyedia", "BLUD")]


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def log_event(event: dict) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    row = {"ts": utc_stamp(), **event}
    line = json.dumps(row, ensure_ascii=False, sort_keys=True)
    with WATCH_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)


def pgrep(pattern: str) -> list[int]:
    result = subprocess.run(["pgrep", "-f", pattern], cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    return [int(line) for line in result.stdout.splitlines() if line.strip().isdigit()]


def runner_pattern(sumber: str, dana: str) -> str:
    return f"run_j4_instansi_filter_shards.py --sumber {sumber} --sumber-dana {dana}"


def worker_pattern(sumber: str, dana: str) -> str:
    return f"inaproc_pg_pipeline.py seed-listing-file .* --sumber {sumber} --sumber-dana {dana}"


def active_count(pattern: str) -> int:
    current = os.getpid()
    return len([pid for pid in pgrep(pattern) if pid != current])


def latest_attempt_mtime() -> float:
    latest = 0.0
    if not ATTEMPT_DIR.exists():
        return latest
    for path in ATTEMPT_DIR.glob("y2026-j4-*-D*.jsonl"):
        try:
            latest = max(latest, path.stat().st_mtime)
        except FileNotFoundError:
            continue
    return latest


def count_split_files() -> dict[str, int]:
    return {
        "j4_swakelola_apbd": len(list(ATTEMPT_DIR.glob("y2026-j4-Swakelola-APBD-D*.jsonl"))),
        "j4_penyedia_blud": len(list(ATTEMPT_DIR.glob("y2026-j4-Penyedia-BLUD-D*.jsonl"))),
    }


def start_runner(sumber: str, dana: str, max_parallel: int, timeout: float) -> str:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = utc_stamp()
    log_path = LOG_DIR / f"supervisor-j4-{sumber.lower()}-{dana.lower()}-{stamp}.log"
    with log_path.open("w", encoding="utf-8") as log:
        subprocess.Popen(
            [
                "python3",
                "scripts/run_j4_instansi_filter_shards.py",
                "--sumber",
                sumber,
                "--sumber-dana",
                dana,
                "--max-parallel",
                str(max_parallel),
                "--timeout",
                str(timeout),
            ],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    return str(log_path.relative_to(ROOT))


def stop_filter(sumber: str, dana: str) -> None:
    current = os.getpid()
    patterns = [runner_pattern(sumber, dana), worker_pattern(sumber, dana)]
    for pattern in patterns:
        for pid in pgrep(pattern):
            if pid == current:
                continue
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                continue


def run_once(args: argparse.Namespace) -> None:
    latest = latest_attempt_mtime()
    stale_seconds = time.time() - latest if latest else None
    stale = stale_seconds is None or stale_seconds >= args.stale_minutes * 60
    files = count_split_files()

    for sumber, dana in FILTERS:
        runners = active_count(runner_pattern(sumber, dana))
        workers = active_count(worker_pattern(sumber, dana))
        event = {
            "event": "j4-watch-check",
            "sumber": sumber,
            "sumber_dana": dana,
            "runners": runners,
            "workers": workers,
            "stale": stale,
            "stale_seconds": int(stale_seconds) if stale_seconds is not None else None,
            **files,
        }
        if runners == 0 and workers == 0:
            log_path = start_runner(sumber, dana, args.max_parallel, args.timeout)
            log_event({**event, "action": "started", "log": log_path})
            continue
        if args.restart_stale and stale:
            stop_filter(sumber, dana)
            time.sleep(args.restart_delay)
            log_path = start_runner(sumber, dana, args.max_parallel, args.timeout)
            log_event({**event, "action": "restarted-stale", "log": log_path})
            continue
        log_event({**event, "action": "keep-running"})


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch and restart productive j4 instansi split runners.")
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--max-parallel", type=int, default=6)
    parser.add_argument("--timeout", type=float, default=45.0)
    parser.add_argument("--stale-minutes", type=int, default=15)
    parser.add_argument("--restart-stale", action="store_true")
    parser.add_argument("--restart-delay", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    while True:
        run_once(args)
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
