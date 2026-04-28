#!/usr/bin/env python3
import argparse
import json
import os
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import resume_incomplete_shards as resume


ROOT = Path(__file__).resolve().parents[1]
ATTEMPT_DIR = ROOT / "archives" / "rup-attempts-2026"
LOG_DIR = ROOT / "logs" / "resume-shards"
WATCH_LOG = LOG_DIR / "watchdog.log"
WORKER_PATTERN = "inaproc_pg_pipeline.py seed-listing-file"
SUPERVISOR_PATTERN = "scripts/resume_incomplete_shards.py"


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


def count_workers() -> int:
    return len([pid for pid in pgrep(WORKER_PATTERN) if pid != os.getpid()])


def count_supervisors() -> int:
    return len([pid for pid in pgrep(SUPERVISOR_PATTERN) if pid != os.getpid()])


def latest_attempt_mtime() -> float:
    latest = 0.0
    if not ATTEMPT_DIR.exists():
        return latest
    for path in ATTEMPT_DIR.glob("*.jsonl"):
        try:
            latest = max(latest, path.stat().st_mtime)
        except FileNotFoundError:
            continue
    return latest


def start_resume(max_parallel: int) -> str:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = utc_stamp()
    log_path = LOG_DIR / f"supervisor-{stamp}.log"
    with log_path.open("w", encoding="utf-8") as log:
        subprocess.Popen(
            ["python3", "scripts/resume_incomplete_shards.py"],
            cwd=ROOT,
            env={**os.environ, "MAX_PARALLEL": str(max_parallel)},
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    return str(log_path.relative_to(ROOT))


def stop_running_scrapers() -> None:
    current = os.getpid()
    for pattern in [SUPERVISOR_PATTERN, WORKER_PATTERN]:
        for pid in pgrep(pattern):
            if pid == current:
                continue
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                continue


def run_once(args: argparse.Namespace) -> None:
    workers = count_workers()
    supervisors = count_supervisors()
    jobs = resume.incomplete_jobs()
    latest_mtime = latest_attempt_mtime()
    stale_seconds = time.time() - latest_mtime if latest_mtime else None
    stale = stale_seconds is None or stale_seconds >= args.stale_minutes * 60
    state = {
        "event": "watch-check",
        "workers": workers,
        "supervisors": supervisors,
        "incomplete_jobs": len(jobs),
        "stale_seconds": int(stale_seconds) if stale_seconds is not None else None,
        "stale": stale,
    }

    if not jobs:
        log_event({**state, "action": "idle-complete"})
        return
    if workers == 0 and supervisors == 0:
        log_path = start_resume(args.max_parallel)
        log_event({**state, "action": "started", "log": log_path, "max_parallel": args.max_parallel})
        return
    if args.restart_stale and stale:
        stop_running_scrapers()
        time.sleep(args.restart_delay)
        log_path = start_resume(args.max_parallel)
        log_event({**state, "action": "restarted-stale", "log": log_path, "max_parallel": args.max_parallel})
        return
    log_event({**state, "action": "keep-running"})


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch INAPROC shard resume workers and restart when idle.")
    parser.add_argument("--interval", type=int, default=900, help="Seconds between checks.")
    parser.add_argument("--max-parallel", type=int, default=8, help="Worker count for new resume supervisors.")
    parser.add_argument("--stale-minutes", type=int, default=20, help="No attempt file changes after this is stale.")
    parser.add_argument("--restart-stale", action="store_true", help="Kill and restart active workers when stale.")
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
