#!/usr/bin/env python3
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "20"))
ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = ROOT / "archives" / "rup-sharded-2026"
ATTEMPT_DIR = ROOT / "archives" / "rup-attempts-2026"
RESUME_LOG_DIR = ROOT / "logs" / "resume-shards"
FUNDS = ["APBN", "APBNP", "APBD", "APBDP", "PHLN", "PNBP", "BLUD", "GABUNGAN", "LAINNYA"]
SOURCES = ["Penyedia", "Swakelola"]


def parse_log(path: Path) -> dict:
    final = None
    last = None
    text = path.read_text(encoding="utf-8", errors="ignore") if path.exists() else ""
    for line in text.splitlines():
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("event") == "seed-page":
            last = obj
    idx = text.rfind('{\n  "run_id"')
    if idx != -1:
        try:
            final = json.loads(text[idx:])
        except json.JSONDecodeError:
            final = None
    page = (last or {}).get("page") or (final or {}).get("pages") or 0
    total_pages = (last or {}).get("total_pages") or 0
    status = (final or {}).get("status")
    errors = (final or {}).get("errors") or 0
    rows = (last or {}).get("total_rows") or (final or {}).get("rows") or 0
    complete = bool(status == "ok" and errors == 0 and total_pages and page >= total_pages)
    empty = bool(status == "ok" and rows == 0 and errors > 0 and not total_pages)
    return {"page": page, "total_pages": total_pages, "complete": complete, "status": status, "errors": errors, "rows": rows, "empty": empty}


def merge_log_states(paths: list[Path]) -> dict:
    states = [parse_log(path) for path in paths if path.exists()]
    if not states:
        return {"page": 0, "total_pages": 0, "complete": False, "status": None, "errors": 0, "rows": 0, "empty": False}
    total_pages = max((state.get("total_pages") or 0) for state in states)
    page = max((state.get("page") or 0) for state in states)
    complete = any(state.get("complete") for state in states) or bool(total_pages and page >= total_pages)
    empty = all(state.get("empty") for state in states if state.get("status")) and not page
    best = max(states, key=lambda state: (state.get("page") or 0, state.get("rows") or 0))
    return {
        "page": page,
        "total_pages": total_pages,
        "complete": complete,
        "status": best.get("status"),
        "errors": best.get("errors") or 0,
        "rows": max((state.get("rows") or 0) for state in states),
        "empty": empty,
    }


def log_paths_for_job(job: dict) -> list[Path]:
    paths = [job["log_dir"] / f"{job['name']}.log"]
    paths.extend(sorted(RESUME_LOG_DIR.glob(f"{job['name']}-from-*.log")))
    return paths


def job_priority(job: dict) -> tuple[int, int, int, str]:
    start_page = int(job.get("start_page") or 1)
    total_pages = int(job.get("total_pages") or 0)
    is_instansi = 0 if job.get("instansi") else 1
    return (start_page, is_instansi, total_pages, job["name"])


def base_shards() -> list[dict]:
    jobs = []
    for jenis in ["1", "2", "3", "4", "5"]:
        for sumber in SOURCES:
            for dana in FUNDS:
                name = f"y2026-j{jenis}-{sumber}-{dana}"
                if name == "y2026-j4-Penyedia-APBD":
                    continue
                jobs.append({"name": name, "jenis": jenis, "sumber": sumber, "dana": dana, "instansi": None, "log_dir": ROOT / "logs" / "shards-2026"})
    return jobs


def instansi_shards() -> list[dict]:
    jobs = []
    for log_path in sorted((ROOT / "logs" / "shards-2026-instansi").glob("y2026-j4-Penyedia-APBD-D*.log")):
        match = re.search(r"-(D\d+)\.log$", log_path.name)
        if not match:
            continue
        code = match.group(1)
        jobs.append({"name": log_path.stem, "jenis": "4", "sumber": "Penyedia", "dana": "APBD", "instansi": code, "log_dir": ROOT / "logs" / "shards-2026-instansi"})
    return jobs


def incomplete_jobs() -> list[dict]:
    jobs = []
    for job in base_shards() + instansi_shards():
        state = merge_log_states(log_paths_for_job(job))
        if state["empty"]:
            continue
        if state["complete"]:
            continue
        if not state["total_pages"] and state["rows"] == 0:
            continue
        if state["total_pages"] and state["page"] >= state["total_pages"] and state["errors"] == 0:
            continue
        job = {**job, **state, "start_page": max(1, (state["page"] or 0) + 1)}
        jobs.append(job)
    return sorted(jobs, key=job_priority)


def launch(job: dict) -> subprocess.Popen:
    RESUME_LOG_DIR.mkdir(parents=True, exist_ok=True)
    ATTEMPT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = ATTEMPT_DIR / f"{job['name']}-from-{job['start_page']}-{stamp}.jsonl"
    log_path = RESUME_LOG_DIR / f"{job['name']}-from-{job['start_page']}-{stamp}.log"
    cmd = [
        "python3",
        "inaproc_pg_pipeline.py",
        "seed-listing-file",
        "--output",
        str(output.relative_to(ROOT)),
        "--max-pages",
        "999999",
        "--start-page",
        str(job["start_page"]),
        "--page-size",
        "100",
        "--timeout",
        "60",
        "--tahun",
        "2026",
        "--jenis-klpd",
        job["jenis"],
        "--sumber",
        job["sumber"],
        "--sumber-dana",
        job["dana"],
        "--truncate",
    ]
    if job.get("instansi"):
        cmd.extend(["--instansi", job["instansi"]])
    log = log_path.open("w", encoding="utf-8")
    return subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)


def main() -> None:
    children: dict[str, subprocess.Popen] = {}
    pending = incomplete_jobs()
    manifest = RESUME_LOG_DIR / "manifest.jsonl"
    RESUME_LOG_DIR.mkdir(parents=True, exist_ok=True)
    manifest_rows = []
    for job in pending:
        row = {key: (str(value) if isinstance(value, Path) else value) for key, value in job.items()}
        manifest_rows.append(json.dumps(row, ensure_ascii=False))
    manifest.write_text("\n".join(manifest_rows) + "\n", encoding="utf-8")
    print(json.dumps({"event": "resume-manifest", "jobs": len(pending), "max_parallel": MAX_PARALLEL}), flush=True)

    while pending or children:
        for name, proc in list(children.items()):
            if proc.poll() is not None:
                children.pop(name)
        while pending and len(children) < MAX_PARALLEL:
            job = pending.pop(0)
            children[job["name"]] = launch(job)
            print(json.dumps({"event": "resume-start", "name": job["name"], "start_page": job["start_page"], "total_pages": job.get("total_pages")}), flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
