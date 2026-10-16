"""Measure API admission and end-to-end completion separately; no external dependencies."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import platform
import time
import uuid
from api import BASE, call, submission, wait_health

parser = argparse.ArgumentParser()
parser.add_argument("--count", type=int, default=100)
parser.add_argument("--concurrency", type=int, default=8)
parser.add_argument("--output", default="target/benchmark.json")
args = parser.parse_args()
if not 1 <= args.count <= 1000 or not 1 <= args.concurrency <= 128:
    parser.error("count must be 1..1000; concurrency must be 1..128")
wait_health()
prefix = "bench-" + str(uuid.uuid4())


def submit(index):
    started = time.perf_counter()
    task = call("POST", "/api/tasks", submission(f"{prefix}-{index}", "sha256", {"text": "x" * 256}))
    return task["id"], (time.perf_counter() - started) * 1000


started = time.perf_counter()
with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
    accepted = list(pool.map(submit, range(args.count)))
admission_seconds = time.perf_counter() - started
remaining = {task_id for task_id, _ in accepted}
failed = []
deadline = time.monotonic() + 180
while remaining:
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        tasks = list(pool.map(lambda task_id: call("GET", "/api/tasks/" + task_id), remaining))
    for task in tasks:
        if task["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            remaining.remove(task["id"])
            if task["status"] != "SUCCEEDED":
                failed.append(task["id"])
    if time.monotonic() > deadline:
        raise SystemExit(f"Completion timed out with {len(remaining)} tasks outstanding")
    if remaining:
        time.sleep(0.3)
total_seconds = time.perf_counter() - started
latencies = sorted(latency for _, latency in accepted)


def percentile(value):
    return round(latencies[max(0, math.ceil(len(latencies) * value) - 1)], 2)


result = {"measuredAt": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(),
          "baseUrl": BASE, "handler": "sha256", "payloadTextBytes": 256,
          "count": args.count, "clientConcurrency": args.concurrency,
          "admissionSeconds": round(admission_seconds, 3),
          "admissionRequestsPerSecond": round(args.count / admission_seconds, 2),
          "admissionLatencyMs": {"p50": percentile(.5), "p95": percentile(.95), "p99": percentile(.99)},
          "completionSeconds": round(total_seconds, 3),
          "endToEndTasksPerSecond": round(args.count / total_seconds, 2), "failed": failed}
output = Path(args.output)
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(json.dumps(result, indent=2), encoding="utf-8")
print(json.dumps(result, indent=2))
if failed:
    raise SystemExit("Some tasks failed")
