"""End-to-end acceptance test; --chaos restarts only this Compose project's services."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from api import BASE, ROOT, call, submission, wait_health, wait_task

parser = argparse.ArgumentParser()
parser.add_argument("--chaos", action="store_true", help="kill one TaskForge worker and restart its database")
args = parser.parse_args()
results = {}
prefix = "smoke-" + str(uuid.uuid4())
wait_health()

try:
    urllib.request.urlopen(BASE + "/api/tasks", timeout=10)
    raise AssertionError("Unauthenticated API access was accepted")
except urllib.error.HTTPError as error:
    assert error.code == 401
results["authentication"] = "passed"
for path in ("/api;route=1/tasks", "//api/tasks", "/actuator/prometheus;route=1"):
    try:
        urllib.request.urlopen(BASE + path, timeout=10)
        raise AssertionError(f"URL variant bypassed authentication: {path}")
    except urllib.error.HTTPError as error:
        assert error.code in {400, 401}
results["authentication_url_variants"] = "passed"

request = submission(prefix + "-echo", payload={"message": "hello"})
task = call("POST", "/api/tasks", request)
assert call("POST", "/api/tasks", request)["id"] == task["id"]
assert wait_task(task["id"], {"SUCCEEDED"})["result"]["message"] == "hello"
assert len(call("GET", f"/api/tasks/{task['id']}/attempts")) == 1
results["execution_and_idempotency"] = "passed"

request["payload"] = {"message": "conflict"}
try:
    call("POST", "/api/tasks", request)
    raise AssertionError("Conflicting idempotency key accepted")
except urllib.error.HTTPError as error:
    assert error.code == 409

report = call("POST", "/api/tasks", submission(prefix + "-report", "report", {"values": [1, 2, 3]}))
assert wait_task(report["id"], {"SUCCEEDED"})["result"]["sum"] == 6
future = (datetime.now(timezone.utc) + timedelta(seconds=3)).isoformat()
scheduled = call("POST", "/api/tasks", submission(prefix + "-future", availableAt=future))
assert scheduled["status"] == "PENDING"
assert wait_task(scheduled["id"], {"SUCCEEDED"})
results["report_and_scheduled_execution"] = "passed"

cancelled = call("POST", "/api/tasks", submission(prefix + "-cancel", "delay", {"milliseconds": 10000}))
wait_task(cancelled["id"], {"RUNNING"})
assert call("POST", f"/api/tasks/{cancelled['id']}/cancel")["status"] == "CANCELLED"
results["running_cancellation"] = "passed"

template = submission(prefix + "-template")
template.pop("requestKey")
schedule = call("POST", "/api/schedules", {"name": prefix, "cron": "*/2 * * * * *", "zone": "UTC", "task": template})
deadline = time.monotonic() + 10
while not call("GET", f"/api/schedules/{schedule['id']}/tasks"):
    assert time.monotonic() < deadline, "Cron did not create a task"
    time.sleep(0.2)
call("PUT", f"/api/schedules/{schedule['id']}/enabled", {"enabled": False})
results["cron_materialization_and_pause"] = "passed"

if args.chaos:
    task = call("POST", "/api/tasks", submission(prefix + "-recovery", "delay", {"milliseconds": 6000}))
    owner = wait_task(task["id"], {"RUNNING"})["owner"]
    assert owner in {"worker-a", "worker-b"}, f"Unexpected owner: {owner}"
    print(f"Fault test: stopping {owner} during execution", flush=True)
    try:
        subprocess.run(["docker", "compose", "kill", "--signal", "SIGKILL", owner], cwd=ROOT, check=True)
        subprocess.run(["docker", "compose", "stop", "-t", "0", owner], cwd=ROOT, check=True)
        recovered = wait_task(task["id"], {"SUCCEEDED"}, timeout=50)
        attempts = call("GET", f"/api/tasks/{task['id']}/attempts")
        assert [attempt["status"] for attempt in attempts] == ["LOST", "SUCCEEDED"]
        assert attempts[0]["nodeId"] != attempts[1]["nodeId"]
        results["worker_crash_recovery"] = {"attempts": recovered["attempts"], "nodes": [a["nodeId"] for a in attempts]}
    finally:
        subprocess.run(["docker", "compose", "start", owner], cwd=ROOT, check=True)
    subprocess.run(["docker", "compose", "restart", "db"], cwd=ROOT, check=True)
    wait_health()
    assert call("GET", "/api/tasks/" + task["id"])["status"] == "SUCCEEDED"
    results["database_restart_persistence"] = "passed"

print(json.dumps(results, indent=2, ensure_ascii=False))
