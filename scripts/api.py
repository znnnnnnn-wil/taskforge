"""Small standard-library client shared by local verification scripts."""
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def connection():
    values = {}
    if (ROOT / ".env").exists():
        for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.startswith("#"):
                name, value = line.split("=", 1)
                values[name] = value
    key = os.environ.get("TASKFORGE_API_KEY", values.get("TASKFORGE_API_KEY", "local-development-key"))
    port = values.get("TASKFORGE_PORT", "8080")
    return os.environ.get("TASKFORGE_URL", f"http://127.0.0.1:{port}"), key


BASE, KEY = connection()


def call(method, path, body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(BASE + path, data=data, method=method,
                                     headers={"X-API-Key": KEY, "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def submission(key, handler="echo", payload=None, **overrides):
    request = dict(requestKey=key, handler=handler, payload=payload or {}, priority=0,
                   maxAttempts=3, retryDelaySeconds=1, timeoutSeconds=60)
    request.update(overrides)
    return request


def wait_task(task_id, wanted, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        task = call("GET", "/api/tasks/" + task_id)
        if task["status"] in wanted:
            return task
        time.sleep(0.15)
    raise AssertionError(f"Task {task_id} did not reach {wanted}; last status: {task['status']}")


def wait_health(timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if call("GET", "/actuator/health")["status"] == "UP":
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.5)
    raise AssertionError("API health check timed out")
