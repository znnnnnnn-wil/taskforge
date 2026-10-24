"""Verify RabbitMQ dispatch and optional broker faults in this Compose project."""
import argparse
import json
import re
import subprocess
import time
import uuid
from api import ROOT, call, submission, wait_health, wait_task


def compose(*args):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True,
                          text=True, capture_output=True).stdout


def received_count():
    # The key is expanded inside each container and is sent only to its local HTTP socket.
    request = r'''test "$RABBIT_ENABLED" = true && exec 3<>/dev/tcp/127.0.0.1/8080 &&
printf 'GET /actuator/prometheus HTTP/1.0\r\nHost: localhost\r\nX-API-Key: %s\r\n\r\n' "$TASKFORGE_API_KEY" >&3 &&
cat <&3'''
    total = 0
    for worker in ("worker-a", "worker-b"):
        body = compose("exec", "-T", worker, "bash", "-c", request)
        assert "200" in body.splitlines()[0], "Worker metrics endpoint is unavailable"
        total += sum(float(value) for value in re.findall(
            r"^taskforge_rabbit_received_total(?:\{[^}]*\})?\s+([0-9.eE+-]+)", body, re.M))
    return total


def queue_size():
    rows = json.loads(compose("exec", "-T", "rabbitmq", "rabbitmqctl", "-q", "list_queues",
                              "name", "messages_ready", "--formatter", "json"))
    return sum(int(row["messages_ready"]) for row in rows if row["name"] == "taskforge.ready")


def wait_for_worker_metrics():
    # The preceding crash test may have restarted a Worker later than the API.
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            return received_count()
        except (subprocess.CalledProcessError, AssertionError, OSError):
            time.sleep(0.5)
    raise AssertionError("Worker metrics did not become ready after restart")


def wait_for_queue(minimum):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if queue_size() >= minimum:
            return
        time.sleep(1)
    raise AssertionError(f"Ready queue did not retain at least {minimum} messages")


def batch(prefix, count):
    return [call("POST", "/api/tasks", submission(f"{prefix}-{index}")) for index in range(count)]


def complete(tasks):
    for task in tasks:
        wait_task(task["id"], {"SUCCEEDED"}, timeout=120)
        attempts = call("GET", f"/api/tasks/{task['id']}/attempts")
        assert len(attempts) == 1 and attempts[0]["status"] == "SUCCEEDED"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chaos", action="store_true", help="stop/restart this project's broker and workers")
    parser.add_argument("--count", type=int, default=12)
    args = parser.parse_args()
    if not 1 <= args.count <= 100:
        parser.error("count must be 1..100")
    prefix = "rabbit-smoke-" + str(uuid.uuid4())
    results = {}
    wait_health()
    before = wait_for_worker_metrics()
    tasks = batch(prefix + "-normal", args.count)
    complete(tasks)
    after = wait_for_worker_metrics()
    assert after - before >= args.count, "Tasks did not pass through RabbitMQ consumers"
    results["rabbit_dispatch"] = {"tasks": args.count, "receivedAtLeast": args.count, "attemptsPerTask": 1}

    if args.chaos:
        print("Fault test: stopping RabbitMQ; accepting tasks through PostgreSQL Outbox", flush=True)
        try:
            compose("stop", "-t", "10", "rabbitmq")
            # Do not wait for aggregate health here: Rabbit health is intentionally DOWN.
            tasks = batch(prefix + "-outage", args.count)
            for task in tasks:
                current = call("GET", "/api/tasks/" + task["id"])
                assert current["status"] == "PENDING" and current["attempts"] == 0
        finally:
            compose("up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq")
        wait_health(timeout=120)
        complete(tasks)
        results["outbox_broker_outage"] = {"accepted": args.count, "completedAfterRecovery": args.count}

        print("Fault test: restarting RabbitMQ while ready messages await workers", flush=True)
        try:
            compose("stop", "-t", "30", "worker-a", "worker-b")
            queued = batch(prefix + "-durable", args.count)
            wait_for_queue(args.count)
            compose("restart", "rabbitmq")
            compose("up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq")
            wait_for_queue(args.count)
        finally:
            compose("up", "-d", "--wait", "--wait-timeout", "120", "worker-a", "worker-b")
        wait_health(timeout=120)
        complete(queued)
        results["broker_restart_with_backlog"] = {"retainedAtLeast": args.count, "completed": args.count}

    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
