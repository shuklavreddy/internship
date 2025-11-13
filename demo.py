"""Demo script to exercise queuectl features quickly.

This script enqueues a few jobs (some succeed, some fail) and runs workers
for a short while to demonstrate retry and DLQ.
"""
import subprocess
import time
import os
import json

ROOT = os.path.dirname(__file__)
PY = "python"


def run(args):
    # args is a list
    print(">", " ".join(args))
    return subprocess.check_call(args)


def main():
    # enqueue a success
    job1 = {"id": "demo-ok", "command": "echo hello demo", "max_retries": 3}
    run([PY, "queuectl.py", "enqueue", json.dumps(job1)])

    # enqueue a failing command (cross-platform using python -c)
    job2 = {"id": "demo-fail", "command": f"{PY} -c \"import sys; sys.exit(2)\"", "max_retries": 2}
    run([PY, "queuectl.py", "enqueue", json.dumps(job2)])

    # enqueue an invalid command
    job3 = {"id": "demo-bad", "command": "nonexistentcommand123", "max_retries": 2}
    run([PY, "queuectl.py", "enqueue", json.dumps(job3)])

    print("Starting worker for 10 seconds to process jobs...")
    # start worker in a subprocess (foreground) and terminate after 10s
    p = subprocess.Popen([PY, "queuectl.py", "worker", "start", "--count", "2"])
    try:
        time.sleep(10)
    finally:
        # request stop
        subprocess.check_call([PY, "queuectl.py", "worker", "stop"])
        p.wait()

    print("Final status:")
    run([PY, "queuectl.py", "status"])
    run([PY, "queuectl.py", "dlq", "list"])


if __name__ == '__main__':
    main()
