#!/usr/bin/env python3
"""
queuectl - Minimal background job queue CLI

Run with: python queuectl.py <command>

This file contains the CLI entrypoint and command wiring.
"""
import argparse
import json
import os
import sys
from datetime import datetime

from qctl import db, worker

DB_PATH = os.path.join(os.path.dirname(__file__), "queue.db")


def ensure_db():
    db.init_db(DB_PATH)


def cmd_enqueue(args):
    ensure_db()
    raw = args.job
    try:
        job = json.loads(raw)
    except Exception as e:
        print(f"Invalid JSON: {e}")
        return 2

    now = datetime.utcnow().isoformat() + "Z"
    job.setdefault("state", "pending")
    job.setdefault("attempts", 0)
    job.setdefault("max_retries", 3)
    job.setdefault("created_at", now)
    job.setdefault("updated_at", now)

    try:
        db.enqueue_job(DB_PATH, job)
        print(f"Enqueued job {job.get('id')}")
    except Exception as e:
        print(f"Failed to enqueue: {e}")
        return 1


def cmd_worker_start(args):
    ensure_db()
    # Remove any existing stop file
    worker.clear_stopfile(DB_PATH)
    print(f"Starting workers (count={args.count}) - Ctrl-C or 'queuectl worker stop' to stop")
    try:
        worker.run_workers(DB_PATH, count=args.count, base=args.backoff_base)
    except KeyboardInterrupt:
        print("Interrupted - shutting down workers")


def cmd_worker_stop(args):
    ensure_db()
    worker.request_stop(DB_PATH)
    print("Stop requested. Workers will exit after finishing current jobs.")


def cmd_status(args):
    ensure_db()
    stats = db.get_stats(DB_PATH)
    print("Job states:")
    for k, v in stats.get("states", {}).items():
        print(f"  {k}: {v}")
    print(f"Active workers (stopfile present? {worker.is_stop_requested(DB_PATH)})")


def cmd_list(args):
    ensure_db()
    rows = db.list_jobs(DB_PATH, state=args.state)
    for r in rows:
        print(json.dumps(r, default=str))


def cmd_dlq_list(args):
    ensure_db()
    rows = db.list_jobs(DB_PATH, state="dead")
    for r in rows:
        print(json.dumps(r, default=str))


def cmd_dlq_retry(args):
    ensure_db()
    db.retry_dead_job(DB_PATH, args.job_id)
    print(f"Requested retry of {args.job_id}")


def cmd_config_set(args):
    ensure_db()
    db.set_config(DB_PATH, args.key, args.value)
    print(f"Set config {args.key} = {args.value}")


def cmd_config_get(args):
    ensure_db()
    val = db.get_config(DB_PATH, args.key)
    print(val)


def build_parser():
    p = argparse.ArgumentParser(prog="queuectl")
    sp = p.add_subparsers(dest="cmd")

    # enqueue
    en = sp.add_parser("enqueue", help="Enqueue a job JSON string")
    en.add_argument("job", help='Job JSON e.g. "{\"id\":\"job1\",\"command\":\"sleep 2\"}"')
    en.set_defaults(func=cmd_enqueue)

    # worker
    w = sp.add_parser("worker", help="Worker management")
    wsp = w.add_subparsers(dest="sub")

    wstart = wsp.add_parser("start", help="Start worker(s)")
    wstart.add_argument("--count", type=int, default=1, help="Number of concurrent worker threads")
    wstart.add_argument("--backoff-base", type=int, default=2, help="Exponential backoff base")
    wstart.set_defaults(func=cmd_worker_start)

    wstop = wsp.add_parser("stop", help="Request workers to stop gracefully")
    wstop.set_defaults(func=cmd_worker_stop)

    # status
    s = sp.add_parser("status", help="Show summary of job states and worker status")
    s.set_defaults(func=cmd_status)

    # list
    l = sp.add_parser("list", help="List jobs by state")
    l.add_argument("--state", choices=["pending", "processing", "completed", "failed", "dead"], help="Filter by state")
    l.set_defaults(func=cmd_list)

    # dlq
    d = sp.add_parser("dlq", help="DLQ operations")
    dsp = d.add_subparsers(dest="sub")
    dl = dsp.add_parser("list", help="List DLQ jobs")
    dl.set_defaults(func=cmd_dlq_list)
    dr = dsp.add_parser("retry", help="Retry a job from DLQ")
    dr.add_argument("job_id")
    dr.set_defaults(func=cmd_dlq_retry)

    # config
    c = sp.add_parser("config", help="Configuration management")
    csp = c.add_subparsers(dest="sub")
    cs = csp.add_parser("set", help="Set config key")
    cs.add_argument("key")
    cs.add_argument("value")
    cs.set_defaults(func=cmd_config_set)
    cg = csp.add_parser("get", help="Get config key")
    cg.add_argument("key")
    cg.set_defaults(func=cmd_config_get)

    return p


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
