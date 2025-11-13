"""Worker implementation: spawns worker threads to process jobs."""
import os
import sqlite3
import threading
import time
import subprocess
from datetime import datetime

STOPFILE = "queue.worker.stop"
PIDFILE = "queue.worker.pid"


def _pid_path(dbpath):
    base = os.path.dirname(dbpath)
    return os.path.join(base, PIDFILE)


def _stopfile_path(dbpath):
    base = os.path.dirname(dbpath)
    return os.path.join(base, STOPFILE)


def clear_stopfile(dbpath):
    p = _stopfile_path(dbpath)
    try:
        if os.path.exists(p):
            os.remove(p)
    except Exception:
        pass


def request_stop(dbpath):
    p = _stopfile_path(dbpath)
    with open(p, "w"):
        pass


def is_stop_requested(dbpath):
    return os.path.exists(_stopfile_path(dbpath))


def _write_pid(dbpath):
    p = _pid_path(dbpath)
    with open(p, "w") as f:
        f.write(str(os.getpid()))


def _remove_pid(dbpath):
    try:
        os.remove(_pid_path(dbpath))
    except Exception:
        pass


def run_workers(dbpath, count=1, base=2):
    """Run worker threads in foreground. Blocks until stop requested."""
    from queue import db

    # ensure db exists
    db.init_db(dbpath)

    clear_stopfile(dbpath)
    _write_pid(dbpath)
    stop_event = threading.Event()

    def _worker_loop(idx):
        print(f"Worker-{idx} started")
        while not stop_event.is_set() and not is_stop_requested(dbpath):
            try:
                job = db.fetch_and_lock_job(dbpath)
            except sqlite3.OperationalError as e:
                print("DB busy, sleeping", e)
                time.sleep(0.5)
                continue
            if not job:
                time.sleep(0.5)
                continue
            jid = job["id"]
            cmd = job["command"]
            attempts = job.get("attempts", 0)
            max_retries = job.get("max_retries", 3)
            print(f"Worker-{idx} picked job {jid} (attempts={attempts}) -> {cmd}")
            # Execute command
            try:
                # run in shell so typical shell commands are available
                res = subprocess.run(cmd, shell=True)
                if res.returncode == 0:
                    db.complete_job(dbpath, jid)
                    print(f"Job {jid} completed")
                else:
                    err = f"Exit {res.returncode}"
                    backoff_base = int(db.get_config(dbpath, "backoff_base") or base)
                    db.fail_job(dbpath, jid, attempts, max_retries, err, backoff_base=backoff_base)
                    print(f"Job {jid} failed: {err}")
            except Exception as e:
                err = str(e)
                backoff_base = int(db.get_config(dbpath, "backoff_base") or base)
                db.fail_job(dbpath, jid, attempts, max_retries, err, backoff_base=backoff_base)
                print(f"Job {jid} raised: {err}")

        print(f"Worker-{idx} exiting")

    threads = []
    for i in range(count):
        t = threading.Thread(target=_worker_loop, args=(i + 1,), daemon=True)
        threads.append(t)
        t.start()

    try:
        # wait until stop requested
        while True:
            if is_stop_requested(dbpath):
                print("Stop requested via CLI")
                break
            time.sleep(0.5)
    finally:
        stop_event.set()
        # allow threads to finish
        for t in threads:
            t.join()
        _remove_pid(dbpath)
        # clean stopfile
        try:
            os.remove(_stopfile_path(dbpath))
        except Exception:
            pass
