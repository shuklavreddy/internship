"""Worker implementation: spawns worker threads to process jobs. (qctl copy)"""
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


def run_workers(dbpath, count=1, base=2, default_timeout=30, logs_dir=None):
    """Run worker threads in foreground. Blocks until stop requested."""
    from qctl import db

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
            job_timeout = job.get("timeout") or default_timeout
            job_log = job.get("log_path")
            if not job_log:
                # default log path
                logs_dir_local = logs_dir
                if logs_dir_local is None:
                    logs_dir_local = os.path.join(os.path.dirname(dbpath), "logs")
                try:
                    os.makedirs(logs_dir_local, exist_ok=True)
                except Exception:
                    pass
                job_log = os.path.join(logs_dir_local, f"{jid}.log")
                # persist log_path
                try:
                    conn = sqlite3.connect(dbpath)
                    conn.execute("UPDATE jobs SET log_path=? WHERE id=?", (job_log, jid))
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
            print(f"Worker-{idx} picked job {jid} (attempts={attempts}) -> {cmd}")
            # Execute command
            try:
                # run in shell so typical shell commands are available
                # capture output and enforce timeout
                proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=job_timeout)
                stdout = proc.stdout or ""
                stderr = proc.stderr or ""
                # write logs
                try:
                    with open(job_log, "a", encoding="utf-8") as f:
                        f.write(f"--- RUN {datetime.utcnow().isoformat()}Z ---\n")
                        if stdout:
                            f.write("STDOUT:\n")
                            f.write(stdout)
                            f.write("\n")
                        if stderr:
                            f.write("STDERR:\n")
                            f.write(stderr)
                            f.write("\n")
                except Exception:
                    pass

                if proc.returncode == 0:
                    db.complete_job(dbpath, jid)
                    _inc_metric(dbpath, 'jobs_processed', 1)
                    print(f"Job {jid} completed")
                else:
                    err = f"Exit {proc.returncode}: {stderr.strip()[:200]}"
                    backoff_base = int(db.get_config(dbpath, "backoff_base") or base)
                    db.fail_job(dbpath, jid, attempts, max_retries, err, backoff_base=backoff_base)
                    _inc_metric(dbpath, 'jobs_failed', 1)
                    _inc_metric(dbpath, 'jobs_retried', 1)
                    print(f"Job {jid} failed: {err}")
            except subprocess.TimeoutExpired as e:
                err = f"Timeout after {job_timeout}s"
                # write partial output if any
                try:
                    with open(job_log, "a", encoding="utf-8") as f:
                        if e.stdout:
                            f.write("STDOUT:\n")
                            f.write(e.stdout)
                        if e.stderr:
                            f.write("STDERR:\n")
                            f.write(e.stderr)
                        f.write("\n")
                except Exception:
                    pass
                backoff_base = int(db.get_config(dbpath, "backoff_base") or base)
                db.fail_job(dbpath, jid, attempts, max_retries, err, backoff_base=backoff_base)
                _inc_metric(dbpath, 'jobs_failed', 1)
                _inc_metric(dbpath, 'jobs_retried', 1)
                print(f"Job {jid} timed out")
            except Exception as e:
                err = str(e)
                backoff_base = int(db.get_config(dbpath, "backoff_base") or base)
                db.fail_job(dbpath, jid, attempts, max_retries, err, backoff_base=backoff_base)
                _inc_metric(dbpath, 'jobs_failed', 1)
                _inc_metric(dbpath, 'jobs_retried', 1)
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


def _inc_metric(dbpath, key, amount=1):
    try:
        conn = sqlite3.connect(dbpath)
        cur = conn.cursor()
        cur.execute("SELECT value FROM metrics WHERE key=?", (key,))
        r = cur.fetchone()
        if r:
            cur.execute("UPDATE metrics SET value = value + ? WHERE key=?", (amount, key))
        else:
            cur.execute("INSERT INTO metrics(key, value) VALUES(?,?)", (key, amount))
        conn.commit()
        conn.close()
    except Exception:
        pass
