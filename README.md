# queuectl — Minimal Background Job Queue (Python)

Small, self-contained CLI tool that implements a persistent job queue with workers, retries and a Dead Letter Queue (DLQ).

Contents
- `queuectl.py` — CLI entrypoint
- `qctl/db.py` — SQLite persistence and job operations
- `qctl/worker.py` — worker runtime and execution
- `demo.py` — demo script that runs a short end-to-end flow
- `tests/` — basic unit tests

Goals (requirements coverage)
- Enqueue and manage background jobs
- Multiple worker support (concurrent threads)
- Retry with exponential backoff and configurable base
- Dead Letter Queue for permanently failed jobs
- Persistent storage (SQLite) across restarts
- CLI interface for all operations

Prerequisites
- Python 3.8+ (stdlib-only; no external packages required)

Quick setup
1. Open a PowerShell (or terminal) and change to the project folder where `queuectl.py` lives.
2. (Optional) Create a Python virtualenv: `python -m venv .venv; .\.venv\Scripts\Activate.ps1` (PowerShell)

How to run

Enqueue a job (example):
```powershell
python queuectl.py enqueue "{\"id\":\"job1\",\"command\":\"echo hello\",\"max_retries\":3}"
```

Start workers (foreground):
```powershell
python queuectl.py worker start --count 2
```

Request workers to stop from another shell (graceful):
```powershell
python queuectl.py worker stop
```

Show queue status:
```powershell
python queuectl.py status
```

List jobs by state:
```powershell
python queuectl.py list --state pending
```

DLQ ops:
```powershell
python queuectl.py dlq list
python queuectl.py dlq retry job1
```

Config (backoff base):
```powershell
python queuectl.py config get backoff_base
python queuectl.py config set backoff_base 3
```

Job JSON spec
Each job is a JSON object with at least the following fields:

```json
{
	"id": "unique-job-id",
	"command": "echo 'Hello'",
	"state": "pending",
	"attempts": 0,
	"max_retries": 3,
	"created_at": "2025-11-04T10:30:00Z",
	"updated_at": "2025-11-04T10:30:00Z"
}
```

Job lifecycle
- `pending` — waiting to be picked by a worker
- `processing` — currently executing
- `completed` — succeeded
- `failed` — failed but retryable (transient)
- `dead` — permanently failed and moved to the DLQ

Retry & backoff
- Backoff delay: delay = base ^ attempts (seconds). Default `base` is 2 and can be set via `config set backoff_base N`.
- After attempts exceed `max_retries`, job moves to `dead`.

Persistence
- Uses SQLite located next to `queuectl.py` (file `queue.db`). WAL mode enabled for better concurrency.

Worker behavior
- Workers are threads inside the running `worker start` process. They atomically claim a job using an UPDATE with a subselect, preventing duplicate processing.
- Graceful shutdown is signalled via a small stopfile; workers finish their current job then exit.

Testing
- Run the unit tests (basic coverage for enqueue, DLQ and retry):
```powershell
python -m unittest discover -v tests
```

Demo
- Run `demo.py` to enqueue a few jobs (succeeding, failing) and run a short worker session that demonstrates retries and DLQ behavior:
```powershell
python demo.py
```

Assumptions & trade-offs
- Workers run in the foreground; to run in background, start the process in a detached terminal or use OS service tooling.
- Simple design focused on correctness: SQLite + in-process threads. This keeps the project self-contained for an internship assignment.

Submission checklist
- [ ] All required commands functional (enqueue, worker start/stop, status, list, dlq, config)
- [ ] Jobs persist after restart (SQLite)
- [ ] Retry & backoff implemented
- [ ] DLQ operational with retry
- [ ] Tests included and passing

If you want, I can:
- Add job output logging and timeouts
- Add a small script to run workers as a background service on Windows
- Prepare the repository for GitHub (add LICENSE, .gitignore, and a short script to create a release zip)

License: MIT


//GOOGLE DRIVE
https://drive.google.com/drive/folders/1WGogvcKFFtz2A2yjqdtrWyw4ZNDe6Zxn?usp=sharing
