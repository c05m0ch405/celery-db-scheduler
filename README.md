# celery-db-scheduler

[![PyPI version](https://img.shields.io/pypi/v/celery-db-scheduler.svg)](https://pypi.org/project/celery-db-scheduler/)
[![Python](https://img.shields.io/pypi/pyversions/celery-db-scheduler.svg)](https://pypi.org/project/celery-db-scheduler/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/c05m0ch405/celery-db-scheduler/actions/workflows/publish.yml/badge.svg)](https://github.com/c05m0ch405/celery-db-scheduler/actions/workflows/publish.yml)

A **SQLAlchemy-backed Celery Beat scheduler** that stores task schedules in a PostgreSQL database instead of a static config file. Schedules can be created, updated, and deleted at runtime with no Beat restart required.

---

## Features

- **Dynamic scheduling** — add or remove tasks at runtime via database rows; Beat picks up changes within 60 seconds.
- **One-time tasks** — schedule a task to fire once at a specific UTC datetime; the library marks it disabled automatically after execution.
- **Recurring tasks** — standard interval-based schedules (`interval_seconds`).
- **Cron tasks** — wall-clock schedules via a 5-field cron expression (`minute hour day month weekday`), evaluated in each row's `timezone`.
- **Time-window support** — restrict when a task may run using `start_datetime`, `end_datetime`, `daily_start_time`, `daily_end_time`, and a per-schedule `timezone`.
- **Async-first workers** — built-in async SQLAlchemy session factory for FastAPI / asyncio workers.
- **Sync Beat process** — the Beat scheduler uses a standard synchronous SQLAlchemy session, compatible with all Celery Beat deployments.
- **Pydantic v2 schemas** — ready-made request/response models for exposing schedules via a REST API.

---

## Installation

```bash
# Core library + async PostgreSQL driver (asyncpg)
pip install celery-db-scheduler

# Also install a synchronous PostgreSQL driver for the Beat process.
# Pre-built binary (recommended for most environments):
pip install "celery-db-scheduler[sync]"

# Compile from source (recommended for Alpine / musl builds):
pip install "celery-db-scheduler[sync-src]"
```

**Requirements:** Python 3.12+, PostgreSQL (tested with 14+).

---

## Quickstart

### 1. Set environment variables

The Beat process and the async workers use separate database connections:

```bash
# Synchronous connection — used by celery beat
export DATABASE_URL="postgresql://user:pass@localhost/mydb"

# Asynchronous connection — used by celery workers
export ASYNC_DATABASE_URL="postgresql+asyncpg://user:pass@localhost/mydb"
```

### 2. Create the `task_schedules` table

Import the library's `Base` alongside your own models so a single
`metadata.create_all()` (or Alembic `env.py`) covers everything:

```python
# myapp/db.py
from sqlalchemy import create_engine
from celery_db_scheduler import Base

# Import your own models here so their tables are also created
import myapp.models  # noqa: F401

engine = create_engine("postgresql://user:pass@localhost/mydb")
Base.metadata.create_all(engine)  # creates the task_schedules table
```

With **Alembic**, add the library's metadata to your `env.py`:

```python
# alembic/env.py
from celery_db_scheduler import Base as SchedulerBase
from myapp.db import Base as AppBase

target_metadata = [AppBase.metadata, SchedulerBase.metadata]
```

### 3. Configure your Celery app

```python
# myapp/celery_app.py
import os

from celery import Celery

celery_app = Celery("myapp", broker="redis://localhost:6379/0")

celery_app.conf.update(
    result_backend="redis://localhost:6379/0",
    beat_scheduler="celery_db_scheduler.beat.scheduler.DynamicDatabaseScheduler",
    # Optional: skip cron runs missed by more than this many seconds (default 3600)
    beat_cron_starting_deadline=int(os.getenv("BEAT_CRON_STARTING_DEADLINE", "3600")),
)
```

### 4. Write a scheduled task

Use `@auto_disable_one_time_task()` to have the library automatically mark a
one-time task as disabled after it runs. The decorated function **must** accept
`task_id` as its first argument.

```python
# myapp/tasks.py
from celery_db_scheduler import auto_disable_one_time_task, validate_and_get_schedule
from celery_db_scheduler.utils.session import get_session, run_async_safely

from myapp.celery_app import celery_app


async def _send_report_async(task_id: int, recipient: str) -> None:
    async for session in get_session():
        schedule = await validate_and_get_schedule(session, task_id)
        if not schedule:
            return
        # Your business logic here
        print(f"Sending report to {recipient}")


@celery_app.task(name="myapp.tasks.send_report")
@auto_disable_one_time_task()
def send_report(task_id: int, recipient: str) -> None:
    run_async_safely(_send_report_async, task_id, recipient)
```

### 5. Create schedules in the database

Use the CRUD helpers directly, or expose them through your API:

```python
# Run once at a specific time
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from celery_db_scheduler.crud.task_schedule import create_schedule
from celery_db_scheduler.schemas.task_schedule import TaskScheduleCreate


async def schedule_one_time_report(db: AsyncSession) -> None:
    await create_schedule(
        db,
        TaskScheduleCreate(
            task_name="myapp.tasks.send_report",
            task_kwargs={"recipient": "admin@example.com"},
            start_datetime=datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc),
            interval_seconds=None,  # None = one-time
            enabled=True,
            description="Monthly report — July 2026",
        ),
    )


async def schedule_recurring_cleanup(db: AsyncSession) -> None:
    await create_schedule(
        db,
        TaskScheduleCreate(
            task_name="myapp.tasks.cleanup_old_records",
            interval_seconds=3600,  # every hour
            # Only run between 01:00 and 05:00 UTC
            daily_start_time="01:00:00",
            daily_end_time="05:00:00",
            timezone="UTC",
            enabled=True,
        ),
    )
```

### 6. Start Beat and a worker

```bash
# Terminal 1 — worker
celery -A myapp.celery_app worker --loglevel=info

# Terminal 2 — Beat scheduler
celery -A myapp.celery_app beat \
    --scheduler celery_db_scheduler.beat.scheduler.DynamicDatabaseScheduler \
    --loglevel=info
```

Beat will poll the database every 60 seconds. Any schedule row you add, update,
or delete is reflected in the next sync cycle — no restart needed.

---

## `TaskSchedule` field reference

| Field | Type | Description |
|---|---|---|
| `task_name` | `str` | Dotted Celery task name, e.g. `"myapp.tasks.send_report"` |
| `interval_seconds` | `int \| None` | Recurrence interval. `None` = one-time or cron task |
| `cron_expression` | `str \| None` | Standard 5-field cron (`minute hour day month weekday`). When set, fires on wall-clock times in `timezone` instead of using `interval_seconds` |
| `task_args` | `list` | Positional arguments passed to the task |
| `task_kwargs` | `dict` | Keyword arguments passed to the task |
| `enabled` | `bool` | Set to `False` to pause the task |
| `start_datetime` | `datetime` | Earliest time the task may run (required for one-time tasks) |
| `end_datetime` | `datetime` | Task is skipped after this time |
| `daily_start_time` | `time` | Daily window start (evaluated in `timezone`) |
| `daily_end_time` | `time` | Daily window end (evaluated in `timezone`) |
| `timezone` | `str` | IANA timezone name, default `"UTC"` |
| `last_run_at` | `datetime` | Set automatically by the scheduler |

---

## Contributing

Contributions are welcome! This section covers everything you need to get a development environment running.

### Prerequisites

- Python 3.12+
- PostgreSQL (for integration tests) or Docker
- [hatch](https://hatch.pypa.io/) (`pip install hatch`)

### Setup

```bash
git clone https://github.com/c05m0ch405/celery-db-scheduler.git
cd celery-db-scheduler

# Install the package and all dev dependencies in an isolated environment
pip install -e ".[sync]"
pip install pytest pytest-cov pytest-asyncio
```

### Running the tests

```bash
# All tests with coverage report
pytest tests/ -v --tb=short --cov=celery_db_scheduler --cov-report=term-missing

# A single test module
pytest tests/test_scheduler.py -v
```

### Code style

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
pip install ruff
ruff check .       # lint
ruff format .      # auto-format
```

### Building a distribution locally

```bash
hatch build        # produces dist/celery_db_scheduler-*.whl and *.tar.gz
twine check dist/* # verify the package metadata before uploading
```

### Submitting a pull request

1. **Fork** the repository and create a feature branch from `main`.
2. Add or update tests to cover your change — aim for full coverage of new logic.
3. Ensure `pytest` and `ruff check .` both pass with no errors.
4. Open a pull request with a clear description of what was changed and why.

For significant changes, please open an issue first to discuss the approach.

---

## License

[MIT](https://opensource.org/licenses/MIT) © c05m0ch405
