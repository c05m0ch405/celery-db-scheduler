"""celery-db-scheduler: SQLAlchemy-backed Celery Beat scheduler.

Quickstart
----------
1. Set ``DATABASE_URL`` (sync, for Beat) and ``ASYNC_DATABASE_URL`` (async,
   for workers) environment variables.
2. Point Celery Beat at the scheduler::

       celery -A myapp beat --scheduler celery_db_scheduler.beat.scheduler.DynamicDatabaseScheduler

3. Use ``Base.metadata.create_all(engine)`` (or Alembic) to create the
   ``task_schedules`` table.
"""

from celery_db_scheduler.base import Base
from celery_db_scheduler.beat.scheduler import (
    CronSchedule,
    DynamicDatabaseScheduler,
    OneTimeSchedule,
)
from celery_db_scheduler.models.task_schedule import TaskSchedule
from celery_db_scheduler.schemas.task_schedule import TaskScheduleCreate, TaskScheduleRead
from celery_db_scheduler.utils.decorators import (
    auto_disable_one_time_task,
    managed_session,
    validate_and_get_schedule,
)

__all__ = [
    "Base",
    "CronSchedule",
    "DynamicDatabaseScheduler",
    "OneTimeSchedule",
    "TaskSchedule",
    "TaskScheduleCreate",
    "TaskScheduleRead",
    "auto_disable_one_time_task",
    "managed_session",
    "validate_and_get_schedule",
]
