import inspect
import logging
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any

from celery_db_scheduler.crud import task_schedule as crud
from celery_db_scheduler.models.task_schedule import TaskSchedule
from celery_db_scheduler.utils.session import get_session, is_task_in_active_window

logger = logging.getLogger(__name__)


def auto_disable_one_time_task():
    """Decorator that automatically disables a task after its first execution.

    Works for both async and sync Celery task functions.  The decorated
    function **must** accept ``task_id`` as its first positional argument.

    Async tasks use the library's built-in async session (``ASYNC_DATABASE_URL``).
    Sync tasks perform a lazy import of ``get_session`` from ``app.session``
    (the integrating application's sync session factory).
    """

    def decorator(task_func):
        if inspect.iscoroutinefunction(task_func):

            @wraps(task_func)
            async def async_wrapper(task_id, *args, **kwargs):
                result = await task_func(task_id, *args, **kwargs)
                async with managed_session() as session:  # pragma: no branch
                    schedule = await session.get(TaskSchedule, task_id)
                    if (
                        schedule
                        and schedule.interval_seconds is None
                        and not schedule.cron_expression
                        and schedule.enabled
                    ):
                        schedule.enabled = False
                        await session.commit()
                        logger.info(f"Auto-disabled one-time task ID={task_id}")
                return result

            return async_wrapper
        else:

            @wraps(task_func)
            def sync_wrapper(task_id, *args, **kwargs):
                result = task_func(task_id, *args, **kwargs)
                # Lazy import: the integrating application must expose a sync
                # session generator at app.session.get_session.
                from app.session import get_session as sync_get_session  # noqa: PLC0415

                for session in sync_get_session():
                    schedule = session.get(TaskSchedule, task_id)
                    if (
                        schedule
                        and schedule.interval_seconds is None
                        and not schedule.cron_expression
                        and schedule.enabled
                    ):
                        schedule.enabled = False
                        session.commit()
                        logger.info(f"Auto-disabled one-time task ID={task_id}")
                return result

            return sync_wrapper

    return decorator


@asynccontextmanager
async def managed_session():
    """Async context manager that properly handles the async session lifecycle."""
    session_gen = get_session()
    session = await anext(session_gen)
    try:
        yield session
    finally:
        try:
            await session_gen.aclose()
        except Exception as e:  # pragma: no cover
            logger.error(f"Error closing session: {str(e)}")


async def validate_and_get_schedule(session, task_id) -> Any:
    """Look up a schedule by ID and verify it falls within its active time window.

    Returns the ``TaskSchedule`` instance, or ``None`` if it does not exist or
    is currently outside its configured time window.
    """
    schedule = await crud.get_schedule_by_id(session, task_id)
    if not schedule:  # pragma: no cover
        logger.warning(f"No schedule found with ID: {task_id}")
        return None

    if not is_task_in_active_window(schedule):
        logger.info(f"Task {task_id} skipped due to inactive time window.")
        return None

    return schedule
