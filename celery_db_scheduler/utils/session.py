import asyncio
import logging
import os
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

ASYNC_DATABASE_URL = os.getenv(
    "ASYNC_DATABASE_URL",
    "postgresql+asyncpg://db_user:db_pass@localhost/db_name",
)

engine = create_async_engine(ASYNC_DATABASE_URL)  # type: ignore
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)  # type: ignore


async def get_session():
    session = async_session()
    try:
        yield session
    finally:
        await session.close()
        await engine.dispose()


def run_async_safely(async_func, *args, **kwargs):
    """Run an async function safely from a synchronous context.

    Creates a dedicated event loop with proper cleanup so it can be called
    from Celery worker threads without conflicting with existing loops.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        return loop.run_until_complete(async_func(*args, **kwargs))
    except Exception as e:
        logger.error(f"Error in async function execution: {e}", exc_info=True)
        raise
    finally:
        try:
            tasks = asyncio.all_tasks(loop)
            if tasks:  # pragma: no cover
                for task in tasks:
                    task.cancel()
                loop.run_until_complete(
                    asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True), timeout=5.0
                    )
                )
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception as e:  # pragma: no cover
            logger.error(f"Error during event loop cleanup: {e}", exc_info=True)
        finally:
            loop.close()


def is_task_in_active_window(schedule) -> bool:  # pragma: no cover
    import pytz  # type: ignore

    now = datetime.now(timezone.utc)

    try:
        schedule_tz = pytz.timezone(schedule.timezone)
    except Exception:
        logger.warning(f"Invalid timezone {schedule.timezone}, using UTC")
        schedule_tz = timezone.utc

    now_in_schedule_tz = now.astimezone(schedule_tz)
    now_time = now_in_schedule_tz.time()

    logger.info(
        f"[Window Check] now_utc={now.isoformat()} "
        f"now_local={now_in_schedule_tz.isoformat()} ({schedule.timezone}) "
        f"now_time={now_time} "
        f"daily_start_time={schedule.daily_start_time} "
        f"daily_end_time={schedule.daily_end_time}"
    )

    if schedule.start_datetime and now < schedule.start_datetime:
        logger.info("Task outside window: before start_datetime")
        return False

    if schedule.end_datetime and now >= schedule.end_datetime:
        logger.info("Task outside window: after end_datetime")
        return False

    if schedule.daily_start_time and now_time < schedule.daily_start_time:
        logger.info(
            f"Task outside window: too early ({now_time} < {schedule.daily_start_time})"
        )
        return False

    if schedule.daily_end_time and now_time >= schedule.daily_end_time:
        logger.info(
            f"Task outside window: too late ({now_time} >= {schedule.daily_end_time})"
        )
        return False

    logger.info("Task is within active window")
    return True
