import logging
import os
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from celery.beat import Scheduler  # type: ignore
from celery.schedules import crontab, schedule  # type: ignore
from celery.utils.time import ffwd  # type: ignore
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from celery_db_scheduler.beat.registry import get_task_registry
from celery_db_scheduler.crud.task_schedule import get_enabled_schedules_sync
from celery_db_scheduler.models.task_schedule import TaskSchedule

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://username:password@localhost/db-test",
)

_sync_engine = None
SyncSession = None


def _init_session() -> None:
    """Lazily create the sync engine and session factory."""
    global _sync_engine, SyncSession
    if SyncSession is None:
        _sync_engine = create_engine(DATABASE_URL)
        SyncSession = sessionmaker(_sync_engine)


class OneTimeSchedule(schedule):
    """Schedule for one-time task execution."""

    def __init__(self, run_at: datetime, nowfun=None):
        super().__init__(nowfun=nowfun)

        # Normalize to UTC
        if run_at.tzinfo is None:
            run_at = run_at.replace(tzinfo=timezone.utc)
        else:
            run_at = run_at.astimezone(timezone.utc)

        self.run_at = run_at
        self._has_run = False

    def is_due(self, last_run_at):
        now = self.now()

        logger.info(
            f"OneTimeSchedule check: run_at={self.run_at.isoformat()}, "
            f"now={now.isoformat()}, _has_run={self._has_run}, last_run_at={last_run_at}"
        )

        if self._has_run:
            logger.info("Task already executed in this scheduler instance, skipping")
            return False, 365 * 24 * 60 * 60

        if now < self.run_at:
            delay = max(1, int((self.run_at - now).total_seconds()))
            logger.info(f"Not yet due, checking again in {delay}s")
            return False, delay

        logger.info("OneTimeSchedule is DUE NOW!")
        return True, 60  # Run now, retry in 60s if needed

    def mark_as_run(self):
        """Mark this schedule as having been executed."""
        self._has_run = True
        logger.info("Marked OneTimeSchedule as run")

    def __repr__(self):
        return f"<OneTimeSchedule: {self.run_at.isoformat()}, has_run={self._has_run}>"


class CronSchedule(crontab):
    """Crontab evaluated in the schedule's own timezone instead of Celery's.

    The expression uses the standard 5 fields:
    minute hour day_of_month month_of_year day_of_week.
    """

    def __init__(self, expression: str, tz_name: str = "UTC", nowfun=None, app=None):
        fields = expression.split()
        if len(fields) != 5:
            raise ValueError(
                f"Cron expression must have 5 fields, got {len(fields)}: {expression!r}"
            )
        minute, hour, day_of_month, month_of_year, day_of_week = fields
        super().__init__(
            minute=minute,
            hour=hour,
            day_of_week=day_of_week,
            day_of_month=day_of_month,
            month_of_year=month_of_year,
            nowfun=nowfun,
            app=app,
        )
        self.expression = expression
        self.tz = ZoneInfo(tz_name)

    def now(self):
        return self.maybe_make_aware(super().now()).astimezone(self.tz)

    def remaining_delta(self, last_run_at, tz=None, ffwd=ffwd):
        # crontab matches hour/day against last_run_at's own tzinfo, which is UTC
        # when loaded from the DB, so it must be converted first.
        last_run_at = self.maybe_make_aware(last_run_at).astimezone(self.tz)
        return super().remaining_delta(last_run_at, tz=tz, ffwd=ffwd)

    def __repr__(self):
        return f"<CronSchedule: {self.expression} ({self.tz.key})>"


class DynamicDatabaseScheduler(Scheduler):
    """Database-backed Celery Beat scheduler with one-time task support."""

    def __init__(self, *args, **kwargs):
        self._last_sync = 0.0
        self._entries = {}
        self._sync_interval = 60.0  # Sync every 60 seconds
        super().__init__(*args, **kwargs)
        self.sync()  # Initial load

    def sync(self):
        """Load enabled schedules from the database."""
        logger.info("Syncing periodic tasks from database...")
        _init_session()
        try:
            with SyncSession() as session:
                schedules = get_enabled_schedules_sync(session)
                new_entries = {}

                for schedule_obj in schedules:
                    logger.debug(
                        f"DB schedule {schedule_obj.id}: task={schedule_obj.task_name}, "
                        f"interval={schedule_obj.interval_seconds}, "
                        f"last_run_at={schedule_obj.last_run_at}"
                    )

                    task = get_task_registry().get(schedule_obj.task_name)
                    if not task:
                        logger.error(f"Unknown task: {schedule_obj.task_name}")
                        continue

                    args = schedule_obj.task_args or []
                    kwargs = schedule_obj.task_kwargs or {}
                    entry_name = f"{schedule_obj.task_name}-{schedule_obj.id}"

                    is_cron = bool(schedule_obj.cron_expression)

                    # Build appropriate schedule
                    if is_cron:
                        try:
                            sched = CronSchedule(
                                schedule_obj.cron_expression,
                                schedule_obj.timezone or "UTC",
                                app=self.app,
                            )
                        except (ValueError, ZoneInfoNotFoundError) as e:
                            logger.error(
                                f"Invalid cron schedule for task {schedule_obj.id} "
                                f"({schedule_obj.cron_expression!r}, "
                                f"tz={schedule_obj.timezone!r}): {e}"
                            )
                            continue
                        logger.debug(f"Loaded cron task {entry_name}: {sched!r}")
                    elif schedule_obj.interval_seconds is None:
                        # One-time task
                        if not schedule_obj.start_datetime:
                            logger.warning(
                                f"One-time task {schedule_obj.id} "
                                "missing start_datetime, skipping"
                            )
                            continue

                        run_at_utc = schedule_obj.start_datetime.astimezone(timezone.utc)

                        existing_entry = self._entries.get(entry_name)
                        should_create_new = True

                        if existing_entry and isinstance(
                            existing_entry.schedule, OneTimeSchedule
                        ):
                            existing_schedule = existing_entry.schedule
                            time_changed = existing_schedule.run_at != run_at_utc
                            was_reset = (
                                schedule_obj.last_run_at is None
                                and existing_schedule._has_run
                            )

                            if time_changed or was_reset:
                                logger.info(
                                    f"OneTimeSchedule for {entry_name} was modified "
                                    f"(time_changed={time_changed}, "
                                    f"was_reset={was_reset}), creating new schedule"
                                )
                                should_create_new = True
                            else:
                                sched = existing_schedule
                                should_create_new = False
                                logger.debug(
                                    f"Preserved existing OneTimeSchedule for {entry_name} "
                                    f"(has_run={sched._has_run})"
                                )

                        if should_create_new:
                            sched = OneTimeSchedule(run_at_utc)
                            if schedule_obj.last_run_at is not None:
                                sched._has_run = True
                                logger.info(
                                    f"Loaded one-time task {entry_name} "
                                    "- already executed in DB"
                                )
                            else:
                                logger.info(
                                    f"Loaded one-time task {entry_name} "
                                    f"for {run_at_utc.isoformat()}"
                                )
                    else:
                        # Recurring task
                        sched = schedule(run_every=schedule_obj.interval_seconds)
                        logger.debug(
                            f"Loaded recurring task {entry_name} "
                            f"every {schedule_obj.interval_seconds}s"
                        )

                    # Determine last_run_at
                    existing_entry = self._entries.get(entry_name)
                    is_recurring = is_cron or schedule_obj.interval_seconds is not None
                    if is_recurring and existing_entry:
                        # Recurring task: preserve in-memory state
                        last_run_at = existing_entry.last_run_at
                    else:
                        # One-time task or new task: use database value.
                        # A new cron task without last_run_at gets Celery's default
                        # (now), so it first fires at the next matching time.
                        last_run_at = schedule_obj.last_run_at

                        if (
                            last_run_at is None
                            and not is_cron
                            and schedule_obj.interval_seconds is not None
                        ):
                            if schedule_obj.start_datetime:
                                start_utc = schedule_obj.start_datetime.astimezone(
                                    timezone.utc
                                )
                                last_run_at = start_utc - timedelta(
                                    seconds=schedule_obj.interval_seconds
                                )
                                logger.info(
                                    f"Recurring task {entry_name} "
                                    f"will start at {start_utc.isoformat()}"
                                )
                            else:
                                last_run_at = datetime.now(timezone.utc) - timedelta(
                                    seconds=schedule_obj.interval_seconds
                                )  # pragma: no cover

                    if last_run_at is None and is_cron:
                        last_run_at = self.app.now()

                    entry_kwargs = {
                        "name": entry_name,
                        "task": task.name,
                        "schedule": sched,
                        "args": [schedule_obj.id, *args],
                        "kwargs": kwargs,
                        "options": {},
                        "total_run_count": (
                            existing_entry.total_run_count if existing_entry else 0
                        ),
                    }

                    if last_run_at is not None:
                        entry_kwargs["last_run_at"] = last_run_at

                    entry = self.Entry(**entry_kwargs)
                    new_entries[entry.name] = entry

                removed = set(self._entries.keys()) - set(new_entries.keys())
                added = set(new_entries.keys()) - set(self._entries.keys())

                if removed:
                    logger.info(f"Removed tasks: {removed}")
                if added:
                    logger.info(f"Added tasks: {added}")

                self._entries = new_entries
                logger.info(f"Sync complete: {len(self._entries)} tasks loaded")

        except Exception as e:
            logger.error(f"Failed to sync periodic tasks: {e}", exc_info=True)

    def tick(self, event_t=None):
        """Check which tasks are due and execute them."""
        current_time = time.monotonic()
        if current_time - self._last_sync > self._sync_interval:
            logger.debug("Periodic sync triggered")
            self.sync()
            self._last_sync = current_time

        remaining_times = []

        for name, entry in list(self._entries.items()):
            try:
                is_due, next_time_to_run = entry.is_due()

                if is_due:
                    logger.info(f"Task {name} is due - sending to Celery")
                    self.apply_async(entry, producer=self.producer)

                    if isinstance(entry.schedule, OneTimeSchedule):
                        entry.schedule.mark_as_run()

                    now = self.app.now()
                    entry.last_run_at = now
                    entry.total_run_count += 1
                    self._update_last_run_at(name, now)

                if next_time_to_run is not None:
                    remaining_times.append(next_time_to_run)

            except Exception as e:  # pragma: no cover
                logger.error(f"Error processing task {name}: {e}", exc_info=True)

        next_wakeup = min(remaining_times) if remaining_times else self.max_interval
        return min(next_wakeup, self._sync_interval)

    def _update_last_run_at(self, entry_name: str, last_run_at: datetime) -> None:
        """Persist last_run_at to the database."""
        _init_session()
        try:
            task_id = int(entry_name.split("-")[-1])
            with SyncSession() as session:
                schedule_obj = session.get(TaskSchedule, task_id)
                if schedule_obj:
                    schedule_obj.last_run_at = last_run_at
                    session.commit()
                    logger.debug(f"Updated last_run_at for task {entry_name}")
                else:
                    logger.warning(f"Task {task_id} not found in database")
        except Exception as e:
            logger.error(
                f"Failed to persist last_run_at for {entry_name}: {e}", exc_info=True
            )

    @property
    def schedule(self):
        return self._entries
