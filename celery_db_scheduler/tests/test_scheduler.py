# celery_db_scheduler/beat/test_scheduler.py

import sys
import time
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from celery_db_scheduler.base import Base
from celery_db_scheduler.models.task_schedule import TaskSchedule
from celery_db_scheduler.utils.decorators import auto_disable_one_time_task

from ..beat.scheduler import CronSchedule, DynamicDatabaseScheduler, OneTimeSchedule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc(**kwargs) -> datetime:
    return datetime.now(timezone.utc) + timedelta(**kwargs)


def _naive(**kwargs) -> datetime:
    return datetime.now() + timedelta(**kwargs)


def _make_schedule_row(
    id=1,
    task_name="myapp.add",
    interval_seconds=None,
    cron_expression=None,
    timezone="UTC",
    start_datetime=None,
    last_run_at=None,
    task_args=None,
    task_kwargs=None,
    is_enabled=True,
):
    row = MagicMock(spec=TaskSchedule)
    row.id = id
    row.task_name = task_name
    row.interval_seconds = interval_seconds
    row.cron_expression = cron_expression
    row.timezone = timezone
    row.start_datetime = start_datetime
    row.last_run_at = last_run_at
    row.task_args = task_args or []
    row.task_kwargs = task_kwargs or {}
    row.is_enabled = is_enabled
    return row


def _fake_task(name="myapp.add"):
    t = MagicMock()
    t.name = name
    return t


# ---------------------------------------------------------------------------
# SQLite fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def sqlite_engine():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)


@pytest.fixture()
def sqlite_session(sqlite_engine):
    Session = sessionmaker(sqlite_engine)
    with Session() as session:
        yield session
        session.rollback()


@pytest.fixture()
def sqlite_session_factory(sqlite_engine):
    return sessionmaker(sqlite_engine)


# ---------------------------------------------------------------------------
# Scheduler factory (patches DB + registry + Celery app)
# ---------------------------------------------------------------------------

@pytest.fixture()
def fake_registry():
    return {"myapp.add": _fake_task("myapp.add")}


@pytest.fixture()
def mock_celery_app():
    app = MagicMock()
    app.now.return_value = datetime.now(timezone.utc)
    app.conf = MagicMock()
    app.conf.beat_max_loop_interval = 300
    app.conf.beat_cron_starting_deadline = 3600
    return app


def _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=None):
    """Return a DynamicDatabaseScheduler with all external deps patched."""
    schedules = schedules or []

    monkeypatch.setattr(
        "celery_db_scheduler.beat.scheduler.get_enabled_schedules_sync",
        lambda session: schedules,
    )
    monkeypatch.setattr(
        "celery_db_scheduler.beat.scheduler.get_task_registry",
        lambda: fake_registry,
    )

    fake_session_ctx = MagicMock()
    fake_session_ctx.__enter__ = MagicMock(return_value=MagicMock())
    fake_session_ctx.__exit__ = MagicMock(return_value=False)
    monkeypatch.setattr(
        "celery_db_scheduler.beat.scheduler.SyncSession",
        MagicMock(return_value=fake_session_ctx),
    )

    scheduler = DynamicDatabaseScheduler.__new__(DynamicDatabaseScheduler)
    scheduler.app = mock_celery_app
    scheduler.producer = MagicMock()
    scheduler.max_interval = 300
    scheduler._last_sync = 0.0
    scheduler._entries = {}
    scheduler._sync_interval = 60.0
    scheduler.Entry = _make_entry_class()
    scheduler.apply_async = MagicMock()
    return scheduler


def _make_entry_class():
    """Minimal stand-in for celery.beat.ScheduleEntry."""

    class Entry:
        def __init__(self, name, task, schedule, args, kwargs, options,
                     total_run_count=0, last_run_at=None, **_):
            self.name = name
            self.task = task
            self.schedule = schedule
            self.args = args
            self.kwargs = kwargs
            self.options = options
            self.total_run_count = total_run_count
            self.last_run_at = last_run_at

        def is_due(self):
            return self.schedule.is_due(self.last_run_at)

    return Entry


# ===========================================================================
# OneTimeSchedule tests
# ===========================================================================

class TestOneTimeSchedule:

    def test_is_due_future(self):
        sched = OneTimeSchedule(run_at=_utc(seconds=120))
        is_due, next_check = sched.is_due(None)
        assert is_due is False
        assert next_check >= 1

    def test_is_due_past(self):
        sched = OneTimeSchedule(run_at=_utc(seconds=-5))
        is_due, next_check = sched.is_due(None)
        assert is_due is True
        assert next_check == 60

    def test_is_due_already_run(self):
        sched = OneTimeSchedule(run_at=_utc(seconds=-5))
        sched._has_run = True
        is_due, next_check = sched.is_due(None)
        assert is_due is False
        assert next_check == 365 * 24 * 60 * 60

    def test_mark_as_run_sets_flag(self):
        sched = OneTimeSchedule(run_at=_utc(seconds=-1))
        assert sched._has_run is False
        sched.mark_as_run()
        assert sched._has_run is True

    def test_naive_datetime_converted_to_utc(self):
        naive = _naive(seconds=60)
        sched = OneTimeSchedule(run_at=naive)
        assert sched.run_at.tzinfo is not None

    def test_aware_datetime_converted_to_utc(self):
        aware = datetime.now(tz=timezone(timedelta(hours=5)))
        sched = OneTimeSchedule(run_at=aware)
        assert sched.run_at.tzinfo == timezone.utc

    def test_repr(self):
        sched = OneTimeSchedule(run_at=_utc(seconds=30))
        r = repr(sched)
        assert "OneTimeSchedule" in r
        assert "has_run=False" in r

    def test_is_due_exactly_on_time(self):
        # run_at slightly in the past simulates "now"
        sched = OneTimeSchedule(run_at=_utc(seconds=-1))
        is_due, _ = sched.is_due(None)
        assert is_due is True

    def test_next_check_delay_is_reasonable(self):
        sched = OneTimeSchedule(run_at=_utc(seconds=200))
        _, delay = sched.is_due(None)
        assert 190 <= delay <= 210


# ===========================================================================
# DynamicDatabaseScheduler.sync tests
# ===========================================================================

class TestDynamicDatabaseSchedulerSync:

    def test_sync_empty_db(self, monkeypatch, fake_registry, mock_celery_app):
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[])
        s.sync()
        assert s._entries == {}

    def test_sync_recurring_task(self, monkeypatch, fake_registry, mock_celery_app):
        row = _make_schedule_row(interval_seconds=30, start_datetime=_utc(seconds=-60))
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        assert len(s._entries) == 1
        entry = list(s._entries.values())[0]
        assert entry.task == "myapp.add"

    def test_sync_one_time_task_not_yet_run(self, monkeypatch, fake_registry, mock_celery_app):
        row = _make_schedule_row(
            interval_seconds=None,
            start_datetime=_utc(seconds=60),
            last_run_at=None,
        )
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        assert len(s._entries) == 1
        entry = list(s._entries.values())[0]
        assert isinstance(entry.schedule, OneTimeSchedule)
        assert entry.schedule._has_run is False

    def test_sync_one_time_task_already_executed(self, monkeypatch, fake_registry, mock_celery_app):
        row = _make_schedule_row(
            interval_seconds=None,
            start_datetime=_utc(seconds=-60),
            last_run_at=_utc(seconds=-30),
        )
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        entry = list(s._entries.values())[0]
        assert entry.schedule._has_run is True

    def test_sync_unknown_task_skipped(self, monkeypatch, mock_celery_app):
        row = _make_schedule_row(task_name="unknown.task", interval_seconds=30)
        s = _build_scheduler(monkeypatch, {}, mock_celery_app, schedules=[row])
        s.sync()
        assert s._entries == {}

    def test_sync_one_time_missing_start_datetime_skipped(
        self, monkeypatch, fake_registry, mock_celery_app
    ):
        row = _make_schedule_row(interval_seconds=None, start_datetime=None)
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        assert s._entries == {}

    def test_sync_preserves_existing_one_time_entry(
        self, monkeypatch, fake_registry, mock_celery_app
    ):
        run_at = _utc(seconds=120)
        row = _make_schedule_row(interval_seconds=None, start_datetime=run_at)
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()

        # Grab the schedule object created in the first sync
        entry_name = list(s._entries.keys())[0]
        original_sched = s._entries[entry_name].schedule

        # Second sync — entry should be preserved (same object)
        s.sync()
        preserved_sched = s._entries[entry_name].schedule
        assert preserved_sched is original_sched

    def test_sync_replaces_one_time_entry_when_time_changes(
        self, monkeypatch, fake_registry, mock_celery_app
    ):
        run_at_v1 = _utc(seconds=120)
        row = _make_schedule_row(interval_seconds=None, start_datetime=run_at_v1)
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        entry_name = list(s._entries.keys())[0]
        original_sched = s._entries[entry_name].schedule

        # Change start_datetime on the row
        row.start_datetime = _utc(seconds=999)
        s.sync()
        new_sched = s._entries[entry_name].schedule
        assert new_sched is not original_sched

    def test_sync_recurring_task_no_start_datetime(
        self, monkeypatch, fake_registry, mock_celery_app
    ):
        row = _make_schedule_row(interval_seconds=60, start_datetime=None, last_run_at=None)
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        # Should load without error, last_run_at derived from now
        assert len(s._entries) == 1

    def test_sync_task_removed_when_no_longer_in_db(
        self, monkeypatch, fake_registry, mock_celery_app
    ):
        row = _make_schedule_row(id=1, interval_seconds=30, start_datetime=_utc(seconds=-60))
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        assert len(s._entries) == 1

        # Second sync returns no schedules
        monkeypatch.setattr(
            "celery_db_scheduler.beat.scheduler.get_enabled_schedules_sync",
            lambda session: [],
        )
        s.sync()
        assert s._entries == {}

    def test_sync_exception_does_not_raise(self, monkeypatch, mock_celery_app):
        monkeypatch.setattr(
            "celery_db_scheduler.beat.scheduler.get_enabled_schedules_sync",
            MagicMock(side_effect=RuntimeError("DB down")),
        )
        monkeypatch.setattr(
            "celery_db_scheduler.beat.scheduler.get_task_registry",
            lambda: {},
        )
        fake_ctx = MagicMock()
        fake_ctx.__enter__ = MagicMock(return_value=MagicMock())
        fake_ctx.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr(
            "celery_db_scheduler.beat.scheduler.SyncSession",
            MagicMock(return_value=fake_ctx),
        )
        s = _build_scheduler(monkeypatch, {}, mock_celery_app, schedules=[])
        # Must not raise
        s.sync()


# ===========================================================================
# DynamicDatabaseScheduler.tick tests
# ===========================================================================

class TestDynamicDatabaseSchedulerTick:

    def _scheduler_with_entries(self, monkeypatch, fake_registry, mock_celery_app, entries):
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[])
        s._entries = {e.name: e for e in entries}
        return s

    def test_tick_due_task_fires_apply_async(self, monkeypatch, fake_registry, mock_celery_app):
        Entry = _make_entry_class()
        sched = OneTimeSchedule(run_at=_utc(seconds=-1))
        entry = Entry(
            name="myapp.add-1", task="myapp.add",
            schedule=sched, args=[1], kwargs={}, options={},
        )
        s = self._scheduler_with_entries(monkeypatch, fake_registry, mock_celery_app, [entry])
        s._last_sync = time.monotonic()  # prevent re-sync

        s.tick()
        s.apply_async.assert_called_once()

    def test_tick_not_due_task_does_not_fire(self, monkeypatch, fake_registry, mock_celery_app):
        Entry = _make_entry_class()
        sched = OneTimeSchedule(run_at=_utc(seconds=999))
        entry = Entry(
            name="myapp.add-1", task="myapp.add",
            schedule=sched, args=[1], kwargs={}, options={},
        )
        s = self._scheduler_with_entries(monkeypatch, fake_registry, mock_celery_app, [entry])
        s._last_sync = time.monotonic()

        s.tick()
        s.apply_async.assert_not_called()

    def test_tick_one_time_task_marked_as_run(self, monkeypatch, fake_registry, mock_celery_app):
        Entry = _make_entry_class()
        sched = OneTimeSchedule(run_at=_utc(seconds=-1))
        entry = Entry(
            name="myapp.add-1", task="myapp.add",
            schedule=sched, args=[1], kwargs={}, options={},
        )
        s = self._scheduler_with_entries(monkeypatch, fake_registry, mock_celery_app, [entry])
        s._last_sync = time.monotonic()
        # Patch _update_last_run_at to avoid DB call
        s._update_last_run_at = MagicMock()

        s.tick()
        assert sched._has_run is True

    def test_tick_increments_total_run_count(self, monkeypatch, fake_registry, mock_celery_app):
        Entry = _make_entry_class()
        sched = OneTimeSchedule(run_at=_utc(seconds=-1))
        entry = Entry(
            name="myapp.add-1", task="myapp.add",
            schedule=sched, args=[1], kwargs={}, options={}, total_run_count=3,
        )
        s = self._scheduler_with_entries(monkeypatch, fake_registry, mock_celery_app, [entry])
        s._last_sync = time.monotonic()
        s._update_last_run_at = MagicMock()

        s.tick()
        assert entry.total_run_count == 4

    def test_tick_triggers_sync_after_interval(self, monkeypatch, fake_registry, mock_celery_app):
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[])
        s._last_sync = time.monotonic() - s._sync_interval - 1  # force sync
        s.sync = MagicMock()

        s.tick()
        s.sync.assert_called_once()

    def test_tick_returns_float(self, monkeypatch, fake_registry, mock_celery_app):
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[])
        s._last_sync = time.monotonic()
        result = s.tick()
        assert isinstance(result, (int, float))

    def test_tick_returns_min_of_remaining_times(self, monkeypatch, fake_registry, mock_celery_app):
        Entry = _make_entry_class()
        sched = OneTimeSchedule(run_at=_utc(seconds=50))
        entry = Entry(
            name="myapp.add-1", task="myapp.add",
            schedule=sched, args=[1], kwargs={}, options={},
        )
        s = self._scheduler_with_entries(monkeypatch, fake_registry, mock_celery_app, [entry])
        s._last_sync = time.monotonic()

        result = s.tick()
        assert result <= s._sync_interval


# ===========================================================================
# DynamicDatabaseScheduler._update_last_run_at tests
# ===========================================================================

class TestUpdateLastRunAt:

    def test_update_existing_task(self, monkeypatch, sqlite_session_factory, mock_celery_app, fake_registry):
        # Insert a real row into SQLite
        Session = sqlite_session_factory
        with Session() as session:
            row = TaskSchedule(
                task_name="myapp.add",
                interval_seconds=30,
                enabled=True,
            )
            session.add(row)
            session.commit()
            task_id = row.id

        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app)
        monkeypatch.setattr(
            "celery_db_scheduler.beat.scheduler.SyncSession",
            Session,
        )
        now = datetime.now(timezone.utc)
        s._update_last_run_at(f"myapp.add-{task_id}", now)

        with Session() as session:
            updated = session.get(TaskSchedule, task_id)
            assert updated.last_run_at is not None

    def test_update_missing_task_logs_warning(
        self, monkeypatch, sqlite_session_factory, mock_celery_app, fake_registry, caplog
    ):
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app)
        monkeypatch.setattr(
            "celery_db_scheduler.beat.scheduler.SyncSession",
            sqlite_session_factory,
        )

        import logging
        with caplog.at_level(logging.WARNING, logger="celery_db_scheduler.beat.scheduler"):
            s._update_last_run_at("myapp.add-99999", datetime.now(timezone.utc))

        assert "not found" in caplog.text.lower()

    def test_update_bad_entry_name_does_not_raise(
        self, monkeypatch, fake_registry, mock_celery_app
    ):
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app)
        # Should catch the ValueError from int("bad") and log it
        s._update_last_run_at("bad-entry-name", datetime.now(timezone.utc))


# ===========================================================================
# schedule property
# ===========================================================================

class TestScheduleProperty:

    def test_schedule_property_returns_entries(self, monkeypatch, fake_registry, mock_celery_app):
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[])
        s._entries = {"foo": MagicMock()}
        assert s.schedule is s._entries


# ===========================================================================
# CronSchedule tests
# ===========================================================================

class TestCronSchedule:
    """Tests for CronSchedule (wall-clock schedules in a given timezone)."""

    @staticmethod
    def _cron(mock_celery_app, now, expression="0 20 * * *", tz_name="Europe/Berlin"):
        return CronSchedule(
            expression, tz_name, nowfun=lambda: now, app=mock_celery_app
        )

    def test_rejects_wrong_field_count(self):
        with pytest.raises(ValueError, match="5 fields"):
            CronSchedule("0 20 * *", "UTC")

    def test_rejects_unknown_timezone(self):
        from zoneinfo import ZoneInfoNotFoundError

        with pytest.raises(ZoneInfoNotFoundError):
            CronSchedule("0 20 * * *", "Mars/Olympus")

    def test_repr(self):
        assert repr(CronSchedule("0 20 * * *", "Europe/Berlin")) == (
            "<CronSchedule: 0 20 * * * (Europe/Berlin)>"
        )

    def test_not_due_before_local_time(self, mock_celery_app):
        now = datetime(2026, 10, 29, 18, 59, 30, tzinfo=timezone.utc)
        last_run = datetime(2026, 10, 28, 19, 0, tzinfo=timezone.utc)

        is_due, next_in = self._cron(mock_celery_app, now).is_due(last_run)

        assert is_due is False
        assert next_in == 30

    def test_due_at_local_time(self, mock_celery_app):
        now = datetime(2026, 10, 29, 19, 0, 1, tzinfo=timezone.utc)
        last_run = datetime(2026, 10, 28, 19, 0, tzinfo=timezone.utc)

        is_due, next_in = self._cron(mock_celery_app, now).is_due(last_run)

        assert is_due is True
        assert next_in == 86399

    def test_naive_last_run_at_is_treated_as_utc(self, mock_celery_app):
        now = datetime(2026, 10, 29, 19, 0, 1, tzinfo=timezone.utc)

        is_due, _ = self._cron(mock_celery_app, now).is_due(datetime(2026, 10, 28, 19, 0))

        assert is_due is True

    def test_keeps_local_time_across_dst_change(self, mock_celery_app):
        last_run = datetime(2026, 10, 24, 18, 0, tzinfo=timezone.utc)

        before = self._cron(
            mock_celery_app, datetime(2026, 10, 25, 18, 59, tzinfo=timezone.utc)
        )
        at = self._cron(
            mock_celery_app, datetime(2026, 10, 25, 19, 0, tzinfo=timezone.utc)
        )

        assert before.is_due(last_run) == (False, 60)
        assert at.is_due(last_run)[0] is True

    def test_catches_up_run_missed_within_deadline(self, mock_celery_app):
        now = datetime(2026, 10, 29, 19, 30, tzinfo=timezone.utc)
        last_run = datetime(2026, 10, 28, 19, 0, tzinfo=timezone.utc)

        assert self._cron(mock_celery_app, now).is_due(last_run)[0] is True

    def test_skips_run_missed_beyond_deadline(self, mock_celery_app):
        now = datetime(2026, 10, 30, 2, 0, tzinfo=timezone.utc)
        last_run = datetime(2026, 10, 28, 19, 0, tzinfo=timezone.utc)

        is_due, next_in = self._cron(mock_celery_app, now).is_due(last_run)

        assert is_due is False
        assert next_in == 17 * 3600


class TestSchedulerCronTasks:
    def test_loads_cron_task_in_schedule_timezone(
        self, monkeypatch, fake_registry, mock_celery_app
    ):
        last_run = datetime(2026, 10, 28, 19, 0, tzinfo=timezone.utc)
        row = _make_schedule_row(
            interval_seconds=None,
            cron_expression="0 20 * * *",
            timezone="Europe/Berlin",
            last_run_at=last_run,
        )
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        entry = s.schedule[f"{row.task_name}-{row.id}"]

        assert isinstance(entry.schedule, CronSchedule)
        assert entry.schedule.tz.key == "Europe/Berlin"
        assert entry.last_run_at == last_run

    def test_new_cron_task_waits_for_next_match(
        self, monkeypatch, fake_registry, mock_celery_app
    ):
        row = _make_schedule_row(
            interval_seconds=None,
            cron_expression="0 20 * * *",
        )
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        entry = s.schedule[f"{row.task_name}-{row.id}"]

        assert entry.last_run_at is not None
        assert entry.is_due()[0] is False

    def test_cron_task_preserves_in_memory_last_run_at(
        self, monkeypatch, fake_registry, mock_celery_app
    ):
        row = _make_schedule_row(
            interval_seconds=None,
            cron_expression="0 20 * * *",
        )
        s = _build_scheduler(monkeypatch, fake_registry, mock_celery_app, schedules=[row])
        s.sync()
        entry_name = f"{row.task_name}-{row.id}"
        last_run = datetime.now(timezone.utc) - timedelta(minutes=5)
        s.schedule[entry_name].last_run_at = last_run
        s.sync()

        assert s.schedule[entry_name].last_run_at == last_run

    @pytest.mark.parametrize(
        "cron_expression, tz_name",
        [("0 20 * *", "UTC"), ("0 25 * * *", "UTC"), ("0 20 * * *", "Mars/Olympus")],
    )
    def test_invalid_cron_task_is_skipped(
        self, monkeypatch, fake_registry, mock_celery_app, cron_expression, tz_name
    ):
        row = _make_schedule_row(
            interval_seconds=None,
            cron_expression=cron_expression,
            timezone=tz_name,
        )
        with patch("celery_db_scheduler.beat.scheduler.logger") as mock_logger:
            s = _build_scheduler(
                monkeypatch, fake_registry, mock_celery_app, schedules=[row]
            )
            s.sync()
            assert len(s.schedule) == 0

        assert any(
            "Invalid cron schedule" in str(call.args[0])
            for call in mock_logger.error.call_args_list
        )


def test_auto_disable_one_time_task_sync_does_not_disable_cron_task():
    schedule = MagicMock(spec=TaskSchedule)
    schedule.interval_seconds = None
    schedule.cron_expression = "0 20 * * *"
    schedule.enabled = True

    fake_session = MagicMock()
    fake_session.get.return_value = schedule

    def fake_get_session():
        yield fake_session

    @auto_disable_one_time_task()
    def fake_task(task_id):
        return "done"

    fake_app = types.ModuleType("app")
    fake_session_mod = types.ModuleType("app.session")
    fake_session_mod.get_session = fake_get_session
    fake_app.session = fake_session_mod
    with patch.dict(sys.modules, {"app": fake_app, "app.session": fake_session_mod}):
        assert fake_task(3000) == "done"

    fake_session.commit.assert_not_called()
    assert schedule.enabled is True
