import json
from datetime import datetime, time

from sqlalchemy import Boolean, Integer, String, Text, Time
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from celery_db_scheduler.base import Base


class TaskSchedule(Base):
    __tablename__ = "task_schedules"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    task_name: Mapped[str] = mapped_column(String, nullable=False)
    interval_seconds: Mapped[int] = mapped_column(Integer, nullable=True)
    _task_args: Mapped[str | None] = mapped_column(Text, nullable=True)
    _task_kwargs: Mapped[str | None] = mapped_column(Text, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    start_datetime: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    end_datetime: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    daily_start_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    daily_end_time: Mapped[time | None] = mapped_column(Time, nullable=True)
    last_run_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
        default=None,
    )
    timezone: Mapped[str] = mapped_column(
        String, nullable=False, default="UTC", server_default="UTC"
    )

    @property
    def task_args(self):
        if self._task_args:
            try:
                return json.loads(self._task_args)
            except json.JSONDecodeError:  # pragma: no cover
                return []
        return []  # pragma: no cover

    @task_args.setter
    def task_args(self, value):
        self._task_args = json.dumps(value or [])

    @property
    def task_kwargs(self):
        if self._task_kwargs:
            try:
                return json.loads(self._task_kwargs)
            except json.JSONDecodeError:  # pragma: no cover
                return {}
        return {}  # pragma: no cover

    @task_kwargs.setter
    def task_kwargs(self, value):
        self._task_kwargs = json.dumps(value or {})
