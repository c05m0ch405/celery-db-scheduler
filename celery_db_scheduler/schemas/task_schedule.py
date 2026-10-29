from datetime import datetime, time
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class TaskScheduleBase(BaseModel):
    task_name: str
    interval_seconds: Optional[int] = None
    cron_expression: Optional[str] = None
    task_args: Optional[List[Any]] = Field(default=None)
    task_kwargs: Optional[Dict[str, Any]] = Field(default=None)
    enabled: bool = True
    description: Optional[str] = None
    start_datetime: Optional[datetime] = None
    end_datetime: Optional[datetime] = None
    daily_start_time: Optional[time] = None
    daily_end_time: Optional[time] = None


class TaskScheduleCreate(TaskScheduleBase):
    pass


class TaskScheduleRead(TaskScheduleBase):
    id: int

    class Config:
        from_attributes = True  # Pydantic v2
