from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from celery_db_scheduler.models.task_schedule import TaskSchedule
from celery_db_scheduler.schemas.task_schedule import TaskScheduleCreate


def get_enabled_schedules_sync(db: Session):
    result = db.execute(select(TaskSchedule).where(TaskSchedule.enabled.is_(True)))
    return result.scalars().all()


async def create_schedule(db: AsyncSession, schedule: TaskScheduleCreate):
    db_schedule = TaskSchedule(**schedule.model_dump())
    db.add(db_schedule)
    await db.commit()
    await db.refresh(db_schedule)
    return db_schedule


async def get_schedule_by_id(db: AsyncSession, schedule_id: int):
    result = await db.execute(
        select(TaskSchedule).where(TaskSchedule.id == schedule_id)
    )
    return result.scalar_one_or_none()
