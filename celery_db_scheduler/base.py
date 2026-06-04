from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """SQLAlchemy declarative base shared by all celery-db-scheduler models.

    Import and use this Base when integrating the library's models into your
    own SQLAlchemy metadata so a single ``metadata.create_all()`` covers
    everything.
    """
