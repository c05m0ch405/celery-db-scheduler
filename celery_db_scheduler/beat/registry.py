from celery import current_app  # type: ignore


def get_task_registry() -> dict:
    """Return all user-registered Celery tasks (excludes built-in celery.* tasks)."""
    return {
        name: task
        for name, task in current_app.tasks.items()
        if not name.startswith("celery.")
    }
