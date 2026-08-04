"""Import ORM model modules here so Alembic autogenerate can discover them."""

from app.models.task import Task

__all__ = ["Task"]
