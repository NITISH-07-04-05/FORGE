from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.dependencies import get_db

# This router exposes simple database checks without requiring ORM models.
router = APIRouter(prefix="/database", tags=["Database"])


@router.get("/ping")
def database_ping(db: Annotated[Session, Depends(get_db)]) -> dict[str, int]:
    result = db.execute(text("SELECT 1"))

    return {"database": result.scalar_one()}


