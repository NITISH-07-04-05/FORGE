from collections.abc import Generator

from sqlalchemy.orm import Session

from app.db.session import SessionLocal


def get_db() -> Generator[Session, None, None]:
    # FastAPI uses this dependency to provide one session per request.
    db = SessionLocal()

    try:
        yield db

    finally:
        # Closing the session always returns the connection to the pool.
        db.close()
