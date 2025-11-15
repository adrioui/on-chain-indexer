from collections.abc import Generator
from threading import Lock

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from .config import get_settings

settings = get_settings()
connect_args = (
    {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
)


class Base(DeclarativeBase):
    pass


engine = create_engine(
    settings.database_url, echo=False, pool_pre_ping=True, connect_args=connect_args
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


_init_lock = Lock()
_initialized = False


def init_db() -> None:
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        Base.metadata.create_all(bind=engine)
        _initialized = True


def get_session() -> Generator:
    init_db()
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
