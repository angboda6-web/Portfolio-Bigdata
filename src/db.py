from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url

load_dotenv()


def default_database_url(base_dir: Path, explicit_url: str | None = None) -> str:
    if explicit_url:
        return explicit_url
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url
    return f"sqlite:///{(base_dir / 'warehouse' / 'sales.db').resolve().as_posix()}"


def create_database_engine(database_url: str) -> Engine:
    url = make_url(database_url)
    if url.drivername.startswith("sqlite") and url.database and url.database != ":memory:":
        Path(url.database).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
    return create_engine(url, future=True, connect_args=connect_args)
