"""
config.py
---------
Central DB configuration. Creates an SQLAlchemy engine and sessionmaker
for connecting to your Neon Postgres database.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

# Ensure .env is loaded
import load_env  # noqa: F401

# --- Read environment variables ---
NEON_URL = os.getenv("NEON_URL")
if not NEON_URL:
    raise RuntimeError("NEON_URL is not set. Did you create a .env file?")

SQLALCHEMY_ECHO = os.getenv("SQLALCHEMY_ECHO", "false").lower() == "true"

# Normalize URL so SQLAlchemy uses the psycopg driver instead of psycopg2
db_url = make_url(NEON_URL)
if db_url.drivername == "postgresql":
    db_url = db_url.set(drivername="postgresql+psycopg")

# --- Create SQLAlchemy engine ---
engine = create_engine(
    db_url,
    echo=SQLALCHEMY_ECHO,
    pool_pre_ping=True,   # check connection before using
)

# --- Session factory ---
SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)

def get_db():
    """
    Provides a database session.
    Usage:
        with get_db() as db:
            db.execute(...)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
