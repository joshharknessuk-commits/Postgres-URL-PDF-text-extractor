"""
db_healthcheck.py
-----------------
Quick script to verify the Neon Postgres connection works.
"""

import load_env  # ensures .env is loaded
from config import engine
from sqlalchemy import text

def main():
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT version(), current_database(), now()")
            ).fetchone()
            print("✅ Connected successfully!")
            print(f"Postgres version: {result[0]}")
            print(f"Database:        {result[1]}")
            print(f"Time:            {result[2]}")
    except Exception as e:
        print("❌ Connection failed")
        print(e)

if __name__ == "__main__":
    main()