"""
load_env.py
----------
Lightweight helper to load a .env file from the project root.
Import this at the top of any script that needs environment variables.
"""

from pathlib import Path
from dotenv import load_dotenv

# Look for a .env in the project root (same dir as this file)
env_path = Path(__file__).resolve().parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
