#simple db connection utility

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env (if present)
load_dotenv()
DB_URL = os.getenv("DB_URL")
if not DB_URL:
    raise RuntimeError("DB_URL environment variable is not set. Set it in .env or in OS environment.")

_engine = create_engine(DB_URL)


def load_engine():
    global _engine
    return _engine