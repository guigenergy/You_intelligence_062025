import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

# Schema para operação (plead, public, etc.)
DB_SCHEMA = os.getenv("DB_SCHEMA", "lead")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "dbname": os.getenv("DB_NAME", "youon"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "your_password_here"),
    "port": os.getenv("DB_PORT", "5432"),
    "sslmode": "require",
    # Passa o search_path para o PostgreSQL
    "options": f"-csearch_path={DB_SCHEMA}"
}

@contextmanager
def get_db_connection():
    # Passa todas as configurações, incluindo options
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_db_cursor(commit=False, dict_cursor=True):
    with get_db_connection() as conn:
        cursor_factory = RealDictCursor if dict_cursor else None
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            # Garante que o search_path esteja correto antes de qualquer operação
            cur.execute(f"SET search_path TO {DB_SCHEMA}")
            try:
                yield cur
                if commit:
                    conn.commit()
            except Exception:
                conn.rollback()
                raise