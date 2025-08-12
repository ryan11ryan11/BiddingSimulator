from sqlalchemy import create_engine
from core.config import settings

# SQLAlchemy 동기 엔진 (psycopg3)
engine = create_engine(settings.pg_dsn, future=True, pool_pre_ping=True)