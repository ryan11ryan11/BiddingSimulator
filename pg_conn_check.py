import os, sys
from sqlalchemy import create_engine, text
print("PG_DSN set?:", bool(os.getenv("PG_DSN")))
dsn = os.getenv("PG_DSN")
if not dsn:
    print("PG_DSN is missing"); sys.exit(2)
try:
    e = create_engine(dsn, future=True, pool_pre_ping=True)
    with e.connect() as c:
        who, db = c.execute(text("select current_user, current_database()")).one()
        print("DB OK:", who, db)
except Exception as ex:
    print("DB ERROR:", type(ex).__name__, ex)
    sys.exit(1)
