import os
from sqlalchemy import create_engine, text

dsn = os.environ["PG_DSN"]  # 예: postgresql+psycopg2://user:pass@localhost:5432/biddingsim
engine = create_engine(dsn, future=True, pool_pre_ping=True)
with engine.begin() as conn:
    conn.execute(text("UPDATE t_notice SET main_cnstty_nm = NULL WHERE main_cnstty_nm = ''"))
print("Done")
