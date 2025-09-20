import os
from sqlalchemy import create_engine, text
e = create_engine(os.environ["PG_DSN"], future=True, pool_pre_ping=True)
with e.begin() as c:
    n = c.execute(text("SELECT COUNT(*) FROM t_notice WHERE main_cnstty_nm = ''")).scalar()
print("still_blank =", n)
