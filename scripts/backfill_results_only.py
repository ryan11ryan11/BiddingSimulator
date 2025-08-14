# scripts/backfill_results_only.py
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time

from apps.etl.tasks import collect_and_load_results_cnstwk  # 이미 너의 tasks.py에 있음

TZ = ZoneInfo("Asia/Seoul")
DAYS = 180  # 필요한 기간으로 조절

def main():
    today = datetime.now(TZ).date()
    for i in range(1, DAYS + 1):
        d = today - timedelta(days=i)
        bgn = datetime(d.year, d.month, d.day, 0, 0, tzinfo=TZ).strftime("%Y%m%d%H%M")
        end = datetime(d.year, d.month, d.day, 23, 59, tzinfo=TZ).strftime("%Y%m%d%H%M")
        n = collect_and_load_results_cnstwk(bgn, end)
        print(f"[RESULT {d}] inserted={n}")
        time.sleep(0.3)  # 호출 여유

if __name__ == "__main__":
    main()
