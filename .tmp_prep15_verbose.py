from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time

from core.config import settings
from core.clients.scsbid_info import ScsbidInfo
from apps.etl.tasks import parse_prepar_detail_items   # 기존 파서 재사용
from core.db.dao import upsert_prep15_bulk

sc = ScsbidInfo(settings.scsbid_base, settings.service_key)

# 1일 윈도우 (원하면 days=7 등으로 확대)
now = datetime.now(ZoneInfo("Asia/Seoul"))
bgn = (now - timedelta(days=1)).strftime("%Y%m%d0000")
end = now.strftime("%Y%m%d2359")

page = 1
page_size = 100
saved = 0

# 첫 페이지로 totalCount 파악
resp = sc.get_prepar_pc_detail_cnstwk(inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=page, num_rows=page_size)
body = (resp or {}).get("response",{}).get("body",{})
total = int(body.get("totalCount") or 0)
print(f"window: {bgn}-{end}, totalCount={total}")

while True:
    if page > 1:
        resp = sc.get_prepar_pc_detail_cnstwk(inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=page, num_rows=page_size)

    m = parse_prepar_detail_items(resp)
    batch = 0
    for _, rows in (m or {}).items():
        upsert_prep15_bulk(rows)
        batch += len(rows)
    saved += batch
    print(f"page {page:>4}: +{batch:>4}  saved={saved}/{total}")

    if page * page_size >= total:
        break
    page += 1
    time.sleep(0.3)   # 호출 한도 피하기 위한 대기 (느리면 0.5로)
print("DONE:", saved)
