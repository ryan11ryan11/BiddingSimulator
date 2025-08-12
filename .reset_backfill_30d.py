from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time, math

from core.config import settings
from core.clients.bid_public_info import BidPublicInfo
from core.clients.scsbid_info import ScsbidInfo
from apps.etl.tasks import (
    parse_notice_items,
    fetch_bsis_map_cnstwk,
    parse_prepar_detail_items,
)
from core.db.dao import upsert_notice, upsert_prep15_bulk

bp = BidPublicInfo(settings.bid_public_base, settings.service_key)
sc = ScsbidInfo(settings.scsbid_base, settings.service_key)

def collect_notices_with_progress(bgn: str, end: str, page_size: int = 100, sleep_s: float = 0.4):
    # 총건수 파악
    resp = bp.get("getBidPblancListInfoCnstwk", {
        "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end,
        "pageNo": 1, "numOfRows": 1
    })
    body = (resp or {}).get("response",{}).get("body",{})
    total = int(body.get("totalCount") or 0)
    pages = max(1, math.ceil(total / page_size))
    # 같은 윈도우의 기초금액 맵(보강용)
    bsis_map = fetch_bsis_map_cnstwk(bgn, end)

    saved = 0
    for page in range(1, pages+1):
        resp = bp.get("getBidPblancListInfoCnstwk", {
            "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end,
            "pageNo": page, "numOfRows": page_size
        })
        rows = parse_notice_items(resp, work_type_hint="Cnstwk")
        batch = 0
        for r in rows:
            if not r["base_amount"]:
                hit = bsis_map.get((r["bid_no"], r["ord"]))
                if hit and hit["base"]:
                    r["base_amount"] = hit["base"]
                    if hit["low"] is not None:  r["range_low"] = hit["low"]
                    if hit["high"] is not None: r["range_high"] = hit["high"]
            if r["bid_no"] and r["base_amount"]:
                upsert_notice(r)
                batch += 1
        saved += batch
        pct = 100.0 * min(page, pages) / pages
        print(f"[NOTICE {bgn}-{end}] page {page}/{pages} +{batch}  saved={saved}/{total}  ({pct:.1f}% done)")
        time.sleep(sleep_s)
    return saved, total

def collect_prep15_with_progress(bgn: str, end: str, page_size: int = 100, sleep_s: float = 0.4):
    # 총건수 파악
    resp = sc.get_prepar_pc_detail_cnstwk(
        inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=1, num_rows=1
    )
    body = (resp or {}).get("response",{}).get("body",{})
    total = int(body.get("totalCount") or 0)
    if total == 0:
        print(f"[PREP15 {bgn}-{end}] totalCount=0 -> skip")
        return 0, 0
    pages = max(1, math.ceil(total / page_size))

    saved = 0
    for page in range(1, pages+1):
        resp = sc.get_prepar_pc_detail_cnstwk(
            inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=page, num_rows=page_size
        )
        m = parse_prepar_detail_items(resp)
        batch = 0
        if m:
            for _, rows in m.items():
                upsert_prep15_bulk(rows)
                batch += len(rows)
        saved += batch
        pct = 100.0 * min(page, pages) / pages
        print(f"[PREP15 {bgn}-{end}] page {page}/{pages} +{batch}  saved={saved}/{total}  ({pct:.1f}% done)")
        time.sleep(sleep_s)
    return saved, total

def main(days: int = 30):
    tz = ZoneInfo("Asia/Seoul")
    now = datetime.now(tz)
    total_saved_n = total_saved_p = 0
    grand_total_n = grand_total_p = 0

    for d in range(days, 0, -1):
        bgn = (now - timedelta(days=d)).strftime("%Y%m%d0000")
        end = (now - timedelta(days=d)).strftime("%Y%m%d2359")
        print("="*80)
        print(f"WINDOW {bgn} ~ {end}")

        try:
            n_saved, n_total = collect_notices_with_progress(bgn, end)
        except Exception as e:
            print(f"[NOTICE {bgn}-{end}] ERROR: {e}")
            n_saved, n_total = 0, 0

        try:
            p_saved, p_total = collect_prep15_with_progress(bgn, end)
        except Exception as e:
            print(f"[PREP15 {bgn}-{end}] ERROR: {e}")
            p_saved, p_total = 0, 0

        total_saved_n += n_saved; grand_total_n += n_total
        total_saved_p += p_saved; grand_total_p += p_total

        # 일별 요약
        print(f"[DAY SUMMARY] notices {n_saved}/{n_total}, prep15 {p_saved}/{p_total}")

    print("="*80)
    print(f"[OVERALL] notices saved {total_saved_n}/{grand_total_n}, prep15 saved {total_saved_p}/{grand_total_p}")

if __name__ == "__main__":
    main(30)
