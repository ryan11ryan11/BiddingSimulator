# .backfill_resume_12m.py
"""
재시작 가능한 통합 백필 스크립트 (공고/예가15/결과 모두)
- 한 곳(BACKFILL_DAYS)에서 수집 기간(일)을 관리
- stream별 워터마크(t_etl_watermark)로 페이지 재개/조기종료
- 환경변수로 기간/대상 스트림/페이지크기 조절

예)
  # 6개월(180일) 전체 수집
  BACKFILL_DAYS=180 python .backfill_resume_12m.py

  # 결과만 90일 수집
  $env:BACKFILL_DAYS=90; $env:ENABLE_NOTICE=0; $env:ENABLE_PREP15=0; $env:ENABLE_RESULT=1; python .backfill_resume_12m.py
"""

from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import os
import math
import time
from typing import Tuple, Dict, List, Optional

from sqlalchemy import text

from core.config import settings
from core.db.engine import engine

from core.clients.bid_public_info import BidPublicInfo
from core.clients.scsbid_info import ScsbidInfo

# apps.etl.tasks에서 파서/보조함수만 재사용
from apps.etl.tasks import (
    parse_notice_items,
    fetch_bsis_map_cnstwk,
    parse_prepar_detail_items,
    parse_result_items,
)
from core.db.dao import upsert_notice, upsert_prep15_bulk, upsert_result

# ---------------------------------------------------------------------
# 단일 진실 소스: 수집 기간(일 단위)
# ---------------------------------------------------------------------
BACKFILL_DAYS: int = int(os.getenv("BACKFILL_DAYS", "180"))

# 호출 한도/페이지 크기
SLEEP_S = float(os.getenv("SLEEP_S", "0.5"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "300"))  # 엔드포인트 허용 범위에 맞춰 조정

# 스트림 ON/OFF (1/0)
ENABLE_NOTICE = os.getenv("ENABLE_NOTICE", "1") == "1"
ENABLE_PREP15 = os.getenv("ENABLE_PREP15", "1") == "1"
ENABLE_RESULT = os.getenv("ENABLE_RESULT", "1") == "1"

bp = BidPublicInfo(settings.bid_public_base, settings.service_key)
sc = ScsbidInfo(settings.scsbid_base, settings.service_key)

# ---------------------------------------------------------------------
# 워터마크 유틸
# ---------------------------------------------------------------------
def upsert_watermark(
    stream: str,
    bucket: date,
    last_page: Optional[int] = None,
    total_pages: Optional[int] = None,
    total_count: Optional[int] = None,
):
    sql = text(
        """
        INSERT INTO t_etl_watermark(stream, bucket, last_page, total_pages, total_count, updated_at)
        VALUES (:stream, :bucket, COALESCE(:last_page,0), :total_pages, :total_count, now())
        ON CONFLICT (stream, bucket) DO UPDATE SET
          last_page   = COALESCE(EXCLUDED.last_page, t_etl_watermark.last_page),
          total_pages = COALESCE(EXCLUDED.total_pages, t_etl_watermark.total_pages),
          total_count = COALESCE(EXCLUDED.total_count, t_etl_watermark.total_count),
          updated_at  = now();
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "stream": stream,
                "bucket": bucket,
                "last_page": last_page,
                "total_pages": total_pages,
                "total_count": total_count,
            },
        )

def get_watermark(stream: str, bucket: date) -> Dict:
    sql = text("SELECT * FROM t_etl_watermark WHERE stream=:s AND bucket=:b")
    with engine.begin() as conn:
        row = conn.execute(sql, {"s": stream, "b": bucket}).mappings().first()
        return dict(row) if row else {}

# ---------------------------------------------------------------------
# totalCount → pages 계산
# ---------------------------------------------------------------------
def total_pages_for_notice(bgn: str, end: str) -> Tuple[int, int]:
    resp = bp.get(
        "getBidPblancListInfoCnstwk",
        {"inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end, "pageNo": 1, "numOfRows": 1},
    )
    body = (resp or {}).get("response", {}).get("body", {})
    total = int(body.get("totalCount") or 0)
    pages = 0 if total == 0 else max(1, math.ceil(total / PAGE_SIZE))
    return total, pages

def total_pages_for_prep15(bgn: str, end: str) -> Tuple[int, int]:
    resp = sc.get_prepar_pc_detail_cnstwk(
        inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=1, num_rows=1
    )
    body = (resp or {}).get("response", {}).get("body", {})
    total = int(body.get("totalCount") or 0)
    pages = 0 if total == 0 else max(1, math.ceil(total / PAGE_SIZE))
    return total, pages

def total_pages_for_result(bgn: str, end: str) -> Tuple[int, int]:
    # 결과 목록 (공사)
    resp = sc.get_openg_result_list_cnstwk(
        inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=1, num_rows=1
    )
    body = (resp or {}).get("response", {}).get("body", {})
    total = int(body.get("totalCount") or 0)
    pages = 0 if total == 0 else max(1, math.ceil(total / PAGE_SIZE))
    return total, pages

# ---------------------------------------------------------------------
# 내부 헬퍼: bsis 맵 키 기반 폴백
# ---------------------------------------------------------------------
def _safe_fetch_bsis_map(
    bgn: str, end: str, keys: List[tuple]
) -> Dict[tuple, Dict]:
    """
    keys 기반 최소 조회를 우선 시도하고, 미지원(TypeError) 시 날짜 범위 1회 조회로 폴백.
    """
    try:
        return fetch_bsis_map_cnstwk(bgn, end, keys=keys)  # type: ignore
    except TypeError:
        return fetch_bsis_map_cnstwk(bgn, end)

# ---------------------------------------------------------------------
# 스트림별 버킷 처리
# ---------------------------------------------------------------------
def process_notice_bucket(bucket: date):
    if not ENABLE_NOTICE:
        return
    stream = "notice:cnstwk"
    tz = ZoneInfo("Asia/Seoul")
    bgn = datetime(bucket.year, bucket.month, bucket.day, 0, 0, tzinfo=tz).strftime("%Y%m%d%H%M")
    end = datetime(bucket.year, bucket.month, bucket.day, 23, 59, tzinfo=tz).strftime("%Y%m%d%H%M")

    wm = get_watermark(stream, bucket)
    if not wm:
        total, pages = total_pages_for_notice(bgn, end)
        upsert_watermark(stream, bucket, last_page=0, total_pages=pages, total_count=total)
        wm = get_watermark(stream, bucket)

    total = wm.get("total_count") or 0
    pages = 0 if total == 0 else max(1, math.ceil(total / PAGE_SIZE))
    last = min(wm.get("last_page") or 0, max(0, pages - 1))

    if pages == 0 and total == 0:
        print(f"[NOTICE {bucket}] total=0 -> skip")
        return

    early_stop = False
    bsis_cache: Dict[tuple, Dict] = {}

    for page in range(last + 1, pages + 1):
        for attempt in range(6):
            try:
                resp = bp.get(
                    "getBidPblancListInfoCnstwk",
                    {
                        "inqryDiv": 1,
                        "inqryBgnDt": bgn,
                        "inqryEndDt": end,
                        "pageNo": page,
                        "numOfRows": PAGE_SIZE,
                    },
                )
                rows = parse_notice_items(resp, work_type_hint="Cnstwk")
                fetched = len(rows)
                if not rows:
                    upsert_watermark(stream, bucket, last_page=page, total_pages=page)
                    print(f"[NOTICE {bucket}] page {page} empty -> stop early")
                    early_stop = True
                else:
                    # base_amount 부족분만 bsis 조회
                    missing_keys: List[tuple] = []
                    for r in rows:
                        if not r.get("base_amount"):
                            bid_no = r.get("bid_no")
                            ord_ = r.get("ord")
                            if bid_no and ord_ is not None:
                                missing_keys.append((bid_no, ord_))
                    if missing_keys:
                        if not bsis_cache:
                            bsis_cache.update(_safe_fetch_bsis_map(bgn, end, missing_keys))
                        else:
                            try:
                                from apps.etl.tasks import fetch_bsis_map_cnstwk as _f
                                bsis_cache.update(_f(bgn, end, keys=missing_keys))  # type: ignore
                            except TypeError:
                                pass

                    batch = 0
                    for r in rows:
                        if not r.get("base_amount"):
                            hit = bsis_cache.get((r.get("bid_no"), r.get("ord")))
                            if hit and hit.get("base"):
                                r["base_amount"] = hit["base"]
                                if hit.get("low") is not None:
                                    r["range_low"] = hit["low"]
                                if hit.get("high") is not None:
                                    r["range_high"] = hit["high"]
                        if r.get("bid_no") and r.get("base_amount"):
                            upsert_notice(r)
                            batch += 1

                    upsert_watermark(stream, bucket, last_page=page)
                    pct = 100.0 * page / pages if pages else 100.0
                    print(f"[NOTICE {bucket}] page {page}/{pages} fetched={fetched} inserted={batch} ~ ({pct:.1f}% done)")
                time.sleep(SLEEP_S); break
            except Exception as e:
                wait = min(60, 2 ** attempt * 2)
                print(f"[NOTICE {bucket}] page {page} ERROR: {str(e)[:180]} ... retry in {wait}s")
                time.sleep(wait)
        else:
            print(f"[NOTICE {bucket}] page {page} hard-fail -> continue next page")
        if early_stop:
            break

def process_prep15_bucket(bucket: date):
    if not ENABLE_PREP15:
        return
    stream = "prep15:cnstwk"
    tz = ZoneInfo("Asia/Seoul")
    bgn = datetime(bucket.year, bucket.month, bucket.day, 0, 0, tzinfo=tz).strftime("%Y%m%d%H%M")
    end = datetime(bucket.year, bucket.month, bucket.day, 23, 59, tzinfo=tz).strftime("%Y%m%d%H%M")

    wm = get_watermark(stream, bucket)
    if not wm:
        total, pages = total_pages_for_prep15(bgn, end)
        upsert_watermark(stream, bucket, last_page=0, total_pages=pages, total_count=total)
        wm = get_watermark(stream, bucket)

    total = wm.get("total_count") or 0
    pages = 0 if total == 0 else max(1, math.ceil(total / PAGE_SIZE))
    last = min(wm.get("last_page") or 0, max(0, pages - 1))

    if pages == 0 and total == 0:
        print(f"[PREP15 {bucket}] total=0 -> skip")
        return

    early_stop = False

    for page in range(last + 1, pages + 1):
        for attempt in range(6):
            try:
                resp = sc.get_prepar_pc_detail_cnstwk(
                    inqry_div=1,
                    inqry_bgn_dt=bgn,
                    inqry_end_dt=end,
                    page_no=page,
                    num_rows=PAGE_SIZE,
                )
                m = parse_prepar_detail_items(resp)
                fetched = 0
                batch = 0
                if m:
                    for _, rows in m.items():
                        fetched += len(rows)
                        if rows:
                            upsert_prep15_bulk(rows)
                            batch += len(rows)
                if fetched == 0:
                    upsert_watermark(stream, bucket, last_page=page, total_pages=page)
                    print(f"[PREP15 {bucket}] page {page} empty -> stop early")
                    early_stop = True
                else:
                    upsert_watermark(stream, bucket, last_page=page)
                    pct = 100.0 * page / pages if pages else 100.0
                    print(f"[PREP15 {bucket}] page {page}/{pages} fetched={fetched} inserted={batch} ~ ({pct:.1f}% done)")
                time.sleep(SLEEP_S); break
            except Exception as e:
                wait = min(60, 2 ** attempt * 2)
                print(f"[PREP15 {bucket}] page {page} ERROR: {str(e)[:180]} ... retry in {wait}s")
                time.sleep(wait)
        else:
            print(f"[PREP15 {bucket}] page {page} hard-fail -> continue next page")
        if early_stop:
            break

def process_result_bucket(bucket: date):
    if not ENABLE_RESULT:
        return
    stream = "result:cnstwk"
    tz = ZoneInfo("Asia/Seoul")
    bgn = datetime(bucket.year, bucket.month, bucket.day, 0, 0, tzinfo=tz).strftime("%Y%m%d%H%M")
    end = datetime(bucket.year, bucket.month, bucket.day, 23, 59, tzinfo=tz).strftime("%Y%m%d%H%M")

    wm = get_watermark(stream, bucket)
    if not wm:
        total, pages = total_pages_for_result(bgn, end)
        upsert_watermark(stream, bucket, last_page=0, total_pages=pages, total_count=total)
        wm = get_watermark(stream, bucket)

    total = wm.get("total_count") or 0
    pages = 0 if total == 0 else max(1, math.ceil(total / PAGE_SIZE))
    last = min(wm.get("last_page") or 0, max(0, pages - 1))

    if pages == 0 and total == 0:
        print(f"[RESULT {bucket}] total=0 -> skip")
        return

    early_stop = False

    for page in range(last + 1, pages + 1):
        for attempt in range(6):
            try:
                resp = sc.get_openg_result_list_cnstwk(
                    inqry_div=1,
                    inqry_bgn_dt=bgn,
                    inqry_end_dt=end,
                    page_no=page,
                    num_rows=PAGE_SIZE,
                )
                rows = parse_result_items(resp)
                fetched = len(rows)
                batch = 0
                if not rows:
                    upsert_watermark(stream, bucket, last_page=page, total_pages=page)
                    print(f"[RESULT {bucket}] page {page} empty -> stop early")
                    early_stop = True
                else:
                    for r in rows:
                        if r.get("bid_no"):
                            upsert_result(r)
                            batch += 1
                    upsert_watermark(stream, bucket, last_page=page)
                    pct = 100.0 * page / pages if pages else 100.0
                    print(f"[RESULT {bucket}] page {page}/{pages} fetched={fetched} inserted={batch} ~ ({pct:.1f}% done)")
                time.sleep(SLEEP_S); break
            except Exception as e:
                wait = min(60, 2 ** attempt * 2)
                print(f"[RESULT {bucket}] page {page} ERROR: {str(e)[:180]} ... retry in {wait}s")
                time.sleep(wait)
        else:
            print(f"[RESULT {bucket}] page {page} hard-fail -> continue next page")
        if early_stop:
            break

# ---------------------------------------------------------------------
# 메인: 과거 → 최근 순서로 버킷 순회
# ---------------------------------------------------------------------
def backfill_days(days: int = BACKFILL_DAYS):
    tz = ZoneInfo("Asia/Seoul")
    today_kst = datetime.now(tz).date()
    start = today_kst - timedelta(days=days)
    buckets = [start + timedelta(days=i) for i in range(1, days + 1)]  # 과거→최근

    for b in buckets:
        print("=" * 72)
        print(f"BUCKET {b.isoformat()} (KST day)  streams:"
              f"{'N' if ENABLE_NOTICE else ''}"
              f"{' P15' if ENABLE_PREP15 else ''}"
              f"{' R' if ENABLE_RESULT else ''}")
        # 순서는 상황에 따라 바꿔도 무방
        process_notice_bucket(b)
        process_prep15_bucket(b)
        process_result_bucket(b)

if __name__ == "__main__":
    backfill_days(BACKFILL_DAYS)
