# .backfill_resume_12m.py
"""
백필 기간(backfill window)을 한 곳에서 관리하기 위한 스크립트.

- 코드 내 상단의 BACKFILL_DAYS만 변경하면 전체 수집 기간이 바뀐다.
- 운영 환경에서는 환경변수 BACKFILL_DAYS로도 동일 값을 덮어쓸 수 있다.
    예) BACKFILL_DAYS=180 python .backfill_resume_12m.py
"""

from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import os
import math
import time
from typing import Tuple, Dict, List, Optional

from core.config import settings
from core.db.engine import engine
from sqlalchemy import text

from core.clients.bid_public_info import BidPublicInfo
from core.clients.scsbid_info import ScsbidInfo
from apps.etl.tasks import (
    parse_notice_items,
    fetch_bsis_map_cnstwk,
    parse_prepar_detail_items,
)
from core.db.dao import upsert_notice, upsert_prep15_bulk

# ---------------------------------------------------------------------
# 단일 진실 소스: 수집 기간(일 단위)
# 이 값만 바꾸면 전체 백필 기간이 변경됨. (환경변수 BACKFILL_DAYS로도 덮어쓰기 가능)
# 기본값: 365일 (약 12개월)
# ---------------------------------------------------------------------
BACKFILL_DAYS: int = int(os.getenv("BACKFILL_DAYS", "180"))

SLEEP_S = 0.5       # 호출 한도 여유
PAGE_SIZE = 999     # 엔드포인트 허용 최대를 실측해 조정 (예: 999→500→200→100)

bp = BidPublicInfo(settings.bid_public_base, settings.service_key)
sc = ScsbidInfo(settings.scsbid_base, settings.service_key)


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


def total_pages_for_notice(bgn: str, end: str) -> Tuple[int, int]:
    # totalCount만 얻고, pages는 '현재 PAGE_SIZE'로 계산
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


def _safe_fetch_bsis_map(
    bgn: str, end: str, keys: List[Tuple[str, int]]
) -> Dict[Tuple[str, int], Dict]:
    """
    keys 기반 최소 조회를 우선 시도하고, 미지원(TypeError) 시 날짜 범위 1회 조회로 폴백.
    """
    try:
        # tasks.fetch_bsis_map_cnstwk 가 keys 파라미터를 지원하는 경우
        return fetch_bsis_map_cnstwk(bgn, end, keys=keys)  # type: ignore
    except TypeError:
        # 구버전: 날짜 범위 전량 1회 조회 (페이지마다 반복 호출하지 않도록 이 함수가 한 번만 호출되게 함)
        return fetch_bsis_map_cnstwk(bgn, end)


def process_notice_bucket(bucket: date):
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
    # ★ 현재 PAGE_SIZE로 재계산
    pages = 0 if total == 0 else max(1, math.ceil(total / PAGE_SIZE))
    # ★ 클램프(워터마크에 남은 last_page가 새 pages보다 클 수 있음)
    last = min(wm.get("last_page") or 0, max(0, pages - 1))

    if pages == 0 and total == 0:
        print(f"[NOTICE {bucket}] total=0 -> skip")
        return

    early_stop = False
    bsis_cache: Dict[Tuple[str, int], Dict] = {}

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
                    # ★ 빈 페이지 → 조기 종료 + 다음번을 빠르게 하기 위해 total_pages 갱신
                    upsert_watermark(stream, bucket, last_page=page, total_pages=page)
                    print(f"[NOTICE {bucket}] page {page} empty -> stop early")
                    early_stop = True
                else:
                    # ★ 필요한 경우에만 bsis 조회
                    missing_keys: List[Tuple[str, int]] = []
                    for r in rows:
                        if not r.get("base_amount"):
                            bid_no = r.get("bid_no")
                            ord_ = r.get("ord")
                            if bid_no and ord_ is not None:
                                missing_keys.append((bid_no, ord_))
                    if missing_keys:
                        # keys 기반 조회를 우선 시도, 미지원이면 날짜범위 1회 조회(캐시됨)
                        if not bsis_cache:
                            # 최초 한 번만 채우기 (keys 지원이면 필요한 키만, 아니면 날짜범위 전체)
                            bsis_cache.update(_safe_fetch_bsis_map(bgn, end, missing_keys))
                        else:
                            # 이미 캐시가 있다면 추가 키만 보강 시도 (keys 지원 시)
                            try:
                                bsis_cache.update(
                                    fetch_bsis_map_cnstwk(bgn, end, keys=missing_keys)  # type: ignore
                                )
                            except TypeError:
                                pass  # 이미 날짜범위 전체 캐시를 갖고 있다고 가정

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
                    print(
                        f"[NOTICE {bucket}] page {page}/{pages} fetched={fetched} inserted={batch} ~ ({pct:.1f}% done)"
                    )
                time.sleep(SLEEP_S)
                break
            except Exception as e:
                msg = str(e)
                wait = min(60, 2**attempt * 2)
                print(f"[NOTICE {bucket}] page {page} ERROR: {msg[:180]} ... retry in {wait}s")
                time.sleep(wait)
        else:
            print(f"[NOTICE {bucket}] page {page} hard-fail -> continue next page")

        if early_stop:
            break


def process_prep15_bucket(bucket: date):
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
    # ★ 현재 PAGE_SIZE로 재계산
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
                    # ★ 빈 페이지 → 조기 종료
                    upsert_watermark(stream, bucket, last_page=page, total_pages=page)
                    print(f"[PREP15 {bucket}] page {page} empty -> stop early")
                    early_stop = True
                else:
                    upsert_watermark(stream, bucket, last_page=page)
                    pct = 100.0 * page / pages if pages else 100.0
                    print(
                        f"[PREP15 {bucket}] page {page}/{pages} fetched={fetched} inserted={batch} ~ ({pct:.1f}% done)"
                    )
                time.sleep(SLEEP_S)
                break
            except Exception as e:
                msg = str(e)
                wait = min(60, 2**attempt * 2)
                print(f"[PREP15 {bucket}] page {page} ERROR: {msg[:180]} ... retry in {wait}s")
                time.sleep(wait)
        else:
            print(f"[PREP15 {bucket}] page {page} hard-fail -> continue next page")

        if early_stop:
            break


def backfill_days(days: int = BACKFILL_DAYS):
    tz = ZoneInfo("Asia/Seoul")
    today_kst = datetime.now(tz).date()
    start = today_kst - timedelta(days=days)
    # 과거 → 최근 순서
    buckets = [start + timedelta(days=i) for i in range(1, days + 1)]

    for b in buckets:
        print("=" * 72)
        print(f"BUCKET {b.isoformat()}  (KST day)")
        # 공고 → prep15 순서 (원하면 바꿀 수 있음)
        process_notice_bucket(b)
        process_prep15_bucket(b)


if __name__ == "__main__":
    # 한 곳(BACKFILL_DAYS)만 변경하면 전체 수집 기간이 바뀌도록 처리
    backfill_days(BACKFILL_DAYS)
