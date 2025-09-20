from __future__ import annotations
"""
backfill_base_amount_nodao.py — DB DAO 없이 바로 쓰는 보강 스크립트
- 날짜 구간의 공고를 다시 긁음
- base_amount 비어있는 것들을 BSIS로 보강
- 보강된 레코드를 upsert_notice(upsert_notice_bulk 있으면 그걸로)로 업데이트

실행 예:
  .\.venv\Scripts\Activate.ps1
  py .\backfill_base_amount_nodao.py --from 20240101 --to 20240131
  py .\backfill_base_amount_nodao.py --days 3 --include-today
"""

import math, time, logging, threading
from typing import Dict, Tuple, List, Optional, Set, Iterable
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FUTimeout

from core.config import settings
from core.clients.bid_public_info import BidPublicInfo
from apps.etl.tasks import parse_notice_items
from core.db.dao import upsert_notice

try:
    from core.db.dao import upsert_notice_bulk
except Exception:  # optional
    upsert_notice_bulk = None  # type: ignore

# ========== 설정 ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("backfill_nodao")

bp = BidPublicInfo(settings.bid_public_base, settings.service_key)

DEFAULT_PAGE_SIZE = 1000
FALLBACK_PAGE_SIZE = 1000
REQUEST_TIMEOUT = 15
WRAPPER_RETRIES = 1

DEFAULT_TPS_SLEEP = 0.10  # ≈10 req/s
LIST_PAGE_CONCURRENCY = 4
EMPTY_PAGE_BREAK_THRESHOLD = 3
NOTICE_INQRY_DIV = 1      # 공고 조회 기준(등록일)
BSIS_INQRY_DIV = 1        # BSIS 조회 기준(공개일 가설) — 필요시 1로 바꿔 테스트
BULK_BUFFER = 500

# ========== 리미터 ==========
import time as _time, threading as _threading
class RateLimiter:
    def __init__(self, min_interval: float):
        self.min_interval = float(min_interval)
        self._lock = _threading.Lock()
        self._next = 0.0
    def wait(self):
        with self._lock:
            now = _time.monotonic()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + self.min_interval
        if delay > 0:
            _time.sleep(delay)

HTTP_RATE_LIMITER = RateLimiter(DEFAULT_TPS_SLEEP)
_EXEC = ThreadPoolExecutor(max_workers=8)

# ========== 유틸 ==========
def _call_with_timeout(fn, *args, timeout: int = REQUEST_TIMEOUT, **kwargs):
    fut = _EXEC.submit(fn, *args, **kwargs)
    try:
        return fut.result(timeout=timeout)
    except FUTimeout:
        log.error(f"REQUEST TIMEOUT ({timeout}s): {getattr(fn,'__name__',fn)}")
        return None

def _bp_get(op: str, params: Dict):
    prms = dict(params); prms.setdefault("type", "json")
    HTTP_RATE_LIMITER.wait()
    try:
        return _call_with_timeout(bp.get, op, prms, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        log.warning(f"[bp] GET fail {op}: {e}")
        return None

def _extract_items(resp_dict: Dict) -> List[Dict]:
    body = (resp_dict or {}).get("response", {}).get("body", {}) or {}
    items = body.get("items")
    if isinstance(items, dict) and "item" in items:
        items = items["item"]
    if items is None:
        return []
    if isinstance(items, dict):
        return [items]
    if isinstance(items, list):
        return items
    return []

def _to_ord_int(v) -> int:
    try:
        return int(str(v).lstrip("0") or "0")
    except Exception:
        return 0

def _to_ord_str3(v) -> str:
    return f"{_to_ord_int(v):03d}"

def _safe_float(x) -> Optional[float]:
    if x is None:
        return None
    xs = str(x).replace("+", "").strip()
    try:
        return float(xs)
    except Exception:
        return None

def _safe_int_from_money(s) -> int:
    try:
        return int(str(s).replace(",", "").strip() or "0")
    except Exception:
        return 0

def _iter_days_str(start_yyyymmdd: str, end_yyyymmdd: str):
    y, m, d = int(start_yyyymmdd[:4]), int(start_yyyymmdd[4:6]), int(start_yyyymmdd[6:8])
    y2, m2, d2 = int(end_yyyymmdd[:4]), int(end_yyyymmdd[4:6]), int(end_yyyymmdd[6:8])
    cur = date(y, m, d)
    end = date(y2, m2, d2)
    while cur <= end:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)

# ========== BSIS 맵 ==========
def build_bsis_map(bgn: str, end: str, page_size: int = DEFAULT_PAGE_SIZE) -> Dict[Tuple[str, str], Dict]:
    head = _bp_get("getBidPblancListInfoCnstwkBsisAmount", {
        "inqryDiv": BSIS_INQRY_DIV, "inqryBgnDt": bgn, "inqryEndDt": end,
        "pageNo": 1, "numOfRows": 1,
    })
    if not head:
        return {}
    total = int((((head.get("response", {}) or {}).get("body", {}) or {}).get("totalCount")) or 0)
    if total <= 0:
        return {}

    pages = max(1, math.ceil(total / max(1, page_size)))
    m: Dict[Tuple[str, str], Dict] = {}
    def fetch_page(p: int, rows: int):
        tried_small = False
        local_rows = rows
        while True:
            resp = _bp_get("getBidPblancListInfoCnstwkBsisAmount", {
                "inqryDiv": BSIS_INQRY_DIV, "inqryBgnDt": bgn, "inqryEndDt": end,
                "pageNo": p, "numOfRows": local_rows,
            })
            if resp:
                return resp
            if not tried_small and local_rows > FALLBACK_PAGE_SIZE:
                log.warning(f"[BSIS {bgn}-{end}] page {p} failed -> downshift to {FALLBACK_PAGE_SIZE}")
                local_rows = FALLBACK_PAGE_SIZE
                tried_small = True
                continue
            log.warning(f"[BSIS {bgn}-{end}] page {p} fail -> skip")
            return None
    with ThreadPoolExecutor(max_workers=LIST_PAGE_CONCURRENCY) as pool:
        futures = {pool.submit(fetch_page, p, page_size): p for p in range(1, pages + 1)}
        for fut in as_completed(futures):
            resp = fut.result()
            items = _extract_items(resp) if resp else []
            for it in items:
                bid_no = (it.get("bidNtceNo") or it.get("bidntceno") or "").strip()
                ord3   = _to_ord_str3(it.get("bidNtceOrd") or it.get("bidntceord") or "0")
                base_s = (it.get("bssamt") or it.get("BSSAMT") or "0")
                low_s  = it.get("rsrvtnPrceRngBgnRate")
                high_s = it.get("rsrvtnPrceRngEndRate")
                open_dt = (it.get("bssamtOpenDt") or it.get("bssamtopendt") or it.get("BSSAMTOPENDT"))
                base, low, high = _safe_int_from_money(base_s), _safe_float(low_s), _safe_float(high_s)
                if bid_no:
                    rec = {"base": base}
                    if low is not None:  rec["low"] = low
                    if high is not None: rec["high"] = high
                    if open_dt:          rec["open_dt"] = str(open_dt)
                    m[(bid_no, ord3)] = rec
    return m

# ========== 공고 재수집 & 보강 ==========
def fetch_notices(bgn: str, end: str, page_size: int = DEFAULT_PAGE_SIZE) -> List[Dict]:
    """날짜창의 공고를 전부 가져와 dict list로 반환(work_type_hint='Cnstwk')."""
    head = _bp_get("getBidPblancListInfoCnstwk", {
        "inqryDiv": NOTICE_INQRY_DIV, "inqryBgnDt": bgn, "inqryEndDt": end,
        "pageNo": 1, "numOfRows": 1
    })
    if not head:
        return []
    body = (head or {}).get("response", {}).get("body", {}) or {}
    total = int(body.get("totalCount") or 0)
    if total <= 0:
        return []

    pages = max(1, math.ceil(total / max(1, page_size)))
    rows_all: List[Dict] = []
    empty_streak = 0
    for page in range(1, pages + 1):
        tried_small = False
        while True:
            resp = _bp_get("getBidPblancListInfoCnstwk", {
                "inqryDiv": NOTICE_INQRY_DIV, "inqryBgnDt": bgn, "inqryEndDt": end,
                "pageNo": page, "numOfRows": page_size
            })
            if resp: break
            if not tried_small and page_size > FALLBACK_PAGE_SIZE:
                log.warning(f"[NOTICE {bgn}-{end}] page {page} failed -> downshift to {FALLBACK_PAGE_SIZE}")
                page_size = FALLBACK_PAGE_SIZE
                tried_small = True
                continue
            log.warning(f"[NOTICE {bgn}-{end}] page {page} fail -> continue")
            resp = None
            break
        if not resp:
            continue
        try:
            rows = parse_notice_items(resp, work_type_hint="Cnstwk") or []
        except Exception as e:
            log.error(f"[NOTICE {bgn}-{end}] page {page} parse_notice_items ERROR: {e}")
            rows = []
        if not rows:
            empty_streak += 1
            if empty_streak >= EMPTY_PAGE_BREAK_THRESHOLD:
                log.info(f"[NOTICE {bgn}-{end}] early stop after {EMPTY_PAGE_BREAK_THRESHOLD} empty pages")
                break
            continue
        empty_streak = 0
        rows_all.extend(rows)
    return rows_all

def backfill_range(start_day: str, end_day: str, window_expand_days: int = 2, bulk_buffer: int = BULK_BUFFER) -> int:
    """
    - start_day/end_day: YYYYMMDD (포함)
    - window_expand_days: BSIS 조회시 앞/뒤로 확장(등록/공개일 불일치 흡수)
    """
    total_updates = 0
    for day in _iter_days_str(start_day, end_day):
        bgn = f"{day}0000"; end = f"{day}2359"
        log.info(f"=== {bgn} ~ {end} ===")

        # 1) 공고 재수집
        notices = fetch_notices(bgn, end)
        if not notices:
            log.info(f"[{day}] no notices")
            continue

        # 2) base 없는 항목만 골라두고, BSIS 맵 준비
        missing = [r for r in notices if not r.get("base_amount")]
        if not missing:
            log.info(f"[{day}] no missing base_amount")
            continue

        # 날짜창 확장해서 BSIS 조회(등록/공개 불일치 흡수)
        day_date = date(int(day[:4]), int(day[4:6]), int(day[6:8]))
        bgn2 = (day_date - timedelta(days=window_expand_days)).strftime("%Y%m%d") + "0000"
        end2 = (day_date + timedelta(days=window_expand_days)).strftime("%Y%m%d") + "2359"
        bsis_map = build_bsis_map(bgn2, end2)
        log.info(f"[{day}] missing={len(missing)}  bsis_keys={len(bsis_map)}")

        # 3) 매칭 & 업데이트(upsert_notice[_bulk])
        buffer: List[Dict] = []
        def flush():
            nonlocal total_updates
            if not buffer: return
            try:
                if upsert_notice_bulk is not None:
                    upsert_notice_bulk(buffer)  # type: ignore
                    total_updates += len(buffer)
                else:
                    ok = 0
                    for r in buffer:
                        upsert_notice(r)
                        ok += 1
                    total_updates += ok
            finally:
                buffer.clear()

        for r in missing:
            bid_no = (r.get("bid_no") or "").strip()
            ord3 = _to_ord_str3(r.get("ord"))
            hit = (bsis_map.get((bid_no, ord3))
                   or bsis_map.get((bid_no, "001"))
                   or bsis_map.get((bid_no, "000")))
            if hit and hit.get("base"):
                r2 = dict(r)
                r2["base_amount"] = hit["base"]
                if hit.get("low") is not None:  r2["range_low"]  = hit["low"]
                if hit.get("high") is not None: r2["range_high"] = hit["high"]
                if hit.get("open_dt"):          r2["base_open_dt"] = hit["open_dt"]
                buffer.append(r2)
                if len(buffer) >= bulk_buffer:
                    flush()

        flush()
        log.info(f"[{day}] updated={total_updates} (cumulative)")
    return total_updates

# ========== cli ==========
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="from_date", help="시작일 YYYYMMDD")
    p.add_argument("--to", dest="to_date", help="종료일 YYYYMMDD")
    p.add_argument("--days", type=int, default=2, help="최근 N일(오늘 포함)")
    p.add_argument("--include-today", action="store_true")
    p.add_argument("--window-expand", type=int, default=2)
    args = p.parse_args()

    tz = ZoneInfo("Asia/Seoul"); now = datetime.now(tz)
    if args.from_date and args.to_date:
        start_day, end_day = args.from_date, args.to_date
    else:
        start_day = (now - timedelta(days=args.days-1)).strftime("%Y%m%d")
        end_day   = now.strftime("%Y%m%d") if args.include_today else (now - timedelta(days=0)).strftime("%Y%m%d")
    updated = backfill_range(start_day, end_day, window_expand_days=args.window_expand)
    log.info(f"[SUMMARY] total updated={updated}")
