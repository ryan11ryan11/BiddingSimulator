from __future__ import annotations

"""
Backfill 수집 스크립트(견고화+성능 패치 버전)

핵심 개선(Integrated Perf Patch):
- HTTP 연결 재사용: requests.Session + 커넥션 풀(keep-alive)
- BSIS on-demand 키조회 병렬화(레이트리미터 준수)
- 선취(prefetch) 최적화: 기본 공사만(필요 시 확장), HEAD(totalCount)로 정확 페이지 수집
- 재카운트(recount) 실질 비활성화(백필 최적)
- 백오프/지터/타임아웃 튜닝(단기 장애시 지연 최소화)
- on-demand 로깅 강화(raw/norm_new/now)

실행 예(Windows PowerShell):
  .\.venv\Scripts\Activate.ps1
  # 옵션: 성능 파라미터 즉석 조정
  $env:CONCURRENCY_DAYS = "12"; $env:RATE_BID_PUBLIC_TPS = "12"; $env:RECOUNT_EVERY = "100000"
  py .\final.py
"""

# ============================== 설정값 ==============================
BACKFILL_DAYS: int = 180
DATES: list[str] = []
DATE_FROM: str | None = None
DATE_TO:   str | None = None

CONCURRENCY_DAYS: int = 3
RATE_BID_PUBLIC_TPS: float = 8.0
RATE_SCSBID_TPS: float = 8.0
PAGE_SIZE: int = 999
SLEEP_S: float = 0.0
RECOUNT_EVERY: int = 5
REQUEST_JITTER_MAX_S: float = 3.0

BSIS_PREFETCH_ENABLED: bool = True
BSIS_ONDEMAND_CHUNK: int = 40
MAX_BSIS_ERRORS_PER_BUCKET: int = 3  # (현재 카운팅 로그만, 강제 차단은 하지 않음)

STORE_NOTICE_WITHOUT_BASE: bool = False

DIAG_DEBUG: bool = True
DIAG_SAMPLE_N: int = 5

VERSION = "final.py / BSIS-resilient v3-perf (session, parallel, headcount)"

# ============================== 표준/외부 ==============================
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple, Dict, List, Optional
from sqlalchemy import text

import math, time, random, threading, signal, sys, json, os
import xml.etree.ElementTree as ET
import requests
from urllib.parse import urljoin, unquote

# ============================== 프로젝트 의존 ==============================
from core.config import settings
from core.db.engine import engine
from core.clients.bid_public_info import BidPublicInfo
from core.clients.scsbid_info import ScsbidInfo
from apps.etl.tasks import parse_notice_items, parse_prepar_detail_items
from core.db.dao import upsert_notice, upsert_prep15_bulk

# ============================== 중단 제어 ==============================
STOP = threading.Event()

def _install_signal_handlers():
    def _handler(signum, frame):
        if not STOP.is_set():
            print(f"\n[STOP] Signal {signum} received -> stopping...")
            STOP.set()
    for sig in (getattr(signal, "SIGINT", None),
                getattr(signal, "SIGTERM", None),
                getattr(signal, "SIGBREAK", None),   # Windows Ctrl+Break
                getattr(signal, "SIGTSTP", None)):   # *nix Ctrl+Z (Windows에선 무시됨)
        if sig is not None:
            try: signal.signal(sig, _handler)
            except Exception: pass

def _sleep_or_stop(seconds: float):
    if seconds > 0: STOP.wait(seconds)

# ============================== 로깅/유틸 ==============================
def _dbg(bucket: date | str, msg: str):
    if DIAG_DEBUG: print(f"[DEBUG {bucket}] {msg}")

def _sample_pairs(rows: List[Dict], k1: str, k2: str, n: int = DIAG_SAMPLE_N) -> List:
    return [(r.get(k1), r.get(k2)) for r in rows[:max(0, n)]]

# ord 정규화
def _to_ord_int(v) -> int:
    try: return int(str(v).lstrip("0") or "0")
    except Exception: return 0

def _to_ord_str3(v) -> str: return f"{_to_ord_int(v):03d}"

def _ord_forms(v):
    s = str(v) if v is not None else "0"
    i = _to_ord_int(s)
    return {s, i, f"{i:03d}"}

def _normalize_bsis_cache(raw: Dict[Tuple[str, object], Dict]) -> Dict[Tuple[str, object], Dict]:
    new: Dict[Tuple[str, object], Dict] = {}
    for (bid_no, ord_v), val in (raw or {}).items():
        for f in _ord_forms(ord_v): new[(bid_no, f)] = val
    return new

def _in_cache(cache: Dict, bid_no, ord_v) -> bool:
    for f in _ord_forms(ord_v):
        if (bid_no, f) in cache: return True
    return False

def _get_bsis_hit(cache: Dict, bid_no, ord_v):
    for f in _ord_forms(ord_v):
        hit = cache.get((bid_no, f))
        if hit: return hit
    return None

# ============================== 레이트 리미터 ==============================
class TokenBucketRateLimiter:
    def __init__(self, rate: float, capacity: Optional[float] = None):
        self.rate = max(0.01, rate)
        self.capacity = capacity if capacity is not None else self.rate
        self.tokens = self.capacity
        self.last = time.monotonic()
        self.lock = threading.Lock()
    def acquire(self, tokens: float = 1.0):
        while not STOP.is_set():
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                    self.last = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                sleep_sec = min((tokens - self.tokens) / self.rate, 0.02)
            _sleep_or_stop(sleep_sec)
        raise KeyboardInterrupt("Interrupted")

# ============================== HTTP 세션/클라이언트 ==============================
# Per-thread Session + connection pool
_thread_local = threading.local()

def _get_session() -> requests.Session:
    s = getattr(_thread_local, "sess", None)
    if s is None:
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=64, pool_maxsize=64, max_retries=0
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.headers.update({
            "Accept": "application/json, application/xml;q=0.9, */*;q=0.1",
            "Connection": "keep-alive"
        })
        _thread_local.sess = s
    return s

bp = BidPublicInfo(settings.bid_public_base, settings.service_key)
sc = ScsbidInfo(settings.scsbid_base, settings.service_key)

# env override helpers (optional performance tuning without editing code)
def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

# apply overrides / perf defaults
CONCURRENCY_DAYS = _env_int("CONCURRENCY_DAYS", 12 if CONCURRENCY_DAYS == 3 else CONCURRENCY_DAYS)
RATE_BID_PUBLIC_TPS = _env_float("RATE_BID_PUBLIC_TPS", RATE_BID_PUBLIC_TPS)
RATE_SCSBID_TPS = _env_float("RATE_SCSBID_TPS", RATE_SCSBID_TPS)
RECOUNT_EVERY = _env_int("RECOUNT_EVERY", 100000)

limiter_bp = TokenBucketRateLimiter(RATE_BID_PUBLIC_TPS)
limiter_sc = TokenBucketRateLimiter(RATE_SCSBID_TPS)

# ============================== 클라이언트 래퍼 ==============================
def bp_get(op: str, params: Dict) -> Dict:
    if STOP.is_set(): raise KeyboardInterrupt()
    limiter_bp.acquire()
    prms = dict(params); prms.setdefault("type", "json")
    return bp.get(op, prms)

def sc_get_prepar_pc_detail_cnstwk(**kwargs) -> Dict:
    if STOP.is_set(): raise KeyboardInterrupt()
    limiter_sc.acquire()
    return sc.get_prepar_pc_detail_cnstwk(**kwargs)

# ============================== 워터마크 ==============================
def upsert_watermark(stream: str, bucket: date, last_page: Optional[int]=None,
                     total_pages: Optional[int]=None, total_count: Optional[int]=None):
    sql = text("""
        INSERT INTO t_etl_watermark(stream, bucket, last_page, total_pages, total_count, updated_at)
        VALUES (:stream, :bucket, COALESCE(:last_page,0), :total_pages, :total_count, now())
        ON CONFLICT (stream, bucket) DO UPDATE SET
          last_page   = COALESCE(EXCLUDED.last_page, t_etl_watermark.last_page),
          total_pages = COALESCE(EXCLUDED.total_pages, t_etl_watermark.total_pages),
          total_count = COALESCE(EXCLUDED.total_count, t_etl_watermark.total_count),
          updated_at  = now();
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"stream": stream, "bucket": bucket, "last_page": last_page,
                           "total_pages": total_pages, "total_count": total_count})

def get_watermark(stream: str, bucket: date) -> Dict:
    sql = text("SELECT * FROM t_etl_watermark WHERE stream=:s AND bucket=:b")
    with engine.begin() as conn:
        row = conn.execute(sql, {"s": stream, "b": bucket}).mappings().first()
        return dict(row) if row else {}

# ============================== 총 페이지 ==============================
def total_pages_for_notice(bgn: str, end: str) -> Tuple[int,int]:
    body = (bp_get("getBidPblancListInfoCnstwk",
                   {"inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end, "pageNo": 1, "numOfRows": 1})
            or {}).get("response", {}).get("body", {})
    total = int(body.get("totalCount") or 0)
    pages = 0 if total==0 else max(1, math.ceil(total / PAGE_SIZE))
    return total, pages

def total_pages_for_prep15(bgn: str, end: str) -> Tuple[int,int]:
    body = (sc_get_prepar_pc_detail_cnstwk(inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end,
                                           page_no=1, num_rows=1)
            or {}).get("response", {}).get("body", {})
    total = int(body.get("totalCount") or 0)
    pages = 0 if total==0 else max(1, math.ceil(total / PAGE_SIZE))
    return total, pages

# ============================== BSIS 호출 유틸 ==============================
# 더 공격적인 타임아웃/지터(성능 튜닝)
HTTP_TIMEOUT = (2.0, 5.0)  # (connect, read)
REQUEST_JITTER_MAX_S = 0.3 # 지터 축소

# 공사/용역/물품 기초금액 엔드포인트
BSIS_ENDPOINTS = {
    "cnstwk": "getBidPblancListInfoCnstwkBsisAmount",  # 공사
    "servc" : "getBidPblancListInfoServcBsisAmount",   # 용역
    "thng"  : "getBidPblancListInfoThngBsisAmount",    # 물품
}
# 선취 시 포함할 타입(기본: 공사만, 필요시 확장)
BSIS_PREFETCH_TYPES = ("cnstwk",)

def _norm_service_key(k: str) -> str:
    try:
        return unquote(k) if "%" in k else k
    except Exception:
        return k

def _bid_base_url() -> str:
    return settings.bid_public_base.rstrip("/") + "/"

# Per-thread Session을 이용한 raw GET (ServiceKey 단일화)
def _http_get_raw(endpoint: str, params: Dict) -> requests.Response:
    if STOP.is_set(): raise KeyboardInterrupt()
    limiter_bp.acquire()
    url = urljoin(_bid_base_url(), endpoint)
    svc = _norm_service_key(settings.service_key)
    q = dict(params)
    q.setdefault("type", "json")
    q["ServiceKey"] = svc  # 단일화
    sess = _get_session()
    return sess.get(url, params=q, timeout=HTTP_TIMEOUT)

# JSON/XML/HTML/빈본문 유연 파서
def _parse_items_any_format(text_body: str) -> Tuple[str, str, List[Dict]]:
    tb = (text_body or "").strip()
    if not tb:
        return ("NODATA", "empty body", [])
    # 1) JSON
    try:
        js = json.loads(tb)
        header = ((js.get("response") or {}).get("header") or {})
        body = ((js.get("response") or {}).get("body") or {})
        code = str(header.get("resultCode") or "00")
        msg  = str(header.get("resultMsg") or "")
        items = body.get("items")
        if isinstance(items, dict) and "item" in items: items = items["item"]
        if items is None: items = []
        if isinstance(items, dict): items = [items]
        if not isinstance(items, list): items = []
        return (code, msg, items)
    except Exception:
        pass
    # 2) XML
    try:
        root = ET.fromstring(tb)
        code = (root.findtext(".//header/resultCode") or "00").strip()
        msg  = (root.findtext(".//header/resultMsg")  or "").strip()
        items_el = root.findall(".//body/items/item")
        items: List[Dict] = []
        for it in items_el:
            row: Dict[str, Optional[str]] = {}
            for ch in it:
                row[ch.tag] = (ch.text or "").strip() if ch.text is not None else None
            items.append(row)
        return (code, msg, items)
    except Exception:
        return ("NODATA", "unrecognized format", [])

def _bsis_build_map(items: List[Dict]) -> Dict[Tuple[str, object], Dict]:
    m: Dict[Tuple[str, object], Dict] = {}
    for it in items or []:
        bid_no = (it.get("bidNtceNo") or it.get("bidntceno") or "").strip()
        ord_v  = (it.get("bidNtceOrd") or it.get("bidntceord") or "0").strip()
        base_s = (it.get("bssamt") or it.get("BSSAMT") or "0")
        low_s  = it.get("rsrvtnPrceRngBgnRate")
        high_s = it.get("rsrvtnPrceRngEndRate")
        try: base = int(str(base_s).replace(",", "").strip() or "0")
        except Exception: base = 0
        def fnum(x):
            if x is None: return None
            xs = str(x).replace("+","").strip()
            try: return float(xs)
            except Exception: return None
        rec = {"base": base}
        low = fnum(low_s); high = fnum(high_s)
        if low is not None:  rec["low"]  = low
        if high is not None: rec["high"] = high
        if bid_no:
            m[(bid_no, _to_ord_str3(ord_v))] = rec
    return m

# ====== HEAD: 날짜 범위 totalCount 조회 ======
def _bsis_total_pages(endpoint: str, bgn: str, end: str) -> int:
    resp = _http_get_raw(endpoint, {
        "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end,
        "pageNo": 1, "numOfRows": 1
    })
    tb = (resp.text or "").strip()
    total = 0
    try:
        js = json.loads(tb)
        total = int(((js.get("response") or {}).get("body") or {}).get("totalCount") or 0)
    except Exception:
        try:
            root = ET.fromstring(tb)
            t = root.findtext(".//body/totalCount")
            total = int(t or 0)
        except Exception:
            total = 0
    return 0 if total==0 else max(1, math.ceil(total / PAGE_SIZE))

# 날짜 범위 수집(HEAD로 pages 계산)
def _bsis_collect_by_date(endpoint: str, bgn: str, end: str) -> List[Dict]:
    items_all: List[Dict] = []
    pages = _bsis_total_pages(endpoint, bgn, end)
    if pages == 0: return items_all
    for page in range(1, pages+1):
        if STOP.is_set(): break
        resp = _http_get_raw(endpoint, {
            "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end,
            "pageNo": page, "numOfRows": PAGE_SIZE
        })
        _, _, items = _parse_items_any_format(resp.text)
        if items: items_all.extend(items)
    return items_all

# ====== 키별 조회(공사→용역→물품, 최초 히트 사용) - 병렬화 ======
# ▶ 개선: 유형/차수 자동 판별 후 BSIS 단일 시도 (브루트포스 제거)
#  - 공사/용역/물품 공고목록 API(getBidPblancListInfoCnstwk/Servc/Thng, inqryDiv=2)
#    를 이용해 해당 bidNo의 실제 유형과 존재하는 차수 목록을 먼저 찾는다.
#  - 그러고 나서 판별된 유형의 BSIS로, (ord 힌트 → 판별된 차수 내림차순) 순으로 조회.
#  - 기타공고(Etc)는 BSIS 미대상으로 즉시 종료.

_DETECT_CACHE: Dict[str, Dict[str, object]] = {}
_DETECT_LOCK = threading.Lock()


def _bp_extract_items(resp_dict: Dict) -> List[Dict]:
    body = (resp_dict or {}).get("response", {}).get("body", {})
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


def _detect_type_and_ords_for_bid(bid_no: str) -> Tuple[Optional[str], List[str]]:
    # 캐시 우선
    with _DETECT_LOCK:
        ent = _DETECT_CACHE.get(bid_no)
        if ent:
            return ent.get("type"), ent.get("ords", [])  # type: ignore

    cand = []  # (type, ords)
    # 기타공고(Etc) 먼저 확인: 있으면 BSIS 미대상
    try:
        etc_resp = bp_get("getBidPblancListInfoEtc", {"inqryDiv":2, "bidNtceNo":bid_no, "pageNo":1, "numOfRows":PAGE_SIZE})
        etc_items = _abp_extract_items(etc_resp)
        if etc_items:
            with _DETECT_LOCK:
                _DETECT_CACHE[bid_no] = {"type":"etc", "ords":[]}
            return "etc", []
    except Exception:
        pass

    # 공사/용역/물품 순으로 존재 여부 확인
    for t, op in (("cnstwk","getBidPblancListInfoCnstwk"),
                  ("servc","getBidPblancListInfoServc"),
                  ("thng" ,"getBidPblancListInfoThng")):
        try:
            resp = bp_get(op, {"inqryDiv":2, "bidNtceNo":bid_no, "pageNo":1, "numOfRows":PAGE_SIZE})
            items = _bp_extract_items(resp)
            if items:
                ords = []
                for it in items:
                    o = it.get("bidNtceOrd") or it.get("bidntceord") or "000"
                    ords.append(_to_ord_str3(o))
                ords = sorted({o for o in ords})
                cand.append((t, ords))
        except Exception:
            continue

    dtype: Optional[str] = None
    ords: List[str] = []
    if cand:
        # 다수 유형이 나온다면(드묾) 첫 후보 채택
        dtype, ords = cand[0]

    with _DETECT_LOCK:
        _DETECT_CACHE[bid_no] = {"type": dtype if dtype else None, "ords": ords}
    return dtype, ords


def _bsis_fetch_by_detect(bid_no: str, ord3: str) -> Dict[Tuple[str, object], Dict]:
    dtype, ords = _detect_type_and_ords_for_bid(bid_no)
    if dtype is None or dtype == "etc":
        if DIAG_DEBUG:
            _dbg("BSIS", f"detect {bid_no} -> type={dtype} ords={ords}")
        return {}
    ep = BSIS_ENDPOINTS[dtype]
    # 우선순위: ord 힌트 → 판별된 차수(내림차순)
    try_list = []
    if ord3:
        try_list.append(_to_ord_str3(ord3))
    for o in sorted(ords, reverse=True):
        if o not in try_list:
            try_list.append(o)

    for o in try_list:
        try:
            resp = _http_get_raw(ep, {
                "inqryDiv": 2, "bidNtceNo": bid_no, "bidNtceOrd": o,
                "pageNo": 1, "numOfRows": PAGE_SIZE
            })
            code, msg, items = _parse_items_any_format(resp.text)
            if DIAG_DEBUG:
                _dbg("BSIS", f"detect-hit {dtype} {bid_no}-{o} -> code={code} msg={msg!r} items={len(items)}")
            if items:
                return _bsis_build_map(items)
        except KeyboardInterrupt:
            raise
        except Exception:
            pass
    return {}

def _bsis_collect_by_keys(keys: List[Tuple[str, str]]) -> Dict[Tuple[str, object], Dict]:
    out_map: Dict[Tuple[str, object], Dict] = {}
    if not keys: return out_map
    max_workers = 16  # 실측 후 8~32 사이에서 조정 권장
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="bsis") as ex:
        futures = [ex.submit(_bsis_fetch_by_detect, b, _to_ord_str3(o)) for (b,o) in keys]
        for fut in as_completed(futures):
            try:
                got = fut.result()
                if got: out_map.update(got)
            except KeyboardInterrupt:
                raise
            except Exception:
                pass
    return out_map

# 최종 공개: 날짜 범위(선취) 또는 키별(on-demand)
def fetch_bsis_map_resilient(bgn: str, end: str,
                             keys: Optional[List[Tuple[str, object]]] = None) -> Dict[Tuple[str,object],Dict]:
    if STOP.is_set(): raise KeyboardInterrupt()
    if keys:
        norm = [(str(b), _to_ord_str3(o)) for (b, o) in keys]
        return _bsis_collect_by_keys(norm)
    else:
        items_all: List[Dict] = []
        for t in BSIS_PREFETCH_TYPES:
            items_all += _bsis_collect_by_date(BSIS_ENDPOINTS[t], bgn, end)
        return _bsis_build_map(items_all)

# ============================== NOTICE ==============================
def _notice_batch_insertable(r: Dict) -> bool:
    if r.get("bid_no") and r.get("base_amount"): return True
    if STORE_NOTICE_WITHOUT_BASE and r.get("bid_no"): return True
    return False

def process_notice_bucket(bucket: date):
    if STOP.is_set(): return
    stream = "notice:cnstwk"
    tz = ZoneInfo("Asia/Seoul")
    bgn = datetime(bucket.year,bucket.month,bucket.day,0,0,tzinfo=tz).strftime("%Y%m%d%H%M")
    end = datetime(bucket.year,bucket.month,bucket.day,23,59,tzinfo=tz).strftime("%Y%m%d%H%M")

    wm = get_watermark(stream, bucket)
    if not wm:
        total, pages = total_pages_for_notice(bgn, end)
        upsert_watermark(stream, bucket, last_page=0, total_pages=pages, total_count=total)
        wm = get_watermark(stream, bucket)

    total = wm.get("total_count") or 0
    pages = 0 if total==0 else max(1, math.ceil(total / PAGE_SIZE))
    last  = min(wm.get("last_page") or 0, max(0, pages-1))

    if pages==0 and total==0:
        print(f"[NOTICE {bucket}] total=0 -> skip"); return

    # ---- BSIS 선취 ----
    bsis_cache: Dict[Tuple[str,object],Dict] = {}
    if BSIS_PREFETCH_ENABLED:
        try:
            raw = fetch_bsis_map_resilient(bgn, end)
            bsis_cache = _normalize_bsis_cache(raw or {})
            print(f"[NOTICE {bucket}] BSIS prefetch size={len(bsis_cache)} (normalized)")
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[NOTICE {bucket}] BSIS prefetch ERROR (non-fatal): {str(e)[:160]}")

    empty_streak = 0
    early_stop = False

    for page in range(last+1, pages+1):
        if STOP.is_set(): break
        for attempt in range(6):
            if STOP.is_set(): break
            try:
                resp = bp_get("getBidPblancListInfoCnstwk",
                              {"inqryDiv":1,"inqryBgnDt":bgn,"inqryEndDt":end,
                               "pageNo":page,"numOfRows":PAGE_SIZE})
                rows = parse_notice_items(resp, work_type_hint="Cnstwk")
                fetched = len(rows)

                if DIAG_DEBUG:
                    _dbg(bucket, f"[NOTICE p{page}] rows={fetched} "
                                 f"no_bid_no={sum(1 for r in rows if not r.get('bid_no'))} "
                                 f"no_base_before={sum(1 for r in rows if not r.get('base_amount'))} "
                                 f"samples={_sample_pairs(rows,'bid_no','ord')}")

                if fetched==0:
                    empty_streak+=1
                    upsert_watermark(stream, bucket, last_page=page)
                    print(f"[NOTICE {bucket}] page {page} empty (streak={empty_streak})")
                    if empty_streak>=2:
                        upsert_watermark(stream, bucket, last_page=page, total_pages=page)
                        print(f"[NOTICE {bucket}] empty twice -> stop early, total_pages={page}")
                        early_stop=True
                    _sleep_or_stop(SLEEP_S)
                    break

                empty_streak=0

                # ---- base_amount 보강 ----
                missing_keys_raw: List[Tuple[str,object]] = []
                for r in rows:
                    if not r.get("base_amount"):
                        bno, ov = r.get("bid_no"), r.get("ord")
                        if bno is not None and ov is not None:
                            missing_keys_raw.append((bno, ov))

                if missing_keys_raw:
                    need_str3 = [(b, _to_ord_str3(o))
                                 for (b,o) in missing_keys_raw
                                 if not _in_cache(bsis_cache, b, o)]
                    if DIAG_DEBUG:
                        _dbg(bucket, f"[NOTICE p{page}] missing_keys={len(missing_keys_raw)} "
                                     f"need_fetch={len(need_str3)} sample_missing={need_str3[:DIAG_SAMPLE_N]}")
                    if need_str3:
                        for i in range(0, len(need_str3), max(1, BSIS_ONDEMAND_CHUNK)):
                            if STOP.is_set(): break
                            chunk = need_str3[i:i+BSIS_ONDEMAND_CHUNK]
                            try:
                                prev = len(bsis_cache)
                                got = fetch_bsis_map_resilient(bgn, end, keys=chunk) or {}
                                norm = _normalize_bsis_cache(got)
                                # 실제 신규 추가 개수 계산
                                norm_new = 0
                                for k, v in norm.items():
                                    if k not in bsis_cache:
                                        bsis_cache[k] = v
                                        norm_new += 1
                                if DIAG_DEBUG:
                                    _dbg(bucket, f"[NOTICE p{page}] bsis_cache +=raw={len(got)} norm_new={norm_new} -> now {len(bsis_cache)} (normalized)")
                            except KeyboardInterrupt:
                                raise
                            except Exception as e:
                                print(f"[NOTICE {bucket}] on-demand BSIS error (non-fatal): {str(e)[:160]}")
                                break

                batch_buf: List[Dict] = []
                for r in rows:
                    if not r.get("base_amount"):
                        hit = _get_bsis_hit(bsis_cache, r.get("bid_no"), r.get("ord"))
                        if hit and hit.get("base"):
                            r["base_amount"] = hit["base"]
                            if hit.get("low") is not None:  r["range_low"] = hit["low"]
                            if hit.get("high") is not None: r["range_high"] = hit["high"]
                    if _notice_batch_insertable(r):
                        batch_buf.append(r)

                if DIAG_DEBUG:
                    _dbg(bucket, f"[NOTICE p{page}] batch_candidates={len(batch_buf)} "
                                 f"missing_base_after={sum(1 for r in rows if not r.get('base_amount'))}")

                inserted = 0
                if batch_buf:
                    def _upsert_notice_bulk(rows: List[Dict]) -> int:
                        if not rows: return 0
                        try:
                            from core.db.dao import upsert_notice_bulk  # type: ignore
                            upsert_notice_bulk(rows)  # type: ignore
                            return len(rows)
                        except Exception:
                            c=0
                            for r in rows:
                                upsert_notice(r); c+=1
                            return c
                    inserted = _upsert_notice_bulk(batch_buf)

                upsert_watermark(stream, bucket, last_page=page)

                if inserted==0 and fetched>0 and not STORE_NOTICE_WITHOUT_BASE:
                    _dbg(bucket, f"[NOTICE p{page}] WARNING: inserted=0")

                pct = 100.0 * min(page, pages)/pages if pages else 100.0
                print(f"[NOTICE {bucket}] page {page}/{pages} fetched={fetched} inserted={inserted} ~ ({pct:.1f}% done)")

                if page % max(1, RECOUNT_EVERY) == 0 and not early_stop:
                    try:
                        new_total, new_pages = total_pages_for_notice(bgn, end)
                        if new_total!=total or new_pages!=pages:
                            total, pages = new_total, (0 if new_total==0 else max(1, new_pages))
                            upsert_watermark(stream, bucket, total_pages=pages, total_count=total)
                            print(f"[NOTICE {bucket}] recount: total={total}, pages={pages}")
                    except Exception as e:
                        print(f"[NOTICE {bucket}] recount ERROR: {str(e)[:180]}")

                _sleep_or_stop(SLEEP_S)
                break

            except KeyboardInterrupt:
                STOP.set(); break
            except Exception as e:
                if STOP.is_set(): break
                wait = min(10.0, (2 ** attempt))
                jitter = random.uniform(0.0, REQUEST_JITTER_MAX_S)
                print(f"[NOTICE {bucket}] page {page} ERROR: {str(e)[:180]} ... retry in {wait+jitter:.1f}s")
                _sleep_or_stop(wait + jitter)
        else:
            print(f"[NOTICE {bucket}] page {page} hard-fail -> continue next page")
        if early_stop or STOP.is_set(): break

# ============================== PREP15 ==============================
def process_prep15_bucket(bucket: date):
    if STOP.is_set(): return
    stream = "prep15:cnstwk"
    tz = ZoneInfo("Asia/Seoul")
    bgn = datetime(bucket.year,bucket.month,bucket.day,0,0,tzinfo=tz).strftime("%Y%m%d%H%M")
    end = datetime(bucket.year,bucket.month,bucket.day,23,59,tzinfo=tz).strftime("%Y%m%d%H%M")

    wm = get_watermark(stream, bucket)
    if not wm:
        total, pages = total_pages_for_prep15(bgn, end)
        upsert_watermark(stream, bucket, last_page=0, total_pages=pages, total_count=total)
        wm = get_watermark(stream, bucket)

    total = wm.get("total_count") or 0
    pages = 0 if total==0 else max(1, math.ceil(total / PAGE_SIZE))
    last  = min(wm.get("last_page") or 0, max(0, pages-1))

    if pages==0 and total==0:
        print(f"[PREP15 {bucket}] total=0 -> skip"); return

    empty_streak=0; early_stop=False
    for page in range(last+1, pages+1):
        if STOP.is_set(): break
        for attempt in range(6):
            if STOP.is_set(): break
            try:
                resp = sc_get_prepar_pc_detail_cnstwk(inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end,
                                                      page_no=page, num_rows=PAGE_SIZE)
                m = parse_prepar_detail_items(resp)
                fetched = 0; batch = 0
                if m:
                    for _, rows in m.items():
                        fetched += len(rows)
                        if rows:
                            upsert_prep15_bulk(rows); batch += len(rows)

                if fetched==0:
                    empty_streak+=1
                    upsert_watermark(stream, bucket, last_page=page)
                    print(f"[PREP15 {bucket}] page {page} empty (streak={empty_streak})")
                    if empty_streak>=2:
                        upsert_watermark(stream, bucket, last_page=page, total_pages=page)
                        print(f"[PREP15 {bucket}] empty twice -> stop early, total_pages={page}")
                        early_stop=True
                    _sleep_or_stop(SLEEP_S)
                    break
                else:
                    empty_streak=0
                    upsert_watermark(stream, bucket, last_page=page)
                    pct = 100.0 * min(page, pages)/pages if pages else 100.0
                    print(f"[PREP15 {bucket}] page {page}/{pages} fetched={fetched} inserted={batch} ~ ({pct:.1f}% done)")

                if page % max(1, RECOUNT_EVERY) == 0 and not early_stop:
                    try:
                        new_total, new_pages = total_pages_for_prep15(bgn, end)
                        if new_total!=total or new_pages!=pages:
                            total, pages = new_total, (0 if new_total==0 else max(1, new_pages))
                            upsert_watermark(stream, bucket, total_pages=pages, total_count=total)
                            print(f"[PREP15 {bucket}] recount: total={total}, pages={pages}")
                    except Exception as e:
                        print(f"[PREP15 {bucket}] recount ERROR: {str(e)[:180]}")

                _sleep_or_stop(SLEEP_S)
                break
            except KeyboardInterrupt:
                STOP.set(); break
            except Exception as e:
                if STOP.is_set(): break
                wait = min(10.0, (2 ** attempt))
                jitter = random.uniform(0.0, REQUEST_JITTER_MAX_S)
                print(f"[PREP15 {bucket}] page {page} ERROR: {str(e)[:180]} ... retry in {wait+jitter:.1f}s")
                _sleep_or_stop(wait + jitter)
        else:
            print(f"[PREP15 {bucket}] page {page} hard-fail -> continue next page")
        if early_stop or STOP.is_set(): break

# ============================== 실행 루프 ==============================
def process_day_bucket(bucket: date):
    if STOP.is_set(): return
    print("="*72)
    print(f"BUCKET {bucket.isoformat()}  (KST day)  [{VERSION}]")
    process_notice_bucket(bucket)
    process_prep15_bucket(bucket)

def _run_buckets(buckets: List[date]):
    workers = max(1, CONCURRENCY_DAYS)
    if workers == 1:
        for b in buckets:
            if STOP.is_set(): break
            process_day_bucket(b)
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bucket") as ex:
            futures = {ex.submit(process_day_bucket, b): b for b in buckets}
            try:
                for fut in as_completed(futures):
                    b = futures[fut]
                    if STOP.is_set(): break
                    try: fut.result()
                    except KeyboardInterrupt:
                        STOP.set(); break
                    except Exception as e:
                        print(f"[MAIN] BUCKET {b} ERROR: {str(e)[:200]}")
            finally:
                if STOP.is_set():
                    try: ex.shutdown(wait=False, cancel_futures=True)
                    except Exception: pass

# ============================== 날짜 구성 ==============================
def _parse_iso_date(s: str) -> date: return datetime.strptime(s.strip(), "%Y-%m-%d").date()

def _unique_sorted_dates(dates: List[date]) -> List[date]: return sorted({d for d in dates})

def build_buckets_from_config() -> Optional[List[date]]:
    picked: List[date] = []
    for s in (DATES or []): picked.append(_parse_iso_date(str(s)))
    if DATE_FROM and DATE_TO:
        d0, d1 = _parse_iso_date(DATE_FROM), _parse_iso_date(DATE_TO)
        if d1 < d0: d0, d1 = d1, d0
        cur = d0
        while cur <= d1: picked.append(cur); cur += timedelta(days=1)
    return _unique_sorted_dates(picked) if picked else None

def backfill_days(days: int = BACKFILL_DAYS):
    tz = ZoneInfo("Asia/Seoul"); today = datetime.now(tz).date()
    start = today - timedelta(days=days)
    buckets = [start + timedelta(days=i) for i in range(1, days+1)]
    _run_buckets(buckets)

# ============================== 엔트리 ==============================
if __name__ == "__main__":
    _install_signal_handlers()
    picked = build_buckets_from_config()
    try:
        if picked:
            print(f"[MODE] 설정된 날짜 사용: {picked[0]} .. {picked[-1]} (총 {len(picked)}일)")
            _run_buckets(picked)
        else:
            print(f"[MODE] 기본 모드: 최근 {BACKFILL_DAYS}일 백필")
            backfill_days(BACKFILL_DAYS)
    except KeyboardInterrupt:
        STOP.set()
        print("\n[MAIN] Interrupted by user. Shutting down...")
