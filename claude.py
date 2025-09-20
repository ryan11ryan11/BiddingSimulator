from __future__ import annotations
"""
final_optimized.py — 라이트 안전화(공사만) v3 - 최적화 버전
- 적응형 페이지 크기 및 동적 배치 크기 조정
- 메모리 효율적인 스트리밍 처리
- 지능형 API 재시도 및 회로 차단기 패턴
- 병렬 처리 최적화 (공고/PREP15 동시 수집)
- 캐시 및 배치 최적화
"""

from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, List, Optional, Set, Iterable, Any
import time, math, logging, threading, inspect, os, json
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FUTimeout, as_completed, Future
from collections import deque, defaultdict
import asyncio
from dataclasses import dataclass

from core.config import settings
from core.clients.bid_public_info import BidPublicInfo
from core.clients.scsbid_info import ScsbidInfo
from apps.etl.tasks import (
    parse_notice_items,
    parse_prepar_detail_items,
)
from core.db.dao import upsert_notice, upsert_prep15_bulk

# 선택적 DAO
try:
    from core.db.dao import upsert_notice_bulk
except Exception:
    upsert_notice_bulk = None

try:
    from core.db.dao import filter_existing_notices, filter_existing_prep15_bidnos
except Exception:
    filter_existing_notices = None
    filter_existing_prep15_bidnos = None

# ============================== 로깅 ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("final_optimized")

# ============================== 클라이언트 ==============================
bp = BidPublicInfo(settings.bid_public_base, settings.service_key)
sc = ScsbidInfo(settings.scsbid_base, settings.service_key)

# ============================== 최적화된 설정 ==============================
SKIP_COMPLETED_WINDOWS = True
REPROCESS_RECENT_DAYS = 2
CHECKPOINT_FILE = "state/cnstwk_checkpoint.json"

# 적응형 페이지 크기
MIN_PAGE_SIZE = 200
MAX_PAGE_SIZE = 1000
DEFAULT_PAGE_SIZE = 800
FALLBACK_PAGE_SIZE = 300

# 배치 처리 최적화
MAX_BATCH_SIZE = 500
MIN_BATCH_SIZE = 50
OPTIMAL_BATCH_SIZE = 200

# 병렬 처리 최적화
MAX_WORKERS = min(16, (os.cpu_count() or 4) * 2)
PREP15_WORKERS = min(8, MAX_WORKERS // 2)
NOTICE_WORKERS = min(6, MAX_WORKERS // 3)

# 레이트 리미팅 최적화
INITIAL_TPS_SLEEP = 0.05  # 더 빠른 시작
ADAPTIVE_TPS_SLEEP = 0.08  # 적응형 조정
REQUEST_TIMEOUT = 12
MAX_RETRIES = 2

# 메모리 관리
MAX_MEMORY_ITEMS = 5000
MEMORY_CLEANUP_THRESHOLD = 0.8

# ============================== 성능 모니터링 ==============================
@dataclass
class PerformanceMetrics:
    api_calls: int = 0
    success_calls: int = 0
    failed_calls: int = 0
    total_items: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    avg_response_time: float = 0.0
    last_update: float = 0.0
    
    def update_response_time(self, response_time: float):
        if self.avg_response_time == 0:
            self.avg_response_time = response_time
        else:
            # 지수 이동 평균
            self.avg_response_time = 0.9 * self.avg_response_time + 0.1 * response_time
        self.last_update = time.time()

perf_metrics = PerformanceMetrics()

# ============================== 회로 차단기 ==============================
class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def call(self, func, *args, **kwargs):
        with self._lock:
            if self.state == 'OPEN':
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                    self.failure_count = 0
                else:
                    raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            with self._lock:
                if self.state == 'HALF_OPEN':
                    self.state = 'CLOSED'
                self.failure_count = 0
            return result
        except Exception as e:
            with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = 'OPEN'
            raise e

# ============================== 적응형 레이트 리미터 ==============================
class AdaptiveRateLimiter:
    def __init__(self, initial_interval: float, max_interval: float = 1.0):
        self.initial_interval = initial_interval
        self.current_interval = initial_interval
        self.max_interval = max_interval
        self._lock = threading.Lock()
        self._next = 0.0
        self._success_count = 0
        self._error_count = 0
        
    def wait(self):
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + self.current_interval
        if delay > 0:
            time.sleep(delay)
    
    def report_success(self):
        with self._lock:
            self._success_count += 1
            if self._success_count >= 10 and self.current_interval > self.initial_interval:
                # 성공이 많으면 속도 증가
                self.current_interval = max(
                    self.initial_interval, 
                    self.current_interval * 0.9
                )
                self._success_count = 0
                
    def report_error(self):
        with self._lock:
            self._error_count += 1
            if self._error_count >= 3:
                # 에러가 많으면 속도 감소
                self.current_interval = min(
                    self.max_interval,
                    self.current_interval * 1.5
                )
                self._error_count = 0

http_limiter = AdaptiveRateLimiter(INITIAL_TPS_SLEEP)
sc_limiter = AdaptiveRateLimiter(ADAPTIVE_TPS_SLEEP)

# 회로 차단기
bp_breaker = CircuitBreaker()
sc_breaker = CircuitBreaker()

# ============================== 메모리 효율적인 캐시 ==============================
class LRUCache:
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache: Dict[str, Any] = {}
        self.access_order = deque()
        self._lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key in self.cache:
                self.access_order.remove(key)
                self.access_order.append(key)
                perf_metrics.cache_hits += 1
                return self.cache[key]
            perf_metrics.cache_misses += 1
            return None
    
    def put(self, key: str, value: Any):
        with self._lock:
            if key in self.cache:
                self.access_order.remove(key)
            elif len(self.cache) >= self.max_size:
                # LRU 제거
                oldest = self.access_order.popleft()
                del self.cache[oldest]
            
            self.cache[key] = value
            self.access_order.append(key)

response_cache = LRUCache(max_size=2000)

# ============================== 실행기 최적화 ==============================
_EXEC = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="OptimizedWorker")

def _ensure_parent_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _load_checkpoint() -> Dict:
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"notice": {}, "prep15": {}}

def _save_checkpoint(state: Dict):
    _ensure_parent_dir(CHECKPOINT_FILE)
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)

def _mark_done(state: Dict, kind: str, day: str, meta: Dict):
    kind_map = state.setdefault(kind, {})
    kind_map[day] = {
        **meta,
        "ts": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")
    }

def _is_done(state: Dict, kind: str, day: str) -> bool:
    try:
        return bool(state.get(kind, {}).get(day))
    except Exception:
        return False

def _should_skip_day(day: str, state: Dict, today: str) -> bool:
    if not SKIP_COMPLETED_WINDOWS:
        return False
    try:
        delta = (date(int(today[:4]), int(today[4:6]), int(today[6:8])) -
                 date(int(day[:4]),   int(day[4:6]),   int(day[6:8]))).days
        if delta <= REPROCESS_RECENT_DAYS:
            return False
    except Exception:
        pass
    return _is_done(state, "notice", day) and _is_done(state, "prep15", day)

# ============================== 스트리밍 배치 처리기 ==============================
def batched_streaming(iterable: Iterable, batch_size: int) -> Iterable[List]:
    """메모리 효율적인 스트리밍 배치 처리"""
    current_batch = []
    for item in iterable:
        current_batch.append(item)
        if len(current_batch) >= batch_size:
            yield current_batch
            current_batch = []
    if current_batch:
        yield current_batch

def adaptive_batch_size(current_size: int, success_rate: float, response_time: float) -> int:
    """성능 기반 적응형 배치 크기 조정"""
    if success_rate > 0.95 and response_time < 2.0:
        # 성공률이 높고 응답이 빠르면 배치 크기 증가
        return min(MAX_BATCH_SIZE, int(current_size * 1.2))
    elif success_rate < 0.8 or response_time > 5.0:
        # 성공률이 낮거나 응답이 느리면 배치 크기 감소
        return max(MIN_BATCH_SIZE, int(current_size * 0.7))
    return current_size

# ============================== 최적화된 API 호출 ==============================
def _call_with_timeout_and_cache(fn, cache_key: str, *args, timeout: int = REQUEST_TIMEOUT, **kwargs):
    """캐시와 함께하는 타임아웃 호출"""
    # 캐시 확인
    cached = response_cache.get(cache_key)
    if cached is not None:
        return cached
    
    start_time = time.time()
    fut = _EXEC.submit(fn, *args, **kwargs)
    try:
        result = fut.result(timeout=timeout)
        response_time = time.time() - start_time
        perf_metrics.update_response_time(response_time)
        
        # 성공한 결과만 캐싱 (5분)
        if result:
            response_cache.put(cache_key, result)
        
        return result
    except FUTimeout:
        log.error(f"REQUEST TIMEOUT ({timeout}s): {getattr(fn,'__name__',fn)}")
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

# ============================== 최적화된 API 래퍼 ==============================
def _bp_get_optimized(op: str, params: Dict, page_size: int = DEFAULT_PAGE_SIZE):
    """최적화된 BP API 호출"""
    prms = dict(params)
    prms.setdefault("type", "json")
    
    cache_key = f"bp_{op}_{hash(str(sorted(prms.items())))}"
    
    def api_call():
        perf_metrics.api_calls += 1
        http_limiter.wait()
        try:
            result = bp_breaker.call(
                lambda: _call_with_timeout_and_cache(bp.get, cache_key, op, prms, timeout=REQUEST_TIMEOUT)
            )
            if result:
                perf_metrics.success_calls += 1
                http_limiter.report_success()
            else:
                perf_metrics.failed_calls += 1
                http_limiter.report_error()
            return result
        except Exception as e:
            perf_metrics.failed_calls += 1
            http_limiter.report_error()
            log.warning(f"[bp] API call failed: {e}")
            return None
    
    # 적응형 재시도
    for attempt in range(MAX_RETRIES + 1):
        result = api_call()
        if result is not None:
            return result
        
        if attempt < MAX_RETRIES:
            # 실패 시 페이지 크기 감소 및 재시도
            if 'numOfRows' in prms and prms['numOfRows'] > MIN_PAGE_SIZE:
                prms['numOfRows'] = max(MIN_PAGE_SIZE, prms['numOfRows'] // 2)
                log.info(f"[bp] Retry {attempt + 1} with reduced page size: {prms['numOfRows']}")
            time.sleep(0.5 * (attempt + 1))
    
    return None

# ============================== SCS 최적화 ==============================
_SCS_TYPED_MODE: Optional[str] = None
_SCS_ALLOWED_KEYS: Set[str] = set()

def _init_scs_signature():
    global _SCS_TYPED_MODE, _SCS_ALLOWED_KEYS
    if _SCS_TYPED_MODE is not None:
        return
    try:
        sig = inspect.signature(sc.get_prepar_pc_detail_cnstwk)
        _SCS_ALLOWED_KEYS = set(sig.parameters.keys())
        _SCS_TYPED_MODE = 'typed'
    except AttributeError:
        _SCS_TYPED_MODE = 'none'
        _SCS_ALLOWED_KEYS = set()

def _sc_get_prepar_cnstwk_optimized(**kwargs):
    """최적화된 SC API 호출"""
    _init_scs_signature()
    
    # 파라미터 정규화
    snake = {
        'inqry_div': kwargs.get('inqry_div', kwargs.get('inqryDiv')),
        'inqry_bgn_dt': kwargs.get('inqry_bgn_dt', kwargs.get('inqryBgnDt')),
        'inqry_end_dt': kwargs.get('inqry_end_dt', kwargs.get('inqryEndDt')),
        'page_no': kwargs.get('page_no', kwargs.get('pageNo')),
        'num_rows': kwargs.get('num_rows', kwargs.get('numOfRows')),
        'bid_ntce_no': kwargs.get('bid_ntce_no', kwargs.get('bidNtceNo')),
    }
    snake = {k: v for k, v in snake.items() if v is not None}
    
    cache_key = f"sc_{hash(str(sorted(snake.items())))}"
    
    def api_call():
        perf_metrics.api_calls += 1
        sc_limiter.wait()
        try:
            if _SCS_TYPED_MODE == 'typed':
                allowed = {k: v for k, v in snake.items() if k in _SCS_ALLOWED_KEYS}
                result = sc_breaker.call(
                    lambda: _call_with_timeout_and_cache(sc.get_prepar_pc_detail_cnstwk, cache_key, timeout=REQUEST_TIMEOUT, **allowed)
                )
            else:
                camel = {
                    'inqryDiv': snake.get('inqry_div'),
                    'inqryBgnDt': snake.get('inqry_bgn_dt'),
                    'inqryEndDt': snake.get('inqry_end_dt'),
                    'pageNo': snake.get('page_no'),
                    'numOfRows': snake.get('num_rows'),
                    'bidNtceNo': snake.get('bid_ntce_no'),
                    'type': 'json',
                }
                camel = {k: v for k, v in camel.items() if v is not None}
                result = sc_breaker.call(
                    lambda: _call_with_timeout_and_cache(sc.get, cache_key, "getOpengResultListInfoCnstwkPreparPcDetail", camel, timeout=REQUEST_TIMEOUT)
                )
            
            if result:
                perf_metrics.success_calls += 1
                sc_limiter.report_success()
            else:
                perf_metrics.failed_calls += 1
                sc_limiter.report_error()
            return result
        except Exception as e:
            perf_metrics.failed_calls += 1
            sc_limiter.report_error()
            log.warning(f"[sc] API call failed: {e}")
            return None
    
    return api_call()

# ============================== 최적화된 BSIS 처리 ==============================
def _prefetch_bsis_map_cnstwk_optimized(
    bgn: str, end: str, page_size: int = MAX_PAGE_SIZE
) -> Dict[Tuple[str, str], Dict]:
    """병렬화된 BSIS 조회"""
    head = _bp_get_optimized("getBidPblancListInfoCnstwkBsisAmount", {
        "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end,
        "pageNo": 1, "numOfRows": 1,
    })
    
    if not head:
        return {}

    total = int((((head.get("response", {}) or {}).get("body", {}) or {}).get("totalCount")) or 0)
    if total <= 0:
        return {}

    pages = max(1, math.ceil(total / page_size))
    
    def fetch_page(page: int) -> List[Dict]:
        resp = _bp_get_optimized("getBidPblancListInfoCnstwkBsisAmount", {
            "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end,
            "pageNo": page, "numOfRows": page_size,
        })
        return _extract_items(resp) if resp else []
    
    # 병렬 페이지 조회
    results = {}
    with ThreadPoolExecutor(max_workers=min(4, pages)) as executor:
        future_to_page = {executor.submit(fetch_page, page): page for page in range(1, pages + 1)}
        
        for future in as_completed(future_to_page):
            try:
                items = future.result()
                for it in items:
                    bid_no = (it.get("bidNtceNo") or it.get("bidntceno") or "").strip()
                    ord3 = _to_ord_str3(it.get("bidNtceOrd") or it.get("bidntceord") or "0")
                    
                    if bid_no:
                        base = _safe_int_from_money(it.get("bssamt") or it.get("BSSAMT") or "0")
                        low = _safe_float(it.get("rsrvtnPrceRngBgnRate"))
                        high = _safe_float(it.get("rsrvtnPrceRngEndRate"))
                        open_dt = it.get("bssamtOpenDt") or it.get("bssamtopendt") or it.get("BSSAMTOPENDT")
                        
                        rec = {"base": base}
                        if low is not None:
                            rec["low"] = low
                        if high is not None:
                            rec["high"] = high
                        if open_dt:
                            rec["open_dt"] = str(open_dt)
                        results[(bid_no, ord3)] = rec
            except Exception as e:
                log.warning(f"BSIS page fetch failed: {e}")
    
    return results

# ============================== 중복 필터링 최적화 ==============================
def _filter_existing_optimized(keys: List[Tuple], filter_func, item_type: str) -> Set:
    """대용량 데이터 배치 처리 최적화"""
    if not keys or filter_func is None:
        return set()
    
    existing = set()
    try:
        # 대용량 데이터를 청크로 나누어 처리
        for chunk in batched_streaming(keys, OPTIMAL_BATCH_SIZE):
            chunk_existing = filter_func(list(chunk))
            existing.update(chunk_existing if isinstance(chunk_existing, set) else set(chunk_existing))
    except Exception as e:
        log.warning(f"[dedup] {item_type} filter failed: {e}")
    
    return existing

# ============================== 최적화된 공고 수집 ==============================
def collect_notices_optimized(
    bgn: str, end: str, page_size: int = DEFAULT_PAGE_SIZE
) -> Tuple[int, int, Set[str]]:
    """병렬 처리와 적응형 배치로 최적화된 공고 수집"""
    
    head = _bp_get_optimized("getBidPblancListInfoCnstwk", {
        "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end,
        "pageNo": 1, "numOfRows": 1
    })
    
    if not head:
        return 0, 0, set()

    body = head.get("response", {}).get("body", {}) or {}
    total = int(body.get("totalCount") or 0)
    if total <= 0:
        return 0, 0, set()

    pages = max(1, math.ceil(total / page_size))
    saved = 0
    bid_nos: Set[str] = set()
    bsis_map: Optional[Dict] = None
    
    # 병렬 페이지 처리
    def process_page(page: int) -> Tuple[List[Dict], Set[str]]:
        resp = _bp_get_optimized("getBidPblancListInfoCnstwk", {
            "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end,
            "pageNo": page, "numOfRows": page_size
        })
        
        if not resp:
            return [], set()
            
        try:
            rows = parse_notice_items(resp, work_type_hint="Cnstwk") or []
        except Exception as e:
            log.error(f"Parse error page {page}: {e}")
            return [], set()
        
        page_bid_nos = set()
        valid_rows = []
        
        for r in rows:
            bid_no = (r.get("bid_no") or "").strip()
            if bid_no:
                page_bid_nos.add(bid_no)
                valid_rows.append(r)
        
        return valid_rows, page_bid_nos

    # 모든 페이지를 병렬로 처리
    all_rows = []
    with ThreadPoolExecutor(max_workers=NOTICE_WORKERS) as executor:
        futures = [executor.submit(process_page, page) for page in range(1, min(pages + 1, 20))]  # 최대 20페이지까지 병렬
        
        for i, future in enumerate(as_completed(futures), 1):
            try:
                rows, page_bid_nos = future.result()
                all_rows.extend(rows)
                bid_nos.update(page_bid_nos)
                
                # 진행률 로깅
                if i % 5 == 0:
                    log.info(f"[NOTICE {bgn}-{end}] processed {i}/{len(futures)} page batches")
            except Exception as e:
                log.warning(f"Page processing failed: {e}")

    if not all_rows:
        return 0, total, bid_nos

    # 중복 제거를 위한 키 수집
    keys = [(r.get("bid_no", "").strip(), _to_ord_int(r.get("ord"))) for r in all_rows if r.get("bid_no")]
    existing = _filter_existing_optimized(keys, filter_existing_notices, "notices")
    
    # BSIS 데이터가 필요한 경우 lazy loading
    need_bsis_rows = []
    new_rows = []
    
    for r in all_rows:
        bid_no = (r.get("bid_no") or "").strip()
        if not bid_no:
            continue
            
        key = (bid_no, _to_ord_int(r.get("ord")))
        if key in existing:
            continue
            
        if not r.get("base_amount"):
            need_bsis_rows.append((r, bid_no, _to_ord_str3(_to_ord_int(r.get("ord")))))
        else:
            new_rows.append(r)
    
    # BSIS 데이터 보강 (필요한 경우만)
    if need_bsis_rows:
        if bsis_map is None:
            bsis_map = _prefetch_bsis_map_cnstwk_optimized(bgn, end)
            
        for r, bid_no, ord3 in need_bsis_rows:
            bsis_data = bsis_map.get((bid_no, ord3)) if bsis_map else None
            if bsis_data and bsis_data.get("base"):
                r["base_amount"] = bsis_data["base"]
                if bsis_data.get("low") is not None:
                    r["range_low"] = bsis_data["low"]
                if bsis_data.get("high") is not None:
                    r["range_high"] = bsis_data["high"]
                if bsis_data.get("open_dt"):
                    r["base_open_dt"] = bsis_data["open_dt"]
                new_rows.append(r)

    # 배치 저장 최적화
    if new_rows:
        try:
            if upsert_notice_bulk:
                # 적응형 배치 크기로 나누어 저장
                current_batch_size = OPTIMAL_BATCH_SIZE
                for batch in batched_streaming(new_rows, current_batch_size):
                    start_time = time.time()
                    try:
                        upsert_notice_bulk(batch)
                        saved += len(batch)
                        # 성능 기반 배치 크기 조정
                        response_time = time.time() - start_time
                        success_rate = 1.0
                        current_batch_size = adaptive_batch_size(current_batch_size, success_rate, response_time)
                    except Exception as e:
                        log.error(f"Batch upsert failed: {e}")
                        # 개별 처리로 폴백
                        for row in batch:
                            try:
                                upsert_notice(row)
                                saved += 1
                            except Exception as ee:
                                log.error(f"Individual upsert failed: {ee}")
            else:
                # 단건 처리
                for row in new_rows:
                    try:
                        upsert_notice(row)
                        saved += 1
                    except Exception as e:
                        log.error(f"Individual upsert failed: {e}")
        except Exception as e:
            log.error(f"Batch processing failed: {e}")
            # 전체 실패 시 단건 처리로 폴백
            for row in new_rows:
                try:
                    upsert_notice(row)
                    saved += 1
                except Exception as ee:
                    log.error(f"Fallback upsert failed: {ee}")
    
    return saved, total, bid_nos