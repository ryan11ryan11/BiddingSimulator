# apps/etl/tasks.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import time

from core.config import settings
from core.clients.bid_public_info import BidPublicInfo
from core.clients.scsbid_info import ScsbidInfo
from core.db.dao import (
    upsert_notice,
    upsert_result,
    upsert_prep15_bulk,
)

# -------------------------------------------------------------------
# Clients
# -------------------------------------------------------------------
bp = BidPublicInfo(settings.bid_public_base, settings.service_key)
sc = ScsbidInfo(settings.scsbid_base, settings.service_key)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _first(d: Dict, names: List[str], default=None):
    for k in names:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

def _ensure_list(x: Any) -> List[Dict]:
    """
    나라장터 OpenAPI에서 흔한 구조:
    {response:{body:{items:{item: [...]}}}}
    위/변형들을 전부 안전하게 리스트로 펴 준다.
    """
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        if "item" in x:
            v = x["item"]
            return v if isinstance(v, list) else ([v] if v else [])
        body = x.get("response", {}).get("body", {})
        items = body.get("items")
        if items is not None:
            return _ensure_list(items)
    return []

def _paged_get(client, path: str, base_params: Dict, page_size: int = 100):
    """
    공통 페이지네이터: 각 페이지의 resp, total, page를 yield
    """
    page = 1
    total = None
    while True:
        params = dict(base_params, pageNo=page, numOfRows=page_size)
        resp = client.get(path, params)
        body = (resp or {}).get("response", {}).get("body", {})
        if total is None:
            total = int(body.get("totalCount") or 0)
        yield resp, total, page
        if page * page_size >= (total or 0):
            break
        page += 1
        time.sleep(0.2)  # soft throttle

# -------------------------------------------------------------------
# Fetch "basis amount / range" map for Cnstwk (한 번에)
# -------------------------------------------------------------------
def fetch_bsis_map_cnstwk(bgn: str, end: str) -> Dict[tuple, dict]:
    """
    getBidPblancListInfoCnstwkBsisAmount (inqryDiv=1, 기간 조회) 페이지 순회
    -> {(bid_no, ord): {"base":..., "low":..., "high":...}}
    """
    m: Dict[Tuple[str, str], Dict[str, Optional[float]]] = {}

    for resp, total, page in _paged_get(
        bp,
        "getBidPblancListInfoCnstwkBsisAmount",
        {"inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end},
        page_size=100,
    ):
        items = _ensure_list(resp) or _ensure_list((resp or {}).get("response", {}).get("body", {}).get("items", {}))
        for it in items:
            bid_no = str(_first(it, ["bidNtceNo", "BIDNTCENO", "bidNo"]))
            ord_ = str(_first(it, ["bidNtceOrd", "BIDNTCEORD", "bidOrd", "ntceOrd"], "1"))
            try:
                base = int(str(_first(it, ["bssAmt", "bssamt", "baseAmt"], 0)).replace(",", ""))
            except Exception:
                base = 0
            low  = _first(it, ["plnprcEsttRngBgnRate", "rsrvtnPrceRngBgnRate", "rngBgnRate"])
            high = _first(it, ["plnprcEsttRngEndRate", "rsrvtnPrceRngEndRate", "rngEndRate"])

            def _to_pct(x):
                if x is None:
                    return None
                x = float(str(x).replace("%", ""))
                return x / 100.0 if abs(x) > 1 else x

            m[(bid_no, ord_)] = {"base": base, "low": _to_pct(low), "high": _to_pct(high)}

        time.sleep(0.2)  # soft throttle

    return m

# -------------------------------------------------------------------
# Parsers
# -------------------------------------------------------------------
def parse_notice_items(resp: Dict, work_type_hint: str) -> List[Dict]:
    """
    공고 리스트 파서
    """
    out: List[Dict] = []
    for it in _ensure_list(resp):
        bid_no = str(_first(it, ["bidNtceNo", "BIDNTCENO", "bidNo"]))
        ord_ = str(_first(it, ["bidNtceOrd", "BIDNTCEORD", "bidOrd", "ntceOrd"], "1"))

        base = _first(it, ["bssAmt", "bssamt", "baseAmt", "BIDBSSAMT"]) or 0
        try:
            base = int(str(base).replace(",", ""))
        except Exception:
            base = 0

        low = _first(it, ["plnprcEsttRngBgnRate", "rsrvtnPrceRngBgnRate", "rngBgnRate"])
        high = _first(it, ["plnprcEsttRngEndRate", "rsrvtnPrceRngEndRate", "rngEndRate"])

        def _to_pct(x):
            if x is None:
                return None
            x = float(str(x).replace("%", ""))
            return x / 100.0 if abs(x) > 1 else x

        range_low = _to_pct(low)
        range_high = _to_pct(high)

        owner_id = str(_first(it, ["dminsttCd", "asignBdgtInsttCd", "insttCd", "ownerId"]) or "")
        announced_at = _first(it, ["bidNtceDt", "ntceDt", "rgstDt"])  # YYYYMMDDHHMM or YYYYMMDD
        if announced_at:
            try:
                s = str(announced_at)
                if len(s) == 12:
                    announced_at = datetime.strptime(s, "%Y%m%d%H%M")
                else:
                    announced_at = datetime.strptime(s[:8], "%Y%m%d")
            except Exception:
                announced_at = None

        row = {
            "bid_no": bid_no,
            "ord": ord_,
            "base_amount": base,
            "range_low": range_low,
            "range_high": range_high,
            "lower_rate": None,
            "owner_id": owner_id,
            "work_type": work_type_hint,
            "announced_at": announced_at,
            "vat_included": True,
        }
        if bid_no:
            out.append(row)
    return out

def parse_prepar_detail_items(resp: Dict) -> Dict[str, List[Dict]]:
    """
    예비가격상세(prep15) 파서
    returns mapping: "{bid_no}|{ord}" -> List[rows]
    comp_sno(항목 순번) 누락 시 공고 내에서 1..N을 유추해 채움
    """
    def _to_int(x):
        try:
            return int(str(x).replace(",", "")) if x is not None else None
        except Exception:
            return None

    m: Dict[str, List[Dict]] = {}

    items = _ensure_list(resp)
    if not items:
        body = (resp or {}).get("response", {}).get("body", {})
        items = _ensure_list(body.get("items", {}))
    if not items:
        return m

    for it in items:
        low = {str(k).lower(): v for k, v in it.items()}

        bid_no = str(
            low.get("bidntceno") or low.get("bidno") or low.get("bid_ntce_no") or low.get("bidnotice_no") or ""
        ).strip()
        ord_ = str(
            low.get("bidntceord") or low.get("bidord") or low.get("ntceord") or low.get("bid_ntce_ord") or "1"
        ).strip()

        comp_sno = low.get("compsno") or low.get("compno") or low.get("rsrvtnprcesno") or low.get("sno")
        comp_sno = _to_int(comp_sno)

        bsis_plnprc = (
            low.get("bsisplnprc") or low.get("rsrvtnprce") or low.get("bsisplnprcamt") or low.get("rsrvtnamt") or 0
        )
        plnprc_final = low.get("plnprc") or low.get("finalplnprc") or low.get("fnlplnprc")

        drawn_yn = str(low.get("drwtyn") or low.get("drawyn") or "N").upper() == "Y"
        drwt_seq = low.get("drwtnum") or low.get("drwtordr") or low.get("drawseq")
        drwt_seq = _to_int(drwt_seq)

        row = {
            "bid_no": bid_no,
            "ord": ord_ if ord_ else "1",
            "comp_sno": comp_sno,  # 없으면 아래서 채움
            "bsis_plnprc": _to_int(bsis_plnprc) or 0,
            "drawn_flag": drawn_yn,
            "draw_seq": drwt_seq,
            "final_plnprc": _to_int(plnprc_final),
        }

        if not bid_no:
            continue

        key = f"{row['bid_no']}|{row['ord']}"
        if row["comp_sno"] is None or row["comp_sno"] <= 0:
            row["comp_sno"] = len(m.get(key, [])) + 1

        m.setdefault(key, []).append(row)

    return m

def parse_result_items(resp: Dict) -> List[Dict]:
    """
    개찰결과(result) 파서: 예정가격(est_price), 예비가격평균(presmpt_price) 등
    """
    def _to_int_or_none(x):
        try:
            return int(str(x).replace(",", ""))
        except Exception:
            return None

    out: List[Dict] = []

    items = _ensure_list(resp)
    if not items:
        body = (resp or {}).get("response", {}).get("body", {})
        items = _ensure_list(body.get("items", {}))
    if not items:
        return out

    for it in items:
        # 다양한 키 별칭 흡수
        bid_no = str(_first(it, ["bidNtceNo", "bidNo", "BIDNTCENO"]))
        ord_   = str(_first(it, ["bidNtceOrd", "bidOrd", "BIDNTCEORD"], "1"))

        est    = _to_int_or_none(_first(it, ["estmtPrice","estPrice","prdprc","EPRC","esttPric","esttAmt"]))
        prcmp  = _to_int_or_none(_first(it, ["presmptPrce","presmptPrice","PRCMP","prsmptPrc"]))
        cnt    = _to_int_or_none(_first(it, ["opengBddprcnt","bidderCnt","BIDDERCNT"]))
        rebid  = str(_first(it, ["rbidYn","reBidYn","REBDYN"], "N")).upper() == "Y"
        rldt   = _first(it, ["rlOpengDt","opengDt","OPENGDT"])  # YYYYMMDDHHMM

        row = {
            "bid_no": bid_no,
            "ord": ord_,
            "est_price": est,
            "presmpt_price": prcmp,
            "bidders_cnt": cnt,
            "rebid_flag": rebid,
            "rl_openg_dt": None,
        }
        # 날짜 파싱(있을 때만)
        if isinstance(rldt, str) and len(rldt) == 12:
            try:
                row["rl_openg_dt"] = datetime.strptime(rldt, "%Y%m%d%H%M")
            except Exception:
                pass

        if bid_no:
            out.append(row)

    return out

# -------------------------------------------------------------------
# Extract → Transform → Load
# -------------------------------------------------------------------
def collect_and_load_prepar_detail_cnstwk(bgn: str, end: str) -> int:
    """
    예비가격상세(prep15) 수집 → t_prep15 벌크 upsert
    """
    page = 1
    saved = 0
    page_size = 100
    while True:
        resp = sc.get_prepar_pc_detail_cnstwk(
            inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=page, num_rows=page_size
        )
        body = (resp or {}).get("response", {}).get("body", {})
        total = int(body.get("totalCount") or 0)

        m = parse_prepar_detail_items(resp)
        if m:
            for _, rows in m.items():
                upsert_prep15_bulk(rows)
                saved += len(rows)

        if page * page_size >= total:
            break
        page += 1
        time.sleep(0.3)  # 호출 한도 여유
    return saved

def collect_and_load_notices(bgn: str, end: str, work: str = "Cnstwk") -> int:
    """
    공고 수집: 기간 윈도우로 공고 리스트를 받고,
    같은 윈도우의 '기초금액/범위' 맵을 한 번에 불러 보강 후 t_notice upsert
    """
    if work != "Cnstwk":
        # TODO: 필요 시 Servc/Thng/Frgcpt 버전 추가
        pass

    bsis_map = fetch_bsis_map_cnstwk(bgn, end)  # 윈도우 단위로 한 번에 확보
    saved = 0

    for resp, total, page in _paged_get(
        bp,
        f"getBidPblancListInfo{work}",
        {"inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end},
        page_size=100,
    ):
        rows = parse_notice_items(resp, work_type_hint=work)

        for r in rows:
            if not r["base_amount"]:
                hit = bsis_map.get((r["bid_no"], r["ord"]))
                if hit and hit["base"]:
                    r["base_amount"] = hit["base"]
                    if hit["low"] is not None:
                        r["range_low"] = hit["low"]
                    if hit["high"] is not None:
                        r["range_high"] = hit["high"]
            if r["bid_no"] and r["base_amount"]:
                upsert_notice(r)
                saved += 1

        time.sleep(0.2)  # soft throttle

    return saved

def collect_and_load_results_cnstwk(bgn: str, end: str, page_size: int = 100) -> int:
    """
    개찰결과(result) 수집 → t_result upsert
    """
    saved = 0
    page = 1
    while True:
        # inqry_div=1 : 등록일시 기간 필터
        resp = sc.get_openg_result_list_cnstwk(
            inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=page, num_rows=page_size
        )
        body = (resp or {}).get("response", {}).get("body", {})
        total = int(body.get("totalCount") or 0)

        rows = parse_result_items(resp)
        if not rows:
            # 데이터가 없더라도 total 기준으로 마지막 페이지까지는 시도
            if page * page_size >= total:
                break
        else:
            for r in rows:
                upsert_result(r)
            saved += len(rows)

        if page * page_size >= total:
            break
        page += 1
        time.sleep(0.2)
    return saved

# (옵션) 단일 공고의 기초금액/범위만 직접 조회할 때 사용할 수 있는 유틸
def _fetch_bsis_amount_for_cnstwk(bid_no: str, bid_ord: str = "1") -> Optional[Tuple[int, Optional[float], Optional[float]]]:
    res = bp.get(
        "getBidPblancListInfoCnstwkBsisAmount",
        {"inqryDiv": 2, "bidNtceNo": bid_no, "bidNtceOrd": bid_ord, "pageNo": 1, "numOfRows": 10},
    )
    items = _ensure_list(res) or _ensure_list((res or {}).get("response", {}).get("body", {}).get("items", {}))
    if not items:
        return None
    it = items[0]
    base = int(str(_first(it, ["bssAmt", "bssamt", "baseAmt"], 0)).replace(",", ""))
    low = _first(it, ["plnprcEsttRngBgnRate", "rsrvtnPrceRngBgnRate", "rngBgnRate"])
    high = _first(it, ["plnprcEsttRngEndRate", "rsrvtnPrceRngEndRate", "rngEndRate"])

    def _to_pct(x):
        if x is None:
            return None
        x = float(str(x).replace("%", ""))
        return x / 100.0 if abs(x) > 1 else x

    return base, _to_pct(low), _to_pct(high)
