# apps/etl/tasks.py
from typing import Any, Dict, List, Optional
from datetime import datetime
import time

from core.config import settings
from core.clients.bid_public_info import BidPublicInfo
from core.clients.scsbid_info import ScsbidInfo
from core.db.dao import upsert_notice, upsert_result, upsert_prep15_bulk

bp = BidPublicInfo(settings.bid_public_base, settings.service_key)
sc = ScsbidInfo(settings.scsbid_base, settings.service_key)

# -------------------------
# Generic helpers
# -------------------------

def _first(d: Dict, names: List[str], default=None):
    for k in names:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default

def _ensure_list(x: Any) -> List[Dict]:
    """
    나라장터 OpenAPI는 {response:{body:{items:{item:[...]}}} 패턴이 흔함.
    다양하게 와도 리스트로 평탄화해서 돌려줌.
    """
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        if "item" in x:
            v = x["item"]
            return v if isinstance(v, list) else [v]
        body = x.get("response", {}).get("body", {})
        items = body.get("items")
        if items is not None:
            return _ensure_list(items)
    return []

def _to_int(x) -> Optional[int]:
    try:
        return int(str(x).replace(",", "")) if x is not None else None
    except Exception:
        return None

def _paged_get(client, path, base_params, page_size=100):
    page = 1
    total = None
    while True:
        params = dict(base_params, pageNo=page, numOfRows=page_size)
        resp = client.get(path, params)
        body = (resp or {}).get("response", {}).get("body", {})
        total = int(body.get("totalCount") or 0) if total is None else total
        yield resp, total, page
        if page * page_size >= total:
            break
        page += 1
        time.sleep(0.2)  # soft throttle

# -------------------------
# BSIS(기초금액) 보강용
# -------------------------

def fetch_bsis_map_cnstwk(bgn: str, end: str) -> Dict[tuple, dict]:
    """
    getBidPblancListInfoCnstwkBsisAmount (inqryDiv=1, 기간 조회)
    -> {(bid_no, ord): {"base":..., "low":..., "high":...}}
    """
    m: Dict[tuple, dict] = {}
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
            base = _to_int(_first(it, ["bssAmt", "bssamt", "baseAmt"], 0)) or 0
            low  = _first(it, ["plnprcEsttRngBgnRate", "rsrvtnPrceRngBgnRate", "rngBgnRate"])
            high = _first(it, ["plnprcEsttRngEndRate", "rsrvtnPrceRngEndRate", "rngEndRate"])

            def _to_pct(x):
                if x is None: return None
                x = float(str(x).replace("%",""))
                return x/100.0 if abs(x)>1 else x

            m[(bid_no, ord_)] = {"base": base, "low": _to_pct(low), "high": _to_pct(high)}
        time.sleep(0.2)
    return m

# -------------------------
# Parsers (notice / prep15 / result)
# -------------------------

def parse_notice_items(resp: Dict, work_type_hint: str) -> List[Dict]:
    out: List[Dict] = []
    for it in _ensure_list(resp):
        bid_no = str(_first(it, ["bidNtceNo", "BIDNTCENO", "bidNo"]))
        ord_ = str(_first(it, ["bidNtceOrd", "BIDNTCEORD", "bidOrd", "ntceOrd"], "1"))
        base = _to_int(_first(it, ["bssAmt", "bssamt", "baseAmt", "BIDBSSAMT"])) or 0
        low = _first(it, ["plnprcEsttRngBgnRate", "rsrvtnPrceRngBgnRate", "rngBgnRate"])
        high = _first(it, ["plnprcEsttRngEndRate", "rsrvtnPrceRngEndRate", "rngEndRate"])

        def _to_pct(x):
            if x is None: return None
            x = float(str(x).replace("%",""))
            return x/100.0 if abs(x)>1 else x

        owner_id = str(_first(it, ["dminsttCd", "asignBdgtInsttCd", "insttCd", "ownerId"]) or "")
        announced_at = _first(it, ["bidNtceDt", "ntceDt", "rgstDt"])
        if announced_at:
            try:
                announced_at = datetime.strptime(str(announced_at)[:12], "%Y%m%d%H%M") if len(str(announced_at))>=12 else datetime.strptime(str(announced_at)[:8], "%Y%m%d")
            except Exception:
                announced_at = None

        row = {
            "bid_no": bid_no,
            "ord": ord_ if ord_ else "1",
            "base_amount": base,
            "range_low": _to_pct(low),
            "range_high": _to_pct(high),
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
    returns mapping: "{bid_no}|{ord}" -> List[rows]
    comp_sno 누락 시 순번 유추로 채움.
    """
    m: Dict[str, List[Dict]] = {}
    items = _ensure_list(resp)
    if not items:
        body = (resp or {}).get("response", {}).get("body", {})
        items = _ensure_list(body.get("items", {}))
    if not items:
        return m

    for it in items:
        low = {str(k).lower(): v for k, v in it.items()}

        bid_no = str(low.get("bidntceno") or low.get("bidno") or "").strip()
        ord_raw = low.get("bidntceord") or low.get("bidord") or low.get("ntceord") or "1"
        try:
            ord_ = str(int(str(ord_raw).strip()))
        except Exception:
            ord_ = str(ord_raw or "1")

        comp_sno = _to_int(low.get("compsno") or low.get("compno") or low.get("rsrvtnprcesno") or low.get("sno"))

        bsis_plnprc = low.get("bsisplnprc") or low.get("rsrvtnprce") or low.get("bsisplnprcamt") or low.get("rsrvtnamt") or 0
        plnprc_final = low.get("plnprc") or low.get("finalplnprc") or low.get("fnlplnprc")

        drawn_yn = str(low.get("drwtyn") or low.get("drawyn") or "N").upper() == "Y"
        drwt_seq = _to_int(low.get("drwtnum") or low.get("drwtordr") or low.get("drawseq"))

        row = {
            "bid_no": bid_no,
            "ord": ord_,
            "comp_sno": comp_sno,
            "bsis_plnprc": _to_int(bsis_plnprc) or 0,
            "drawn_flag": drawn_yn,
            "draw_seq": drwt_seq,
            "final_plnprc": _to_int(plnprc_final),
        }

        if not row["comp_sno"] or row["comp_sno"] <= 0:
            key = f"{row['bid_no']}|{row['ord']}"
            row["comp_sno"] = len(m.get(key, [])) + 1

        if bid_no and row["comp_sno"]:
            m.setdefault(f"{row['bid_no']}|{row['ord']}", []).append(row)

    return m

def parse_result_items(resp: Dict) -> List[Dict]:
    """
    개찰결과(result) 파서: 예정가격(est_price)을 다양한 별칭으로 수집 (영구 보강 포인트)
    - est_price 후보: plnprc / finalPlnprc / fnlPlnprc / estmtPrice / estPrice / esttAmt / opengEsttPric / opengEstPrice ...
    - presmpt_price 후보: presmptPrce / presmptPrice / prcmp / prsmptprc / rsrvtnPrceAvrg ...
    - ord는 숫자 문자열로 정규화
    """
    out: List[Dict] = []
    items = _ensure_list(resp)
    if not items:
        body = (resp or {}).get("response", {}).get("body", {})
        items = _ensure_list(body.get("items", {}))
    if not items:
        return out

    for it in items:
        low = {str(k).lower(): v for k, v in it.items()}

        bid_no = str(low.get("bidntceno") or low.get("bidno") or "").strip()
        ord_raw = low.get("bidntceord") or low.get("bidord") or "1"
        try:
            ord_ = str(int(str(ord_raw).strip()))
        except Exception:
            ord_ = str(ord_raw or "1")

        # 예정가격(Est) 후보
        est_candidates = [
            "plnprc", "finalplnprc", "fnlplnprc",
            "estmtprice", "estprice", "esttamt", "estamt",
            "opengesttpric", "opengestprice",
        ]
        est: Optional[int] = None
        for k in est_candidates:
            if k in low and low[k] not in (None, "", "0"):
                est = _to_int(low[k])
                if est: break

        # 예비가격 평균(있으면 참고)
        presmpt_candidates = [
            "presmptprce", "presmptprice", "prcmp", "prsmptprc",
            "rsrvtnprceavrg", "rsrvtnprceavrgamt",
        ]
        prcmp: Optional[int] = None
        for k in presmpt_candidates:
            if k in low and low[k] not in (None, "", "0"):
                prcmp = _to_int(low[k])
                break

        bidders = _to_int(low.get("opengbddprcnt") or low.get("biddercnt"))
        rebid = str(low.get("rbidyn") or low.get("rebidyn") or "N").upper() == "Y"
        rldt = low.get("rlopengdt") or low.get("opengdt")

        row = {
            "bid_no": bid_no,
            "ord": ord_,
            "est_price": est,
            "presmpt_price": prcmp,
            "bidders_cnt": bidders,
            "rebid_flag": rebid,
            "rl_openg_dt": None,
        }
        if isinstance(rldt, str) and len(rldt) >= 12:
            try:
                row["rl_openg_dt"] = datetime.strptime(str(rldt)[:12], "%Y%m%d%H%M")
            except Exception:
                pass

        if bid_no:
            out.append(row)

    return out

# -------------------------
# ETL: collect/load
# -------------------------

def collect_and_load_prepar_detail_cnstwk(bgn: str, end: str) -> int:
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
        time.sleep(0.3)
    return saved

def collect_and_load_notices(bgn: str, end: str, work: str = "Cnstwk"):
    if work != "Cnstwk":
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
        fixed = []
        for r in rows:
            if not r["base_amount"]:
                hit = bsis_map.get((r["bid_no"], r["ord"]))
                if hit and hit["base"]:
                    r["base_amount"] = hit["base"]
                    if hit["low"] is not None:  r["range_low"] = hit["low"]
                    if hit["high"] is not None: r["range_high"] = hit["high"]
            if r["bid_no"] and r["base_amount"]:
                upsert_notice(r); fixed.append(r)
        saved += len(fixed)
        time.sleep(0.2)
    return saved

def collect_and_load_results_cnstwk(bgn: str, end: str) -> int:
    """
    결과 수집(기간) – est_price가 다양한 키로 와도 파서에서 흡수하여 upsert.
    """
    page = 1
    saved = 0
    page_size = 100
    while True:
        resp = sc.get_openg_result_list_cnstwk(
            inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=page, num_rows=page_size
        )
        body = (resp or {}).get("response", {}).get("body", {})
        total = int(body.get("totalCount") or 0)

        rows = parse_result_items(resp)
        for r in rows:
            if r.get("bid_no"):
                upsert_result(r)
                saved += 1

        if page * page_size >= total:
            break
        page += 1
        time.sleep(0.25)
    return saved
