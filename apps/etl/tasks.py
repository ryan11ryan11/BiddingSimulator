from typing import Any, Dict, List, Optional
from datetime import datetime
from core.config import settings
from core.clients.bid_public_info import BidPublicInfo
from core.clients.scsbid_info import ScsbidInfo
from core.db.dao import upsert_notice, upsert_result, upsert_prep15_bulk
import time

bp = BidPublicInfo(settings.bid_public_base, settings.service_key)
sc = ScsbidInfo(settings.scsbid_base, settings.service_key)

# -------------------------
# Generic JSON helpers
# -------------------------

def _paged_get(client, path, base_params, page_size=100):
    page = 1
    total = None
    while True:
        params = dict(base_params, pageNo=page, numOfRows=page_size)
        resp = client.get(path, params)
        body = (resp or {}).get("response", {}).get("body", {})
        total = int(body.get("totalCount") or 0) if total is None else total
        yield resp, total, page
        # 다음 페이지 결정
        if page * page_size >= total:
            break
        page += 1
        time.sleep(0.2)  # soft throttle

def fetch_bsis_map_cnstwk(bgn: str, end: str) -> Dict[tuple, dict]:
    """
    getBidPblancListInfoCnstwkBsisAmount (inqryDiv=1, 기간 조회) 를 페이지 돌며
    {(bid_no, ord): {"base":..., "low":..., "high":...}} 맵으로 만든다.
    """
    m = {}
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
                if x is None: return None
                x = float(str(x).replace("%",""))
                return x/100.0 if abs(x)>1 else x
            m[(bid_no, ord_)] = {"base": base, "low": _to_pct(low), "high": _to_pct(high)}
        # (선택) 진행 로그
        # print(f"[bsis] page={page} / total~{total}")
        time.sleep(0.2)  # soft throttle
    return m


def _first(d: Dict, names: List[str], default=None):
    for k in names:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def _ensure_list(x: Any) -> List[Dict]:
    # 나라장터 OpenAPI는 {response:{body:{items:{item:[]}}}} 패턴이 흔함
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        if "item" in x:
            v = x["item"]
            return v if isinstance(v, list) else [v]
        # body/items/item 형태 탐색
        body = x.get("response", {}).get("body", {})
        items = body.get("items")
        if items is not None:
            return _ensure_list(items)
    return []


# -------------------------
# Parsers
# -------------------------

def parse_notice_items(resp: Dict, work_type_hint: str) -> List[Dict]:
    out = []
    for it in _ensure_list(resp):
        bid_no = str(_first(it, ["bidNtceNo", "BIDNTCENO", "bidNo"]))
        ord_ = str(_first(it, ["bidNtceOrd", "BIDNTCEORD", "bidOrd", "ntceOrd"], "1"))
        base = _first(it, ["bssAmt", "bssamt", "baseAmt", "BIDBSSAMT"]) or 0
        try:
            base = int(str(base).replace(",", ""))
        except Exception:
            base = 0
        # 예정가격 범위(±a%)
        low = _first(it, ["plnprcEsttRngBgnRate", "rsrvtnPrceRngBgnRate", "rngBgnRate"])  # 예: -2
        high = _first(it, ["plnprcEsttRngEndRate", "rsrvtnPrceRngEndRate", "rngEndRate"])  # 예: +2
        def _to_pct(x):
            if x is None: return None
            x = float(str(x).replace("%", ""))
            return x / 100.0 if abs(x) > 1 else x  # 2 → 0.02, 0.02 유지
        range_low = _to_pct(low)
        range_high = _to_pct(high)

        owner_id = str(_first(it, ["dminsttCd", "asignBdgtInsttCd", "insttCd", "ownerId"]) or "")
        announced_at = _first(it, ["bidNtceDt", "ntceDt", "rgstDt"])  # YYYYMMDDHHMM 형태
        if announced_at:
            try:
                if len(str(announced_at)) == 12:
                    announced_at = datetime.strptime(str(announced_at), "%Y%m%d%H%M")
                else:
                    announced_at = datetime.strptime(str(announced_at)[:8], "%Y%m%d")
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


# def parse_prepar_detail_items(resp: Dict) -> Dict[str, List[Dict]]:
#     # returns mapping key (bid_no, ord) -> rows[prep15]
#     m: Dict[str, List[Dict]] = {}
#     for it in _ensure_list(resp):
#         bid_no = str(_first(it, ["bidNtceNo", "bidNo", "BIDNTCENO"]))
#         ord_ = str(_first(it, ["bidNtceOrd", "bidOrd", "BIDNTCEORD"], "1"))
#         comp_sno = int(_first(it, ["compSno", "compno", "rsrvtnPrceSno", "sno"], 0))
#         bsis_plnprc = _first(it, ["bsisPlnprc", "rsrvtnPrce", "bsisPlnprcAmt", "rsrvtnAmt"], 0)
#         plnprc_final = _first(it, ["plnprc", "finalPlnprc", "fnlPlnprc"], None)
#         drawn_yn = str(_first(it, ["drwtYn", "drawYn"], "N")).upper() == "Y"
#         drwt_seq = _first(it, ["drwtNum", "drwtOrdr", "drawSeq"], None)
#         # normalize ints
#         def _to_int(x):
#             try:
#                 return int(str(x).replace(",", "")) if x is not None else None
#             except Exception:
#                 return None
#         row = {
#             "bid_no": bid_no,
#             "ord": ord_,
#             "comp_sno": comp_sno,
#             "bsis_plnprc": _to_int(bsis_plnprc) or 0,
#             "drawn_flag": drawn_yn,
#             "draw_seq": int(drwt_seq) if drwt_seq is not None else None,
#             "final_plnprc": _to_int(plnprc_final),
#         }
#         if bid_no and comp_sno:
#             m.setdefault(f"{bid_no}|{ord_}", []).append(row)
#     return m

def parse_prepar_detail_items(resp: Dict) -> Dict[str, List[Dict]]:
    """
    returns mapping: "{bid_no}|{ord}" -> List[rows]
    키 변형/대소문자 차이를 최대한 흡수하고, comp_sno가 누락될 경우 순번을 유추해 채운다.
    """
    def _to_int(x):
        try:
            return int(str(x).replace(",", "")) if x is not None else None
        except Exception:
            return None

    m: Dict[str, List[Dict]] = {}

    # items 찾아내기 (response/body/items/item 패턴/변형 모두 커버)
    items = _ensure_list(resp)
    if not items:
        body = (resp or {}).get("response", {}).get("body", {})
        items = _ensure_list(body.get("items", {}))
    if not items:
        return m  # 이 페이지는 데이터 없음

    for it in items:
        # 키를 소문자로 통일
        low = {str(k).lower(): v for k, v in it.items()}

        # 필드 매핑(여러 별칭 대응)
        bid_no = str(
            low.get("bidntceno") or low.get("bidno") or low.get("bid_ntce_no") or low.get("bidnotice_no") or ""
        ).strip()
        ord_ = str(
            low.get("bidntceord") or low.get("bidord") or low.get("ntceord") or low.get("bid_ntce_ord") or "1"
        ).strip()

        comp_sno = (
            low.get("compsno") or low.get("compno") or low.get("rsrvtnprcesno") or low.get("sno")
        )
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
        # comp_sno가 없다면, 해당 공고 안에서 1부터 순차 부여
        if row["comp_sno"] is None or row["comp_sno"] <= 0:
            row["comp_sno"] = len(m.get(key, [])) + 1

        # 최종 append
        m.setdefault(key, []).append(row)

    return m



# -------------------------
# Extract → Transform → Load
# -------------------------

# def collect_and_load_prepar_detail_cnstwk(bgn: str, end: str) -> int:
#     page = 1
#     saved = 0
#     page_size = 100
#     while True:
#         resp = sc.get_prepar_pc_detail_cnstwk(
#             inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=page, num_rows=page_size
#         )
#         body = (resp or {}).get("response",{}).get("body",{})
#         total = int(body.get("totalCount") or 0)

#         m = parse_prepar_detail_items(resp)
#         if not m:
#             break
#         for _, rows in m.items():
#             upsert_prep15_bulk(rows)
#             saved += len(rows)

#         if page * page_size >= total:
#             break
#         page += 1
#     return saved

# --- 교체 대상: apps/etl/tasks.py 안의 collect_and_load_prepar_detail_cnstwk 함수 전체를 이걸로 바꿔주세요 ---
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
        # 비어 있어도 다음 페이지로 계속 감
        if page * page_size >= total:
            break
        page += 1
        time.sleep(0.3)  # 호출한도 완화
    return saved


def _fetch_bsis_amount_for_cnstwk(bid_no: str, bid_ord: str = "1"):
    res = bp.get("getBidPblancListInfoCnstwkBsisAmount", {
        "inqryDiv": 2,           # 1:등록일시 / 2:공고번호
        "bidNtceNo": bid_no,
        "bidNtceOrd": bid_ord,   # ★ 추가
        "pageNo": 1,
        "numOfRows": 10,
    })
    items = _ensure_list(res) or _ensure_list((res or {}).get("response",{}).get("body",{}).get("items",{}))
    if not items:
        return None
    it = items[0]
    base = int(str(_first(it, ["bssAmt","bssamt","baseAmt"], 0)).replace(",",""))
    low  = _first(it, ["plnprcEsttRngBgnRate","rsrvtnPrceRngBgnRate","rngBgnRate"])
    high = _first(it, ["plnprcEsttRngEndRate","rsrvtnPrceRngEndRate","rngEndRate"])
    def _to_pct(x):
        if x is None: return None
        x = float(str(x).replace("%",""))
        return x/100.0 if abs(x)>1 else x
    return base, _to_pct(low), _to_pct(high)

# 2) 공고 수집: 페이징 + bsis 맵 조인 (per-bid 호출 제거)
def collect_and_load_notices(bgn: str, end: str, work: str = "Cnstwk"):
    if work != "Cnstwk":
        # 다른 업무는 동일 패턴으로 함수 하나 더 만들어 쓰세요 (Servc/Thng/Frgcpt)
        pass

    bsis_map = fetch_bsis_map_cnstwk(bgn, end)  # ★ 윈도우 단위로 한 번에 확보
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
        # (선택) 진행 로그
        # print(f"[list] page={page} / total~{total} parsed={len(rows)} saved+={len(fixed)}")
        time.sleep(0.2)  # soft throttle

    # print(f"[done] notices saved={saved}")
    return saved
