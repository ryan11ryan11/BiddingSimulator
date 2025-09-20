#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BiddingSimulator — optimized collector

개찰결과(anchor) + 공고/면허/기초금액 범위 수집 → (선택) 표준데이터셋 보조 →
주/부공종·면허 NULL만 단건(inqryDiv=2) 보강 → PREP15로 예정가격/하한 계산

호출 최소화 전략:
  1) 평가정보(EVAL)와 공사목록(CNSTWK)을 등록일시 범위형(inqryDiv=1)으로 먼저 대량 수집
  2) 면허제한(LICENSE)은 범위형 우선 + (선택) 표준데이터셋으로 보조, 단건은 마지막에 소량만
  3) CNSTWK 범위 응답의 indstrytyMfrcFldEvlYn='N'이면 EVAL 단건 호출 스킵
  4) 단건 호출 큐는 상기 범위 수집으로도 채워지지 않은 ‘진짜 NULL’만 포함, 총 cap로 제한
  5) UPSERT는 ‘기존값 우선’(Non-NULL 유지)

기존 스크립트에서 추가/변경:
  - fetch_eval_mfrc_range, fetch_cnstwk_range, (선택) fetch_std_bid_range 추가
  - pass-1에서 범위형 결과를 먼저 병합하여 t_notice/t_license 채움
  - need_single 산정 로직 개선(범위형/표준데이터셋 결과 반영, EVAL 스킵 규칙 적용)

환경변수:
  G2B_BID_PUBLIC_BASE, G2B_SCSBID_BASE, G2B_SERVICE_KEY, PG_DSN,
  G2B_PER_PAGE, G2B_TIMEOUT, G2B_TPS_SLEEP, G2B_INSECURE

사용 예(파워쉘):
  $env:G2B_SERVICE_KEY="(your key)"
  $env:PG_DSN="postgresql+psycopg2://user:pass@localhost:5432/biddingsim"
  .\.venv\Scripts\python.exe .\collectData_optimized.py \
    --from 20250101 --to 20250131 \
    --license-lookback-days 365 --single-backfill-cap 300 \
    --use-std-dataset --debug
"""

from __future__ import annotations
import os, time, ssl, argparse
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ----------------------- 설정 -----------------------
BID_PUBLIC_BASE = os.getenv("G2B_BID_PUBLIC_BASE",
    "http://apis.data.go.kr/1230000/ad/BidPublicInfoService")
SCSBID_BASE = os.getenv("G2B_SCSBID_BASE",
    "http://apis.data.go.kr/1230000/as/ScsbidInfoService")
STD_BASE = os.getenv("G2B_STD_BASE",
    "http://apis.data.go.kr/1230000/ao/PubDataOpnStdService")
SERVICE_KEY = os.getenv("G2B_SERVICE_KEY", "")
PG_DSN = os.getenv("PG_DSN", "postgresql+psycopg2://user:pass@localhost:5432/postgres")

PER_PAGE_DEFAULT = int(os.getenv("G2B_PER_PAGE", "200"))
REQUEST_TIMEOUT = int(os.getenv("G2B_TIMEOUT", "20"))
DEFAULT_TPS_SLEEP = float(os.getenv("G2B_TPS_SLEEP", "0.35"))
INSECURE = os.getenv("G2B_INSECURE", "0") == "1"

# 비즈니스 규칙
DEFAULT_MAX_PRES_PRICE = 300_000_000   # 0이면 제한 없음
KEYWORDS = ["금속", "창호", "지붕판금", "건축물조립"]
CODE_HINTS = ["4991"]                  # 금속창호ㆍ지붕건축물조립공사업

# 오퍼레이션
OP_OPENGLIST_CNSTWK = "getOpengResultListInfoCnstwkPPSSrch"        # 낙찰정보: 개찰결과 공사 목록(개찰일 범위)
OP_PPS_CNSTWK       = "getBidPblancListInfoCnstwkPPSSrch"           # 공고: 공사 검색(개찰일 범위)
OP_PREP15           = "getOpengResultListInfoCnstwkPreparPcDetail"  # 낙찰정보: 예비가격 상세(단건)
OP_LICENSE_LIMIT    = "getBidPblancListInfoLicenseLimit"            # 공고: 면허제한(범위/단건)
OP_BSIS_CNSTWK      = "getBidPblancListInfoCnstwkBsisAmount"        # 공고: 공사 기초금액(등록일 범위)
OP_CNSTWK_LIST      = "getBidPblancListInfoCnstwk"                  # 공고: 공사 목록(등록일 범위/단건)
OP_EVAL_MFRC        = "getBidPblancListEvaluationIndstrytyMfrcInfo" # 공고: 평가정보(범위/단건)
OP_ETC_LIST         = "getBidPblancListInfoEtc"                     # 공고: 기타공고 목록(등록일 범위)
OP_STD_BID          = "getDataSetOpnStdBidPblancInfo"               # 표준데이터셋: 공고 메타(보조)

# ----------------------- 세션/TLS -----------------------
SESSION = requests.Session()

class TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        try:
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        except Exception:
            pass
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        try:
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        except Exception:
            pass
        kwargs["ssl_context"] = ctx
        return super().proxy_manager_for(*args, **kwargs)

SESSION.mount("https://", TLS12Adapter())
SESSION.headers.update({"User-Agent": "G2B-Collector/PG-Optimized (+requests)"})

# ----------------------- 로깅/유틸 -----------------------
DEBUG = False

def log_info(s):
    print(f"[INFO] {s}")

def log_warn(s):
    print(f"[WARN] {s}")

def log_dbg(s):
    if DEBUG:
        print(f"[DBG]  {s}")

def to_int_safe(x) -> Optional[int]:
    try:
        if x is None:
            return None
        s = str(x).replace(",", "").strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None

def to_float_safe(x) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).replace("+", "").strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None

def to_ord_int(x) -> int:
    try:
        s = str(x).strip().lstrip("0")
        if s:
            return int(s)
        return 0
    except Exception:
        return 0

def parse_yyyymmdd(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None

def floor_10won(x: float) -> int:
    return int(x // 10) * 10

def to_date_from_yyyymmdd(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except Exception:
        return None

def to_dt_from_compact(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    digits = "".join(ch for ch in str(s) if ch.isdigit())
    try:
        if len(digits) >= 14:
            return datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        if len(digits) >= 12:
            return datetime.strptime(digits[:12], "%Y%m%d%H%M")
        if len(digits) >= 8:
            return datetime.strptime(digits[:8], "%Y%m%d")
        return None
    except Exception:
        return None

# ----------------------- HTTP 공통 -----------------------
def http_get(base: str, op: str, params: Dict) -> Optional[Dict]:
    if not SERVICE_KEY:
        raise RuntimeError("환경변수 G2B_SERVICE_KEY 필요")
    prms = dict(params)
    prms["ServiceKey"] = SERVICE_KEY
    prms["type"] = "json"
    url = f"{base.rstrip('/')}/{op}"
    log_dbg(f"REQ: GET {url} params={{...}}")
    try:
        resp = SESSION.get(url, params=prms, timeout=REQUEST_TIMEOUT, verify=not INSECURE)
        log_dbg(f"RESP: status={resp.status_code} ctype={resp.headers.get('Content-Type','')}")
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            log_warn(f"JSON 파싱 실패 body[:180]={resp.text[:180]}...")
            return None
    except requests.exceptions.RequestException as e:
        log_warn(f"RequestException: {e}")
        return None

def extract_items(j: Optional[Dict]) -> List[Dict]:
    if not j:
        return []
    body = (j.get("response") or {}).get("body") or {}
    items = body.get("items")
    if items is None:
        if isinstance(body, dict) and "item" in body:
            items = body
        else:
            return []
    if isinstance(items, dict):
        if "item" in items:
            v = items.get("item")
            if isinstance(v, list):
                return v
            if isinstance(v, dict):
                return [v]
        return [items]
    if isinstance(items, list):
        return items
    return []

# ----------------------- 범위 수집 -----------------------

def fetch_openg_cnstwk_range(openg_bgn: str, openg_end: str, per_page: int) -> Dict[Tuple[str,int], Dict]:
    page = 1
    out: Dict[Tuple[str,int], Dict] = {}
    while True:
        data = http_get(SCSBID_BASE, OP_OPENGLIST_CNSTWK, {
            "inqryDiv": 2,
            "inqryBgnDt": f"{openg_bgn}0000",
            "inqryEndDt": f"{openg_end}2359",
            "pageNo": page,
            "numOfRows": per_page,
        })
        rows = extract_items(data)
        log_dbg(f"OPENGLIST page={page} rows={len(rows)}")
        if not rows:
            break
        for it in rows:
            bid_no = (it.get("bidNtceNo") or "").strip()
            ord_i  = to_ord_int(it.get("bidNtceOrd"))
            if not bid_no:
                continue
            rec = {
                "bid_no": bid_no,
                "ord": ord_i,
                "bid_name": (it.get("bidNtceNm") or "").strip(),
                "rlOpengDt": (it.get("opengDt") or it.get("rlOpengDt") or None),
            }
            out[(bid_no, ord_i)] = rec
        if len(rows) < per_page:
            break
        page += 1
        time.sleep(DEFAULT_TPS_SLEEP)
    return out


def fetch_bidpublic_cnstwk_pps(openg_bgn: str, openg_end: str, per_page: int) -> Dict[Tuple[str,int], Dict]:
    page = 1
    out: Dict[Tuple[str,int], Dict] = {}
    while True:
        data = http_get(BID_PUBLIC_BASE, OP_PPS_CNSTWK, {
            "inqryDiv": 2,
            "inqryBgnDt": f"{openg_bgn}0000",
            "inqryEndDt": f"{openg_end}2359",
            "pageNo": page,
            "numOfRows": per_page,
        })
        rows = extract_items(data)
        log_dbg(f"PPS_CNSTWK page={page} rows={len(rows)}")
        if not rows:
            break
        for it in rows:
            bid_no = (it.get("bidNtceNo") or "").strip()
            ord_i  = to_ord_int(it.get("bidNtceOrd"))
            if not bid_no:
                continue
            sub_list: List[str] = []
            for k, v in it.items():
                if isinstance(k, str) and k.startswith("subsiCnsttyNm"):
                    val = (v or "").strip()
                    if val:
                        sub_list.append(val)
            out[(bid_no, ord_i)] = {
                "bid_no": bid_no,
                "ord": ord_i,
                "bid_name": (it.get("bidNtceNm") or "").strip(),
                "presmptPrce": to_int_safe(it.get("presmptPrce")),
                "mainCnsttyNm": (it.get("mainCnsttyNm") or "").strip(),
                "subsiCnsttyNm_list": sub_list,
                "cntrctCnclsMthdNm": (it.get("cntrctCnclsMthdNm") or "").strip(),
                "lower_rate_pct": to_float_safe(it.get("sucsfbidLwltRate")),
            }
        if len(rows) < per_page:
            break
        page += 1
        time.sleep(DEFAULT_TPS_SLEEP)
    return out


def fetch_license_limit_range(reg_bgn: str, reg_end: str, per_page: int) -> Dict[Tuple[str,int], Dict[str, List[str]]]:
    page = 1
    out: Dict[Tuple[str,int], Dict[str, List[str]]] = {}
    while True:
        data = http_get(BID_PUBLIC_BASE, OP_LICENSE_LIMIT, {
            "inqryDiv": 1,
            "inqryBgnDt": f"{reg_bgn}0000",
            "inqryEndDt": f"{reg_end}2359",
            "pageNo": page,
            "numOfRows": per_page,
        })
        rows = extract_items(data)
        log_dbg(f"LICENSE_RANGE page={page} rows={len(rows)}")
        if not rows:
            break
        for it in rows:
            bid_no = (it.get("bidNtceNo") or "").strip()
            ord_i  = to_ord_int(it.get("bidNtceOrd"))
            if not bid_no:
                continue
            perms: List[str] = []
            mfrc : List[str] = []
            for k, v in it.items():
                if not isinstance(v, str):
                    continue
                key = str(k).lower()
                val = v.strip()
                if not val:
                    continue
                if "permsn" in key:
                    perms.append(val)
                if "mfrc" in key or "indstrytymfrcfld" in key:
                    mfrc.append(val)
            if (bid_no, ord_i) in out:
                out[(bid_no, ord_i)]["perms"].extend(perms)
                out[(bid_no, ord_i)]["mfrc"].extend(mfrc)
            else:
                out[(bid_no, ord_i)] = {"perms": perms[:], "mfrc": mfrc[:]}
        if len(rows) < per_page:
            break
        page += 1
        time.sleep(DEFAULT_TPS_SLEEP)
    for k, v in out.items():
        v["perms"] = sorted(set(v.get("perms") or []))
        v["mfrc"]  = sorted(set(v.get("mfrc")  or []))
    return out


def fetch_bsis_amount_range(reg_bgn: str, reg_end: str, per_page: int) -> Dict[Tuple[str,int], Dict]:
    page = 1
    out: Dict[Tuple[str,int], Dict] = {}
    while True:
        data = http_get(BID_PUBLIC_BASE, OP_BSIS_CNSTWK, {
            "inqryDiv": 1,
            "inqryBgnDt": f"{reg_bgn}0000",
            "inqryEndDt": f"{reg_end}2359",
            "pageNo": page,
            "numOfRows": per_page,
        })
        rows = extract_items(data)
        log_dbg(f"BSIS_RANGE page={page} rows={len(rows)}")
        if not rows:
            break
        for it in rows:
            bid_no = (it.get("bidNtceNo") or "").strip()
            ord_i  = to_ord_int(it.get("bidNtceOrd"))
            if not bid_no:
                continue
            base_amount   = to_int_safe(it.get("bssamt"))
            rng_low_pct  = to_float_safe(it.get("rsrvtnPrceRngBgnRate"))
            rng_high_pct = to_float_safe(it.get("rsrvtnPrceRngEndRate"))
            opened_dt    = to_dt_from_compact(it.get("bssamtOpenDt"))
            out[(bid_no, ord_i)] = {
                "base_amount": base_amount,
                "range_low_pct": rng_low_pct,
                "range_high_pct": rng_high_pct,
                "base_opened_at": opened_dt,
            }
        if len(rows) < per_page:
            break
        page += 1
        time.sleep(DEFAULT_TPS_SLEEP)
    return out


def fetch_etc_list_range(reg_bgn: str, reg_end: str, per_page: int) -> Dict[Tuple[str,int], Dict]:
    """
    기타공고(연간단가/수의 등)가 공사목록에 안 잡히는 케이스 보완용(등록일 범위).
    기본 메타(bid_name 등)만 보강.
    """
    page = 1
    out: Dict[Tuple[str,int], Dict] = {}
    while True:
        data = http_get(BID_PUBLIC_BASE, OP_ETC_LIST, {
            "inqryDiv": 1,
            "inqryBgnDt": f"{reg_bgn}0000",
            "inqryEndDt": f"{reg_end}2359",
            "pageNo": page,
            "numOfRows": per_page,
        })
        rows = extract_items(data)
        log_dbg(f"ETC_RANGE page={page} rows={len(rows)}")
        if not rows:
            break
        for it in rows:
            bid_no = (it.get("bidNtceNo") or "").strip()
            ord_i  = to_ord_int(it.get("bidNtceOrd"))
            if not bid_no:
                continue
            out[(bid_no, ord_i)] = {
                "bid_name": (it.get("bidNtceNm") or "").strip() or None
            }
        if len(rows) < per_page:
            break
        page += 1
        time.sleep(DEFAULT_TPS_SLEEP)
    return out


def fetch_cnstwk_range(reg_bgn: str, reg_end: str, per_page: int) -> Dict[Tuple[str,int], Dict]:
    """공사목록 등록일시 범위(inqryDiv=1) — main/sub + indstrytyMfrcFldEvlYn"""
    page = 1
    out: Dict[Tuple[str,int], Dict] = {}
    while True:
        j = http_get(BID_PUBLIC_BASE, OP_CNSTWK_LIST, {
            "inqryDiv": 1,
            "inqryBgnDt": f"{reg_bgn}0000",
            "inqryEndDt": f"{reg_end}2359",
            "pageNo": page,
            "numOfRows": per_page,
        })
        rows = extract_items(j)
        log_dbg(f"CNST_RANGE page={page} rows={len(rows)}")
        if not rows:
            break
        for it in rows:
            bid_no = (it.get("bidNtceNo") or "").strip()
            ord_i  = to_ord_int(it.get("bidNtceOrd"))
            if not bid_no:
                continue
            subs: List[str] = []
            for k, v in it.items():
                if isinstance(k, str) and k.startswith("subsiCnsttyNm"):
                    val = (v or "").strip()
                    if val:
                        subs.append(val)
            out[(bid_no, ord_i)] = {
                "mainCnsttyNm": (it.get("mainCnsttyNm") or "").strip() or None,
                "subsiCnsttyNm_list": subs,
                "evalYn": (it.get("indstrytyMfrcFldEvlYn") or "").strip() or None,
                "bid_name": (it.get("bidNtceNm") or "").strip() or None,
            }
        if len(rows) < per_page:
            break
        page += 1
        time.sleep(DEFAULT_TPS_SLEEP)
    return out


def fetch_eval_mfrc_range(reg_bgn: str, reg_end: str, per_page: int) -> Dict[Tuple[str,int], Dict]:
    """평가정보 등록일시 범위(inqryDiv=1) — 주공종(main) + 주력분야 목록(subs)"""
    page = 1
    out: Dict[Tuple[str,int], Dict] = {}
    while True:
        j = http_get(BID_PUBLIC_BASE, OP_EVAL_MFRC, {
            "inqryDiv": 1,
            "inqryBgnDt": f"{reg_bgn}0000",
            "inqryEndDt": f"{reg_end}2359",
            "pageNo": page,
            "numOfRows": per_page,
        })
        rows = extract_items(j)
        log_dbg(f"EVAL_RANGE page={page} rows={len(rows)}")
        if not rows:
            break
        for it in rows:
            bid_no = (it.get("bidNtceNo") or "").strip()
            ord_i  = to_ord_int(it.get("bidNtceOrd"))
            if not bid_no:
                continue
            cnstty_ty = (it.get("cnsttyTyNm") or "").strip()
            tmpNm     = (it.get("tmpNm") or "").strip()
            mfrcNm    = (it.get("indstrytyMfrcFldNm") or "").strip()
            rec = out.get((bid_no, ord_i))
            if rec is None:
                rec = {"main": None, "subs": []}
                out[(bid_no, ord_i)] = rec
            if cnstty_ty == "주공종" and tmpNm and rec.get("main") is None:
                rec["main"] = tmpNm
            if mfrcNm:
                rec["subs"].append(mfrcNm)
        if len(rows) < per_page:
            break
        page += 1
        time.sleep(DEFAULT_TPS_SLEEP)
    for k, v in out.items():
        v["subs"] = sorted(set(v.get("subs") or []))
    return out


def fetch_std_bid_range(bid_bgn: str, bid_end: str, per_page: int) -> Dict[Tuple[str,int], Dict]:
    """표준데이터셋(보조 채널): 업종제한 여부/가능업종명/추정가격 등 — 공고일시 범위.
    주력분야 상세는 없으므로 면허요약·키워드 판정 보조용으로만 사용.
    """
    page = 1
    out: Dict[Tuple[str,int], Dict] = {}
    while True:
        j = http_get(STD_BASE, OP_STD_BID, {
            # 문서 기준: bidNtceBgnDt/bidNtceEndDt (YYYYMMDDhhmm)
            "bidNtceBgnDt": f"{bid_bgn}0000",
            "bidNtceEndDt": f"{bid_end}2359",
            "pageNo": page,
            "numOfRows": per_page,
        })
        rows = extract_items(j)
        log_dbg(f"STD_BID page={page} rows={len(rows)}")
        if not rows:
            break
        for it in rows:
            bid_no = (it.get("bidNtceNo") or "").strip()
            ord_i  = to_ord_int(it.get("bidNtceOrd"))
            if not bid_no:
                continue
            out[(bid_no, ord_i)] = {
                "indstrytyLmtYn": (it.get("indstrytyLmtYn") or "").strip(),
                "bidprcPsblIndstrytyNm": (it.get("bidprcPsblIndstrytyNm") or "").strip(),
                "presmptPrce": to_int_safe(it.get("presmptPrce")),
                "bid_name": (it.get("bidNtceNm") or "").strip() or None,
            }
        if len(rows) < per_page:
            break
        page += 1
        time.sleep(DEFAULT_TPS_SLEEP)
    return out

# ----------------------- 단건 수집 -----------------------

def fetch_cnstwk_list_single(bid_no: str, ord_i: Optional[int]=None) -> Optional[Dict]:
    j = http_get(BID_PUBLIC_BASE, OP_CNSTWK_LIST, {
        "inqryDiv": 2,
        "bidNtceNo": bid_no,
        "bidNtceOrd": ord_i if ord_i is not None else "",
        "pageNo": 1,
        "numOfRows": 50,
    })
    rows = extract_items(j)
    if not rows:
        return None
    rec = None
    if ord_i is not None:
        for it in rows:
            if to_ord_int(it.get("bidNtceOrd")) == ord_i:
                rec = it
                break
    if rec is None:
        rec = rows[0]
    subs: List[str] = []
    for k, v in rec.items():
        if isinstance(k, str) and k.startswith("subsiCnsttyNm"):
            val = (v or "").strip()
            if val:
                subs.append(val)
    return {
        "mainCnsttyNm": (rec.get("mainCnsttyNm") or "").strip() or None,
        "subsiCnsttyNm_list": subs
    }


def fetch_eval_mfrc_single(bid_no: str, ord_i: Optional[int]=None) -> Optional[Dict]:
    j = http_get(BID_PUBLIC_BASE, OP_EVAL_MFRC, {
        "inqryDiv": 2,
        "bidNtceNo": bid_no,
        "bidNtceOrd": ord_i if ord_i is not None else "",
        "pageNo": 1,
        "numOfRows": 100,
    })
    rows = extract_items(j)
    if not rows:
        return None
    main_nm = None
    subs: List[str] = []
    for it in rows:
        if ord_i is not None and to_ord_int(it.get("bidNtceOrd")) != ord_i:
            continue
        cnstty_ty = (it.get("cnsttyTyNm") or "").strip()
        tmpNm     = (it.get("tmpNm") or "").strip()
        mfrcNm    = (it.get("indstrytyMfrcFldNm") or "").strip()
        if cnstty_ty == "주공종" and tmpNm and not main_nm:
            main_nm = tmpNm
        if mfrcNm:
            subs.append(mfrcNm)
    subs = sorted(set(subs))
    if not (main_nm or subs):
        return None
    return {"main": main_nm, "subs": subs}


def fetch_license_limit_single(bid_no: str, ord_i: Optional[int]=None) -> Optional[Dict]:
    j = http_get(BID_PUBLIC_BASE, OP_LICENSE_LIMIT, {
        "inqryDiv": 2,
        "bidNtceNo": bid_no,
        "bidNtceOrd": ord_i if ord_i is not None else "",
        "pageNo": 1,
        "numOfRows": 100,
    })
    rows = extract_items(j)
    if not rows:
        return None
    perms: List[str] = []
    mfrc : List[str] = []
    for it in rows:
        if ord_i is not None and to_ord_int(it.get("bidNtceOrd")) != ord_i:
            continue
        for k, v in it.items():
            if not isinstance(v, str):
                continue
            key = str(k).lower()
            val = v.strip()
            if not val:
                continue
            if "permsn" in key:
                perms.append(val)
            if "mfrc" in key or "indstrytymfrcfld" in key:
                mfrc.append(val)
    perms = sorted(set(perms))
    mfrc  = sorted(set(mfrc))
    if not (perms or mfrc):
        return None
    return {"perms": perms, "mfrc": mfrc}


def fetch_prep15_single(bid_no: str, ord_opt: Optional[int] = None) -> Optional[Dict]:
    j = http_get(SCSBID_BASE, OP_PREP15, {
        "inqryDiv": 2,
        "bidNtceNo": bid_no,
        "bidNtceOrd": ord_opt if ord_opt is not None else "",
        "pageNo": 1,
        "numOfRows": 50,
    })
    rows = extract_items(j)
    if not rows:
        return None
    if ord_opt is not None:
        for it in rows:
            if to_ord_int(it.get("bidNtceOrd")) == ord_opt:
                return {
                    "rlOpengDt": (it.get("rlOpengDt") or None),
                    "plnprc": to_int_safe(it.get("plnprc")),
                    "bssamt": to_int_safe(it.get("bssamt")),
                }
    it = rows[0]
    return {
        "rlOpengDt": (it.get("rlOpengDt") or None),
        "plnprc": to_int_safe(it.get("plnprc")),
        "bssamt": to_int_safe(it.get("bssamt")),
    }

# ----------------------- DB -----------------------

def get_engine() -> Engine:
    return create_engine(PG_DSN, future=True, pool_pre_ping=True)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS t_notice(
  bid_no              TEXT NOT NULL,
  ord                 INTEGER NOT NULL DEFAULT 0,
  section_date        DATE,
  bid_name            TEXT,
  presmpt_prce        BIGINT,
  main_cnstty_nm      TEXT,
  subsi_cnstty_list   TEXT,     -- '|' 조인
  cntrct_mthd         TEXT,
  lower_rate_pct      NUMERIC(10,5),

  base_amount         BIGINT,
  range_low_pct       NUMERIC(10,5),
  range_high_pct      NUMERIC(10,5),
  base_opened_at      TIMESTAMP,

  updated_at          TIMESTAMP DEFAULT now(),
  PRIMARY KEY(bid_no, ord)
);

CREATE INDEX IF NOT EXISTS idx_notice_section ON t_notice(section_date);

ALTER TABLE t_notice ADD COLUMN IF NOT EXISTS base_amount    BIGINT;
ALTER TABLE t_notice ADD COLUMN IF NOT EXISTS range_low_pct  NUMERIC(10,5);
ALTER TABLE t_notice ADD COLUMN IF NOT EXISTS range_high_pct NUMERIC(10,5);
ALTER TABLE t_notice ADD COLUMN IF NOT EXISTS base_opened_at TIMESTAMP;

CREATE TABLE IF NOT EXISTS t_license(
  bid_no   TEXT NOT NULL,
  ord      INTEGER NOT NULL DEFAULT 0,
  perms    TEXT,
  mfrc     TEXT,
  updated_at TIMESTAMP DEFAULT now(),
  PRIMARY KEY(bid_no, ord)
);

CREATE TABLE IF NOT EXISTS t_prep15(
  bid_no        TEXT NOT NULL,
  ord           INTEGER NOT NULL DEFAULT 0,
  rl_openg_dt   TIMESTAMP,
  plnprc        BIGINT,
  bssamt        BIGINT,
  updated_at    TIMESTAMP DEFAULT now(),
  PRIMARY KEY(bid_no, ord)
);

CREATE TABLE IF NOT EXISTS t_floor(
  bid_no              TEXT NOT NULL,
  ord                 INTEGER NOT NULL DEFAULT 0,
  section_date        DATE,
  presmpt_prce        BIGINT,
  expected_plnprc     BIGINT,
  lower_rate_pct      NUMERIC(10,5),
  bid_floor_10won     BIGINT,
  updated_at          TIMESTAMP DEFAULT now(),
  PRIMARY KEY(bid_no, ord)
);
"""

UPSERT_NOTICE_SQL = text("""
INSERT INTO t_notice
(bid_no, ord, section_date, bid_name, presmpt_prce, main_cnstty_nm, subsi_cnstty_list, cntrct_mthd, lower_rate_pct,
 base_amount, range_low_pct, range_high_pct, base_opened_at, updated_at)
VALUES (:bid_no, :ord, :section_date, :bid_name, :presmpt_prce, :main_cnstty_nm, :subsi_cnstty_list, :cntrct_mthd, :lower_rate_pct,
        :base_amount, :range_low_pct, :range_high_pct, :base_opened_at, now())
ON CONFLICT (bid_no, ord) DO UPDATE SET
  section_date      = COALESCE(t_notice.section_date, EXCLUDED.section_date),
  bid_name          = COALESCE(t_notice.bid_name, EXCLUDED.bid_name),
  presmpt_prce      = COALESCE(t_notice.presmpt_prce, EXCLUDED.presmpt_prce),
  main_cnstty_nm    = COALESCE(NULLIF(t_notice.main_cnstty_nm,''), NULLIF(EXCLUDED.main_cnstty_nm,'')),
  subsi_cnstty_list = COALESCE(NULLIF(t_notice.subsi_cnstty_list,''), NULLIF(EXCLUDED.subsi_cnstty_list,'')),
  cntrct_mthd       = COALESCE(t_notice.cntrct_mthd, EXCLUDED.cntrct_mthd),
  lower_rate_pct    = COALESCE(t_notice.lower_rate_pct, EXCLUDED.lower_rate_pct),

  base_amount       = COALESCE(t_notice.base_amount, EXCLUDED.base_amount),
  range_low_pct     = COALESCE(t_notice.range_low_pct, EXCLUDED.range_low_pct),
  range_high_pct    = COALESCE(t_notice.range_high_pct, EXCLUDED.range_high_pct),
  base_opened_at    = COALESCE(t_notice.base_opened_at, EXCLUDED.base_opened_at),

  updated_at        = now();
""")

UPSERT_LICENSE_SQL = text("""
INSERT INTO t_license
(bid_no, ord, perms, mfrc, updated_at)
VALUES (:bid_no, :ord, :perms, :mfrc, now())
ON CONFLICT (bid_no, ord) DO UPDATE SET
  perms      = COALESCE(NULLIF(t_license.perms,''), NULLIF(EXCLUDED.perms,'')),
  mfrc       = COALESCE(NULLIF(t_license.mfrc,''),  NULLIF(EXCLUDED.mfrc,'')),
  updated_at = now();
""")

UPSERT_PREP15_SQL = text("""
INSERT INTO t_prep15
(bid_no, ord, rl_openg_dt, plnprc, bssamt, updated_at)
VALUES (:bid_no, :ord, :rl_openg_dt, :plnprc, :bssamt, now())
ON CONFLICT (bid_no, ord) DO UPDATE SET
  rl_openg_dt = COALESCE(t_prep15.rl_openg_dt, EXCLUDED.rl_openg_dt),
  plnprc      = COALESCE(t_prep15.plnprc,      EXCLUDED.plnprc),
  bssamt      = COALESCE(t_prep15.bssamt,      EXCLUDED.bssamt),
  updated_at  = now();
""")

UPSERT_FLOOR_SQL = text("""
INSERT INTO t_floor
(bid_no, ord, section_date, presmpt_prce, expected_plnprc, lower_rate_pct, bid_floor_10won, updated_at)
VALUES (:bid_no, :ord, :section_date, :presmpt_prce, :expected_plnprc, :lower_rate_pct, :bid_floor_10won, now())
ON CONFLICT (bid_no, ord) DO UPDATE SET
  section_date    = COALESCE(t_floor.section_date,    EXCLUDED.section_date),
  presmpt_prce    = COALESCE(t_floor.presmpt_prce,    EXCLUDED.presmpt_prce),
  expected_plnprc = COALESCE(t_floor.expected_plnprc, EXCLUDED.expected_plnprc),
  lower_rate_pct  = COALESCE(t_floor.lower_rate_pct,  EXCLUDED.lower_rate_pct),
  bid_floor_10won = COALESCE(t_floor.bid_floor_10won, EXCLUDED.bid_floor_10won),
  updated_at      = now();
""")

def ensure_schema(engine: Engine):
    with engine.begin() as conn:
        conn.exec_driver_sql(SCHEMA_SQL)

def upsert_bulk(engine: Engine, sql: text, rows: List[Dict]):
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(sql, rows)

# ----------------------- 후보/키워드 -----------------------

def contains_keywords(main_nm: str, sub_list: List[str], bid_name: str = "") -> bool:
    hay = f"{main_nm} {' '.join(sub_list or [])} {bid_name or ''}"
    return any(kw in hay for kw in KEYWORDS)

def license_matches(limit: Dict[str, List[str]]) -> bool:
    hay = " ".join((limit.get("perms") or []) + (limit.get("mfrc") or []))
    if any(kw in hay for kw in KEYWORDS):
        return True
    if any(code in hay for code in CODE_HINTS):
        return True
    return False

# ----------------------- 메인 -----------------------

def main(start_date: Optional[str], end_date: Optional[str],
         days: int, include_today: bool, per_page: int,
         debug: bool, max_presmpt_price: int, license_lookback_days: int,
         single_backfill_cap: int, use_std_dataset: bool):
    global DEBUG
    DEBUG = debug

    now = datetime.now()
    if start_date and not end_date:
        end_date = start_date
    if not start_date and not end_date:
        stop = 0 if include_today else 1
        start_date = (now - timedelta(days=days)).strftime("%Y%m%d")
        end_date   = (now - timedelta(days=stop)).strftime("%Y%m%d")

    log_info(f"실행 범위(개찰일): {start_date} ~ {end_date} (per_page={per_page}, debug={DEBUG})")

    # 등록일 룩백 (면허/기초금액/기타공고/범위 CNST/EVAL)
    dt_bgn = datetime.strptime(start_date, "%Y%m%d")
    dt_end = datetime.strptime(end_date, "%Y%m%d")
    reg_bgn = (dt_bgn - timedelta(days=max(0, license_lookback_days))).strftime("%Y%m%d")
    reg_end = dt_end.strftime("%Y%m%d")
    log_info(f"등록일 보완 범위: {reg_bgn} ~ {reg_end} (lookback={license_lookback_days}일)")
    if max_presmpt_price == 0:
        log_info("추정가격 상한: 제한없음")
    else:
        log_info(f"추정가격 상한: {max_presmpt_price:,}원(부가세 제외)")

    engine = get_engine()
    ensure_schema(engine)

    # ---------- pass-1: anchor + 범위형 수집 ----------
    og_map   = fetch_openg_cnstwk_range(start_date, end_date, per_page)
    pp_map   = fetch_bidpublic_cnstwk_pps(start_date, end_date, per_page)
    lic_map  = fetch_license_limit_range(reg_bgn, reg_end, per_page)
    bsis_map = fetch_bsis_amount_range(reg_bgn, reg_end, per_page)
    etc_map  = fetch_etc_list_range(reg_bgn, reg_end, per_page)
    
    # 신규 범위형
    cnst_range_map = fetch_cnstwk_range(reg_bgn, reg_end, per_page)
    eval_range_map = fetch_eval_mfrc_range(reg_bgn, reg_end, per_page)
    std_map: Dict[Tuple[str,int], Dict] = {}
    if use_std_dataset:
        std_map = fetch_std_bid_range(reg_bgn, reg_end, per_page)

    log_info(
        "수집 통계: "
        f"개찰={len(og_map)}, PPSSrch={len(pp_map)}, 면허범위={len(lic_map)}, "
        f"기초금액={len(bsis_map)}, 기타공고={len(etc_map)}, CNST범위={len(cnst_range_map)}, "
        f"EVAL범위={len(eval_range_map)}, 표준데이터셋={len(std_map)}"
    )

    # ---------- pass-1 upsert (범위형 먼저 채우기) ----------
    filled_main = 0
    filled_sub  = 0

    notice_rows: List[Dict] = []
    license_rows: List[Dict] = []

    for key, og in og_map.items():
        bid_no, ord_i = key
        pb = pp_map.get(key, {})
        cn_rng = cnst_range_map.get(key, {})
        ev_rng = eval_range_map.get(key, {})
        std    = std_map.get(key, {})
        bsis   = bsis_map.get(key, {})
        etc    = etc_map.get(key, {})

        sec_str = parse_yyyymmdd(og.get("rlOpengDt"))
        if sec_str is None:
            sec_str = end_date
        sec_date = to_date_from_yyyymmdd(sec_str)

        # main/sub 우선순위: PPSSrch → CNST범위 → EVAL범위
        main_nm = (pb.get("mainCnsttyNm") or "").strip()
        sub_list = list(pb.get("subsiCnsttyNm_list", []))
        if not main_nm and cn_rng:
            cand = (cn_rng.get("mainCnsttyNm") or "").strip()
            if cand:
                main_nm = cand
        if not sub_list and cn_rng:
            sub_list = cn_rng.get("subsiCnsttyNm_list", []) or []
        if (not main_nm) and ev_rng:
            main_cand = (ev_rng.get("main") or "").strip()
            if main_cand:
                main_nm = main_cand
        if not sub_list and ev_rng:
            sub_list = ev_rng.get("subs", []) or []

        if main_nm:
            filled_main += 1
        if sub_list:
            filled_sub += 1

        # presmpt_prce: PPSSrch → 표준데이터셋
        pres_val = to_int_safe(pb.get("presmptPrce"))
        if pres_val is None and std:
            pres_val = to_int_safe(std.get("presmptPrce"))

        # bid_name: PPSSrch → CNST범위 → 기타공고 → anchor
        bid_name = None
        v = pb.get("bid_name") if pb else None
        if v:
            bid_name = v
        elif cn_rng and cn_rng.get("bid_name"):
            bid_name = cn_rng.get("bid_name")
        elif etc and etc.get("bid_name"):
            bid_name = etc.get("bid_name")
        else:
            bid_name = og.get("bid_name")

        notice_rows.append({
            "bid_no": bid_no,
            "ord": ord_i,
            "section_date": sec_date,
            "bid_name": bid_name,
            "presmpt_prce": pres_val,
            "main_cnstty_nm": main_nm or None,
            "subsi_cnstty_list": "|".join(sub_list) if sub_list else None,
            "cntrct_mthd": (pb.get("cntrctCnclsMthdNm") or None) if pb else None,
            "lower_rate_pct": to_float_safe(pb.get("lower_rate_pct")) if pb else None,
            "base_amount": to_int_safe(bsis.get("base_amount")),
            "range_low_pct": to_float_safe(bsis.get("range_low_pct")),
            "range_high_pct": to_float_safe(bsis.get("range_high_pct")),
            "base_opened_at": bsis.get("base_opened_at"),
        })

        # 면허/주력분야 텍스트: LICENSE 범위 → 표준데이터셋 요약
        if key in lic_map:
            perms = "|".join(lic_map[key].get("perms") or [])
            mfrc  = "|".join(lic_map[key].get("mfrc")  or [])
            license_rows.append({
                "bid_no": bid_no,
                "ord": ord_i,
                "perms": perms if perms else None,
                "mfrc":  mfrc  if mfrc  else None,
            })
        else:
            std_txt = None
            if std and std.get("bidprcPsblIndstrytyNm"):
                std_txt = std.get("bidprcPsblIndstrytyNm")
            if std_txt:
                license_rows.append({
                    "bid_no": bid_no,
                    "ord": ord_i,
                    "perms": None,
                    "mfrc": std_txt,
                })

    upsert_bulk(engine, UPSERT_NOTICE_SQL, notice_rows)
    upsert_bulk(engine, UPSERT_LICENSE_SQL, license_rows)

    log_info(f"pass-1 upsert: t_notice +{len(notice_rows)}, t_license +{len(license_rows)}")
    log_info(f"범위형으로 즉시 채움 → 주공종 {filled_main}건, 부공종 {filled_sub}건")

    # ---------- pass-2: 선택적 단건 보강 (EVAL/CNST/LIMIT 단건) ----------
    # NULL 대상 선별: PPSSrch/범위형/표준데이터셋으로도 안 채워진 항목만
    # EVAL 단건은 CNST범위 evalYn=='N'이면 스킵
    need_single_keys: List[Tuple[str,int]] = []
    eval_skip = 0
    for key, og in og_map.items():
        pb   = pp_map.get(key)
        cn   = cnst_range_map.get(key)
        ev   = eval_range_map.get(key)
        lic  = lic_map.get(key)
        std  = std_map.get(key)

        main_nm = (pb.get("mainCnsttyNm") or "") if pb else ""
        subs    = (pb.get("subsiCnsttyNm_list") or []) if pb else []

        if (not main_nm) and cn and cn.get("mainCnsttyNm"):
            main_nm = cn.get("mainCnsttyNm") or ""
        if (not subs) and cn and cn.get("subsiCnsttyNm_list"):
            subs = cn.get("subsiCnsttyNm_list") or []
        if (not main_nm) and ev and ev.get("main"):
            main_nm = ev.get("main") or ""
        if (not subs) and ev and ev.get("subs"):
            subs = ev.get("subs") or []

        needs_main_sub = not (main_nm or subs)

        # 면허 단건 필요 여부 (범위/표준데이터셋에서 이미 있으면 스킵)
        needs_license  = False
        if lic is None:
            if std is None:
                needs_license = True
            else:
                has_std = False
                if std.get("bidprcPsblIndstrytyNm"):
                    has_std = True
                if std.get("indstrytyLmtYn"):
                    has_std = True
                needs_license = not has_std

        # EVAL 스킵 규칙
        eval_eligible = True
        if cn and (cn.get("evalYn") or "").upper() == 'N':
            eval_eligible = False
            if needs_main_sub:
                eval_skip += 1

        if needs_main_sub or needs_license:
            need_single_keys.append(key)

    # 중복 제거
    uniq: List[Tuple[str,int]] = []
    seen = set()
    for k in need_single_keys:
        if k not in seen:
            uniq.append(k)
            seen.add(k)

    # cap 적용(총량 cap)
    if single_backfill_cap > 0 and len(uniq) > single_backfill_cap:
        uniq = uniq[:single_backfill_cap]
    log_info(f"단건 보강 후보(총): {len(uniq)}건 (cap={single_backfill_cap}, EVAL스킵={eval_skip})")

    fix_rows_notice: List[Dict] = []
    fix_rows_license: List[Dict] = []
    cnt_eval_ok = cnt_cnst_ok = cnt_lic_ok = 0

    calls_used = 0
    for bid_no, ord_i in uniq:
        if single_backfill_cap > 0 and calls_used >= single_backfill_cap:
            break

        # 현재 상태 재확인
        pb = pp_map.get((bid_no, ord_i))
        cn = cnst_range_map.get((bid_no, ord_i))
        ev = eval_range_map.get((bid_no, ord_i))
        lic = lic_map.get((bid_no, ord_i))
        std = std_map.get((bid_no, ord_i))

        main_nm = (pb.get("mainCnsttyNm") or "") if pb else ""
        subs    = (pb.get("subsiCnsttyNm_list") or []) if pb else []
        if (not main_nm) and cn and cn.get("mainCnsttyNm"):
            main_nm = cn.get("mainCnsttyNm") or ""
        if (not subs) and cn and cn.get("subsiCnsttyNm_list"):
            subs = cn.get("subsiCnsttyNm_list") or []
        if (not main_nm) and ev and ev.get("main"):
            main_nm = ev.get("main") or ""
        if (not subs) and ev and ev.get("subs"):
            subs = ev.get("subs") or []

        needs_main_sub = not (main_nm or subs)

        # 우선순위: (evalYn=='Y' 또는 미지정) → EVAL 단건 → 그래도 없으면 CNSTWK 단건
        main_cand = None
        subs_cand: List[str] = []
        if needs_main_sub:
            eval_ok_to_call = True
            if cn and (cn.get("evalYn") or "").upper() == 'N':
                eval_ok_to_call = False
            if eval_ok_to_call:
                ev_single = fetch_eval_mfrc_single(bid_no, ord_i)
                calls_used += 1
                time.sleep(DEFAULT_TPS_SLEEP)
                if ev_single:
                    cnt_eval_ok += 1
                    m = ev_single.get("main")
                    if m:
                        main_cand = m
                    s = ev_single.get("subs") or []
                    if s:
                        subs_cand = s

            if (main_cand is None) and (not subs_cand):
                # CNSTWK 단건(보조)
                cn_single = fetch_cnstwk_list_single(bid_no, ord_i)
                calls_used += 1
                time.sleep(DEFAULT_TPS_SLEEP)
                if cn_single:
                    cnt_cnst_ok += 1
                    if cn_single.get("mainCnsttyNm"):
                        main_cand = cn_single.get("mainCnsttyNm")
                    lst = cn_single.get("subsiCnsttyNm_list") or []
                    if lst:
                        subs_cand = lst

            if (main_cand is not None) or (len(subs_cand) > 0):
                fix_rows_notice.append({
                    "bid_no": bid_no,
                    "ord": ord_i,
                    "section_date": None,
                    "bid_name": None,
                    "presmpt_prce": None,
                    "main_cnstty_nm": main_cand or None,
                    "subsi_cnstty_list": "|".join(sorted(set(subs_cand))) if subs_cand else None,
                    "cntrct_mthd": None,
                    "lower_rate_pct": None,
                    "base_amount": None,
                    "range_low_pct": None,
                    "range_high_pct": None,
                    "base_opened_at": None,
                })

        # 면허 단건(마지막 우선순위)
        need_license_single = False
        if lic is None:
            has_std = False
            if std and (std.get("bidprcPsblIndstrytyNm") or std.get("indstrytyLmtYn")):
                has_std = True
            need_license_single = not has_std
        if need_license_single:
            li = fetch_license_limit_single(bid_no, ord_i)
            calls_used += 1
            time.sleep(DEFAULT_TPS_SLEEP)
            if li:
                cnt_lic_ok += 1
                perms = "|".join(li.get("perms") or [])
                mfrc  = "|".join(li.get("mfrc")  or [])
                fix_rows_license.append({
                    "bid_no": bid_no, "ord": ord_i,
                    "perms": perms if perms else None,
                    "mfrc":  mfrc  if mfrc  else None,
                })

    upsert_bulk(engine, UPSERT_NOTICE_SQL, fix_rows_notice)
    upsert_bulk(engine, UPSERT_LICENSE_SQL, fix_rows_license)
    log_info(f"단건 보강 upsert: notice {len(fix_rows_notice)}건, license {len(fix_rows_license)}건")
    log_info(f"단건 보강 성과: 평가 {cnt_eval_ok}건, 공사목록 {cnt_cnst_ok}건, 면허제한 {cnt_lic_ok}건")

    # ---------- 후보 선별 ----------
    candidates: List[Tuple[str,int,Dict]] = []
    reason_price = 0
    reason_kw = 0

    for key, og in og_map.items():
        pb = pp_map.get(key, {})
        pres = to_int_safe(pb.get("presmptPrce"))
        if pres is None and key in std_map:
            pres = to_int_safe(std_map[key].get("presmptPrce"))
        if pres is None:
            reason_price += 1
            continue
        if max_presmpt_price > 0 and pres > max_presmpt_price:
            reason_price += 1
            continue

        # 키워드 판정: PPSSrch → CNST범위/EVAL범위 → License범위/표준
        main_nm = (pb.get("mainCnsttyNm") or "")
        subs    = pb.get("subsiCnsttyNm_list", []) or []
        bid_nm  = (pb.get("bid_name") or og.get("bid_name") or "")
        if not (main_nm or subs):
            cn = cnst_range_map.get(key)
            ev = eval_range_map.get(key)
            if cn and (cn.get("mainCnsttyNm") or cn.get("subsiCnsttyNm_list")):
                if not main_nm and cn.get("mainCnsttyNm"):
                    main_nm = cn.get("mainCnsttyNm")
                if not subs and cn.get("subsiCnsttyNm_list"):
                    subs = cn.get("subsiCnsttyNm_list")
            if (not main_nm) and ev and ev.get("main"):
                main_nm = ev.get("main")
            if (not subs) and ev and ev.get("subs"):
                subs = ev.get("subs")
        ok = contains_keywords(main_nm, subs, bid_nm)
        if not ok:
            li = lic_map.get(key)
            if li and license_matches(li):
                ok = True
            elif key in std_map:
                txt = std_map[key].get("bidprcPsblIndstrytyNm") or ""
                if any(kw in txt for kw in KEYWORDS):
                    ok = True
        if not ok:
            reason_kw += 1
            continue

        sec_str = parse_yyyymmdd(og.get("rlOpengDt"))
        if sec_str is None:
            sec_str = end_date
        sec_date = to_date_from_yyyymmdd(sec_str)
        candidates.append((key[0], key[1], {
            "section_date": sec_date,
            "pres": pres,
            "lr": to_float_safe(pb.get("lower_rate_pct")),
        }))

    log_info(f"후보 선별: {len(candidates)}건 (금액 탈락 {reason_price}, 키워드/면허 미적중 {reason_kw})")

    # ---------- 후보 PREP15 → t_prep15 / t_floor ----------
    prep_rows, floor_rows = [], []
    for bid_no, ord_i, meta in candidates:
        lr = meta["lr"]
        if lr is None:
            continue
        prep = fetch_prep15_single(bid_no=bid_no, ord_opt=ord_i)
        time.sleep(DEFAULT_TPS_SLEEP)
        if not prep or prep.get("plnprc") is None:
            continue
        expected = int(prep["plnprc"])
        floor_val = floor_10won(expected * (lr/100.0))
        prep_rows.append({
            "bid_no": bid_no,
            "ord": ord_i,
            "rl_openg_dt": prep.get("rlOpengDt"),
            "plnprc": expected,
            "bssamt": to_int_safe(prep.get("bssamt")),
        })
        floor_rows.append({
            "bid_no": bid_no,
            "ord": ord_i,
            "section_date": meta["section_date"],
            "presmpt_prce": meta["pres"],
            "expected_plnprc": expected,
            "lower_rate_pct": float(lr),
            "bid_floor_10won": int(floor_val),
        })

    upsert_bulk(engine, UPSERT_PREP15_SQL, prep_rows)
    upsert_bulk(engine, UPSERT_FLOOR_SQL,  floor_rows)
    log_info(f"t_prep15 upsert: +{len(prep_rows)} rows")
    log_info(f"t_floor  upsert: +{len(floor_rows)} rows")


# ----------------------- CLI -----------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="from_date", help="개찰일자 시작 YYYYMMDD")
    p.add_argument("--to", dest="to_date", help="개찰일자 종료 YYYYMMDD")
    p.add_argument("--date", dest="single_date", help="개찰일자 단일 YYYYMMDD")
    p.add_argument("--days", type=int, default=7, help="최근 N일(오늘 제외; --include-today로 포함)")
    p.add_argument("--include-today", action="store_true")
    p.add_argument("--per-page", type=int, default=PER_PAGE_DEFAULT)
    p.add_argument("--debug", action="store_true")
    p.add_argument("--max-presmpt-price", type=int, default=DEFAULT_MAX_PRES_PRICE,
                   help="추정가격 상한(원, VAT 제외). 0이면 제한 없음.")
    p.add_argument("--license-lookback-days", type=int, default=60,
                   help="등록일 기준 범위(면허/기초금액/기타공고/CNST/EVAL) 룩백 일수")
    p.add_argument("--single-backfill-cap", type=int, default=300,
                   help="NULL 대상 단건 보강 최대 호출 건수(0이면 비활성화)")
    p.add_argument("--use-std-dataset", action="store_true",
                   help="표준데이터셋(업종제한/가능업종명/추정가격) 보조 수집 사용")

    args = p.parse_args()

    if args.single_date:
        sd = args.single_date
        ed = args.single_date
    else:
        sd = args.from_date
        ed = args.to_date

    main(start_date=sd, end_date=ed, days=args.days,
         include_today=args.include_today, per_page=args.per_page,
         debug=args.debug, max_presmpt_price=args.max_presmpt_price,
         license_lookback_days=args.license_lookback_days,
         single_backfill_cap=args.single_backfill_cap,
         use_std_dataset=args.use_std_dataset)
