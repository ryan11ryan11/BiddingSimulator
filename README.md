# 로컬 실행 예시
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
psql $PG_DSN -f core/db/schema.sql
uvicorn apps.api.main:app --reload


# 1) 의존성 설치 & DB 스키마 적용
psql "$PG_DSN" -f core/db/schema.sql

# 2) 최근 24시간 데이터 적재(공사 예)
python -m apps.etl.flows e2e_collect_load

# 3) API 기동
uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 --reload

# 4) 추정 호출 (예)
curl "http://localhost:8000/estimate?bid_no=2025xxxxxxx&ord=1"


.\run.ps1

 venv 활성화
.\.venv\Scripts\Activate.ps1

################## 0 워터마크 테이블 만들기 (한 번만)

$compose = "C:\Users\ryan1\OneDrive\바탕 화면\낙찰\simulator\docker\docker-compose.yml"
$DB_CID = docker compose -f $compose ps -q db

$wm_sql = @"
CREATE TABLE IF NOT EXISTS t_etl_watermark (
  stream       TEXT     NOT NULL,   -- 예: 'notice:cnstwk', 'prep15:cnstwk'
  bucket       DATE     NOT NULL,   -- 일 단위 버킷(한국시간 기준)
  last_page    INTEGER  DEFAULT 0,  -- 마지막으로 성공 저장한 페이지
  total_pages  INTEGER,
  total_count  INTEGER,
  updated_at   TIMESTAMP DEFAULT now(),
  PRIMARY KEY (stream, bucket)
);
"@
$wm_file = Join-Path $PWD ".wm.sql"
$wm_sql | Set-Content -Encoding UTF8 $wm_file
docker cp $wm_file ${DB_CID}:/wm.sql
docker exec -it $DB_CID psql -U user -d eest -f /wm.sql


################## 1 DB 초기화 (원하면 테이블 비우기)
docker exec -it $DB_CID psql -U user -d eest -c "TRUNCATE t_prep15;"
docker exec -it $DB_CID psql -U user -d eest -c "TRUNCATE t_result;"
docker exec -it $DB_CID psql -U user -d eest -c "TRUNCATE t_notice;"
docker exec -it $DB_CID psql -U user -d eest -c "TRUNCATE t_etl_watermark;"

################## 2 “재시작 가능한 6개월 백필” 스크립트 생성

$code = @'
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import math, time
from typing import Tuple, Dict

from core.config import settings
from core.db.engine import engine
from sqlalchemy import text

from core.clients.bid_public_info import BidPublicInfo
from core.clients.scsbid_info import ScsbidInfo
from apps.etl.tasks import parse_notice_items, fetch_bsis_map_cnstwk, parse_prepar_detail_items
from core.db.dao import upsert_notice, upsert_prep15_bulk

bp = BidPublicInfo(settings.bid_public_base, settings.service_key)
sc = ScsbidInfo(settings.scsbid_base, settings.service_key)

SLEEP_S = 0.5       # 호출 한도 여유
PAGE_SIZE = 100

def upsert_watermark(stream: str, bucket: date, last_page: int=None, total_pages: int=None, total_count: int=None):
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
        conn.execute(sql, {
            "stream": stream, "bucket": bucket,
            "last_page": last_page, "total_pages": total_pages, "total_count": total_count
        })

def get_watermark(stream: str, bucket: date) -> Dict:
    sql = text("SELECT * FROM t_etl_watermark WHERE stream=:s AND bucket=:b")
    with engine.begin() as conn:
        row = conn.execute(sql, {"s": stream, "b": bucket}).mappings().first()
        return dict(row) if row else {}

def total_pages_for_notice(bgn: str, end: str) -> Tuple[int,int]:
    resp = bp.get("getBidPblancListInfoCnstwk", {
        "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end, "pageNo": 1, "numOfRows": 1
    })
    body = (resp or {}).get("response",{}).get("body",{})
    total = int(body.get("totalCount") or 0)
    pages = max(1, math.ceil(total / PAGE_SIZE))
    return total, pages

def total_pages_for_prep15(bgn: str, end: str) -> Tuple[int,int]:
    resp = sc.get_prepar_pc_detail_cnstwk(inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=1, num_rows=1)
    body = (resp or {}).get("response",{}).get("body",{})
    total = int(body.get("totalCount") or 0)
    pages = 0 if total==0 else max(1, math.ceil(total / PAGE_SIZE))
    return total, pages

def process_notice_bucket(bucket: date):
    stream = "notice:cnstwk"
    tz = ZoneInfo("Asia/Seoul")
    bgn = datetime(bucket.year, bucket.month, bucket.day, 0,0,tzinfo=tz).strftime("%Y%m%d%H%M")
    end = datetime(bucket.year, bucket.month, bucket.day, 23,59,tzinfo=tz).strftime("%Y%m%d%H%M")

    wm = get_watermark(stream, bucket)
    if not wm:
        total, pages = total_pages_for_notice(bgn, end)
        upsert_watermark(stream, bucket, last_page=0, total_pages=pages, total_count=total)
        wm = get_watermark(stream, bucket)
    total = wm.get("total_count") or 0
    pages = wm.get("total_pages") or 0
    last = wm.get("last_page") or 0
    if pages == 0 and total == 0:
        print(f"[NOTICE {bucket}] total=0 -> skip")
        return

    # 보강용 기초금액 맵
    bsis_map = fetch_bsis_map_cnstwk(bgn, end)

    for page in range(last+1, pages+1):
        # 재시도 루프(한도초과시 기다렸다가 동일 페이지 재시도)
        for attempt in range(6):
            try:
                resp = bp.get("getBidPblancListInfoCnstwk", {
                    "inqryDiv": 1, "inqryBgnDt": bgn, "inqryEndDt": end,
                    "pageNo": page, "numOfRows": PAGE_SIZE
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
                        upsert_notice(r); batch += 1
                upsert_watermark(stream, bucket, last_page=page)
                pct = 100.0 * page / pages if pages else 100.0
                print(f"[NOTICE {bucket}] page {page}/{pages} +{batch} saved ~ ({pct:.1f}% done)")
                time.sleep(SLEEP_S)
                break
            except Exception as e:
                msg = str(e)
                wait = min(60, 2**attempt * 2)
                print(f"[NOTICE {bucket}] page {page} ERROR: {msg[:120]} ... retry in {wait}s")
                time.sleep(wait)
        else:
            print(f"[NOTICE {bucket}] page {page} hard-fail -> continue next page")

def process_prep15_bucket(bucket: date):
    stream = "prep15:cnstwk"
    tz = ZoneInfo("Asia/Seoul")
    bgn = datetime(bucket.year, bucket.month, bucket.day, 0,0,tzinfo=tz).strftime("%Y%m%d%H%M")
    end = datetime(bucket.year, bucket.month, bucket.day, 23,59,tzinfo=tz).strftime("%Y%m%d%H%M")

    wm = get_watermark(stream, bucket)
    if not wm:
        total, pages = total_pages_for_prep15(bgn, end)
        upsert_watermark(stream, bucket, last_page=0, total_pages=pages, total_count=total)
        wm = get_watermark(stream, bucket)
    total = wm.get("total_count") or 0
    pages = wm.get("total_pages") or 0
    last = wm.get("last_page") or 0
    if pages == 0 and total == 0:
        print(f"[PREP15 {bucket}] total=0 -> skip")
        return

    for page in range(last+1, pages+1):
        for attempt in range(6):
            try:
                resp = sc.get_prepar_pc_detail_cnstwk(
                    inqry_div=1, inqry_bgn_dt=bgn, inqry_end_dt=end, page_no=page, num_rows=PAGE_SIZE
                )
                m = parse_prepar_detail_items(resp)
                batch = 0
                if m:
                    for _, rows in m.items():
                        upsert_prep15_bulk(rows)
                        batch += len(rows)
                upsert_watermark(stream, bucket, last_page=page)
                pct = 100.0 * page / pages if pages else 100.0
                print(f"[PREP15 {bucket}] page {page}/{pages} +{batch} saved ~ ({pct:.1f}% done)")
                time.sleep(SLEEP_S)
                break
            except Exception as e:
                msg = str(e)
                wait = min(60, 2**attempt * 2)
                print(f"[PREP15 {bucket}] page {page} ERROR: {msg[:120]} ... retry in {wait}s")
                time.sleep(wait)
        else:
            print(f"[PREP15 {bucket}] page {page} hard-fail -> continue next page")

def backfill_days(days: int = 180):
    tz = ZoneInfo("Asia/Seoul")
    today_kst = datetime.now(tz).date()
    start = today_kst - timedelta(days=days)
    buckets = [start + timedelta(days=i) for i in range(1, days+1)]  # 과거→최근 순서

    for b in buckets:
        print("="*72)
        print(f"BUCKET {b.isoformat()}  (KST day)")
        # 공고 → prep15 순서 (원하면 바꿀 수 있음)
        process_notice_bucket(b)
        process_prep15_bucket(b)

if __name__ == "__main__":
    backfill_days(180)   # 6개월
'@

$path = Join-Path $PWD ".backfill_resume_12m.py"
$code | Set-Content -Encoding UTF8 $path


################## 3 실행 & 재실행


venv 활성화 상태에서
python .backfill_resume_12m.py
중복 저장 걱정 없음: t_notice/t_prep15는 PK+UPSERT라 이미 있던 키는 덮어쓰기만 함.

워터마크(t_etl_watermark) 덕분에 이미 끝난 날짜 버킷은 자동으로 스킵돼서 빠르게 지나감.

단점: 최근 180일도 “휙” 다시 훑긴 함(금방 스킵되긴 함).

# 레포 루트에서 실행 (현재 위치 OK)
docker compose -f docker/docker-compose.yml up -d db
docker compose -f docker/docker-compose.yml ps

# 백필온리 실행할때 이거 한번만 실행해줌
$env:PYTHONPATH = (Resolve-Path .).Path
python .\scripts\backfill_results_only.py



postgresql://user@localhost:5432/eest

user= user ; pass= pass

DBEAVER


 venv 활성화
.\.venv\Scripts\Activate.ps1

 venv 활성화
.\.venv\Scripts\Activate.ps1

 venv 활성화
.\.venv\Scripts\Activate.ps1


$env:G2B_SERVICE_KEY="x/PZ/5P4XEADwenpLZQj+gcF1SinSHLzZa+waIRUNBDmOq2fVux+tTXi2ZReHBpKtKWQnHNqvmgfR2APKc3PKg=="
py g2b_pipeline_two_stage.py --from 20250101 --to 20250131

조건에 맞는(3억이하 금속창업)것만 받아온다 
py collectDaya.py --from 20250101 --to 20250131

프렙 받기 <후보에서만 받는다.> 위 조건에 맞는 애들거만 가져온다.
py collectDaya.py --from 20250101 --to 20250131 --prep-from-cands

디버그 메세지 포함함
python collectData.py --from 20250101 --to 20250130 --debug


$env:PG_DSN="postgresql+psycopg2://bs:bs_pw@localhost:5432/biddingsim"

python collectData.py `
  --from 20250101 --to 20250131 `
  --per-page 200 --license-lookback-days 60 `
  --debug

$env:PG_DSN="postgresql+psycopg2://user:pass@localhost:5432/postgres"

.\.venv\Scripts\python.exe .\collectData.py `
  --from 20250101 --to 20250131 `
  --license-lookback-days 365 `
  --single-backfill-cap 300 `
  --use-std-dataset `
  --debug



collectall

.\.venv\Scripts\python.exe .\collectData_optimized.py `
  --from 20250101 --to 20250131 `
  --license-lookback-days 365 `
  --single-backfill-cap 800 `
  --scope all `
  --exclude-cntrct 단가 수의 유지보수 `
  --use-std-dataset `
  --create-views `
  --debug
