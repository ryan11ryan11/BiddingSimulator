@"
# --- settings ---
$compose = "docker\docker-compose.yml"
$apiPort = 8000

# 1) Docker DB
docker compose -f $compose up -d db
$DB_CID = docker compose -f $compose ps -q db
Write-Host "DB: $DB_CID"
Start-Sleep -Seconds 3
docker logs -n 20 $DB_CID

# 2) venv + deps (최초 1회면 건너뜀)
if (-not (Test-Path .\.venv\Scripts\Activate.ps1)) {
  py -3 -m venv .venv
}
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip > $null
pip install fastapi uvicorn "sqlalchemy>=2" "psycopg[binary]" requests pydantic prefect python-dotenv > $null

# 3) ETL (최근 7일, KST)
python -c "from apps.etl.flows import e2e_collect_load; print(e2e_collect_load(hours=168))"

# 4) API
Start-Process -NoNewWindow -FilePath uvicorn -ArgumentList "apps.api.main:app","--host","0.0.0.0","--port","$apiPort","--reload"
Start-Sleep -Seconds 2
Invoke-RestMethod "http://localhost:$apiPort/healthz"
"@ | Set-Content -Encoding UTF8 .\run.ps1
