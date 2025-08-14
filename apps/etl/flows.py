from prefect import flow
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from .tasks import collect_and_load_notices, collect_and_load_prepar_detail_cnstwk
from .tasks import collect_and_load_results_cnstwk
from prefect import flow, get_run_logger

@flow(name="e2e-collect-load")
def e2e_collect_load(hours: int = 24):
    logger = get_run_logger()
    logger.info(f"[FLOW] start hours={hours}")
    now = datetime.now(ZoneInfo("Asia/Seoul"))  # ★ KST 기준
    bgn = (now - timedelta(hours=hours)).strftime("%Y%m%d%H%M")
    end = now.strftime("%Y%m%d%H%M")
    r = collect_and_load_results_cnstwk(bgn, end)
    n = collect_and_load_notices(bgn, end, "Cnstwk")
    p = collect_and_load_prepar_detail_cnstwk(bgn, end)
    return {"notices": n, "prep15": p, "results": r}

