from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from apps.etl.tasks import collect_and_load_prepar_detail_cnstwk

now = datetime.now(ZoneInfo("Asia/Seoul"))
bgn = (now - timedelta(days=7)).strftime("%Y%m%d0000")
end = now.strftime("%Y%m%d2359")
print("window:", bgn, end)
print("saved prep15:", collect_and_load_prepar_detail_cnstwk(bgn, end))
