import ssl, requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

class TLS12Adapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **kwargs):
        ctx = ssl.create_default_context()
        # TLS 1.3 비활성화로 호환성 확보
        if hasattr(ssl, "OP_NO_TLSv1_3"):
            ctx.options |= ssl.OP_NO_TLSv1_3
        kwargs["ssl_context"] = ctx
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, **kwargs)

s = requests.Session()
s.mount("https://", TLS12Adapter())

BASE = "https://apis.data.go.kr/1230000/ScsbidInfoService"
p = {
    "serviceKey": "x/PZ/5P4XEADwenpLZQj+gcF1SinSHLzZa+waIRUNBDmOq2fVux+tTXi2ZReHBpKtKWQnHNqvmgfR2APKc3PKg==",
    "_type": "json",
    "inqryDiv": 1,
    "inqryBgnDt": "202501010000",
    "inqryEndDt": "202501012359",
    "pageNo": 1,
    "numOfRows": 1,
}
r = s.get(f"{BASE}/getOpengResultListCnstwk", params=p, timeout=20)
print(r.status_code, r.headers.get("Content-Type"))
print(r.text[:500])
