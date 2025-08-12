from .base_client import OpenAPIClient

class ScsbidInfo(OpenAPIClient):
    # 예비가격상세 (공사/용역/물품/외자)
    def get_prepar_pc_detail_cnstwk(self, inqry_div: int, bid_no: str = None, inqry_bgn_dt: str = None, inqry_end_dt: str = None, page_no: int = 1, num_rows: int = 10):
        return self.get("getOpengResultListInfoCnstwkPreparPcDetail", {
            "inqryDiv": inqry_div,                 # 1:등록일시 등
            "bidNtceNo": bid_no,
            "inqryBgnDt": inqry_bgn_dt,           # YYYYMMDDHHMM
            "inqryEndDt": inqry_end_dt,
            "pageNo": page_no,
            "numOfRows": num_rows,
        })

    def get_prepar_pc_detail_thng(self, **kw):
        return self.get("getOpengResultListInfoThngPreparPcDetail", kw)

    def get_prepar_pc_detail_servc(self, **kw):
        return self.get("getOpengResultListInfoServcPreparPcDetail", kw)

    def get_prepar_pc_detail_frgcpt(self, **kw):
        return self.get("getOpengResultListInfoFrgcptPreparPcDetail", kw)