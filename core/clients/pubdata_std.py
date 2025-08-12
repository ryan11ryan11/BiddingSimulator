from .base_client import OpenAPIClient

class PubDataStd(OpenAPIClient):
    def get_scsbid_info(self, openg_bgn_dt: str, openg_end_dt: str, bsns_div_cd: int, page_no: int = 1, num_rows: int = 10):
        return self.get("getDataSetOpnStdScsbidInfo", {
            "opengBgnDt": openg_bgn_dt,
            "opengEndDt": openg_end_dt,
            "bsnsDivCd": bsns_div_cd,   # 1:물품 ...
            "pageNo": page_no,
            "numOfRows": num_rows,
        })