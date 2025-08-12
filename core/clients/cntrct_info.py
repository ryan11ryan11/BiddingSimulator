from .base_client import OpenAPIClient

class CntrctInfo(OpenAPIClient):
    def get_list_thng(self, inqry_div: int, inqry_bgn_dt: str = None, inqry_end_dt: str = None, unty_cntrct_no: str = None, page_no: int = 1, num_rows: int = 10):
        return self.get("getCntrctInfoListThng", {
            "inqryDiv": inqry_div,  # 1:등록일시, 2:통합계약번호
            "inqryBgnDt": inqry_bgn_dt,
            "inqryEndDt": inqry_end_dt,
            "untyCntrctNo": unty_cntrct_no,
            "pageNo": page_no,
            "numOfRows": num_rows,
        })