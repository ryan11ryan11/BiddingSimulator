from .base_client import OpenAPIClient

class BidPublicInfo(OpenAPIClient):
    # 기초금액조회 (업무별 엔드포인트 존재)
    def get_bsis_amount_cnstwk(self, inqry_div: int, bid_no: str = None, inqry_bgn_dt: str = None, inqry_end_dt: str = None, page_no: int = 1, num_rows: int = 10):
        return self.get("getBidPblancListInfoCnstwkBsisAmount", {
            "inqryDiv": inqry_div,
            "bidNtceNo": bid_no,
            "inqryBgnDt": inqry_bgn_dt,
            "inqryEndDt": inqry_end_dt,
            "pageNo": page_no,
            "numOfRows": num_rows,
        })

    # 공고목록(업무별). 범위(±a%), 하한율 등은 상세/기초금액 응답 활용
    def get_list_pps_cnstwk(self, inqry_div: int, bid_no: str = None, inqry_bgn_dt: str = None, inqry_end_dt: str = None, page_no: int = 1, num_rows: int = 10):
        return self.get("getBidPblancListInfoCnstwk", {
            "inqryDiv": inqry_div,
            "bidNtceNo": bid_no,
            "inqryBgnDt": inqry_bgn_dt,
            "inqryEndDt": inqry_end_dt,
            "pageNo": page_no,
            "numOfRows": num_rows,
        })