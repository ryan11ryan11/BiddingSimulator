# core/clients/scsbid_info.py
from .base_client import OpenAPIClient

class ScsbidInfo(OpenAPIClient):
    """
    조달청(OpenAPI) - 개찰결과/예비가격 상세 클라이언트
    - 참고: inqryDiv (1: 기간, 2: 공고번호)
    - 날짜 포맷: YYYYMMDDHHMM
    """

    # 예비가격상세 (공사)
    def get_prepar_pc_detail_cnstwk(
        self,
        inqry_div: int,
        bid_no: str = None,
        inqry_bgn_dt: str = None,
        inqry_end_dt: str = None,
        page_no: int = 1,
        num_rows: int = 10,
    ):
        return self.get(
            "getOpengResultListInfoCnstwkPreparPcDetail",
            {
                "inqryDiv": inqry_div,        # 1:기간 / 2:공고번호
                "bidNtceNo": bid_no,
                "inqryBgnDt": inqry_bgn_dt,   # YYYYMMDDHHMM
                "inqryEndDt": inqry_end_dt,
                "pageNo": page_no,
                "numOfRows": num_rows,
            },
        )

    # 개찰결과 목록 (공사)  ← est_price(예정가격) 포함
    def get_openg_result_list_cnstwk(
        self,
        inqry_div: int,
        bid_no: str = None,
        inqry_bgn_dt: str = None,
        inqry_end_dt: str = None,
        page_no: int = 1,
        num_rows: int = 10,
    ):
        return self.get(
            "getOpengResultListInfoCnstwk",
            {
                "inqryDiv": inqry_div,        # 1:기간 / 2:공고번호
                "bidNtceNo": bid_no,
                "inqryBgnDt": inqry_bgn_dt,
                "inqryEndDt": inqry_end_dt,
                "pageNo": page_no,
                "numOfRows": num_rows,
            },
        )

    # (필요 시) 기타 물품/용역/외자 엔드포인트도 동일 패턴으로 확장
    def get_prepar_pc_detail_thng(self, **kw):
        return self.get("getOpengResultListInfoThngPreparPcDetail", kw)

    def get_prepar_pc_detail_servc(self, **kw):
        return self.get("getOpengResultListInfoServcPreparPcDetail", kw)

    def get_prepar_pc_detail_frgcpt(self, **kw):
        return self.get("getOpengResultListInfoFrgcptPreparPcDetail", kw)
