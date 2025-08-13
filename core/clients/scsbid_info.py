# core/clients/scsbid_info.py
from __future__ import annotations

from typing import Optional, Dict, Any
from .base_client import OpenAPIClient


class ScsbidInfo(OpenAPIClient):
    """
    SCSBID(조달청) OpenAPI 클라이언트.
    - 예비가격상세(prepar_pc_detail_*): 공사/용역/물품/외자 15개 예비가격 조회
    - 개찰결과(openg_result_list_*): 예정가격(est_price) 등 개찰 결과 조회  ← 학습 타깃
    모든 메서드는 동일한 파라미터 규약을 따릅니다.
      - inqry_div(조회구분): 1=등록일시(기본)
      - bid_no(입찰공고번호) 또는 기간 inqry_bgn_dt~inqry_end_dt(YYYYMMDDHHMM)
      - page_no, num_rows: 페이지네이션
    """

    # =========================
    # 예비가격상세 (15개 예비가격)
    # =========================
    def get_prepar_pc_detail_cnstwk(
        self,
        inqry_div: int,
        bid_no: Optional[str] = None,
        inqry_bgn_dt: Optional[str] = None,
        inqry_end_dt: Optional[str] = None,
        page_no: int = 1,
        num_rows: int = 10,
    ) -> Dict[str, Any]:
        return self.get(
            "getOpengResultListInfoCnstwkPreparPcDetail",
            {
                "inqryDiv": inqry_div,
                "bidNtceNo": bid_no,
                "inqryBgnDt": inqry_bgn_dt,
                "inqryEndDt": inqry_end_dt,
                "pageNo": page_no,
                "numOfRows": num_rows,
            },
        )

    def get_prepar_pc_detail_servc(self, **kw) -> Dict[str, Any]:
        return self.get("getOpengResultListInfoServcPreparPcDetail", kw)

    def get_prepar_pc_detail_thng(self, **kw) -> Dict[str, Any]:
        return self.get("getOpengResultListInfoThngPreparPcDetail", kw)

    def get_prepar_pc_detail_frgcpt(self, **kw) -> Dict[str, Any]:
        return self.get("getOpengResultListInfoFrgcptPreparPcDetail", kw)

    # =========================
    # 개찰결과 (예정가격 등)
    # =========================
    def get_openg_result_list_cnstwk(
        self,
        inqry_div: int,
        bid_no: Optional[str] = None,
        inqry_bgn_dt: Optional[str] = None,
        inqry_end_dt: Optional[str] = None,
        page_no: int = 1,
        num_rows: int = 10,
    ) -> Dict[str, Any]:
        return self.get(
            "getOpengResultListInfoCnstwk",
            {
                "inqryDiv": inqry_div,
                "bidNtceNo": bid_no,
                "inqryBgnDt": inqry_bgn_dt,
                "inqryEndDt": inqry_end_dt,
                "pageNo": page_no,
                "numOfRows": num_rows,
            },
        )

    def get_openg_result_list_servc(self, **kw) -> Dict[str, Any]:
        return self.get("getOpengResultListInfoServc", kw)

    def get_openg_result_list_thng(self, **kw) -> Dict[str, Any]:
        return self.get("getOpengResultListInfoThng", kw)

    def get_openg_result_list_frgcpt(self, **kw) -> Dict[str, Any]:
        return self.get("getOpengResultListInfoFrgcpt", kw)
