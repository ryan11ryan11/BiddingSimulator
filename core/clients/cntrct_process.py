from .base_client import OpenAPIClient

class CntrctProcess(OpenAPIClient):
    def get_open_cnstwk(self, inqry_div: int, bid_no: str = None, bid_ord: str = None, bf_spec_rgst_no: str = None, order_plan_no: str = None, prcrmnt_req_no: str = None, page_no: int = 1, num_rows: int = 10):
        return self.get("getCntrctProcssIntgOpenCnstwk", {
            "inqryDiv": inqry_div,  # 1:입찰공고번호, 2:사전규격등록번호, 3:발주계획번호, 4:조달요청번호
            "bidNtceNo": bid_no,
            "bidNtceOrd": bid_ord,
            "bfSpecRgstNo": bf_spec_rgst_no,
            "orderPlanNo": order_plan_no,
            "prcrmntReqNo": prcrmnt_req_no,
            "pageNo": page_no,
            "numOfRows": num_rows,
        })