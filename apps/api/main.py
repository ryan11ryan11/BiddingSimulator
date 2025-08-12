from fastapi import FastAPI, HTTPException
from .schemas import EstimateResponse, Group, Diagnostics
from core.db.dao import fetch_notice, fetch_prep15, fetch_results_history
from core.features.transforms import size_binner, estimate_rounding_unit, spacing_metrics, centrality_weight
from core.models.empirical import empirical_quantiles
from core.models.simulator import gen_15_values, simulate_E_quantiles
from core.models.ensemble import blend

app = FastAPI(title="E-Estimator")

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/estimate", response_model=EstimateResponse)
def estimate(bid_no: str, ord: int):
    n = fetch_notice(bid_no, str(ord))
    if not n:
        raise HTTPException(status_code=404, detail="notice not found")

    base = int(n["base_amount"]) if n.get("base_amount") else 0
    if base <= 0:
        raise HTTPException(status_code=422, detail="invalid base amount")

    # range_pct 결정 (±a%)
    a_candidates = []
    for k in ("range_low", "range_high"):
        v = n.get(k)
        if v is not None:
            a_candidates.append(abs(float(v)))
    a = max(a_candidates) if a_candidates else 0.02  # 기본 2%

    owner = n.get("owner_id") or ""
    work = n.get("work_type") or "Cnstwk"
    sb = size_binner(base)

    # 1) 경험분포 (owner×work×size_bin)
    hist = fetch_results_history(owner, work, months=12)
    hist_tuples = [(h["owner_id"], work, size_binner(int(h["base_amount"])), int(h["base_amount"]), int(h["est_price"])) for h in hist]
    q_emp, n_emp = empirical_quantiles(hist_tuples, owner, work, sb)

    # 2) 시뮬레이터: 15 → 4 평균분포
    p15 = fetch_prep15(bid_no, str(ord))
    if p15:
        vals15 = [int(r["bsis_plnprc"]) for r in p15]
        vals15 = [v for v in vals15 if v]
        vals15 = sorted({*vals15})  # 중복 제거 후 정렬
        rounding_unit = estimate_rounding_unit(vals15) if vals15 else 1
        q_sim = simulate_E_quantiles(vals15)
        spacing_profile = "learned"
        cent_w = centrality_weight([int(r["comp_sno"]) for r in p15 if r.get("drawn_flag")])
    else:
        rounding_unit = 1
        vals15 = gen_15_values(base, a, rounding_unit=rounding_unit)
        q_sim = simulate_E_quantiles(vals15)
        spacing_profile = "uniform"
        cent_w = 0.0

    # 3) 앙상블
    w, q_final = blend(q_emp, q_sim, base, n_emp, k0=30)

    # 응답 작성
    resp = EstimateResponse(
        bid_no=bid_no,
        ord=ord,
        base=base,
        range_pct=a,
        group=Group(owner_id=owner, work_type=work, size_bin=sb),
        E_quantiles={"q50": round(q_final[0.5], 6) if q_final[0.5] is not None else None,
                     "q80": round(q_final[0.8], 6) if q_final[0.8] is not None else None,
                     "q90": round(q_final[0.9], 6) if q_final[0.9] is not None else None,
                     "q95": round(q_final[0.95], 6) if q_final[0.95] is not None else None},
        diagnostics=Diagnostics(
            empirical_samples=int(n_emp),
            w_empirical=float(round(w, 3)),
            spacing_profile=spacing_profile,
            rounding_unit_est=int(rounding_unit),
            centrality_weight=float(cent_w),
        ),
    )
    return resp