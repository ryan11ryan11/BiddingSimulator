from typing import List, Tuple
import math

ROUND_UNITS = [1, 10, 100, 1000, 10000]

def normalize_vat(amount: int, src_vat_included: bool, target_vat_included: bool = True, vat_rate: float = 0.1) -> int:
    if src_vat_included == target_vat_included:
        return amount
    return int(round(amount * (1 + vat_rate))) if not target_vat_included else int(round(amount / (1 + vat_rate)))

def size_binner(base_amount: int) -> str:
    # 로그스케일 bin (1억/3억/10억/30억 …)
    bins = [1e8, 3e8, 1e9, 3e9, 1e10, 3e10]
    labels = ["100M", "300M", "1B", "3B", "10B", "30B+"]
    for th, lb in zip(bins, labels):
        if base_amount < th:
            return lb
    return labels[-1]

def estimate_rounding_unit(values: List[int]) -> int:
    # 끝자리 히스토그램 기반 대략 추정(최빈 자리수)
    scores = {u: 0 for u in ROUND_UNITS}
    for v in values:
        for u in ROUND_UNITS:
            if v % u == 0:
                scores[u] += 1
    return max(scores, key=scores.get)


def spacing_metrics(vals15: List[int]) -> Tuple[float, float, int]:
    if not vals15 or len(vals15) < 2:
        return (0.0, 0.0, 0)
    diffs = [vals15[i+1] - vals15[i] for i in range(len(vals15)-1)]
    mu = sum(diffs) / len(diffs)
    var = sum((d - mu) ** 2 for d in diffs) / len(diffs)
    rng = int(max(diffs) - min(diffs)) if diffs else 0
    return (round(mu, 2), round(math.sqrt(var), 2), rng)


def centrality_weight(drawn_indices: List[int]) -> float:
    # 인덱스는 1..15, 중앙=8에 가까울수록 가중치 ↑
    if not drawn_indices:
        return 0.0
    center = 8
    maxd = max(center - 1, 15 - center)
    dists = [abs(i - center) for i in drawn_indices]
    score = 1.0 - (sum(dists) / (len(dists) * maxd))
    return float(max(0.0, min(1.0, round(score, 3))))