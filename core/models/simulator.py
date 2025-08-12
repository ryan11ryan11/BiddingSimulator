from itertools import combinations
from typing import List, Dict
from ..utils.quantile import quantiles

def round_to_unit(x: float, unit: int) -> int:
    return int(round(x / unit) * unit)

def gen_15_values(base: int, a: float, rounding_unit: int = 1, custom_vals: List[int] = None) -> List[int]:
    if custom_vals:
        vals = custom_vals
    else:
        step = (2 * a) / 14.0
        vals = [base * (1 - a + i * step) for i in range(15)]
    return [round_to_unit(v, rounding_unit) for v in vals]

def simulate_E_quantiles(vals15: List[int]) -> Dict[float, float]:
    means = [(sum(c) / 4.0) for c in combinations(vals15, 4)]  # 1,365개
    return quantiles(means)