from collections import defaultdict
from typing import Dict, Iterable, Tuple
from ..utils.quantile import quantiles

# history: Iterable[(owner_id, work_type, size_bin, base, E)]

def empirical_quantiles(history: Iterable[Tuple[str, str, str, int, int]], owner_id: str, work_type: str, size_bin: str):
    rs = []
    for (o, w, sb, base, E) in history:
        if o == owner_id and w == work_type and sb == size_bin and base and E:
            rs.append(E / base)
    return quantiles(rs), len(rs)