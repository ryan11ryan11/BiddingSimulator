from typing import List

def quantiles(xs: List[float], probs=(0.5, 0.8, 0.9, 0.95)):
    if not xs:
        return {p: None for p in probs}
    s = sorted(xs)
    n = len(s)
    out = {}
    for p in probs:
        k = p * (n - 1)
        f = int(k)
        c = min(f + 1, n - 1)
        w = k - f
        out[p] = s[f] * (1 - w) + s[c] * w
    return out