from typing import Dict

def blend(q_emp: Dict[float, float], q_sim: Dict[float, float], base: int, n_emp: int, k0: int = 30):
    w = min(1.0, n_emp / (n_emp + k0))
    out = {}
    for p in q_emp.keys():
        r_emp = q_emp[p]
        r_sim = q_sim[p] / base if q_sim[p] is not None else None
        if r_emp is None and r_sim is None:
            out[p] = None
        elif r_emp is None:
            out[p] = r_sim
        elif r_sim is None:
            out[p] = r_emp
        else:
            out[p] = w * r_emp + (1 - w) * r_sim
    return w, out