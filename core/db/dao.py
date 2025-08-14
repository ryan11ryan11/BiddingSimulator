from typing import Iterable, List, Dict, Optional
from sqlalchemy import text
from .engine import engine

# -------------------------
# Upsert helpers
# -------------------------

def upsert_notice(row: Dict):
    sql = text(
        """
        INSERT INTO t_notice(bid_no, ord, base_amount, range_low, range_high, lower_rate,
                             owner_id, work_type, announced_at, vat_included)
        VALUES (:bid_no, :ord, :base_amount, :range_low, :range_high, :lower_rate,
                :owner_id, :work_type, :announced_at, :vat_included)
        ON CONFLICT (bid_no, ord) DO UPDATE SET
          base_amount=EXCLUDED.base_amount,
          range_low=EXCLUDED.range_low,
          range_high=EXCLUDED.range_high,
          lower_rate=EXCLUDED.lower_rate,
          owner_id=EXCLUDED.owner_id,
          work_type=EXCLUDED.work_type,
          announced_at=EXCLUDED.announced_at,
          vat_included=EXCLUDED.vat_included;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, row)


def upsert_result(row: dict):
    """
    t_result UPSERT
    - 새 값이 NULL이면 기존 값을 유지(COALESCE).
    """
    sql = text("""
    INSERT INTO t_result (
        bid_no, ord,
        est_price,
        presmpt_price,
        bidders_cnt,
        rebid_flag,
        rl_openg_dt,
        updated_at
    ) VALUES (
        :bid_no, :ord,
        :est_price,
        :presmpt_price,
        :bidders_cnt,
        :rebid_flag,
        :rl_openg_dt,
        now()
    )
    ON CONFLICT (bid_no, ord) DO UPDATE SET
        est_price     = COALESCE(EXCLUDED.est_price,     t_result.est_price),
        presmpt_price = COALESCE(EXCLUDED.presmpt_price, t_result.presmpt_price),
        bidders_cnt   = COALESCE(EXCLUDED.bidders_cnt,   t_result.bidders_cnt),
        rebid_flag    = COALESCE(EXCLUDED.rebid_flag,    t_result.rebid_flag),
        rl_openg_dt   = COALESCE(EXCLUDED.rl_openg_dt,   t_result.rl_openg_dt),
        updated_at    = now();
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "bid_no": row.get("bid_no"),
            "ord": row.get("ord"),
            "est_price": row.get("est_price"),
            "presmpt_price": row.get("presmpt_price"),
            "bidders_cnt": row.get("bidders_cnt"),
            "rebid_flag": row.get("rebid_flag"),
            "rl_openg_dt": row.get("rl_openg_dt"),
        })


def upsert_prep15_bulk(rows: List[Dict]):
    if not rows:
        return
    sql = text(
        """
        INSERT INTO t_prep15(bid_no, ord, comp_sno, bsis_plnprc, drawn_flag, draw_seq, final_plnprc)
        VALUES (:bid_no, :ord, :comp_sno, :bsis_plnprc, :drawn_flag, :draw_seq, :final_plnprc)
        ON CONFLICT (bid_no, ord, comp_sno) DO UPDATE SET
          bsis_plnprc=EXCLUDED.bsis_plnprc,
          drawn_flag=EXCLUDED.drawn_flag,
          draw_seq=EXCLUDED.draw_seq,
          final_plnprc=EXCLUDED.final_plnprc;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)


# -------------------------
# Fetch helpers (estimation step)
# -------------------------

def fetch_notice(bid_no: str, ord: str) -> Optional[Dict]:
    sql = text("SELECT * FROM t_notice WHERE bid_no=:bid_no AND ord=:ord")
    with engine.begin() as conn:
        row = conn.execute(sql, {"bid_no": bid_no, "ord": str(ord)}).mappings().first()
        return dict(row) if row else None


def fetch_prep15(bid_no: str, ord: str) -> List[Dict]:
    sql = text("SELECT * FROM t_prep15 WHERE bid_no=:b AND ord=:o ORDER BY comp_sno")
    with engine.begin() as conn:
        rows = conn.execute(sql, {"b": bid_no, "o": str(ord)}).mappings().all()
        return [dict(r) for r in rows]


def fetch_results_history(owner_id: str, work_type: str, months: int = 12) -> List[Dict]:
    sql = text(
        """
        SELECT n.owner_id, n.work_type, n.base_amount, r.est_price, r.rl_openg_dt
        FROM t_result r
        JOIN t_notice n USING (bid_no, ord)
        WHERE n.owner_id=:owner_id AND n.work_type=:work_type
          AND r.est_price IS NOT NULL
          AND (r.rl_openg_dt IS NULL OR r.rl_openg_dt >= (now() - (:months || ' months')::interval))
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql, {"owner_id": owner_id, "work_type": work_type, "months": months}).mappings().all()
        return [dict(r) for r in rows]