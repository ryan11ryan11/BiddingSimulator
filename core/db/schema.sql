CREATE TABLE IF NOT EXISTS t_notice (
  bid_no        VARCHAR(40) NOT NULL,
  ord           VARCHAR(3)  NOT NULL,
  base_amount   BIGINT      NOT NULL,
  range_low     NUMERIC(8,5),
  range_high    NUMERIC(8,5),
  lower_rate    NUMERIC(6,3),  -- (가능시)
  owner_id      VARCHAR(32),
  work_type     VARCHAR(16),   -- 물품/용역/공사/외자
  announced_at  TIMESTAMP,
  vat_included  BOOLEAN DEFAULT TRUE,
  PRIMARY KEY (bid_no, ord)
);

CREATE TABLE IF NOT EXISTS t_result (
  bid_no        VARCHAR(40) NOT NULL,
  ord           VARCHAR(3)  NOT NULL,
  est_price     BIGINT,        -- 예정가격(E)
  presmpt_price BIGINT,        -- 추정가격(있을 시)
  bidders_cnt   INT,
  rebid_flag    BOOLEAN,
  rl_openg_dt   TIMESTAMP,
  PRIMARY KEY (bid_no, ord)
);

CREATE TABLE IF NOT EXISTS t_prep15 (
  bid_no        VARCHAR(40) NOT NULL,
  ord           VARCHAR(3)  NOT NULL,
  comp_sno      INT NOT NULL,     -- 1..15
  bsis_plnprc   BIGINT,           -- 후보값
  drawn_flag    BOOLEAN,          -- 선정 4개 여부
  draw_seq      INT,              -- 선정 순번(1..4)
  final_plnprc  BIGINT,           -- 최종 예정가격 스냅샷
  PRIMARY KEY (bid_no, ord, comp_sno)
);

CREATE TABLE IF NOT EXISTS t_meta (
  owner_id        VARCHAR(32) PRIMARY KEY,
  owner_name_norm TEXT,
  rounding_unit_est INT,
  spacing_profile  TEXT
);

-- 보조 인덱스
CREATE INDEX IF NOT EXISTS idx_notice_group ON t_notice(owner_id, work_type);
CREATE INDEX IF NOT EXISTS idx_prep15_triplet ON t_prep15(bid_no, ord, comp_sno);