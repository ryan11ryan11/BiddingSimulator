-- 총 건수
SELECT count(*) AS t_notice FROM t_notice;
SELECT count(*) AS t_prep15 FROM t_prep15;

-- 학습 가능한 표본: E와 Base가 있는 건
SELECT count(*) AS trainable
FROM t_result r JOIN t_notice n USING (bid_no, ord)
WHERE n.base_amount > 0 AND r.est_price IS NOT NULL;

-- 결측/이상치 점검
SELECT COUNT(*) AS no_base FROM t_notice WHERE (base_amount IS NULL OR base_amount<=0);
SELECT COUNT(*) AS no_E    FROM t_result WHERE est_price IS NULL;
