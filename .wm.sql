CREATE TABLE IF NOT EXISTS t_etl_watermark (
  stream       TEXT     NOT NULL,   -- 예: 'notice:cnstwk', 'prep15:cnstwk'
  bucket       DATE     NOT NULL,   -- 일 단위 버킷(한국시간 기준)
  last_page    INTEGER  DEFAULT 0,  -- 마지막으로 성공 저장한 페이지
  total_pages  INTEGER,
  total_count  INTEGER,
  updated_at   TIMESTAMP DEFAULT now(),
  PRIMARY KEY (stream, bucket)
);
