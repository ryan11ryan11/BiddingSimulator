CREATE TABLE IF NOT EXISTS t_etl_watermark (
  stream       TEXT     NOT NULL,
  bucket       DATE     NOT NULL,
  last_page    INTEGER  DEFAULT 0,
  total_pages  INTEGER,
  total_count  INTEGER,
  updated_at   TIMESTAMP DEFAULT now(),
  PRIMARY KEY (stream, bucket)
);
