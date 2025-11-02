-- Page views event table
CREATE TABLE IF NOT EXISTS page_views (
  event_id    VARCHAR(64),
  event_time  DATETIME,
  user_id     VARCHAR(64),
  page        VARCHAR(256),
  referrer    VARCHAR(256),
  device      VARCHAR(64),
  os          VARCHAR(64),
  country     VARCHAR(64),
  ts_ms       BIGINT
)
DUPLICATE KEY(event_id, event_time)
DISTRIBUTED BY HASH(event_id) BUCKETS 8
PROPERTIES (
  "replication_num" = "1"
);

-- Orders event table
CREATE TABLE IF NOT EXISTS orders (
  order_id    VARCHAR(64),
  event_time  DATETIME,
  user_id     VARCHAR(64),
  amount      DOUBLE,
  currency    VARCHAR(16),
  status      VARCHAR(32),
  ts_ms       BIGINT,
  region      VARCHAR(64),
  channel     VARCHAR(64)
)
DUPLICATE KEY(order_id, event_time)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
  "replication_num" = "1"
);