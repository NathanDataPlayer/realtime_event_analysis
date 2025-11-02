-- Routine load for page_views from Kafka JSON
CREATE ROUTINE LOAD eventdb.page_views_rl
ON page_views
COLUMNS(event_id, user_id, page, referrer, device, os, country, ts_ms)
SET(event_time = FROM_UNIXTIME(ts_ms / 1000))
PROPERTIES (
  "desired_concurrent_number" = "3",
  "max_batch_interval" = "5",
  "max_batch_rows" = "50000",
  "max_batch_size" = "209715200",
  "strict_mode" = "false",
  "format" = "json",
  "jsonpaths" = "[\"$.event_id\",\"$.user_id\",\"$.page\",\"$.referrer\",\"$.device\",\"$.os\",\"$.country\",\"$.ts_ms\"]"
)
FROM KAFKA (
  "kafka_broker_list" = "kafka:9092",
  "kafka_topic" = "page_views",
  "property.group.id" = "sr-page-views"
);

-- Routine load for orders from Kafka JSON
CREATE ROUTINE LOAD eventdb.orders_rl
ON orders
COLUMNS(order_id, user_id, amount, currency, status, ts_ms, region, channel)
SET(event_time = FROM_UNIXTIME(ts_ms / 1000))
PROPERTIES (
  "desired_concurrent_number" = "3",
  "max_batch_interval" = "5",
  "max_batch_rows" = "50000",
  "max_batch_size" = "209715200",
  "strict_mode" = "false",
  "format" = "json",
  "jsonpaths" = "[\"$.order_id\",\"$.user_id\",\"$.amount\",\"$.currency\",\"$.status\",\"$.ts_ms\",\"$.region\",\"$.channel\"]"
)
FROM KAFKA (
  "kafka_broker_list" = "kafka:9092",
  "kafka_topic" = "orders",
  "property.group.id" = "sr-orders"
);