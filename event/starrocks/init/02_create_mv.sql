-- Page view count per minute per page
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_pv_per_minute
AS
SELECT
  DATE_TRUNC('minute', event_time) AS minute,
  page,
  COUNT(*) AS pv
FROM page_views
GROUP BY minute, page;

-- Revenue per minute for successful orders
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_revenue_per_minute
AS
SELECT
  DATE_TRUNC('minute', event_time) AS minute,
  SUM(CASE WHEN status IN ('paid', 'completed') THEN amount ELSE 0 END) AS revenue
FROM orders
GROUP BY minute;