CREATE MATERIALIZED VIEW gold_sales_summary AS
SELECT
  region,
  COUNT(*) AS order_count,
  SUM(amount) AS total_sales,
  AVG(amount) AS average_order_value
FROM silver_orders
GROUP BY region;
