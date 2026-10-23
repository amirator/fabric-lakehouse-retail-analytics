# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "lh_olist_retail"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Gold: star schema + reporting aggregates for Direct Lake
# - Dimensions: `dim_date`, `dim_customer`, `dim_product`, `dim_seller`.
# - Facts: `fact_order_items` (order-item grain), `fact_orders` (order grain, delivery and review metrics).
# - Aggregates: `gold_monthly_kpis`, `gold_category_kpis`, `gold_state_kpis` (feed the static web report).
# - GMV = item price (excl. freight) on delivered orders. Late = delivered after the estimated date.

# CELL ********************

# Engine shim: on Fabric `spark` is pre-defined; locally run.py injects a DuckDB connection `con`.
try:
    spark  # noqa: F821
    ENGINE = "spark"
except NameError:
    ENGINE = "duckdb"


def days_between(end_col, start_col):
    """Whole days end - start, in the dialect of the active engine."""
    if ENGINE == "spark":
        return f"datediff({end_col}, {start_col})"
    return f"date_diff('day', CAST({start_col} AS DATE), CAST({end_col} AS DATE))"


def save_table(name, sql):
    """Materialise a SELECT as a lakehouse table (Delta on Fabric, DuckDB table locally)."""
    if ENGINE == "spark":
        spark.sql(sql).write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(name)  # noqa: F821
        n = spark.table(name).count()  # noqa: F821
    else:
        con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")  # noqa: F821
        n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]  # noqa: F821
    print(f"{name}: {n:,} rows")
    return n

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_table("dim_date", """
WITH bounds AS (SELECT CAST(MIN(purchase_ts) AS DATE) AS d0, CAST(MAX(purchase_ts) AS DATE) AS d1 FROM silver_orders),
days AS (SELECT explode(sequence(d0, d1, INTERVAL 1 DAY)) AS date FROM bounds)
SELECT CAST(date AS DATE) AS date, year(date) AS year, month(date) AS month,
       CAST(year(date) * 100 + month(date) AS INT) AS year_month_key,
       concat(CAST(year(date) AS STRING), '-', lpad(CAST(month(date) AS STRING), 2, '0')) AS year_month,
       quarter(date) AS quarter
FROM days
""" if ENGINE == "spark" else """
WITH bounds AS (SELECT CAST(MIN(purchase_ts) AS DATE) AS d0, CAST(MAX(purchase_ts) AS DATE) AS d1 FROM silver_orders),
days AS (SELECT CAST(unnest(generate_series(d0, d1, INTERVAL 1 DAY)) AS DATE) AS date FROM bounds)
SELECT date, year(date) AS year, month(date) AS month,
       CAST(year(date) * 100 + month(date) AS INT) AS year_month_key,
       concat(CAST(year(date) AS VARCHAR), '-', lpad(CAST(month(date) AS VARCHAR), 2, '0')) AS year_month,
       quarter(date) AS quarter
FROM days ORDER BY date
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_table("dim_customer", "SELECT customer_id, customer_unique_id, city, state FROM silver_customers")
save_table("dim_product", "SELECT product_id, category, weight_g, photos_qty FROM silver_products")
save_table("dim_seller", "SELECT seller_id, city, state FROM silver_sellers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_table("fact_orders", f"""
WITH items AS (
  SELECT order_id, COUNT(*) AS item_count, SUM(price) AS items_value, SUM(freight_value) AS freight_value
  FROM silver_order_items GROUP BY order_id),
pay AS (SELECT order_id, SUM(payment_value) AS payment_value FROM silver_payments GROUP BY order_id)
SELECT o.order_id, o.customer_id, CAST(o.purchase_ts AS DATE) AS order_date, o.order_status,
       CASE WHEN o.order_status = 'delivered' AND o.delivered_customer_ts IS NOT NULL THEN 1 ELSE 0 END AS is_delivered,
       COALESCE(i.item_count, 0) AS item_count,
       COALESCE(i.items_value, 0) AS items_value,
       COALESCE(i.freight_value, 0) AS freight_value,
       COALESCE(p.payment_value, 0) AS payment_value,
       CASE WHEN o.delivered_customer_ts IS NOT NULL THEN {days_between('o.delivered_customer_ts', 'o.purchase_ts')} END AS delivery_days,
       CASE WHEN o.delivered_customer_ts IS NULL THEN NULL
            WHEN CAST(o.delivered_customer_ts AS DATE) > CAST(o.estimated_delivery_ts AS DATE) THEN 1 ELSE 0 END AS is_late,
       r.review_score
FROM silver_orders o
LEFT JOIN items i ON o.order_id = i.order_id
LEFT JOIN pay p ON o.order_id = p.order_id
LEFT JOIN silver_reviews r ON o.order_id = r.order_id
""")
save_table("fact_order_items", """
SELECT i.order_id, i.order_item_id, i.product_id, i.seller_id, o.customer_id,
       CAST(o.purchase_ts AS DATE) AS order_date, o.order_status, i.price, i.freight_value
FROM silver_order_items i JOIN silver_orders o ON i.order_id = o.order_id
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_table("gold_monthly_kpis", """
SELECT d.year_month,
       COUNT(*) AS orders,
       SUM(f.is_delivered) AS delivered_orders,
       ROUND(SUM(CASE WHEN f.is_delivered = 1 THEN f.items_value ELSE 0 END), 2) AS gmv,
       ROUND(SUM(CASE WHEN f.is_delivered = 1 THEN f.items_value ELSE 0 END) / NULLIF(SUM(f.is_delivered), 0), 2) AS aov,
       ROUND(AVG(f.is_late) * 100, 2) AS late_pct,
       ROUND(AVG(f.review_score), 3) AS avg_review
FROM fact_orders f JOIN dim_date d ON f.order_date = d.date
GROUP BY d.year_month ORDER BY d.year_month
""")
save_table("gold_category_kpis", """
SELECT p.category, COUNT(DISTINCT i.order_id) AS orders, COUNT(*) AS items,
       ROUND(SUM(i.price), 2) AS gmv, ROUND(AVG(f.review_score), 3) AS avg_review,
       ROUND(AVG(f.is_late) * 100, 2) AS late_pct
FROM fact_order_items i
JOIN fact_orders f ON i.order_id = f.order_id
JOIN dim_product p ON i.product_id = p.product_id
WHERE f.is_delivered = 1
GROUP BY p.category ORDER BY gmv DESC, p.category
""")
save_table("gold_state_kpis", """
SELECT c.state, COUNT(*) AS delivered_orders, ROUND(SUM(f.items_value), 2) AS gmv,
       ROUND(AVG(f.delivery_days), 2) AS avg_delivery_days, ROUND(AVG(f.is_late) * 100, 2) AS late_pct,
       ROUND(AVG(f.review_score), 3) AS avg_review
FROM fact_orders f JOIN dim_customer c ON f.customer_id = c.customer_id
WHERE f.is_delivered = 1
GROUP BY c.state ORDER BY gmv DESC, c.state
""")
save_table("gold_review_by_lateness", """
SELECT CASE WHEN is_late = 1 THEN 'late' ELSE 'on_time' END AS delivery, COUNT(*) AS orders,
       ROUND(AVG(review_score), 3) AS avg_review,
       ROUND(AVG(CASE WHEN review_score <= 2 THEN 1.0 ELSE 0.0 END) * 100, 2) AS low_review_pct
FROM fact_orders WHERE is_delivered = 1 AND review_score IS NOT NULL
GROUP BY CASE WHEN is_late = 1 THEN 'late' ELSE 'on_time' END ORDER BY delivery
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
