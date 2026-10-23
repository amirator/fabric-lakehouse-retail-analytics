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

# ## Silver: typed, de-duplicated, conformed tables
# - `TRY_CAST` every typed column (bad values become NULL and are counted by tests, never dropped silently).
# - De-duplicate on business keys with `ROW_NUMBER()`.
# - Reviews: Olist reuses some `review_id`s across orders, so the key is (`review_id`, `order_id`); one latest review per order is kept.
# - Product categories translated to English; missing ones labelled `unknown`.

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

save_table("silver_orders", """
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_purchase_timestamp) AS rn
  FROM bronze_orders)
SELECT order_id, customer_id, order_status,
       TRY_CAST(order_purchase_timestamp AS TIMESTAMP)      AS purchase_ts,
       TRY_CAST(order_approved_at AS TIMESTAMP)             AS approved_ts,
       TRY_CAST(order_delivered_carrier_date AS TIMESTAMP)  AS delivered_carrier_ts,
       TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_ts,
       TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts
FROM ranked WHERE rn = 1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_table("silver_order_items", """
SELECT order_id, TRY_CAST(order_item_id AS INT) AS order_item_id, product_id, seller_id,
       TRY_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
       TRY_CAST(price AS DECIMAL(12,2)) AS price,
       TRY_CAST(freight_value AS DECIMAL(12,2)) AS freight_value
FROM bronze_order_items
""")
save_table("silver_payments", """
SELECT order_id, TRY_CAST(payment_sequential AS INT) AS payment_sequential, payment_type,
       TRY_CAST(payment_installments AS INT) AS payment_installments,
       TRY_CAST(payment_value AS DECIMAL(12,2)) AS payment_value
FROM bronze_payments
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_table("silver_reviews", """
WITH typed AS (
  SELECT review_id, order_id, TRY_CAST(review_score AS INT) AS review_score,
         TRY_CAST(review_creation_date AS TIMESTAMP) AS review_created_ts,
         TRY_CAST(review_answer_timestamp AS TIMESTAMP) AS review_answered_ts
  FROM bronze_reviews),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY review_answered_ts DESC, review_id) AS rn
  FROM typed WHERE review_score BETWEEN 1 AND 5)
SELECT review_id, order_id, review_score, review_created_ts, review_answered_ts FROM ranked WHERE rn = 1
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

save_table("silver_customers", """
SELECT customer_id, customer_unique_id, lpad(customer_zip_code_prefix, 5, '0') AS zip_prefix,
       customer_city AS city, upper(customer_state) AS state
FROM bronze_customers
""")
save_table("silver_sellers", """
SELECT seller_id, lpad(seller_zip_code_prefix, 5, '0') AS zip_prefix, seller_city AS city, upper(seller_state) AS state
FROM bronze_sellers
""")
save_table("silver_products", """
SELECT p.product_id,
       COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS category,
       TRY_CAST(p.product_weight_g AS INT) AS weight_g,
       TRY_CAST(p.product_photos_qty AS INT) AS photos_qty
FROM bronze_products p
LEFT JOIN bronze_category_translation t ON p.product_category_name = t.product_category_name
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
