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

# ## Bronze: raw Olist CSVs to Delta tables
# Source: `Files/raw/olist/*.csv` (uploaded to the lakehouse) or `data/raw` locally.
# Every column is kept as a string, plus lineage columns `_source_file` and `_ingested_at`.
# No business rules here: bronze is a faithful, replayable copy.

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

SOURCES = {
    "bronze_orders": "olist_orders_dataset.csv",
    "bronze_order_items": "olist_order_items_dataset.csv",
    "bronze_payments": "olist_order_payments_dataset.csv",
    "bronze_reviews": "olist_order_reviews_dataset.csv",
    "bronze_customers": "olist_customers_dataset.csv",
    "bronze_products": "olist_products_dataset.csv",
    "bronze_sellers": "olist_sellers_dataset.csv",
    "bronze_category_translation": "product_category_name_translation.csv",
}
try:
    RAW_DIR  # injected locally
except NameError:
    RAW_DIR = "Files/raw/olist"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone

INGESTED_AT = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
bronze_counts = {}
for table, file in SOURCES.items():
    path = f"{RAW_DIR}/{file}"
    if ENGINE == "spark":
        from pyspark.sql import functions as F
        df = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')  # noqa: F821
              .option("inferSchema", False).csv(path)
              .withColumn("_source_file", F.lit(file))
              .withColumn("_ingested_at", F.lit(INGESTED_AT).cast("timestamp")))
        df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table)
        bronze_counts[table] = spark.table(table).count()  # noqa: F821
        print(f"{table}: {bronze_counts[table]:,} rows")
    else:
        bronze_counts[table] = save_table(table, f"""
            SELECT *, '{file}' AS _source_file, TIMESTAMP '{INGESTED_AT}' AS _ingested_at
            FROM read_csv('{path}', header=true, all_varchar=true, quote='"', escape='"')""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
