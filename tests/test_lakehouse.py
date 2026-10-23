"""Data-validation tests on the locally built lakehouse (run `python run.py` first)."""
import pytest
from conftest import one

# Row counts pinned to the official Olist dataset description (Kaggle / olist/work-at-olist-data).
OFFICIAL = {"bronze_orders": 99441, "bronze_order_items": 112650, "bronze_payments": 103886,
            "bronze_reviews": 99224, "bronze_customers": 99441, "bronze_products": 32951,
            "bronze_sellers": 3095, "bronze_category_translation": 71}


@pytest.mark.parametrize("table,n", OFFICIAL.items())
def test_bronze_matches_official_counts(con, table, n):
    assert one(con, f"SELECT COUNT(*) FROM {table}") == n


def test_bronze_keeps_lineage(con):
    assert one(con, "SELECT COUNT(*) FROM bronze_orders WHERE _source_file IS NULL OR _ingested_at IS NULL") == 0


@pytest.mark.parametrize("table,key", [("silver_orders", "order_id"), ("silver_customers", "customer_id"),
    ("silver_products", "product_id"), ("silver_sellers", "seller_id"), ("silver_reviews", "order_id"),
    ("silver_order_items", "order_id, order_item_id"), ("dim_date", "date"), ("fact_orders", "order_id")])
def test_unique_keys(con, table, key):
    assert one(con, f"SELECT COUNT(*) - COUNT(DISTINCT ({key})) FROM {table}") == 0


def test_typed_casts_lost_nothing(con):
    assert one(con, "SELECT COUNT(*) FROM silver_orders WHERE purchase_ts IS NULL") == 0
    assert one(con, "SELECT COUNT(*) FROM silver_order_items WHERE price IS NULL OR price <= 0") == 0
    assert one(con, "SELECT COUNT(*) FROM silver_payments WHERE payment_value IS NULL") == 0


def test_review_dedup_anomaly_pinned(con):
    # Real source anomaly: 1,098 review rows share an order with another review -> 98,673 orders reviewed.
    assert one(con, "SELECT COUNT(*) FROM bronze_reviews WHERE order_id IN "
                    "(SELECT order_id FROM bronze_reviews GROUP BY 1 HAVING COUNT(*) > 1)") == 1098
    assert one(con, "SELECT COUNT(*) FROM silver_reviews") == 98673


def test_orders_without_items_pinned(con):
    assert one(con, "SELECT COUNT(*) FROM fact_orders WHERE item_count = 0") == 775


def test_untranslated_categories_kept(con):
    cats = {r[0] for r in con.execute("SELECT category FROM silver_products").fetchall()}
    assert {"unknown", "pc_gamer", "portateis_cozinha_e_preparadores_de_alimentos"} <= cats


@pytest.mark.parametrize("fact,col,dim,dcol", [("fact_orders", "customer_id", "dim_customer", "customer_id"),
    ("fact_orders", "order_date", "dim_date", "date"), ("fact_order_items", "product_id", "dim_product", "product_id"),
    ("fact_order_items", "seller_id", "dim_seller", "seller_id"), ("fact_order_items", "order_date", "dim_date", "date")])
def test_foreign_keys_resolve(con, fact, col, dim, dcol):
    assert one(con, f"SELECT COUNT(*) FROM {fact} f LEFT JOIN {dim} d ON f.{col} = d.{dcol} WHERE d.{dcol} IS NULL") == 0


def test_dim_date_has_no_gaps(con):
    assert one(con, "SELECT COUNT(*) FROM dim_date") == one(con, "SELECT date_diff('day', MIN(date), MAX(date)) + 1 FROM dim_date")


def test_facts_reconcile_to_silver(con):
    assert one(con, "SELECT SUM(price) FROM fact_order_items") == one(con, "SELECT SUM(price) FROM silver_order_items")
    assert one(con, "SELECT SUM(items_value) FROM fact_orders") == one(con, "SELECT SUM(price) FROM silver_order_items")
    assert one(con, "SELECT SUM(payment_value) FROM fact_orders") == one(con, "SELECT SUM(payment_value) FROM silver_payments")


def test_gold_parts_sum_to_total(con, kpis):
    for t in ["gold_monthly_kpis", "gold_category_kpis", "gold_state_kpis"]:
        assert abs(float(one(con, f"SELECT SUM(gmv) FROM {t}")) - kpis["gmv"]) < 1.0, t
    assert one(con, "SELECT SUM(orders) FROM gold_monthly_kpis") == kpis["orders"]
    assert one(con, "SELECT SUM(delivered_orders) FROM gold_state_kpis") == kpis["delivered_orders"]


def test_late_flag_only_on_delivered(con):
    assert one(con, "SELECT COUNT(*) FROM fact_orders WHERE delivery_days IS NULL AND is_late IS NOT NULL") == 0
