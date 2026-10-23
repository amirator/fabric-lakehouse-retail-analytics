"""Generate the Direct Lake semantic model (TMDL, PBIP/Fabric git format) from the gold schema in DuckDB.

Column lists come from the real gold tables so the model cannot drift from the notebooks.
Measures are hand-written below and mirrored by SQL checks in tests/test_semantic_model.py.
"""
import hashlib, json, uuid
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[1]
SM = ROOT / "fabric" / "OlistRetail.SemanticModel"
TABLES = ["dim_date", "dim_customer", "dim_product", "dim_seller", "fact_orders", "fact_order_items"]
HIDDEN = {"customer_id", "product_id", "seller_id", "order_date"}
TYPE = {"VARCHAR": "string", "INTEGER": "int64", "BIGINT": "int64", "HUGEINT": "int64", "DATE": "dateTime",
        "TIMESTAMP": "dateTime", "DOUBLE": "double"}
RELATIONSHIPS = [  # (from table.col many side, to table.col one side)
    ("fact_orders", "customer_id", "dim_customer", "customer_id"),
    ("fact_orders", "order_date", "dim_date", "date"),
    ("fact_order_items", "product_id", "dim_product", "product_id"),
    ("fact_order_items", "seller_id", "dim_seller", "seller_id"),
    ("fact_order_items", "order_date", "dim_date", "date"),
]
MEASURES = {  # name: (DAX, format)
    "Orders": ("COUNTROWS ( fact_orders )", "#,0"),
    "Delivered Orders": ("SUM ( fact_orders[is_delivered] )", "#,0"),
    "GMV": ('CALCULATE ( SUM ( fact_orders[items_value] ), fact_orders[is_delivered] = 1 )', '"R$" #,0.00'),
    "AOV": ("DIVIDE ( [GMV], [Delivered Orders] )", '"R$" #,0.00'),
    "Freight": ('CALCULATE ( SUM ( fact_orders[freight_value] ), fact_orders[is_delivered] = 1 )', '"R$" #,0.00'),
    "Late Delivery %": ('CALCULATE ( AVERAGE ( fact_orders[is_late] ), fact_orders[is_delivered] = 1 )', "0.00%"),
    "Avg Delivery Days": ('CALCULATE ( AVERAGE ( fact_orders[delivery_days] ), fact_orders[is_delivered] = 1 )', "0.0"),
    "Avg Review Score": ('CALCULATE ( AVERAGE ( fact_orders[review_score] ), fact_orders[is_delivered] = 1 )', "0.00"),
    "Item GMV": ('CALCULATE ( SUM ( fact_order_items[price] ), fact_order_items[order_status] = "delivered" )', '"R$" #,0.00'),
    "GMV PY": ("CALCULATE ( [GMV], SAMEPERIODLASTYEAR ( dim_date[date] ) )", '"R$" #,0.00'),
    "GMV YoY %": ("DIVIDE ( [GMV] - [GMV PY], [GMV PY] )", "0.0%"),
    "Customers": ("DISTINCTCOUNT ( dim_customer[customer_unique_id] )", "#,0"),
}


def tag(*parts):
    return str(uuid.UUID(hashlib.md5("|".join(parts).encode()).hexdigest()))


def dtype(t):
    return "decimal" if t.startswith("DECIMAL") else TYPE[t]


def main():
    con = duckdb.connect(str(ROOT / "data" / "lakehouse.duckdb"), read_only=True)
    (SM / "definition" / "tables").mkdir(parents=True, exist_ok=True)
    (SM / ".platform").write_text(json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "SemanticModel", "displayName": "OlistRetail"},
        "config": {"version": "2.0", "logicalId": tag("sm")}}, indent=2) + "\n")
    (SM / "definition.pbism").write_text(json.dumps({"version": "4.2", "settings": {}}, indent=2) + "\n")
    d = SM / "definition"
    (d / "database.tmdl").write_text("database\n\tcompatibilityLevel: 1604\n")
    (d / "model.tmdl").write_text("model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: powerBI_V3\n"
                                  "\tsourceQueryCulture: en-US\n\tdataAccessOptions\n\t\tlegacyRedirects\n\t\treturnErrorValuesAsNull\n\n"
                                  + "".join(f"ref table {t}\n" for t in TABLES))
    (d / "expressions.tmdl").write_text(
        "expression DatabaseQuery =\n\t\tlet\n\t\t    database = Sql.Database(\"__SQL_ENDPOINT__\", \"__LAKEHOUSE_ID__\")\n"
        "\t\tin\n\t\t    database\n\tlineageTag: " + tag("expr") + "\n\n\tannotation PBI_IncludeFutureArtifacts = False\n")
    rel = ""
    for ft, fc, tt, tc in RELATIONSHIPS:
        rel += f"relationship {tag(ft, fc, tt)}\n\tfromColumn: {ft}.{fc}\n\ttoColumn: {tt}.{tc}\n\n"
    (d / "relationships.tmdl").write_text(rel)
    for t in TABLES:
        cols = con.execute(f"DESCRIBE {t}").fetchall()
        s = f"table {t}\n\tlineageTag: {tag(t)}\n"
        if t == "dim_date":
            s += "\tdataCategory: Time\n"
        s += "\n"
        if t == "fact_orders":
            for name, (dax, fmt) in MEASURES.items():
                s += f"\tmeasure '{name}' = {dax}\n\t\tformatString: {fmt}\n\t\tlineageTag: {tag(t, name)}\n\n"
        for c, ctype, *_ in cols:
            s += f"\tcolumn {c}\n\t\tdataType: {dtype(ctype)}\n"
            if t == "dim_date" and c == "date":
                s += "\t\tisKey\n"
            if t.startswith("fact") and c in HIDDEN:
                s += "\t\tisHidden\n"
            if dtype(ctype) in ("int64", "decimal", "double") and not c.endswith(("_key", "year", "month", "quarter")):
                s += "\t\tsummarizeBy: sum\n" if c in ("price", "freight_value", "items_value", "payment_value") else "\t\tsummarizeBy: none\n"
            else:
                s += "\t\tsummarizeBy: none\n"
            s += f"\t\tsourceColumn: {c}\n\t\tlineageTag: {tag(t, c)}\n\n"
        s += (f"\tpartition {t} = entity\n\t\tmode: directLake\n\t\tsource\n\t\t\tentityName: {t}\n"
              f"\t\t\tschemaName: dbo\n\t\t\texpressionSource: DatabaseQuery\n\n")
        (d / "tables" / f"{t}.tmdl").write_text(s)
    print(f"TMDL written: {len(TABLES)} tables, {len(RELATIONSHIPS)} relationships, {len(MEASURES)} measures")


if __name__ == "__main__":
    main()
