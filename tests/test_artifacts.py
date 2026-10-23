"""Fabric git-format, semantic model, web report and README checks."""
import json, re
from pathlib import Path
from conftest import ROOT, one

FABRIC = ROOT / "fabric"
SM = FABRIC / "OlistRetail.SemanticModel" / "definition"


def test_fabric_items_have_valid_platform_files():
    items = [p for p in FABRIC.iterdir() if p.is_dir()]
    assert {p.suffix for p in items} == {".Notebook", ".Lakehouse", ".SemanticModel"}
    ids = set()
    for p in items:
        meta = json.loads((p / ".platform").read_text())
        assert meta["metadata"]["type"] == p.suffix[1:] and meta["metadata"]["displayName"] == p.stem
        ids.add(meta["config"]["logicalId"])
    assert len(ids) == len(items)


def test_notebooks_are_fabric_source_format_and_valid_python():
    for nb in FABRIC.glob("*.Notebook"):
        src = (nb / "notebook-content.py").read_text()
        assert src.startswith("# Fabric notebook source")
        assert "synapse_pyspark" in src and "# CELL ********************" in src
        compile(src, nb.name, "exec")
        assert 'saveAsTable' in src  # Fabric path writes Delta tables


def test_tmdl_matches_gold_schema(con):
    model = (SM / "model.tmdl").read_text()
    for f in (SM / "tables").glob("*.tmdl"):
        t = f.stem
        assert f"ref table {t}" in model
        tmdl_cols = re.findall(r"^\tcolumn (\w+)$", f.read_text(), re.M)
        db_cols = [r[0] for r in con.execute(f"DESCRIBE {t}").fetchall()]
        assert tmdl_cols == db_cols, t
        assert "mode: directLake" in f.read_text()


def test_relationships_point_at_real_columns(con):
    rels = re.findall(r"fromColumn: (\w+)\.(\w+)\n\ttoColumn: (\w+)\.(\w+)", (SM / "relationships.tmdl").read_text())
    assert len(rels) == 5
    for ft, fc, tt, tc in rels:
        assert one(con, f"SELECT COUNT(*) - COUNT(DISTINCT {tc}) FROM {tt}") == 0  # one side unique


def test_dax_measures_present_and_mirror_sql(kpis, con):
    txt = (SM / "tables" / "fact_orders.tmdl").read_text()
    for m in ["GMV", "AOV", "Late Delivery %", "Avg Review Score", "GMV YoY %"]:
        assert f"measure '{m}'" in txt
    # SQL equivalent of the DAX GMV measure equals the headline figure
    assert round(float(one(con, "SELECT SUM(items_value) FROM fact_orders WHERE is_delivered = 1")), 2) == kpis["gmv"]


def test_web_report_data_and_offline_assets():
    html = (ROOT / "web" / "index.html").read_text()
    assert 'src="vendor/chart.umd.min.js"' in html and 'src="data/gold.js"' in html
    assert (ROOT / "web" / "vendor" / "chart.umd.min.js").stat().st_size > 100_000
    gold = json.loads((ROOT / "web" / "data" / "gold.json").read_text())
    assert {"headline", "gold_monthly_kpis", "gold_category_kpis", "gold_state_kpis"} <= set(gold)
    assert "not yet deployed to a Fabric tenant" in html


def test_screenshots_exist():
    for f in ["web-report.png", "kpi-cards.png"]:
        assert (ROOT / "docs" / "screenshots" / f).stat().st_size > 10_000


def test_readme_is_in_sync_with_kpis(kpis):
    import subprocess, sys
    readme = (ROOT / "README.md").read_text()
    out = subprocess.run([sys.executable, str(ROOT / "scripts" / "build_readme.py"), "--print"],
                         capture_output=True, text=True, check=True).stdout
    assert readme == out, "README drifted from kpis.json; run python scripts/build_readme.py"
    assert "Fabric-ready, verified locally; not yet deployed to a Fabric tenant" in readme
    assert f"{kpis['gmv']:,.2f}" in readme
