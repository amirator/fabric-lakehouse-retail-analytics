import json
from pathlib import Path
import duckdb, pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def con():
    c = duckdb.connect(str(ROOT / "data" / "lakehouse.duckdb"), read_only=True)
    yield c
    c.close()


@pytest.fixture(scope="session")
def kpis():
    return json.loads((ROOT / "kpis.json").read_text())


def one(con, sql):
    return con.execute(sql).fetchone()[0]
