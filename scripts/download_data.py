"""Fetch the Olist public dataset (publisher-owned GitHub mirror) into data/raw.

Skips any file whose SHA-256 already matches data/manifest.json.
Set OLIST_CACHE=/path/to/raw to copy from a local cache instead of downloading.
"""
import hashlib, json, os, shutil, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
NEEDED = ["olist_customers_dataset.csv", "olist_order_items_dataset.csv", "olist_order_payments_dataset.csv",
          "olist_order_reviews_dataset.csv", "olist_orders_dataset.csv", "olist_products_dataset.csv",
          "olist_sellers_dataset.csv", "product_category_name_translation.csv"]


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def main():
    RAW.mkdir(parents=True, exist_ok=True)
    manifest = {m["file"]: m for m in json.loads((ROOT / "data" / "manifest.json").read_text())}
    cache = os.environ.get("OLIST_CACHE")
    for f in NEEDED:
        m, dest = manifest[f], RAW / f
        if dest.exists() and sha(dest) == m["sha256"]:
            continue
        if cache and (Path(cache) / f).exists():
            shutil.copy(Path(cache) / f, dest)
        else:
            print("downloading", m["url"])
            urllib.request.urlretrieve(m["url"], dest)
        if sha(dest) != m["sha256"]:
            raise SystemExit(f"checksum mismatch for {f}")
    print(f"raw data OK: {len(NEEDED)} files verified by SHA-256")


if __name__ == "__main__":
    main()
