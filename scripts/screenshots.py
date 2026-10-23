"""Render web/index.html (file://) to PNG with headless Chromium via Playwright."""
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "screenshots"


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={"width": 1366, "height": 900})
        errors = []
        pg.on("pageerror", lambda e: errors.append(str(e)))
        pg.goto((ROOT / "web" / "index.html").as_uri())
        pg.wait_for_timeout(800)
        if errors:
            raise SystemExit(f"JS errors: {errors}")
        pg.screenshot(path=str(OUT / "web-report.png"), full_page=True)
        pg.locator(".cards").screenshot(path=str(OUT / "kpi-cards.png"))
        b.close()
    print("screenshots written to", OUT)


if __name__ == "__main__":
    main()
