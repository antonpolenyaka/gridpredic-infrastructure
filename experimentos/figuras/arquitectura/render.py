"""Render fig01_arquitectura.html to PNG with Playwright and print layout checks.

Usage: python3 render.py [scale]
The page draws its arrows with JS and exposes window.__diag() with the checks.
"""
import json
import os
import sys

from playwright.sync_api import sync_playwright

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "fig01_arquitectura.html")
OUT = os.path.join(HERE, "fig01_arquitectura.png")
W, H = 1200, 935
PRINT_WIDTH_CM = 16.0


def main():
    scale = float(sys.argv[1]) if len(sys.argv) > 1 else 3.0
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": W, "height": H}, device_scale_factor=scale)
        page.goto("file://" + SRC)
        page.wait_for_function("window.__ready === true", timeout=10000)
        page.wait_for_timeout(200)
        diag = page.evaluate("window.__diag()")
        page.screenshot(path=OUT, clip={"x": 0, "y": 0, "width": W, "height": H})
        fonts = page.evaluate("""() => [document.fonts.check("19px 'Liberation Sans'"),
            getComputedStyle(document.body).fontFamily]""")
        browser.close()

    # DPI so that Word places the image at the print width by default
    from PIL import Image
    dpi = W * scale / (PRINT_WIDTH_CM / 2.54)
    Image.open(OUT).save(OUT, dpi=(dpi, dpi), optimize=True)
    print("font available:", fonts, "| png dpi %.0f" % dpi)

    pt_per_px = PRINT_WIDTH_CM / 2.54 * 72 / W
    print("canvas %dx%d CSS px, scale %.1f -> %dx%d px" % (W, H, scale, W * scale, H * scale))
    print("smallest font %.2f px = %.2f pt at %.0f cm wide (%s)" % (
        diag["minFont"], diag["minFont"] * pt_per_px, PRINT_WIDTH_CM, diag["minFontEl"]))
    print("content bottom: %.1f px (canvas %d)" % (diag["contentBottom"], H))
    for key in ("overflow", "overlaps", "outside", "labelHits"):
        items = diag[key]
        print("%s: %d" % (key, len(items)))
        for it in items:
            print("   ", json.dumps(it, ensure_ascii=False))
    if diag.get("anchors"):
        print("anchors (top, cy, bottom):", json.dumps(diag["anchors"]))


if __name__ == "__main__":
    main()
