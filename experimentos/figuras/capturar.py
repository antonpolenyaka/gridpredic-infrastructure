"""
Figuras de la memoria a partir del cuadro de mando: sirve la carpeta dashboard/
en local, abre las cuatro páginas con Chromium (Playwright) y guarda en PNG, a
doble resolución, cada bloque que se cita en el documento. Así las figuras
salen siempre de los mismos datos que ve el usuario (dashboard/data) y se
regeneran con una orden cuando cambian.

    pip install playwright && python -m playwright install chromium
    python experimentos/figuras/capturar.py dashboard _runlogs/figuras [--ancho 1400] [--tema light|dark] [--dias 20260128,20251105]
    python experimentos/figuras/capturar.py dashboard _runlogs/figuras --memoria

Para la memoria, --memoria: una columna de 700 px (los bloques salen de unos 660 px CSS) y el texto más pequeño
(12 px) se imprime a unos 7,7 pt con la figura a 16 cm de ancho; con 860 px eran 6,6 pt y con 1400 px menos de 5.
Cada figura mide el texto visible más pequeño que lleva, lo pasa a puntos con el ancho de impresión (--cm, 16 por
defecto) y avisa si queda por debajo de --pt-min (7 por defecto); el resumen va a tamanos.txt en la carpeta de
salida. En las figuras no salen los controles de la página (botones, acciones, selectores y el zoom del mapa).

Qué deja (cuando la página tiene los datos):
- index_<n>_<distribuidora>.png: la sección de cada distribuidora del riesgo de mañana (cifras, mapa, perfil, líneas);
  con --dias, index_<día>_<n>_<distribuidora>.png para cada día archivado o repetido (riesgo_<día>.json).
- index_seguimiento.png: la tabla de lo que dijimos cada tarde y lo que pasó con el viento.
- dias_archivados.png: la tabla de los días archivados (dias.html).
- historial.png: el día a día desde 2022, e historial_<distribuidora>.png para cada red.
- negocio_<bloque>.png: cada sección de la página de cifras (director, guardia, por año, peores días,
  retribución, sector, producción, límites), con todas las distribuidoras y con cada una por separado.
- GridPredic en cifras.pdf: la página de cifras entera, con todas las distribuidoras, en A4.
"""

import argparse
import asyncio
import http.server
import os
import re
import sys
import threading
import unicodedata


def slug(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")[:40]


def serve(folder: str, port: int) -> http.server.ThreadingHTTPServer:
    handler = lambda *args, **kwargs: http.server.SimpleHTTPRequestHandler(*args, directory=folder, **kwargs)  # noqa: E731
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


async def capture(base: str, out: str, width: int, theme: str, days: list = None, cm: float = 16.0,
                  pt_min: float = 7.0):
    from playwright.async_api import async_playwright

    os.makedirs(out, exist_ok=True)
    done = []
    sizes = []  # (figure, css width, smallest text in px, smallest text printed in pt)
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        # Reduced motion: the sections appear without the animation of motion.js, so no block is captured half faded.
        context = await browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=2,
                                            color_scheme=theme, locale="es-ES", timezone_id="Europe/Madrid",
                                            reduced_motion="reduce")
        page = await context.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))
        # The menu is sticky on screen; in a figure of one block it would cover its title.
        # The controls (buttons, actions, selectors) are hidden only while a figure is taken (html.figura): the
        # script still uses the selector of negocio.html between figures.
        await context.add_init_script("document.addEventListener('DOMContentLoaded', () => { const s = document.createElement('style'); s.textContent = 'nav.menu { position: static !important; } html.figura .acciones, html.figura .noimprimir, html.figura .controles, html.figura .boton, html.figura .leaflet-control-zoom { display: none !important; }'; document.head.appendChild(s); });")

        async def shot(element, name):
            path = os.path.join(out, name + ".png")
            await element.scroll_into_view_if_needed()
            await page.evaluate("document.documentElement.classList.add('figura')")
            await page.wait_for_timeout(300)
            measure = await element.evaluate(SMALLEST_TEXT)
            await element.screenshot(path=path)
            await page.evaluate("document.documentElement.classList.remove('figura')")
            done.append(path)
            if measure and measure["px"]:
                pt = measure["px"] * (cm / 2.54 * 72) / measure["width"]
                sizes.append((name, measure["width"], measure["px"], pt))
                if pt < pt_min:
                    print(f"AVISO: {name}: texto de {measure['px']:g} px en {measure['width']:.0f} px de ancho, "
                          f"{pt:.1f} pt impreso a {cm:g} cm (mínimo {pt_min:g})", file=sys.stderr)

        # The risk of tomorrow: one figure per distributor and the follow-up table (wider, so the table of the
        # lines fits), and the same for every archived or replayed day asked with --dias. Under 1024 px (the
        # figures of the thesis) the page keeps that width: one column, and the table of the lines scrolls.
        await page.set_viewport_size({"width": width if width < 1024 else max(width, 1700), "height": 900})
        for day in [""] + list(days or []):
            query = f"?dia={day}" if day else ""
            await page.goto(f"{base}/index.html{query}", wait_until="networkidle")
            await page.wait_for_timeout(2500)
            prefix = f"index_{day}" if day else "index"
            for i, sec in enumerate(await page.query_selector_all("section.dist:not(.seguimiento)")):
                title = await sec.eval_on_selector("h2", "e => e.childNodes[0].textContent.trim()") if await sec.query_selector("h2") else f"seccion_{i}"
                await shot(sec, f"{prefix}_{i}_{slug(title)}")
            follow = await page.query_selector("section.seguimiento")
            if follow and not day:
                await shot(follow, "index_seguimiento")
        await page.set_viewport_size({"width": width, "height": 900})

        # The archive of the days (the table of the days with their levels), when the folder has its index.
        await page.goto(f"{base}/dias.html", wait_until="networkidle")
        await page.wait_for_timeout(1500)
        table = await page.query_selector("#dias")
        if table and await page.query_selector("#dias table"):
            await shot(table, "dias_archivados")

        # The day by day since 2022: the whole page and every distributor alone.
        await page.goto(f"{base}/historial.html", wait_until="networkidle")
        await page.wait_for_timeout(1500)
        main = await page.query_selector("main")
        if main:
            await shot(main, "historial")
        for sec in await page.query_selector_all("main section"):
            if not await sec.query_selector("h2"):
                continue
            title = await sec.eval_on_selector("h2", "e => e.textContent.trim()")
            await shot(sec, f"historial_{slug(title)}")

        # The figures page, all the distributors together and each one alone.
        await page.goto(f"{base}/negocio.html", wait_until="networkidle")
        await page.wait_for_timeout(1500)
        options = await page.eval_on_selector_all("#dist option", "els => els.map(e => [e.value, e.textContent])")
        for value, label in options:
            await page.select_option("#dist", value)
            await page.wait_for_timeout(900)
            suffix = "" if not value else "_" + slug(label)
            for sec in await page.query_selector_all("main section"):
                if await sec.evaluate("e => getComputedStyle(e).display === 'none' || e.classList.contains('noimprimir')"):
                    continue
                title = await sec.eval_on_selector("h2", "e => e.textContent.trim()") if await sec.query_selector("h2") else "bloque"
                await shot(sec, f"negocio_{slug(title)}{suffix}")
        # The figures page as a PDF to send (the print styles hide the controls).
        await page.select_option("#dist", "")
        await page.wait_for_timeout(600)
        await page.emulate_media(media="print")
        pdf = os.path.join(out, "GridPredic en cifras.pdf")
        await page.pdf(path=pdf, format="A4", print_background=True, margin={"top": "12mm", "bottom": "12mm", "left": "10mm", "right": "10mm"})
        done.append(pdf)
        await browser.close()
    if sizes:
        report = os.path.join(out, "tamanos.txt")
        with open(report, "w", encoding="utf-8") as f:
            f.write(f"figura\tancho_css_px\ttexto_min_px\ttexto_min_pt_a_{cm:g}_cm\n")
            for name, w, px, pt in sizes:
                f.write(f"{name}\t{w:.0f}\t{px:g}\t{pt:.1f}\n")
        bajos = [x for x in sizes if x[3] < pt_min]
        print(f"texto impreso más pequeño: {min(x[3] for x in sizes):.1f} pt; {len(bajos)} figuras por debajo de "
              f"{pt_min:g} pt (detalle en {report})", file=sys.stderr)
    if errors:
        print("errores de JavaScript:", errors, file=sys.stderr)
    return done


# The smallest visible text inside an element, in CSS px, and the width of the element. Only text that is rendered
# (not hidden, with some size and opacity); the labels of the map tiles and the attribution count as well.
SMALLEST_TEXT = """el => {
  const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
  let min = 0;
  while (walker.nextNode()) {
    const node = walker.currentNode;
    if (!node.textContent.trim()) continue;
    const p = node.parentElement;
    const st = getComputedStyle(p);
    if (st.display === 'none' || st.visibility === 'hidden' || parseFloat(st.opacity) === 0) continue;
    const r = p.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) continue;
    const px = parseFloat(st.fontSize);
    if (px > 0 && (min === 0 || px < min)) min = px;
  }
  return {px: min, width: el.getBoundingClientRect().width};
}"""


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dashboard", help="carpeta dashboard/ con data/")
    parser.add_argument("salida", help="carpeta de los PNG")
    parser.add_argument("--ancho", type=int, default=1400)
    parser.add_argument("--memoria", action="store_true", help="las figuras de la memoria: --ancho 700")
    parser.add_argument("--cm", type=float, default=16.0, help="ancho de la figura impresa, para medir el texto")
    parser.add_argument("--pt-min", type=float, default=7.0, help="tamaño mínimo del texto impreso (aviso)")
    parser.add_argument("--tema", default="light", choices=["light", "dark"])
    parser.add_argument("--puerto", type=int, default=8791)
    parser.add_argument("--dias", default="", help="días archivados o repetidos a capturar (YYYYMMDD, separados por comas)")
    args = parser.parse_args(argv)
    if args.memoria:
        args.ancho = 700
    server = serve(os.path.abspath(args.dashboard), args.puerto)
    try:
        days = [d.strip() for d in args.dias.split(",") if d.strip()]
        done = asyncio.run(capture(f"http://127.0.0.1:{args.puerto}", args.salida, args.ancho, args.tema, days,
                                   args.cm, args.pt_min))
    finally:
        server.shutdown()
    for path in done:
        print(path)
    print(f"{len(done)} figuras en {args.salida}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
