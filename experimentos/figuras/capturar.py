"""
Figuras de la memoria a partir del cuadro de mando: sirve la carpeta dashboard/
en local, abre las tres páginas con Chromium (Playwright) y guarda en PNG, a
doble resolución, cada bloque que se cita en el documento. Así las figuras
salen siempre de los mismos datos que ve el usuario (dashboard/data) y se
regeneran con una orden cuando cambian.

    pip install playwright && python -m playwright install chromium
    python experimentos/figuras/capturar.py dashboard _runlogs/figuras [--ancho 1200] [--tema light|dark]

Qué deja (cuando la página tiene los datos):
- index_<n>_<distribuidora>.png: la sección de cada distribuidora del riesgo de mañana (cifras, mapa, perfil, líneas).
- index_seguimiento.png: la tabla de lo que dijimos cada tarde y lo que pasó con el viento.
- historial.png: el día a día desde 2022.
- negocio_<bloque>.png: cada sección de la página de cifras (director, guardia, por año, peores días,
  retribución, sector, producción, límites), con todas las distribuidoras y con cada una por separado.
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


async def capture(base: str, out: str, width: int, theme: str):
    from playwright.async_api import async_playwright

    os.makedirs(out, exist_ok=True)
    done = []
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context(viewport={"width": width, "height": 900}, device_scale_factor=2,
                                            color_scheme=theme, locale="es-ES")
        page = await context.new_page()
        errors = []
        page.on("pageerror", lambda e: errors.append(str(e)))

        async def shot(element, name):
            path = os.path.join(out, name + ".png")
            await element.scroll_into_view_if_needed()
            await page.wait_for_timeout(300)
            await element.screenshot(path=path)
            done.append(path)

        # The risk of tomorrow: one figure per distributor and the follow-up table.
        await page.goto(f"{base}/index.html", wait_until="networkidle")
        await page.wait_for_timeout(2500)
        for i, sec in enumerate(await page.query_selector_all("section.dist:not(.seguimiento)")):
            title = await sec.eval_on_selector("h2", "e => e.childNodes[0].textContent.trim()") if await sec.query_selector("h2") else f"seccion_{i}"
            await shot(sec, f"index_{i}_{slug(title)}")
        follow = await page.query_selector("section.seguimiento")
        if follow:
            await shot(follow, "index_seguimiento")

        # The day by day since 2022.
        await page.goto(f"{base}/historial.html", wait_until="networkidle")
        await page.wait_for_timeout(1500)
        main = await page.query_selector("main")
        if main:
            await shot(main, "historial")

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
        await browser.close()
    if errors:
        print("errores de JavaScript:", errors, file=sys.stderr)
    return done


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dashboard", help="carpeta dashboard/ con data/")
    parser.add_argument("salida", help="carpeta de los PNG")
    parser.add_argument("--ancho", type=int, default=1400)
    parser.add_argument("--tema", default="light", choices=["light", "dark"])
    parser.add_argument("--puerto", type=int, default=8791)
    args = parser.parse_args(argv)
    server = serve(os.path.abspath(args.dashboard), args.puerto)
    try:
        done = asyncio.run(capture(f"http://127.0.0.1:{args.puerto}", args.salida, args.ancho, args.tema))
    finally:
        server.shutdown()
    for path in done:
        print(path)
    print(f"{len(done)} figuras en {args.salida}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
