"""
Monta la carpeta que se publica en gridpredic.com: la página pública en la
raíz y el cuadro de mando en /demo/ con la copia congelada de sus datos.

    python web/montar_sitio.py _site                      # la carpeta lista para servir
    python web/montar_sitio.py _site --commit abc1234     # con el commit en version.json
    python -m http.server 8795 --directory _site          # para verla en local

Lo que hace, sin ninguna herramienta de construcción:

- copia web/ (index.html, gridpredic-web.css, web.js, img/, vendor/) a la raíz,
  menos este script, el de congelar la demo, el README y la carpeta demo/;
- copia dashboard/ (las cuatro páginas, gridpredic.css, motion.js, demo.js y
  vendor/) a demo/, y encima web/demo/data/ como demo/data/: la copia de los
  JSON y los partes de guardia que se congeló con web/congelar_demo.ps1
  (dashboard/data no se versiona; lo escriben los jobs cada tarde);
- escribe version.json con el commit y la hora, que el workflow de GitHub
  Pages lee después del despliegue para comprobar que la web se ha
  actualizado; CNAME con el dominio, y .nojekyll para que GitHub sirva los
  ficheros tal cual.

El workflow .github/workflows/pages.yml lo ejecuta en cada push a main.
"""

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DOMAIN = "gridpredic.com"
WEB_SKIP = {"montar_sitio.py", "congelar_demo.ps1", "README.md", "demo"}
DASHBOARD_SKIP = {"data"}
REQUIRED_DATA = ("riesgo.json", "indice.json", "historico.json", "sector.json", "verificacion.json")


def copy_tree(source: str, target: str, skip: set) -> int:
    count = 0
    for name in sorted(os.listdir(source)):
        if name in skip or name.startswith("."):
            continue
        path = os.path.join(source, name)
        dest = os.path.join(target, name)
        if os.path.isdir(path):
            count += copy_tree(path, dest, set())
        else:
            os.makedirs(target, exist_ok=True)
            shutil.copy2(path, dest)
            count += 1
    return count


def build(out: str, commit: str) -> dict:
    if os.path.isdir(out):
        shutil.rmtree(out)
    os.makedirs(out)
    copied = {"web": copy_tree(os.path.join(ROOT, "web"), out, WEB_SKIP)}
    demo = os.path.join(out, "demo")
    copied["demo"] = copy_tree(os.path.join(ROOT, "dashboard"), demo, DASHBOARD_SKIP)
    data = os.path.join(ROOT, "web", "demo", "data")
    missing = [name for name in REQUIRED_DATA if not os.path.exists(os.path.join(data, name))]
    if missing:
        sys.exit(f"Faltan en web/demo/data: {', '.join(missing)}; congelar la copia con web/congelar_demo.ps1")
    copied["datos"] = copy_tree(data, os.path.join(demo, "data"), set())
    version = {"commit": commit or "local", "generado": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "dominio": DOMAIN}
    with open(os.path.join(out, "version.json"), "w", encoding="utf-8") as handle:
        json.dump(version, handle, ensure_ascii=False, indent=2)
    with open(os.path.join(out, "CNAME"), "w", encoding="utf-8") as handle:
        handle.write(DOMAIN + "\n")
    open(os.path.join(out, ".nojekyll"), "w").close()
    for required in ("index.html", os.path.join("demo", "index.html"), os.path.join("demo", "data", "riesgo.json")):
        if not os.path.exists(os.path.join(out, required)):
            sys.exit(f"El sitio no tiene {required}")
    return copied


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Monta la carpeta de gridpredic.com (página pública y demo).")
    parser.add_argument("out", help="carpeta de salida (se vacía antes)")
    parser.add_argument("--commit", default=os.environ.get("GITHUB_SHA", ""), help="commit para version.json")
    args = parser.parse_args(argv)
    copied = build(os.path.abspath(args.out), args.commit)
    print(f"Sitio en {os.path.abspath(args.out)}: {copied['web']} ficheros de la página, {copied['demo']} del cuadro "
          f"de mando y {copied['datos']} de datos de la demo; commit {args.commit or 'local'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
