"""
Las capas de arbolado de alta resolucion de Copernicus (HRL Tree Cover and
Forests: Tree Cover Density y Dominant Leaf Type de 2018, 10 m) para las celdas
de las tres redes, por la API de descargas del CLMS (land.copernicus.eu/api).

La API pide una clave de servicio: en land.copernicus.eu, con EU Login,
My settings -> API Tokens -> Create new Token, y el JSON que da se guarda tal
cual fuera del repositorio (por defecto D:\\DockerData\\arbolado\\clms_key.json).
El script nunca escribe ni imprime la clave.

Pasos (cada uno se puede repetir):

    python bajar_hrl.py buscar   "Tree Cover Density 2018"   # UID e ids de descarga de los productos
    python bajar_hrl.py pedir    <UID> <id de descarga> [anio] # pide el recorte de la caja de las redes
    python bajar_hrl.py estado                                 # en que va cada peticion y su enlace
    python bajar_hrl.py bajar    <url> <fichero>               # baja el zip preparado

Necesita pyjwt y cryptography en el venv del arbolado.
"""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

API = "https://land.copernicus.eu/api"
KEY = os.environ.get("CLMS_KEY", r"D:\DockerData\arbolado\clms_key.json")
# Caja de las 43 celdas de las redes con medio grado de margen (lon_min, lat_min, lon_max, lat_max), WGS84.
BOX = (-7.05, 38.25, -5.55, 40.45)


def access_token() -> str:
    import jwt  # pyjwt

    with open(KEY, encoding="utf-8") as handle:
        key = json.load(handle)
    now = int(time.time())
    claims = {"iss": key["client_id"], "sub": key["user_id"], "aud": key["token_uri"], "iat": now, "exp": now + 300}
    grant = jwt.encode(claims, key["private_key"], algorithm="RS256")
    body = urllib.parse.urlencode({"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                                   "assertion": grant}).encode()
    req = urllib.request.Request(key["token_uri"], data=body,
                                 headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.loads(response.read())["access_token"]


def call(path: str, token: str, body: dict = None, method: str = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(path if path.startswith("http") else f"{API}/{path}", data=data,
                                 method=method or ("POST" if body is not None else "GET"),
                                 headers={"Accept": "application/json", "Content-Type": "application/json",
                                          "Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            return json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        raise SystemExit(f"HTTP {error.code} en {path}: {error.read().decode('utf-8', 'replace')[:600]}") from error


def search(token: str, text: str):
    query = urllib.parse.urlencode([("portal_type", "DataSet"), ("SearchableText", text), ("fullobjects", "1"),
                                    ("b_size", "25")])
    found = call(f"@search?{query}", token)
    for item in found.get("items", []):
        info = item.get("dataset_download_information") or {}
        rows = info.get("items", []) if isinstance(info, dict) else info
        print(f"{item.get('title')} | UID {item.get('UID')}")
        for row in rows or []:
            print(f"    descarga {row.get('@id')} | {row.get('name') or ''} {row.get('collection') or ''} "
                  f"{row.get('full_format') or ''} {row.get('full_source') or ''}")


def request(token: str, uid: str, download_id: str, year: int = None):
    west, south, east, north = BOX
    item = {"DatasetID": uid, "DatasetDownloadInformationID": download_id, "OutputFormat": "Geotiff",
            "OutputGCS": "EPSG:4326", "BoundingBox": [north, east, south, west]}
    if year:
        # Los productos anuales (2018 en adelante) llevan la fecha del 1 de enero de su año: la API quiere ese día
        # como un rango en milisegundos desde 1970 (rechaza el año entero o la fecha como texto).
        start = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        end = start + 86_399_000
        item["TemporalFilter"] = {"StartDate": start, "EndDate": end}
    body = {"Datasets": [item]}
    print(json.dumps(call("@datarequest_post", token, body), indent=1)[:2000])


def status(token: str):
    found = call("@datarequest_search", token)
    for task_id, task in (found.items() if isinstance(found, dict) else enumerate(found)):
        if not isinstance(task, dict):
            continue
        print(task_id, task.get("Status"), task.get("RegistrationDateTime"), task.get("DownloadURL") or "",
              [d.get("DatasetTitle") for d in task.get("Datasets", []) if isinstance(d, dict)])


def download(url: str, path: str):
    urllib.request.urlretrieve(url, path)
    print("bajado", path, os.path.getsize(path), "bytes")


def main(argv):
    if not argv:
        raise SystemExit(__doc__)
    step = argv[0]
    if step == "bajar":
        return download(argv[1], argv[2])
    token = access_token()
    if step == "buscar":
        search(token, " ".join(argv[1:]) or "Tree Cover Density")
    elif step == "pedir":
        request(token, argv[1], argv[2], int(argv[3]) if len(argv) > 3 else None)
    elif step == "estado":
        status(token)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
