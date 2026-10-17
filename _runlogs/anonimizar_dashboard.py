"""Deja los JSON del cuadro de mando con los nombres anonimos de las distribuidoras.

Los jobs escriben desde la 0.15.0 los nombres de riesgo_manana.nombres de config_ml.json
(Distribuidora 1, 2 y 3); este script pasa los ficheros ya escritos con los nombres de las
bases a los mismos nombres, sin tocar los municipios ni las lineas. La correspondencia no se
versiona: la lee de _runlogs/anonimizar_nombres.json, un diccionario {"nombre": "Distribuidora N"}
que solo existe en el portatil.

Uso:
    python _runlogs/anonimizar_dashboard.py D:\\GitHub\\gridpredic-infrastructure\\dashboard\\data
"""
import glob
import json
import os
import sys

NOMBRES_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "anonimizar_nombres.json")
NOMBRES = {}


def rename(value, counter):
    new = NOMBRES.get(value)
    if new is None:
        return value
    counter[0] += 1
    return new


def walk(obj, counter, in_distribuidoras=False):
    """Cambia 'nombre' de los elementos de cualquier lista 'distribuidoras' y 'distribuidora' en cualquier dict."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "distribuidora" and isinstance(value, str):
                obj[key] = rename(value, counter)
            elif key == "nombre" and in_distribuidoras and isinstance(value, str):
                obj[key] = rename(value, counter)
            elif key == "distribuidoras" and isinstance(value, list):
                for item in value:
                    walk(item, counter, True)
            else:
                walk(value, counter, False)
    elif isinstance(obj, list):
        for item in obj:
            walk(item, counter, in_distribuidoras)


def main(folder):
    if not os.path.exists(NOMBRES_JSON):
        sys.exit(f"Falta {NOMBRES_JSON} con la correspondencia de nombres (no se versiona).")
    with open(NOMBRES_JSON, encoding="utf-8") as handle:
        NOMBRES.update(json.load(handle))
    files = sorted(glob.glob(os.path.join(folder, "*.json")))
    for path in files:
        with open(path, encoding="utf-8") as handle:
            try:
                data = json.load(handle)
            except json.JSONDecodeError as error:
                print(f"{os.path.basename(path)}: no es JSON ({error}); se deja")
                continue
        counter = [0]
        walk(data, counter)
        if counter[0]:
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(data, handle, ensure_ascii=False, separators=(",", ":"))
        print(f"{os.path.basename(path)}: {counter[0]} nombres")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.join("dashboard", "data"))
