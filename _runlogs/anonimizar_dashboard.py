"""Deja los JSON del cuadro de mando con los nombres anonimos de las distribuidoras.

Los jobs escriben desde la 0.15.0 los nombres de riesgo_manana.nombres de config_ml.json
(Distribuidora 1, 2 y 3); este script pasa los ficheros ya escritos con los nombres reales
a los mismos nombres, sin tocar los municipios ni las lineas (Valle de Santa Ana es tambien
un municipio y se queda tal cual donde es un lugar).

Uso:
    python _runlogs/anonimizar_dashboard.py D:\\GitHub\\gridpredic-infrastructure\\dashboard\\data
"""
import glob
import json
import os
import sys

NOMBRES = {
    "EOSA": "Distribuidora 1", "Calser_EOSA": "Distribuidora 1",
    "Pitarch": "Distribuidora 2", "Calser_Pitarch": "Distribuidora 2",
    "Valle de Santa Ana": "Distribuidora 3", "Calser_ValleSantaAna": "Distribuidora 3", "ValleSantaAna": "Distribuidora 3",
}


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
