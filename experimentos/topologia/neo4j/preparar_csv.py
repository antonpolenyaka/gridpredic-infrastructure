"""
Prepara los CSV del grafo de la red para Neo4j (D30): los CT del estudio, sus cabeceras, líneas, municipios y
posiciones aguas arriba, y las incidencias de Calser con los CT que cortaron. Lee la misma exportación de Gold
que el resto de guiones de esta carpeta (zonas_20261013_1041) y escribe solo seudónimos:

- las redes como Distribuidora 1, 2 y 3;
- cada municipio con el seudónimo de la demo pública (_runlogs/demo_seudonimos.json, que no se versiona);
- los CT, las cabeceras, las líneas, las posiciones y las incidencias numerados por orden, sin su código real;
- sin coordenadas, nombres de elementos ni textos libres de las incidencias.

    python preparar_csv.py <exportación> <demo_seudonimos.json> <carpeta de salida>

Al acabar busca en los CSV los códigos y nombres reales de entrada y para con un error si encuentra alguno.
"""
import csv
import json
import os
import sys

import pandas as pd

EXP, SEUD, OUT = sys.argv[1], sys.argv[2], sys.argv[3]
RED = {3: 1, 2: 2, 1366: 3}  # distribuidora_id -> número de la Distribuidora N
os.makedirs(OUT, exist_ok=True)


def escribir(nombre, df):
    df.to_csv(os.path.join(OUT, nombre), index=False, quoting=csv.QUOTE_MINIMAL, date_format="%Y-%m-%dT%H:%M:%S")
    print(f"{nombre}: {len(df)} filas")


def numerar(valores, prefijo):
    """Número de orden estable para cada valor distinto (por orden de primera aparición tras ordenar)."""
    unicos = sorted(set(valores))
    return {v: f"{prefijo} {i:04d}" for i, v in enumerate(unicos, start=1)}


seud = json.load(open(SEUD, encoding="utf-8"))["municipios"]
muni_seud = {v["codigo_real"]: v["seudonimo"] for v in seud.values()}

dim = pd.read_parquet(f"{EXP}/dim_ct.parquet")
dim = dim[dim.en_estudio].copy()
cel = pd.read_parquet(f"{EXP}/map_ct_celda.parquet")[["distribuidora_id", "ct_id", "codigo_ine", "celda_id"]]
dim = dim.merge(cel, on=["distribuidora_id", "ct_id"], how="left")
dim["red"] = dim.distribuidora_id.map(RED)
falta = set(dim.codigo_ine) - set(muni_seud)
if falta:
    sys.exit(f"ERROR: {len(falta)} municipios sin seudónimo en {SEUD}")

# Claves seudónimas por red
ct_cod, cab_cod, lin_cod = {}, {}, {}
for red, g in dim.groupby("red"):
    ct_cod.update({(red, k): v for k, v in numerar(g.ct_id, "CT").items()})
    cab_cod.update({(red, k): v for k, v in numerar(g.cabecera_id.dropna().astype(int), "Cabecera").items()})
    lin_cod.update({(red, k): v for k, v in numerar(g.grupo_red_id.dropna().astype(int), "Línea").items()})

cabecera_real = set(dim.cabecera.dropna().astype(str))
dim["codigo"] = [ct_cod[(r, c)] for r, c in zip(dim.red, dim.ct_id)]
dim["municipio"] = dim.codigo_ine.map(muni_seud)
dim["cabecera"] = [cab_cod.get((r, int(c))) if pd.notna(c) else None for r, c in zip(dim.red, dim.cabecera_id)]
dim["linea"] = [lin_cod.get((r, int(c))) if pd.notna(c) else None for r, c in zip(dim.red, dim.grupo_red_id)]
zona = {"TZ_URBAN": "urbana", "TZ_SUBUR": "semiurbana", "TZ_RUCON": "rural concentrada", "TZ_RUDIS": "rural dispersa"}

escribir("distribuidoras.csv", pd.DataFrame({"red": [1, 2, 3], "nombre": [f"Distribuidora {n}" for n in (1, 2, 3)]}))
escribir("municipios.csv", dim[["municipio"]].drop_duplicates().sort_values("municipio"))  # el seudónimo es único entre redes
escribir("cts.csv", pd.DataFrame({
    "red": dim.red, "codigo": dim.codigo, "municipio": dim.municipio, "cabecera": dim.cabecera, "linea": dim.linea,
    "zona": dim.tipo_zona_estat.map(zona), "kva": dim.potencia_tiepi_kva.fillna(0.0), "abonados": dim.num_abonados,
    "salidas": dim.n_salidas.fillna(0).astype(int), "telemetria": dim.tiene_telemetria,
    "vigente_desde": dim.vigente_desde, "vigente_hasta": dim.vigente_hasta_excl,
}).sort_values(["red", "codigo"]))

# Posiciones aguas arriba: una relación por CT y posición, con los cortes que las unen
aa = pd.read_parquet(f"{EXP}/map_aguas_arriba.parquet")
aa["red"] = aa.distribuidora_id.map(RED)
aa = aa[[(r, c) in ct_cod for r, c in zip(aa.red, aa.ct_id)]]
pos_cod = {}
for red, g in aa.groupby("red"):
    pos_cod.update({(red, k): v for k, v in numerar(g.posicion_id, "Posición").items()})
rel = (aa.groupby(["red", "ct_id", "posicion_id"])
       .agg(cortes=("n_cortes", "sum"), primer_corte=("primer_corte_ts", "min"), ultimo_corte=("ultimo_corte_ts", "max"))
       .reset_index())
rel["ct"] = [ct_cod[(r, c)] for r, c in zip(rel.red, rel.ct_id)]
rel["posicion"] = [pos_cod[(r, p)] for r, p in zip(rel.red, rel.posicion_id)]
escribir("posiciones.csv", rel[["red", "posicion"]].drop_duplicates().sort_values(["red", "posicion"]))
escribir("ct_posicion.csv", rel[["red", "ct", "posicion", "cortes", "primer_corte", "ultimo_corte"]])

# Incidencias de Calser desde 2016 (como comun.py: sin el apagón ni el 28.04.2025) y los CT que cortaron
it = pd.read_parquet(f"{EXP}/interrupciones.parquet")
it = it[(it.inicio_ts >= "2016-01-01") & (it.causa != "apagon") &
        (it.inicio_ts.dt.normalize() != pd.Timestamp("2025-04-28"))].copy()
it["red"] = it.distribuidora_id.map(RED)
it = it[[(r, c) in ct_cod for r, c in zip(it.red, it.ct_id)]]
key = ["red", "periodo_id", "incidencia_id"]
inc = (it.groupby(key).agg(inicio=("inicio_ts", "min"), fin=("fin_ts", "max"), causa=("causa", "first"),
                          programada=("causa", lambda s: (s == "programada").any()), cts=("ct_id", "nunique"))
       .reset_index().sort_values(["red", "inicio"]))
inc["codigo"] = [f"Incidencia {i:05d}" for i in range(1, len(inc) + 1)]
inc["fecha"] = inc.inicio.dt.date
inc_cod = {tuple(k): c for k, c in zip(inc[key].itertuples(index=False), inc.codigo)}
escribir("incidencias.csv", inc[["red", "codigo", "inicio", "fin", "fecha", "causa", "programada", "cts"]])
cor = (it.groupby(key + ["ct_id"]).agg(inicio=("inicio_ts", "min"), duracion_s=("duracion_s", "sum")).reset_index())
cor["incidencia"] = [inc_cod[(r, p, i)] for r, p, i in zip(cor.red, cor.periodo_id, cor.incidencia_id)]
cor["ct"] = [ct_cod[(r, c)] for r, c in zip(cor.red, cor.ct_id)]
escribir("incidencia_ct.csv", cor[["red", "incidencia", "ct", "inicio", "duracion_s"]])

# Nada de lo real puede quedar en los CSV
reales = set(dim.ct_id.astype(str)) | set(dim.municipio_nombre.astype(str)) | cabecera_real
reales |= set(aa.elemento_corte_nombre.dropna().astype(str)) | set(dim.codigo_ine.astype(str))
reales |= {"EOSA", "Pitarch", "Valle de Santa Ana", "Calser_"}
reales = {r for r in reales if len(r) >= 4}
NUMERICAS = {"red", "kva", "abonados", "salidas", "cortes", "duracion_s", "cts"}
for f in sorted(os.listdir(OUT)):
    with open(os.path.join(OUT, f), encoding="utf-8") as fh:
        filas = list(csv.reader(fh))
    # las columnas de cantidades pueden coincidir por azar con un código numérico de CT
    texto = [j for j, h in enumerate(filas[0]) if h not in NUMERICAS]
    campos = {fila[j] for fila in filas[1:] for j in texto}
    malos = campos & reales
    if malos:
        sys.exit(f"ERROR: {f} lleva {len(malos)} valores reales")
print("comprobado: ningún código ni nombre real en los CSV")
