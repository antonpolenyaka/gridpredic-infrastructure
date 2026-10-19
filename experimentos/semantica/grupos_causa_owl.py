"""
Los grupos de causas de GridPredic como un T-Box con inferencia RDFS / OWL RL (owlrl), sin servidor.

1. Lee del código del repositorio (con ast, sin importar ni ejecutar nada) las definiciones de grupos de
   causas que usan los trabajos: LOCAL_EXCLUDED, CAUSE_FAMILIES y las listas en línea de riesgo_zonas.py,
   causas_no_meteo de job_ml_riesgo_dia.py, job_ml_meteo_historia.py y config_ml.json, familias_causa de
   job_ml_riesgo_ct.py y CAUSAS de causas_incidencia.py y job_causas_incidencias.py.
2. Compara los conjuntos (inclusiones, nombres iguales con miembros distintos).
3. Escribe un T-Box (una clase por familia y una por grupo, familia rdfs:subClassOf grupo) y tipa cada
   incidencia de la exportación solo con su familia. Cuenta los miembros de cada grupo: con pandas como en
   el código, con SPARQL tras materializar RDFS (owlrl) y con SPARQL reescrito (rdfs:subClassOf*).
4. OWL RL: si los grupos de presentación de riesgo_zonas son una partición (owl:disjointWith) y el bloque
   "meteo" del riesgo anual se toma por la misma clase que la "meteorologia" que se ensena, que choca.
5. SKOS con tres niveles (subcausa, familia, bloque): skos:broaderTransitive con RDFS frente a OWL RL.

Uso: python grupos_causa_owl.py <repo> <carpeta zonas_20261013_1041> [salida.txt]
"""
import ast
import json
import os
import sys
import time

import owlrl
import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef  # noqa: F401
from rdflib.namespace import OWL, RDF, RDFS, SKOS

GPO = Namespace("https://gridpredic.com/ont#")
GP = Namespace("https://gridpredic.com/id/")
ERR = Namespace("http://www.daml.org/2002/03/agents/agent-ont#")


# ---------------------------------------------------------------------------
# 1. Definiciones leídas del código
# ---------------------------------------------------------------------------

def _module(path):
    with open(path, encoding="utf-8") as f:
        return ast.parse(f.read())


def constant(path, name, key=None):
    for node in ast.walk(_module(path)):
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            value = node.value
            if key is None:
                return ast.literal_eval(value), node.lineno
            for k, v in zip(value.keys, value.values):
                if isinstance(k, ast.Constant) and k.value == key:
                    return ast.literal_eval(v), v.lineno
    raise KeyError(f"{name}[{key}] no encontrado en {path}")


def inline_isin_lists(path):
    """Listas literales pasadas a .isin([...]) con nombres de causa (las de las líneas 395 y 440)."""
    out = []
    for node in ast.walk(_module(path)):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "isin":
            if node.args and isinstance(node.args[0], ast.List):
                vals = [e.value for e in node.args[0].elts if isinstance(e, ast.Constant)]
                if vals and all(isinstance(v, str) for v in vals):
                    out.append((vals, node.lineno))
    return out


def config_value(path, key):
    found = []

    def walk(o, p=""):
        if isinstance(o, dict):
            for k, v in o.items():
                if k == key:
                    found.append((p + "/" + k, v))
                walk(v, p + "/" + k)
    with open(path, encoding="utf-8") as f:
        walk(json.load(f))
    return found


def definiciones(repo):
    j = lambda *p: os.path.join(repo, *p)  # noqa: E731
    rz = j("etl", "jobs", "04_ml", "riesgo_zonas.py")
    d = {}
    d["CAUSAS (causas_incidencia.py)"], l1 = constant(j("etl", "jobs", "02_silver", "causas_incidencia.py"), "CAUSAS")
    d["CAUSAS (job_causas_incidencias.py)"], l2 = constant(j("etl", "jobs", "04_ml", "job_causas_incidencias.py"), "CAUSAS")
    loc, l3 = constant(rz, "LOCAL_EXCLUDED")
    fam, l4 = constant(rz, "CAUSE_FAMILIES")
    nm1, l5 = constant(j("etl", "jobs", "04_ml", "job_ml_riesgo_dia.py"), "DEFAULTS", "causas_no_meteo")
    nm2, l6 = constant(j("etl", "jobs", "04_ml", "job_ml_meteo_historia.py"), "DEFAULTS", "causas_no_meteo")
    nm3 = config_value(j("etl", "config", "04_ml", "config_ml.json"), "causas_no_meteo")
    bloques, l7 = constant(j("etl", "jobs", "04_ml", "job_ml_riesgo_ct.py"), "DEFAULTS", "familias_causa")
    inline = inline_isin_lists(rz)
    lineas = {"CAUSAS": (l1, l2), "LOCAL_EXCLUDED": l3, "CAUSE_FAMILIES": l4, "causas_no_meteo": (l5, l6),
              "familias_causa": l7, "isin en linea": [n for _, n in inline]}
    return d, loc, fam, (nm1, nm2, nm3), bloques, inline, lineas


# ---------------------------------------------------------------------------
# 2 - 5
# ---------------------------------------------------------------------------

def incidencias(base):
    it = pd.read_parquet(f"{base}/interrupciones.parquet")
    k = ["distribuidora_id", "periodo_id", "incidencia_id"]
    inc = it.groupby(k).agg(causa=("causa", "first"), subcausa=("subcausa", "first"),
                            inicio=("inicio_ts", "min")).reset_index()
    inc["n"] = range(1, len(inc) + 1)
    return inc


def main():
    repo, base = sys.argv[1], sys.argv[2]
    out_path = sys.argv[3] if len(sys.argv) > 3 else None
    lines = []
    p = lines.append

    d, loc_ex, fam_map, nomet, bloques, inline, lineas = definiciones(repo)
    causas = d["CAUSAS (causas_incidencia.py)"]
    p("1. Definiciones leídas del código (fichero: línea)")
    p(f"   CAUSAS iguales en los dos ficheros: {d['CAUSAS (causas_incidencia.py)'] == d['CAUSAS (job_causas_incidencias.py)']} "
      f"({len(causas)} familias; líneas {lineas['CAUSAS']})")
    p(f"   LOCAL_EXCLUDED (riesgo_zonas.py: {lineas['LOCAL_EXCLUDED']}): {list(loc_ex)}")
    p(f"   CAUSE_FAMILIES (riesgo_zonas.py: {lineas['CAUSE_FAMILIES']}): {fam_map}")
    p(f"   causas_no_meteo (job_ml_riesgo_dia.py, job_ml_meteo_historia.py: {lineas['causas_no_meteo']}) iguales: "
      f"{nomet[0] == nomet[1]}; en config_ml.json: {[k for k, _ in nomet[2]]}, iguales: "
      f"{all(v == nomet[0] for _, v in nomet[2])}")
    p(f"   familias_causa (job_ml_riesgo_ct.py: {lineas['familias_causa']}): {bloques}")
    p(f"   listas en línea de riesgo_zonas.py: {inline}")
    p("")

    todas = set(causas)
    grupos = {
        "CausaLocal": todas - set(loc_ex),                       # riesgo_zonas, caso de negocio
        "RelacionadaConTiempo": todas - set(nomet[0]),           # y_dia_meteo, meteo_historia
        "BloqueMeteoAnual": set(bloques["meteo"]),               # riesgo_ct
        "BloqueFaunaAnual": set(bloques["fauna"]),
        "BloqueEquipoAnual": set(bloques["equipo"]),
        "MeteoRetrasoCascada": set(inline[0][0]),                # riesgo_zonas l. 395 y 440
    }
    grupos["RestoAnual"] = todas - grupos["BloqueMeteoAnual"] - grupos["BloqueFaunaAnual"] - grupos["BloqueEquipoAnual"]
    display = {c: fam_map.get(c, c) for c in causas}
    for g in sorted(set(display.values())):
        grupos[f"Mostrar_{g}"] = {c for c, v in display.items() if v == g}

    p("2. Grupos (sobre las 17 familias)")
    for g, s in grupos.items():
        p(f"   {g:24s} {len(s):2d}: {sorted(s)}")
    chain = ["MeteoRetrasoCascada", "BloqueMeteoAnual", "RelacionadaConTiempo", "CausaLocal"]
    for a, b in zip(chain, chain[1:]):
        p(f"   {a} incluido en {b}: {grupos[a] <= grupos[b]}")
    for b in ("BloqueFaunaAnual", "BloqueEquipoAnual"):
        p(f"   {b} incluido en RelacionadaConTiempo: {grupos[b] <= grupos['RelacionadaConTiempo']}")
    mixed = {c: display[c] for c in sorted(grupos["BloqueMeteoAnual"])}
    p(f"   El bloque 'meteo' del riesgo anual se enseña en riesgo_zonas como: {mixed}")
    p(f"   'meteorologia' significa: en riesgo_dia {len(grupos['RelacionadaConTiempo'])} familias, en el bloque anual "
      f"{len(grupos['BloqueMeteoAnual'])}, en la presentación de riesgo_zonas {len(grupos['Mostrar_meteorologia'])} "
      f"y en el retraso y la cascada {len(grupos['MeteoRetrasoCascada'])}")
    p("")

    # --- 3. T-Box y A-Box
    inc = incidencias(base)
    p(f"3. Incidencias de la exportación: {len(inc)} (causa nula: {int(inc.causa.isna().sum())})")
    # pandas, con las expresiones del código
    t = time.perf_counter()
    pandas_counts = {
        "CausaLocal": int((~inc["causa"].fillna("").isin(loc_ex)).sum()),          # riesgo_zonas l. 130
        "RelacionadaConTiempo": int((~inc["causa"].isin(set(nomet[0]))).sum()),     # job_ml_riesgo_dia l. 906
        "BloqueMeteoAnual": int(inc["causa"].isin(bloques["meteo"]).sum()),         # job_ml_riesgo_ct l. 399
        "BloqueFaunaAnual": int(inc["causa"].isin(bloques["fauna"]).sum()),
        "BloqueEquipoAnual": int(inc["causa"].isin(bloques["equipo"]).sum()),
        "MeteoRetrasoCascada": int(inc["causa"].isin(inline[0][0]).sum()),
    }
    pandas_counts["RestoAnual"] = len(inc) - pandas_counts["BloqueMeteoAnual"] - pandas_counts["BloqueFaunaAnual"] \
        - pandas_counts["BloqueEquipoAnual"]
    t_pandas = time.perf_counter() - t

    g = Graph()
    g.bind("gpo", GPO)
    for c in causas:
        g.add((GPO[f"F_{c}"], RDF.type, OWL.Class))
    for name, members in grupos.items():
        g.add((GPO[name], RDF.type, OWL.Class))
        for c in members:
            g.add((GPO[f"F_{c}"], RDFS.subClassOf, GPO[name]))
    tbox = len(g)
    for r in inc.itertuples():
        if isinstance(r.causa, str):
            g.add((GP[f"inc/{r.n}"], RDF.type, GPO[f"F_{r.causa}"]))
    p(f"   T-Box: {tbox} tripletas (17 clases de familia, {len(grupos)} de grupo); A-Box: {len(g) - tbox}")

    def count(graph, cls, rewrite=False):
        q = (f"SELECT (COUNT(DISTINCT ?i) AS ?n) WHERE {{ ?i a/rdfs:subClassOf* <{GPO[cls]}> }}" if rewrite
             else f"SELECT (COUNT(DISTINCT ?i) AS ?n) WHERE {{ ?i a <{GPO[cls]}> }}")
        return int(next(iter(graph.query(q, initNs={"rdfs": RDFS})))[0])

    t = time.perf_counter()
    rewritten = {k: count(g, k, rewrite=True) for k in pandas_counts}
    t_rewrite = time.perf_counter() - t
    before = {k: count(g, k) for k in pandas_counts}
    g2 = Graph()
    for tr in g:
        g2.add(tr)
    n0 = len(g2)
    t = time.perf_counter()
    owlrl.DeductiveClosure(owlrl.RDFS_Semantics, axiomatic_triples=False).expand(g2)
    t_rdfs = time.perf_counter() - t
    t = time.perf_counter()
    inferred = {k: count(g2, k) for k in pandas_counts}
    t_q = time.perf_counter() - t
    p(f"   RDFS (owlrl, sin tripletas axiomáticas): {n0} -> {len(g2)} tripletas en {t_rdfs:.2f} s")
    p(f"   {'grupo':22s} {'pandas':>7s} {'sin razonar':>12s} {'reescrita':>10s} {'tras RDFS':>10s}")
    for k in pandas_counts:
        p(f"   {k:22s} {pandas_counts[k]:7d} {before[k]:12d} {rewritten[k]:10d} {inferred[k]:10d}")
    p(f"   Tiempos: pandas {1000 * t_pandas:.1f} ms; SPARQL reescrita {1000 * t_rewrite:.0f} ms; "
      f"SPARQL tras materializar {1000 * t_q:.0f} ms (más {t_rdfs:.2f} s de materialización)")
    p("   'RestoAnual' por complemento: tras RDFS solo cuenta lo que se declaró; con una familia nueva sin "
      "clasificar, pandas la metería en el resto y RDFS no:")
    g3 = Graph()
    for tr in g2:
        g3.add(tr)
    g3.add((GP["inc/nueva"], RDF.type, GPO["F_nueva_familia"]))
    resto_rdfs = count(g3, "RestoAnual")
    resto_not_exists = int(next(iter(g3.query(
        """SELECT (COUNT(DISTINCT ?i) AS ?n) WHERE { ?i a ?f . ?f a owl:Class . FILTER(STRSTARTS(STR(?f), STR(gpo:F_)))
            FILTER NOT EXISTS { ?i a gpo:BloqueMeteoAnual } FILTER NOT EXISTS { ?i a gpo:BloqueFaunaAnual }
            FILTER NOT EXISTS { ?i a gpo:BloqueEquipoAnual } }""", initNs={"owl": OWL, "gpo": GPO})))[0])
    resto_not_exists_any = int(next(iter(g3.query(
        """SELECT (COUNT(DISTINCT ?i) AS ?n) WHERE { ?i a ?f . FILTER(STRSTARTS(STR(?f), STR(gpo:F_)))
            FILTER NOT EXISTS { ?i a gpo:BloqueMeteoAnual } FILTER NOT EXISTS { ?i a gpo:BloqueFaunaAnual }
            FILTER NOT EXISTS { ?i a gpo:BloqueEquipoAnual } }""", initNs={"gpo": GPO})))[0])
    p(f"   RestoAnual con la incidencia nueva: RDFS {resto_rdfs}; FILTER NOT EXISTS {resto_not_exists_any} "
      f"(pandas daría {pandas_counts['RestoAnual'] + 1}); la clase declarada de la familia nueva no existe "
      f"y la consulta que exige ?f a owl:Class la pierde ({resto_not_exists})")
    p("")

    # --- 4. OWL RL: partición de la presentación y "meteo" = "meteorologia"
    g4 = Graph()
    for tr in g:
        g4.add(tr)
    disp = sorted(k for k in grupos if k.startswith("Mostrar_"))
    for i, a in enumerate(disp):
        for b in disp[i + 1:]:
            g4.add((GPO[a], OWL.disjointWith, GPO[b]))
    g4.add((GPO["BloqueMeteoAnual"], OWL.equivalentClass, GPO["Mostrar_meteorologia"]))
    t = time.perf_counter()
    owlrl.DeductiveClosure(owlrl.OWLRL_Semantics, axiomatic_triples=False).expand(g4)
    t_owl = time.perf_counter() - t
    errors = sorted(str(o) for _, _, o in g4.triples((None, ERR.error, None)))
    afectadas = inc[inc.causa.isin({c for c in grupos["BloqueMeteoAnual"] if display[c] != "meteorologia"})]
    p(f"4. OWL RL con la presentación como partición y 'meteo' = 'meteorologia': {len(errors)} mensajes de "
      f"inconsistencia en {t_owl:.1f} s")
    import re
    pares = pd.Series([" / ".join(sorted(x.rsplit("#", 1)[-1] for x in re.findall(r"Disjoint classes (\S+) and (\S+) have", e)[0]))
                       for e in errors if e.startswith("Disjoint classes")]).value_counts().to_dict()
    p(f"   pares de clases disjuntas con individuos comunes: {pares}")
    p(f"   ejemplo: {errors[0][:220] if errors else '-'}")
    p(f"   incidencias afectadas (familias del bloque anual que no se enseñan como meteorologia): {len(afectadas)} "
      f"de {len(inc)}: {afectadas.causa.value_counts().to_dict()}")
    p("")

    # --- 5. SKOS con tres niveles
    g5 = Graph()
    g5.add((SKOS.broader, RDFS.subPropertyOf, SKOS.broaderTransitive))
    g5.add((SKOS.broaderTransitive, RDF.type, OWL.TransitiveProperty))
    fam_to_block = {c: b for b, cs in bloques.items() for c in cs}
    for r in inc.dropna(subset=["causa"]).drop_duplicates(["causa", "subcausa"]).itertuples():
        sub = GP[f"causa/{r.causa}/{r.subcausa}"]
        g5.add((sub, SKOS.broader, GP[f"causa/{r.causa}"]))
        if r.causa in fam_to_block:
            g5.add((GP[f"causa/{r.causa}"], SKOS.broader, GP[f"bloque/{fam_to_block[r.causa]}"]))
    for r in inc.dropna(subset=["causa"]).itertuples():
        g5.add((GP[f"inc/{r.n}"], GPO.causa, GP[f"causa/{r.causa}/{r.subcausa}"]))
    q = f"""SELECT (COUNT(DISTINCT ?i) AS ?n) WHERE {{ ?i <{GPO.causa}> ?c . ?c skos:broaderTransitive <{GP['bloque/meteo']}> }}"""
    qp = f"""SELECT (COUNT(DISTINCT ?i) AS ?n) WHERE {{ ?i <{GPO.causa}> ?c . ?c skos:broader+ <{GP['bloque/meteo']}> }}"""
    res = {}
    for name, sem in (("RDFS", owlrl.RDFS_Semantics), ("OWL RL", owlrl.OWLRL_Semantics)):
        gg = Graph()
        for tr in g5:
            gg.add(tr)
        t = time.perf_counter()
        owlrl.DeductiveClosure(sem, axiomatic_triples=False).expand(gg)
        res[name] = (int(next(iter(gg.query(q, initNs={"skos": SKOS})))[0]), time.perf_counter() - t, len(gg))
    path = int(next(iter(g5.query(qp, initNs={"skos": SKOS})))[0])
    p(f"5. SKOS de tres niveles (subcausa -> familia -> bloque), incidencias del bloque meteo por "
      f"skos:broaderTransitive (pandas: {pandas_counts['BloqueMeteoAnual']}):")
    for name, (n, s, size) in res.items():
        p(f"   {name:7s}: {n:4d} ({s:.2f} s, {size} tripletas)")
    p(f"   sin razonar, camino skos:broader+: {path}")
    txt = "\n".join(lines)
    print(txt)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(txt + "\n")


if __name__ == "__main__":
    main()
