"""
Integración de Calser y TedisNet con owl:sameAs frente al mapa que hace el repositorio (d_ct_scada).

- Dos vistas RDF del mismo CT con URIs distintas: Calser (CT con potencia, abonados y municipio) y TedisNet
  (trafo de tipo 145 con su nombre corto, su centro de tipo 144, las posiciones aguas arriba y la cabecera).
- Alineación de esquema pequeña (unas pocas tripletas: subClassOf, subPropertyOf, disjointWith, Functional).
- Generación de enlaces con la regla del nombre compartido (la clave que verificó el repositorio: CT_ID =
  ShortName del elemento de tipo 145 en la misma distribuidora), escrita como CONSTRUCT: es lo que haría
  owl:hasKey con un razonador OWL 2 RL.
- Consulta que necesita las dos fuentes, con owl:sameAs en el patrón (como el ejemplo de DBLP y DBpedia de
  clase) y después del cierre de owl:sameAs (reglas eq-sym, eq-trans, eq-rep de OWL 2 RL, con union-find).
- Escenario de error: enlazar el CT de Calser con el centro (tipo 144) en lugar del trafo (tipo 145), el
  error que el docstring de job_silver_d_ct.py descarta ("Not ExportCode and not type 144"). El cierre une CT
  distintos y el razonamiento lo detecta como violaciones de FunctionalProperty y de disjointWith.

Las claves reales solo existen en memoria; las salidas son recuentos.

Uso: python sameas.py <carpeta de la exportación> -> salida/sameas_salida.txt
"""

import os
import sys
import time
from collections import defaultdict

import pandas as pd
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import OWL, RDF, XSD

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from grafo_rdf import BASE_DEF, DISTRIBUIDORAS, GPO  # noqa: E402

CAL = Namespace("https://gridpredic.com/fuente/calser/")
TED = Namespace("https://gridpredic.com/fuente/tedisnet/")
CALO = Namespace("https://gridpredic.com/fuente/calser#")
TEDO = Namespace("https://gridpredic.com/fuente/tedisnet#")

PREFIX = """
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX gpo:  <https://gridpredic.com/ont#>
PREFIX cal:  <https://gridpredic.com/fuente/calser#>
PREFIX ted:  <https://gridpredic.com/fuente/tedisnet#>
"""

ALINEACION = """
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix gpo:  <https://gridpredic.com/ont#> .
@prefix cal:  <https://gridpredic.com/fuente/calser#> .
@prefix ted:  <https://gridpredic.com/fuente/tedisnet#> .
cal:CT      rdfs:subClassOf gpo:CentroTransformacion .
ted:TrafoCT rdfs:subClassOf gpo:CentroTransformacion .
ted:Centro  owl:disjointWith ted:TrafoCT .
cal:potenciaKVA rdfs:subPropertyOf gpo:potenciaKVA .
gpo:potenciaKVA a owl:FunctionalProperty .
cal:abonados rdfs:subPropertyOf gpo:abonados .
gpo:abonados a owl:FunctionalProperty .
"""

ENLACE_NOMBRE = """
CONSTRUCT { ?c owl:sameAs ?t }
WHERE {
  ?c a cal:CT ; cal:codigoCT ?k ; cal:distribuidora ?d .
  ?t a ted:TrafoCT ; ted:shortName ?k ; ted:distribuidora ?d .
}"""

ENLACE_CENTRO = """
CONSTRUCT { ?c owl:sameAs ?centro }
WHERE {
  ?c a cal:CT ; cal:codigoCT ?k ; cal:distribuidora ?d .
  ?t a ted:TrafoCT ; ted:shortName ?k ; ted:distribuidora ?d ; ted:centro ?centro .
}"""

ENLACE_NOMBRE_ORDEN = """
CONSTRUCT { ?c owl:sameAs ?t }
WHERE {
  ?c cal:codigoCT ?k .
  ?t ted:shortName ?k .
  ?c cal:distribuidora ?d .
  ?t ted:distribuidora ?d .
  ?c a cal:CT .
  ?t a ted:TrafoCT .
}"""

CRUCE_CON_SAMEAS = """
SELECT ?d (COUNT(DISTINCT ?c) AS ?n_ct) (SUM(?kva) AS ?kva)
WHERE {
  ?c cal:potenciaKVA ?kva ; cal:distribuidora ?d .
  ?c owl:sameAs ?t .
  ?t ted:posicionesAguasArriba ?pos .
  FILTER(?pos >= 20)
}
GROUP BY ?d ORDER BY ?d"""

CRUCE_SIN_SAMEAS = """
SELECT ?d (COUNT(DISTINCT ?x) AS ?n_ct) (SUM(?kva) AS ?kva)
WHERE {
  ?x cal:potenciaKVA ?kva ; cal:distribuidora ?d ; ted:posicionesAguasArriba ?pos .
  FILTER(?pos >= 20)
}
GROUP BY ?d ORDER BY ?d"""


def vistas(base: str):
    dim = pd.read_parquet(f"{base}/dim_ct.parquet")
    inter = pd.read_parquet(f"{base}/interrupciones.parquet", columns=["distribuidora_id", "ct_id", "inicio_ts"])
    dim["dist"] = dim["distribuidora_id"].map(DISTRIBUIDORAS)
    dim = dim.sort_values(["dist", "ct_id"]).reset_index(drop=True)
    dim["a"] = dim.groupby("dist").cumcount() + 1
    tr = dim.dropna(subset=["trafo_elemento_id"]).sort_values(["dist", "trafo_elemento_id"]).copy()
    tr["b"] = tr.groupby("dist").cumcount() + 1
    dim = dim.merge(tr[["distribuidora_id", "ct_id", "b"]], on=["distribuidora_id", "ct_id"], how="left")
    ce = dim.dropna(subset=["anchor_id"]).drop_duplicates(["dist", "anchor_id"]).sort_values(["dist", "anchor_id"]).copy()
    ce["cn"] = ce.groupby("dist").cumcount() + 1
    dim = dim.merge(ce[["dist", "anchor_id", "cn"]], on=["dist", "anchor_id"], how="left")

    g = Graph()
    g.parse(data=ALINEACION, format="turtle")
    for p, ns in [("cal", CALO), ("ted", TEDO), ("gpo", GPO), ("owl", OWL)]:
        g.bind(p, ns)
    for r in dim.itertuples(index=False):
        d = Literal(f"d{r.dist}")
        c = CAL[f"d{r.dist}/ct/{r.a:04d}"]
        g.add((c, RDF.type, CALO.CT))
        g.add((c, CALO.codigoCT, Literal(str(r.ct_id))))  # clave real, solo en memoria
        g.add((c, CALO.distribuidora, d))
        g.add((c, CALO.potenciaKVA, Literal(round(float(r.potencia_tiepi_kva), 2), datatype=XSD.decimal)))
        g.add((c, CALO.abonados, Literal(int(r.num_abonados), datatype=XSD.integer)))
        if pd.notna(r.b):
            t = TED[f"elemento/d{r.dist}/trafo/{int(r.b):04d}"]
            g.add((t, RDF.type, TEDO.TrafoCT))
            g.add((t, TEDO.shortName, Literal(str(r.ct_id))))  # ShortName = CT_ID, la clave verificada
            g.add((t, TEDO.distribuidora, d))
            g.add((t, TEDO.posicionesAguasArriba, Literal(int(r.n_posiciones_aguas_arriba), datatype=XSD.integer)))
            if pd.notna(r.cn):
                k = TED[f"elemento/d{r.dist}/centro/{int(r.cn):04d}"]
                g.add((t, TEDO.centro, k))
                g.add((k, RDF.type, TEDO.Centro))
    return g, dim, inter


class UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def cierre_sameas(g: Graph, enlaces) -> dict:
    """Clases de equivalencia (eq-sym, eq-trans) y copia de tripletas a todos los miembros (eq-rep-s, eq-rep-o)."""
    uf = UF()
    for a, b in enlaces:
        uf.union(a, b)
    clases = defaultdict(set)
    for x in list(uf.p):
        clases[uf.find(x)].add(x)
    t0 = time.perf_counter()
    nuevas = 0
    for miembros in clases.values():
        if len(miembros) < 2:
            continue
        suj = [(p, o) for m in miembros for p, o in g.predicate_objects(m) if p != OWL.sameAs]
        obj = [(s, p) for m in miembros for s, p in g.subject_predicates(m) if p != OWL.sameAs]
        for m in miembros:
            for p, o in suj:
                if (m, p, o) not in g:
                    g.add((m, p, o))
                    nuevas += 1
            for s, p in obj:
                if (s, p, m) not in g:
                    g.add((s, p, m))
                    nuevas += 1
            for m2 in miembros:
                if m2 != m and (m, OWL.sameAs, m2) not in g:
                    g.add((m, OWL.sameAs, m2))
                    nuevas += 1
    return {"clases": clases, "nuevas": nuevas, "segundos": time.perf_counter() - t0}


def violaciones(g: Graph, clases: dict) -> dict:
    func = [CALO.potenciaKVA, CALO.abonados]
    conflictos = {p: 0 for p in func}
    disj = 0
    ct_unidos = 0
    for miembros in clases.values():
        cal_cts = [m for m in miembros if str(m).startswith(str(CAL))]
        if len(cal_cts) > 1:
            ct_unidos += 1
        rep = next(iter(miembros))
        for p in func:
            if len(set(g.objects(rep, p))) > 1:
                conflictos[p] += 1
        tipos = set(g.objects(rep, RDF.type))
        if TEDO.Centro in tipos and TEDO.TrafoCT in tipos:
            disj += 1
    return {"clases_con_varios_ct_calser": ct_unidos, "conflictos_funcionales": conflictos, "disjointWith": disj}


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else BASE_DEF
    t0 = time.perf_counter()
    g, dim, inter = vistas(base)
    print(f"Dos vistas y la alineacion: {len(g)} tripletas en {time.perf_counter() - t0:.2f} s; "
          f"alineacion de esquema {len(Graph().parse(data=ALINEACION, format='turtle'))} tripletas")
    n_cal = len(set(g.subjects(RDF.type, CALO.CT)))
    n_ted = len(set(g.subjects(RDF.type, TEDO.TrafoCT)))
    n_cen = len(set(g.subjects(RDF.type, TEDO.Centro)))
    print(f"CT de Calser {n_cal}, trafos de TedisNet {n_ted}, centros de TedisNet {n_cen}")

    # Cobertura de la clave sobre los CT con interrupciones desde 2021 (la cifra del 97,8 % del repositorio)
    con = inter[inter["inicio_ts"] >= "2021-01-01"].drop_duplicates(["distribuidora_id", "ct_id"])
    con = con.merge(dim[["distribuidora_id", "ct_id", "trafo_elemento_id"]], on=["distribuidora_id", "ct_id"], how="left")
    print(f"CT con interrupciones desde 2021 en la exportacion: {len(con)}; con trafo enlazado: "
          f"{int(con['trafo_elemento_id'].notna().sum())} ({con['trafo_elemento_id'].notna().mean() * 100:.1f} %)")

    print("\nA) Enlaces por nombre compartido (CT_ID = ShortName, misma distribuidora)")
    t0 = time.perf_counter()
    enl = list(g.query(PREFIX + ENLACE_NOMBRE))
    t_enl = time.perf_counter() - t0
    for tr in enl:
        g.add(tr)
    print(f"   {len(enl)} owl:sameAs generados en {t_enl * 1000:.0f} ms (regla CONSTRUCT, equivalente a owl:hasKey)")
    t0 = time.perf_counter()
    enl_o = list(g.query(PREFIX + ENLACE_NOMBRE_ORDEN))
    print(f"   La misma regla empezando por la clave: {len(enl_o)} enlaces en {(time.perf_counter() - t0) * 1000:.0f} ms; "
          f"mismos enlaces: {set(enl_o) == set(enl)}")
    t0 = time.perf_counter()
    k = dim.dropna(subset=["trafo_elemento_id"])[["dist", "ct_id", "a", "b"]]
    pares = k.merge(k[["dist", "ct_id", "b"]], on=["dist", "ct_id"])
    print(f"   El mismo enlace como join de pandas por (distribuidora, codigo): {len(pares)} pares en "
          f"{(time.perf_counter() - t0) * 1000:.1f} ms")

    t0 = time.perf_counter()
    r1 = [(str(a), int(b), round(float(c), 1)) for a, b, c in g.query(PREFIX + CRUCE_CON_SAMEAS)]
    t1 = time.perf_counter() - t0
    print(f"   Cruce con owl:sameAs en el patron (CT con 20 posiciones aguas arriba o mas): {r1}  ({t1 * 1000:.0f} ms)")
    t0 = time.perf_counter()
    m = dim[dim["n_posiciones_aguas_arriba"] >= 20].dropna(subset=["trafo_elemento_id"])
    r_pd = [(f"d{d}", int(n), round(float(k), 1)) for d, n, k in
            m.groupby("dist").agg(n=("ct_id", "size"), k=("potencia_tiepi_kva", "sum")).itertuples(name=None)]
    print(f"   Lo mismo con el mapa del repositorio (una fila por CT en dim_ct, pandas): {r_pd}  "
          f"({(time.perf_counter() - t0) * 1000:.1f} ms); iguales: {r1 == r_pd}")

    t0 = time.perf_counter()
    r2 = list(g.query(PREFIX + CRUCE_SIN_SAMEAS))
    print(f"   Sin owl:sameAs en el patron y sin cierre: {len(r2)} filas ({(time.perf_counter() - t0) * 1000:.0f} ms)")

    c = cierre_sameas(g, [(s, o) for s, _, o in enl])
    print(f"   Cierre de owl:sameAs: {len(c['clases'])} clases, {c['nuevas']} tripletas nuevas en {c['segundos']:.2f} s")
    t0 = time.perf_counter()
    r3 = [(str(a), int(b), round(float(k), 1)) for a, b, k in g.query(PREFIX + CRUCE_SIN_SAMEAS)]
    print(f"   Sin owl:sameAs en el patron, despues del cierre: {r3}  ({(time.perf_counter() - t0) * 1000:.0f} ms)")
    print("   (cada CT aparece con sus dos URIs: hace falta un representante canonico para contar)")
    v = violaciones(g, c["clases"])
    print(f"   Violaciones tras el cierre: {v}")

    print("\nB) Error de granularidad: ademas se enlaza cada CT con su centro (tipo 144)")
    g2, _, _ = vistas(base)
    e1 = list(g2.query(PREFIX + ENLACE_NOMBRE))
    e2 = list(g2.query(PREFIX + ENLACE_CENTRO))
    for tr in e1 + e2:
        g2.add(tr)
    c2 = cierre_sameas(g2, [(s, o) for s, _, o in e1 + e2])
    v2 = violaciones(g2, c2["clases"])
    tam = pd.Series([len(x) for x in c2["clases"].values()]).value_counts().sort_index().to_dict()
    print(f"   {len(e2)} enlaces CT - centro; {len(c2['clases'])} clases de equivalencia (tamano: numero de clases) {tam}")
    print(f"   {c2['nuevas']} tripletas nuevas en {c2['segundos']:.2f} s")
    print(f"   Clases que juntan dos CT distintos de Calser: {v2['clases_con_varios_ct_calser']}")
    print(f"   Conflictos de propiedad funcional (dos valores distintos en un individuo): {v2['conflictos_funcionales']}")
    print(f"   Individuos que son a la vez ted:Centro y ted:TrafoCT (disjuntos): {v2['disjointWith']}")
    a = dim.dropna(subset=["anchor_id"]).groupby(["dist", "anchor_id"]).agg(n=("ct_id", "nunique"),
                                                                           kva=("potencia_tiepi_kva", "nunique"),
                                                                           ab=("num_abonados", "nunique"))
    print(f"   Comprobacion en pandas: centros con 2 CT {int((a['n'] > 1).sum())}; de ellos con kVA distintos "
          f"{int(((a['n'] > 1) & (a['kva'] > 1)).sum())} y con abonados distintos {int(((a['n'] > 1) & (a['ab'] > 1)).sum())}")


if __name__ == "__main__":
    main()
