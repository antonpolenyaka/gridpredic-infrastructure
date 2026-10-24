"""
Coste de materializar la inferencia sobre el grafo RDF de grafo_rdf.py (170.273 tripletas, ontología mínima
de GridPredic con SOSA, OWL-Time, GeoSPARQL, SKOS y PROV-O), con owlrl y sin servidor. Es lo que un
triplestore con reglas hacia delante (GraphDB) haría al cargar: RDFS, RDFS con las tripletas axiomáticas
y OWL 2 RL, y qué parte de lo inferido sirve.

Reutiliza construir() de grafo_rdf.py, que solo lee la exportación de Gold.

Uso: python costo_inferencia.py <carpeta zonas_20261013_1041> <dashboard/data/riesgo_20260128.json (el de la operación)> [salida.txt] [variantes]
     variantes: lista separada por comas de rdfs, rdfs_axiomas, owlrl, owlrl_sin_clave (por defecto las tres primeras)
"""
import os
import sys
import time
from collections import Counter

import owlrl
from rdflib import Graph, Literal
from rdflib.namespace import OWL, RDF, RDFS

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import grafo_rdf  # noqa: E402


def copia(g):
    h = Graph()
    for t in g:
        h.add(t)
    return h


def clasificar(nuevas, base_ns="https://gridpredic.com/"):
    c = Counter()
    for s, p, o in nuevas:
        if p == RDF.type and o == RDFS.Resource:
            c["x rdf:type rdfs:Resource"] += 1
        elif p == OWL.sameAs and s == o:
            c["x owl:sameAs x (reflexiva)"] += 1
        elif p == RDF.type and isinstance(s, Literal):
            c["tipo de un literal"] += 1
        elif p == RDF.type and str(o).startswith(base_ns):
            c["tipo propio gpo: (lo útil)"] += 1
        elif p == RDF.type:
            c["tipo de vocabulario externo (sosa, geo, time, prov, skos)"] += 1
        elif p in (RDFS.subClassOf, RDFS.subPropertyOf, OWL.equivalentClass, OWL.equivalentProperty):
            c["esquema (subClassOf, subPropertyOf, equivalencias)"] += 1
        elif str(p).startswith(base_ns) and isinstance(o, Literal):
            c["literal repetido con otro tipo de dato (ruido de owlrl)"] += 1
        elif str(p).startswith(base_ns):
            c["propiedad propia gpo:"] += 1
        else:
            c["otras: " + (str(p).rsplit("#", 1)[-1].rsplit("/", 1)[-1])] += 1
    return c


def main():
    base, riesgo = sys.argv[1], sys.argv[2]
    out_path = sys.argv[3] if len(sys.argv) > 3 else None
    variantes = (sys.argv[4] if len(sys.argv) > 4 else "rdfs,rdfs_axiomas,owlrl").split(",")
    lines = []
    p = lines.append
    t = time.perf_counter()
    g, info = grafo_rdf.construir(base, riesgo)
    p(f"Grafo de grafo_rdf.py: {len(g)} tripletas en {time.perf_counter() - t:.1f} s")
    original = set(g)
    sem = {"rdfs": (owlrl.RDFS_Semantics, False), "rdfs_axiomas": (owlrl.RDFS_Semantics, True),
           "owlrl": (owlrl.OWLRL_Semantics, False), "owlrl_sin_clave": (owlrl.OWLRL_Semantics, False)}
    for v in variantes:
        cls, ax = sem[v]
        h = copia(g)
        if v == "owlrl_sin_clave":
            # owlrl 7.6.2 aplica mal owl:hasKey (une individuos con claves distintas, prueba_haskey.py):
            # se quita el axioma de clave del municipio para medir el coste sin ese fallo
            for s_, p_, o_ in list(h.triples((None, OWL.hasKey, None))):
                h.remove((s_, p_, o_))
        t = time.perf_counter()
        owlrl.DeductiveClosure(cls, axiomatic_triples=ax).expand(h)
        secs = time.perf_counter() - t
        nuevas = set(h) - original
        p("")
        p(f"{v}: {len(g)} -> {len(h)} tripletas (+{len(nuevas)}, {100 * len(nuevas) / len(g):.0f} %) en {secs:.1f} s")
        for k, n in sorted(clasificar(nuevas).items(), key=lambda kv: (-kv[1], kv[0]))[:12]:  # empates por nombre
            p(f"   {k:58s} {n:8d}")
        # errores (inconsistencias) que deja owlrl como nodos err:ErrorMessage
        err = sorted(str(o) for s, pp, o in h if str(pp).endswith("agent-ont#error"))
        if err:
            p(f"   mensajes de error: {len(err)}; ejemplo: {str(err[0])[:160]}")
        del h
    txt = "\n".join(lines)
    print(txt)
    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(txt + "\n")


if __name__ == "__main__":
    main()
