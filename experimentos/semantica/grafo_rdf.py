"""
Grafo RDF pequeño de GridPredic a partir de la exportación de Gold zonas_20261013_1041 y del riesgo del
28.01.2026 de la demo (web/demo/data/riesgo_20260128.json). Solo lee ficheros locales: no consulta ningún
servicio externo.

- TBox: la ontología mínima de GridPredic (gpo:) apoyada en SOSA, OWL-Time, GeoSPARQL, SKOS y PROV-O.
- ABox: distribuidoras, CT, líneas (grupo de red), municipios (con superficie, densidad y altitud, que
  vienen de Wikidata y del modelo de elevación vía contexto_ct_anio), celdas meteorológicas (polígono WKT),
  incidencias con su causa (SKOS) y su intervalo (OWL-Time), interrupciones como nodo n-ario (incidencia,
  CT, duración), la previsión de racha de ICON-EU de la víspera para el 28.01.2026 como sosa:Observation y
  el riesgo municipal de ese día como gpo:EstimacionRiesgo.

Anonimización: los CT, trafos, líneas, cabeceras e incidencias se numeran en secuencia dentro de cada
distribuidora; ningún código real de instalación entra en las URIs ni en las salidas. Las distribuidoras son
Distribuidora 1, 2 y 3. Los municipios se identifican por su código INE, así que los
volcados del grafo no se publican (no se versionan; ver el README).

Las incidencias de Calser se identifican por (distribuidora, periodo, id): el id se repite entre periodos.
Las interrupciones no llevan rdf:type ni las incidencias tampoco: lo infiere RDFS por dominio y rango
(costo_inferencia.py lo materializa).

Uso como módulo: from grafo_rdf import construir; g, info = construir(base, riesgo)
Uso como guion:  python grafo_rdf.py <carpeta de la exportación> [riesgo_json]   (escribe salida/ontologia_gp.ttl,
los volcados gridpredic_grafo.ttl y .nt, que no se versionan, y los tamaños)
"""

import json
import os
import sys
import time

import pandas as pd
from rdflib import BNode, Graph, Literal, Namespace, URIRef  # noqa: F401
from rdflib.namespace import OWL, RDF, RDFS, SKOS, XSD

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "salida")
# La exportación de Gold (zonas_20261013_1041 en _runlogs/ml_resultados del portátil): primer argumento o variable.
BASE_DEF = os.environ.get("GRIDPREDIC_EXPORTACION", "")
RIESGO_DEF = os.path.join(HERE, "..", "..", "web", "demo", "data", "riesgo_20260128.json")

GP = Namespace("https://gridpredic.com/id/")
GPO = Namespace("https://gridpredic.com/ont#")
SOSA = Namespace("http://www.w3.org/ns/sosa/")
TIME = Namespace("http://www.w3.org/2006/time#")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")
PROV = Namespace("http://www.w3.org/ns/prov#")
WD = Namespace("http://www.wikidata.org/entity/")

# DistributorId de Calser (= Id del elemento raíz en TedisNet) -> número anónimo del informe
DISTRIBUIDORAS = {3: 1, 2: 2, 1366: 3}
NIVELES = {"bajo": 1, "medio": 2, "alto": 3, "muy alto": 4}
CELDA_GRADOS = 0.1

ONTOLOGIA_TTL = """
@prefix gpo:  <https://gridpredic.com/ont#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix sosa: <http://www.w3.org/ns/sosa/> .
@prefix time: <http://www.w3.org/2006/time#> .
@prefix geo:  <http://www.opengis.net/ont/geosparql#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix prov: <http://www.w3.org/ns/prov#> .

gpo: a owl:Ontology ;
    rdfs:label "Ontología mínima de GridPredic"@es ;
    rdfs:comment "Clases propias pocas; lo demás se reutiliza de SOSA, OWL-Time, GeoSPARQL, SKOS y PROV-O."@es .

# ---- Clases (9 propias)
gpo:Distribuidora        a owl:Class ; rdfs:subClassOf prov:Organization .
gpo:CentroTransformacion a owl:Class ; rdfs:subClassOf geo:Feature , sosa:FeatureOfInterest .
gpo:Linea                a owl:Class ; rdfs:subClassOf geo:Feature .
gpo:Municipio            a owl:Class ; rdfs:subClassOf geo:Feature .
gpo:CeldaMeteo           a owl:Class ; rdfs:subClassOf geo:Feature , sosa:FeatureOfInterest .
gpo:Incidencia           a owl:Class .
gpo:Interrupcion         a owl:Class ;
    rdfs:comment "Relación n-aria reificada: una incidencia, un CT y su intervalo (como Enrollment en clase)."@es .
gpo:EstimacionRiesgo     a owl:Class ; rdfs:subClassOf prov:Entity .
gpo:CorteScada           a owl:Class ; rdfs:subClassOf sosa:Observation ;
    rdfs:comment "Off de un trafo según el flood fill de TedisNet: phenomenonTime = cuándo, resultTime = cuándo se supo."@es .

# Jerarquía de GeoSPARQL que el razonador local necesita ver
geo:Feature rdfs:subClassOf geo:SpatialObject .
geo:Geometry rdfs:subClassOf geo:SpatialObject .

# ---- Propiedades de objeto
gpo:distribuidora a owl:ObjectProperty ; rdfs:range gpo:Distribuidora .
gpo:enMunicipio   a owl:ObjectProperty ; rdfs:domain gpo:CentroTransformacion ; rdfs:range gpo:Municipio ;
    rdfs:subPropertyOf geo:sfWithin .
gpo:enCelda       a owl:ObjectProperty ; rdfs:range gpo:CeldaMeteo ; rdfs:subPropertyOf geo:sfWithin .
gpo:enLinea       a owl:ObjectProperty ; rdfs:domain gpo:CentroTransformacion ; rdfs:range gpo:Linea .
gpo:cabecera      a owl:ObjectProperty ; rdfs:domain gpo:CentroTransformacion .
gpo:afectaA       a owl:ObjectProperty ; rdfs:domain gpo:Interrupcion ; rdfs:range gpo:CentroTransformacion .
gpo:deIncidencia  a owl:ObjectProperty ; rdfs:domain gpo:Interrupcion ; rdfs:range gpo:Incidencia .
gpo:causa         a owl:ObjectProperty ; rdfs:domain gpo:Incidencia ; rdfs:range skos:Concept .
gpo:sobre         a owl:ObjectProperty ; rdfs:domain gpo:EstimacionRiesgo .
gpo:nivel         a owl:ObjectProperty ; rdfs:domain gpo:EstimacionRiesgo ; rdfs:range skos:Concept .

# ---- Propiedades de datos
gpo:codigoINE      a owl:DatatypeProperty ; rdfs:domain gpo:Municipio ; rdfs:range xsd:string .
gpo:potenciaKVA    a owl:DatatypeProperty , owl:FunctionalProperty ; rdfs:range xsd:decimal .
gpo:abonados       a owl:DatatypeProperty , owl:FunctionalProperty ; rdfs:range xsd:integer .
gpo:superficieKm2  a owl:DatatypeProperty ; rdfs:domain gpo:Municipio ; rdfs:range xsd:decimal ;
    rdfs:comment "Wikidata P2046, valor normalizado (psn:), en km2."@es .
gpo:densidadHabKm2 a owl:DatatypeProperty ; rdfs:domain gpo:Municipio ; rdfs:range xsd:decimal .
gpo:altitudM       a owl:DatatypeProperty ; rdfs:domain gpo:Municipio ; rdfs:range xsd:decimal .
gpo:duracionS      a owl:DatatypeProperty ; rdfs:domain gpo:Interrupcion ; rdfs:range xsd:integer .
gpo:probabilidad   a owl:DatatypeProperty ; rdfs:domain gpo:EstimacionRiesgo ; rdfs:range xsd:decimal .
gpo:paraDia        a owl:DatatypeProperty ; rdfs:domain gpo:EstimacionRiesgo ; rdfs:range xsd:date .
gpo:orden          a owl:DatatypeProperty ; rdfs:range xsd:integer .
gpo:enEstudio      a owl:DatatypeProperty ; rdfs:domain gpo:CentroTransformacion ; rdfs:range xsd:boolean .

# El código INE identifica al municipio (OWL 2: claves)
gpo:Municipio owl:hasKey ( gpo:codigoINE ) .

# ---- Vocabularios controlados
gpo:esquemaCausas a skos:ConceptScheme ; skos:prefLabel "Causas de incidencia de GridPredic"@es .
gpo:esquemaNivel  a skos:ConceptScheme ; skos:prefLabel "Niveles de riesgo del día"@es .
gpo:rachaViento10m a sosa:ObservableProperty ; rdfs:label "racha de viento a 10 m (km/h)"@es .
gpo:ICONEU_vispera a sosa:Procedure ; rdfs:label "previsión de ICON-EU emitida la víspera"@es .
"""


def ontologia() -> Graph:
    g = Graph()
    g.parse(data=ONTOLOGIA_TTL, format="turtle")
    return g


def bind(g: Graph):
    for p, ns in [("gp", GP), ("gpo", GPO), ("sosa", SOSA), ("time", TIME), ("geo", GEO), ("skos", SKOS),
                  ("prov", PROV), ("owl", OWL), ("wd", WD)]:
        g.bind(p, ns)


def slug(text) -> str:
    s = str(text).strip().lower()
    for a, b in zip("áéíóúüñ ", "aeiouun_"):
        s = s.replace(a, b)
    return "".join(ch for ch in s if ch.isalnum() or ch == "_")


def cargar(base: str, riesgo: str) -> dict:
    d = {
        "dim": pd.read_parquet(f"{base}/dim_ct.parquet"),
        "celda": pd.read_parquet(f"{base}/map_ct_celda.parquet"),
        "ctx": pd.read_parquet(f"{base}/contexto_ct_anio.parquet"),
        "inter": pd.read_parquet(f"{base}/interrupciones.parquet"),
        "meteo": pd.read_parquet(f"{base}/meteo_celda_extra__2026.parquet",
                                 columns=["celda_id", "hora", "metp1_racha_1h"]),
    }
    with open(riesgo, encoding="utf-8") as f:
        r = json.load(f)
    rows = []
    for dist in r["distribuidoras"]:
        for m in dist["municipios"]:
            rows.append({"distribuidora_id": int(dist["id"]), "codigo_ine": str(m["codigo_ine"]).zfill(5),
                         "probabilidad": float(m["probabilidad"]), "nivel": m["nivel"]})
    d["riesgo"] = pd.DataFrame(rows)
    d["riesgo_fecha"] = r["fecha"]
    return d


def tablas_anonimas(d: dict) -> dict:
    """Tablas con claves anónimas (las que usan el grafo, pandas y SQLite: mismas filas)."""
    dim = d["dim"].copy()
    dim["dist"] = dim["distribuidora_id"].map(DISTRIBUIDORAS)
    dim = dim.sort_values(["dist", "ct_id"]).reset_index(drop=True)
    dim["ct_n"] = dim.groupby("dist").cumcount() + 1
    dim["ct_uri"] = [f"d{a}/ct/{b:04d}" for a, b in zip(dim["dist"], dim["ct_n"])]
    dim["codigo_ine"] = dim["municipio_id"].astype(str).str[:5]

    def numerar(col: str, prefijo: str) -> dict:
        vals = dim.dropna(subset=[col]).drop_duplicates(["dist", col]).sort_values(["dist", col])
        vals["n"] = vals.groupby("dist").cumcount() + 1
        return {(a, b): f"d{a}/{prefijo}/{n:04d}" for a, b, n in zip(vals["dist"], vals[col], vals["n"])}

    linea = numerar("grupo_red_id", "linea")
    cab = numerar("cabecera_id", "elemento")
    dim["linea_uri"] = [linea.get((a, b)) if pd.notna(b) else None for a, b in zip(dim["dist"], dim["grupo_red_id"])]
    dim["cabecera_uri"] = [cab.get((a, b)) if pd.notna(b) else None for a, b in zip(dim["dist"], dim["cabecera_id"])]
    dim = dim.merge(d["celda"][["distribuidora_id", "ct_id", "celda_id", "ct_lat_municipio", "ct_lon_municipio"]],
                    on=["distribuidora_id", "ct_id"], how="left")

    mun = d["ctx"][d["ctx"]["anio"] == 2026].drop_duplicates("codigo_ine_padron")[
        ["codigo_ine_padron", "area_km2", "densidad_hab_km2", "elevacion_municipio_m"]].rename(
        columns={"codigo_ine_padron": "codigo_ine"})
    coords = dim.drop_duplicates("codigo_ine")[["codigo_ine", "ct_lat_municipio", "ct_lon_municipio", "celda_id"]]
    mun = mun.merge(coords, on="codigo_ine", how="left")

    inter = d["inter"].copy()
    inter["dist"] = inter["distribuidora_id"].map(DISTRIBUIDORAS)
    k = ["dist", "periodo_id", "incidencia_id"]
    inc = inter.groupby(k).agg(inicio=("inicio_ts", "min"), fin=("fin_ts", "max"), causa=("causa", "first"),
                               subcausa=("subcausa", "first")).reset_index().sort_values(k + ["inicio"])
    inc["inc_n"] = inc.groupby("dist").cumcount() + 1
    inc["inc_uri"] = [f"d{a}/incidencia/{b:04d}" for a, b in zip(inc["dist"], inc["inc_n"])]
    # Las subcausas que nombran a otra empresa se renombran (no hay nombres de empresas en las salidas)
    inc["subcausa"] = inc["subcausa"].where(~inc["subcausa"].astype(str).str.startswith("interconexion_"),
                                            "interconexion_otra_empresa")
    inter = inter.merge(inc[k + ["inc_uri"]], on=k, how="left")
    inter = inter.merge(dim[["distribuidora_id", "ct_id", "ct_uri"]], on=["distribuidora_id", "ct_id"], how="left")
    inter = inter.reset_index(drop=True)
    inter["int_uri"] = [f"d{a}/interrupcion/{i + 1:05d}" for i, a in enumerate(inter["dist"])]

    riesgo = d["riesgo"].copy()
    riesgo["dist"] = riesgo["distribuidora_id"].map(DISTRIBUIDORAS)
    riesgo["orden"] = riesgo["nivel"].map(NIVELES)

    meteo = d["meteo"]
    dia = pd.Timestamp(d["riesgo_fecha"]) if d.get("riesgo_fecha") else pd.Timestamp("2026-01-28")
    meteo = meteo[(meteo["hora"] >= dia) & (meteo["hora"] < dia + pd.Timedelta(days=1))].copy()
    meteo = meteo[meteo["celda_id"].isin(dim["celda_id"].dropna().unique())]

    return {"dim": dim, "mun": mun, "inc": inc, "inter": inter, "riesgo": riesgo, "meteo": meteo, "dia": dia}


def wkt_celda(celda_id: str) -> str:
    lat, lon = (float(x) for x in celda_id.split("_"))
    h = CELDA_GRADOS / 2
    pts = [(lon - h, lat - h), (lon + h, lat - h), (lon + h, lat + h), (lon - h, lat + h), (lon - h, lat - h)]
    return "POLYGON((" + ", ".join(f"{x:.2f} {y:.2f}" for x, y in pts) + "))"


def construir(base: str = BASE_DEF, riesgo: str = RIESGO_DEF, con_tbox: bool = True):
    t0 = time.perf_counter()
    d = cargar(base, riesgo)
    t = tablas_anonimas(d)
    t_carga = time.perf_counter() - t0

    t1 = time.perf_counter()
    g = ontologia() if con_tbox else Graph()
    bind(g)
    add = g.add
    dec = lambda v: Literal(round(float(v), 4), datatype=XSD.decimal)  # noqa: E731

    for cal, n in DISTRIBUIDORAS.items():
        u = GP[f"distribuidora/{n}"]
        add((u, RDF.type, GPO.Distribuidora))
        add((u, RDFS.label, Literal(f"Distribuidora {n}", lang="es")))

    for _, r in t["mun"].iterrows():
        m = GP[f"municipio/{r.codigo_ine}"]
        add((m, RDF.type, GPO.Municipio))
        add((m, GPO.codigoINE, Literal(r.codigo_ine)))
        if pd.notna(r.area_km2):
            add((m, GPO.superficieKm2, dec(r.area_km2)))
        if pd.notna(r.densidad_hab_km2):
            add((m, GPO.densidadHabKm2, dec(r.densidad_hab_km2)))
        if pd.notna(r.elevacion_municipio_m):
            add((m, GPO.altitudM, dec(r.elevacion_municipio_m)))
        gm = GP[f"municipio/{r.codigo_ine}/centroide"]
        add((m, GEO.hasGeometry, gm))
        add((gm, RDF.type, GEO.Geometry))
        add((gm, GEO.asWKT, Literal(f"POINT({r.ct_lon_municipio:.5f} {r.ct_lat_municipio:.5f})",
                                    datatype=GEO.wktLiteral)))

    for celda in sorted(t["dim"]["celda_id"].dropna().unique()):
        c = GP[f"celda/{celda}"]
        gc = GP[f"celda/{celda}/geometria"]
        add((c, RDF.type, GPO.CeldaMeteo))
        add((c, GEO.hasGeometry, gc))
        add((gc, RDF.type, GEO.Geometry))
        add((gc, GEO.asWKT, Literal(wkt_celda(celda), datatype=GEO.wktLiteral)))

    lineas = t["dim"].dropna(subset=["linea_uri"]).drop_duplicates("linea_uri")
    for _, r in lineas.iterrows():
        add((GP[r.linea_uri], RDF.type, GPO.Linea))
        add((GP[r.linea_uri], GPO.distribuidora, GP[f"distribuidora/{r.dist}"]))

    for _, r in t["dim"].iterrows():
        ct = GP[r.ct_uri]
        add((ct, RDF.type, GPO.CentroTransformacion))
        add((ct, GPO.distribuidora, GP[f"distribuidora/{r.dist}"]))
        add((ct, GPO.enMunicipio, GP[f"municipio/{r.codigo_ine}"]))
        if pd.notna(r.celda_id):
            add((ct, GPO.enCelda, GP[f"celda/{r.celda_id}"]))
        if r.linea_uri:
            add((ct, GPO.enLinea, GP[r.linea_uri]))
        if r.cabecera_uri:
            add((ct, GPO.cabecera, GP[r.cabecera_uri]))
        add((ct, GPO.potenciaKVA, dec(r.potencia_tiepi_kva)))
        add((ct, GPO.abonados, Literal(int(r.num_abonados), datatype=XSD.integer)))
        add((ct, GPO.enEstudio, Literal(bool(r.en_estudio))))

    familias = sorted(t["inc"]["causa"].dropna().unique())
    for fam in familias:
        f = GP[f"causa/{slug(fam)}"]
        add((f, RDF.type, SKOS.Concept))
        add((f, SKOS.inScheme, GPO.esquemaCausas))
        add((f, SKOS.prefLabel, Literal(str(fam), lang="es")))
        add((f, SKOS.topConceptOf, GPO.esquemaCausas))
    for (fam, sub) in t["inc"][["causa", "subcausa"]].drop_duplicates().itertuples(index=False):
        s = GP[f"causa/{slug(fam)}/{slug(sub)}"]
        add((s, RDF.type, SKOS.Concept))
        add((s, SKOS.inScheme, GPO.esquemaCausas))
        add((s, SKOS.broader, GP[f"causa/{slug(fam)}"]))
        add((s, SKOS.prefLabel, Literal(str(sub), lang="es")))

    for _, r in t["inc"].iterrows():
        i = GP[r.inc_uri]
        add((i, GPO.causa, GP[f"causa/{slug(r.causa)}/{slug(r.subcausa)}"]))
        iv = GP[f"{r.inc_uri}/intervalo"]
        ini = GP[f"{r.inc_uri}/inicio"]
        fin = GP[f"{r.inc_uri}/fin"]
        add((i, TIME.hasTime, iv))
        add((iv, RDF.type, TIME.Interval))
        add((iv, TIME.hasBeginning, ini))
        add((iv, TIME.hasEnd, fin))
        add((ini, TIME.inXSDDateTime, Literal(r.inicio.to_pydatetime(), datatype=XSD.dateTime)))
        add((fin, TIME.inXSDDateTime, Literal(r.fin.to_pydatetime(), datatype=XSD.dateTime)))

    inter = t["inter"]
    for iu, cu, du, dur in zip(inter["int_uri"], inter["ct_uri"], inter["inc_uri"], inter["duracion_s"]):
        x = GP[iu]
        add((x, GPO.deIncidencia, GP[du]))
        if isinstance(cu, str):
            add((x, GPO.afectaA, GP[cu]))
        add((x, GPO.duracionS, Literal(int(dur), datatype=XSD.integer)))

    for nivel, orden in NIVELES.items():
        u = GP[f"nivel/{slug(nivel)}"]
        add((u, RDF.type, SKOS.Concept))
        add((u, SKOS.inScheme, GPO.esquemaNivel))
        add((u, SKOS.prefLabel, Literal(nivel, lang="es")))
        add((u, GPO.orden, Literal(orden, datatype=XSD.integer)))
    dia = t["dia"].strftime("%Y-%m-%d")
    run = GP[f"ejecucion/riesgo/{dia}"]
    add((run, RDF.type, PROV.Activity))
    for _, r in t["riesgo"].iterrows():
        e = GP[f"riesgo/{dia}/d{r.dist}/municipio/{r.codigo_ine}"]
        add((e, RDF.type, GPO.EstimacionRiesgo))
        add((e, GPO.sobre, GP[f"municipio/{r.codigo_ine}"]))
        add((e, GPO.distribuidora, GP[f"distribuidora/{r.dist}"]))
        add((e, GPO.paraDia, Literal(dia, datatype=XSD.date)))
        add((e, GPO.probabilidad, dec(r.probabilidad)))
        add((e, GPO.nivel, GP[f"nivel/{slug(r.nivel)}"]))
        add((e, PROV.wasGeneratedBy, run))

    for _, r in t["meteo"].iterrows():
        hh = r.hora.strftime("%Y%m%dT%H")
        o = GP[f"obs/prevision/{r.celda_id}/{hh}"]
        ti = GP[f"instante/{hh}"]
        add((o, RDF.type, SOSA.Observation))
        add((o, SOSA.hasFeatureOfInterest, GP[f"celda/{r.celda_id}"]))
        add((o, SOSA.observedProperty, GPO.rachaViento10m))
        add((o, SOSA.usedProcedure, GPO.ICONEU_vispera))
        add((o, SOSA.phenomenonTime, ti))
        add((ti, RDF.type, TIME.Instant))
        add((ti, TIME.inXSDDateTime, Literal(r.hora.to_pydatetime(), datatype=XSD.dateTime)))
        add((o, SOSA.hasSimpleResult, dec(r.metp1_racha_1h)))

    t_grafo = time.perf_counter() - t1
    info = {"t_carga_s": t_carga, "t_grafo_s": t_grafo, "triples": len(g), "tablas": t}
    return g, info


def main():
    base = sys.argv[1] if len(sys.argv) > 1 else BASE_DEF
    riesgo = sys.argv[2] if len(sys.argv) > 2 else RIESGO_DEF
    os.makedirs(OUT, exist_ok=True)
    tb = ontologia()
    bind(tb)
    tb.serialize(os.path.join(OUT, "ontologia_gp.ttl"), format="turtle")
    print(f"TBox: {len(tb)} tripletas")

    g, info = construir(base, riesgo)
    t = info["tablas"]
    print(f"Lectura de Parquet y anonimización: {info['t_carga_s']:.2f} s")
    print(f"Construcción del grafo: {info['t_grafo_s']:.2f} s, {info['triples']} tripletas (TBox incluida)")
    print(f"CT {len(t['dim'])}, municipios {len(t['mun'])}, celdas {t['dim']['celda_id'].nunique()}, "
          f"líneas {t['dim']['linea_uri'].nunique()}, incidencias {len(t['inc'])}, interrupciones {len(t['inter'])}, "
          f"estimaciones {len(t['riesgo'])}, observaciones de previsión {len(t['meteo'])}")
    # Tripletas por predicado (las 15 mayores; los empates, por nombre, para que la lista no cambie entre ejecuciones)
    from collections import Counter
    c = Counter(g.namespace_manager.normalizeUri(p) for _, p, _ in g)
    for p, n in sorted(c.items(), key=lambda kv: (-kv[1], kv[0]))[:15]:
        print(f"   {p:28s} {n}")
    t2 = time.perf_counter()
    ttl = os.path.join(OUT, "gridpredic_grafo.ttl")
    g.serialize(ttl, format="turtle")
    t_ttl = time.perf_counter() - t2
    t3 = time.perf_counter()
    nt = os.path.join(OUT, "gridpredic_grafo.nt")
    g.serialize(nt, format="nt", encoding="utf-8")
    t_nt = time.perf_counter() - t3
    print(f"Turtle: {os.path.getsize(ttl) / 1e6:.1f} MB en {t_ttl:.1f} s; N-Triples: {os.path.getsize(nt) / 1e6:.1f} MB "
          f"en {t_nt:.1f} s")
    sizes = {n: os.path.getsize(f"{base}/{n}.parquet") for n in ["dim_ct", "map_ct_celda", "contexto_ct_anio",
                                                                   "interrupciones"]}
    print("Parquet de origen (ficheros enteros, con todas sus columnas):",
          {k: f"{v / 1e3:.0f} kB" for k, v in sizes.items()})


if __name__ == "__main__":
    main()
