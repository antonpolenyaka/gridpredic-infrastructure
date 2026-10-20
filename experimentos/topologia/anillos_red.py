"""El patrón del anillo de fraude aplicado a las dos redes.

Tres preguntas, con la exportación de Gold del 13.10.2026 (sin Spark):
A. Patron estático (link analysis): pares de trafos de distribuidoras distintas que comparten
   m o más elementos aguas arriba en map_aguas_arriba (como "dos personas que comparten dos o
   más datos de contacto").
B. Patron con el evento como nodo: eventos de corte del SCADA (fact_cortes_scada) que dejaron
   en Off trafos de dos distribuidoras a la vez.
C. Las incidencias de Calser que empiezan en el mismo segundo en dos distribuidoras
   (simultanea_otra_red) frente a esos eventos del SCADA.

Uso: python anillos_red.py <carpeta zonas_20261013_1041> <carpeta de salida>
"""
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from grafo_gp import componentes_conexas  # noqa: E402

T0 = time.time()
BASE = Path(sys.argv[1])
SALIDA = Path(sys.argv[2])
SALIDA.mkdir(parents=True, exist_ok=True)
NOMBRE = {3: "D1", 2: "D2", 1366: "D3"}

mapa = pd.read_parquet(BASE / "map_aguas_arriba.parquet")
dim = pd.read_parquet(BASE / "dim_ct.parquet")
cs = pd.read_parquet(BASE / "cortes_scada.parquet")
inc = pd.read_parquet(BASE / "incidencias.parquet")
itr = pd.read_parquet(BASE / "interrupciones.parquet")

lineas = []


def out(txt=""):
    print(txt)
    lineas.append(str(txt))


# ---------------------------------------------------------------- A. patrón estático
out("## A. Pares de trafos de dos redes que comparten elementos aguas arriba")
mapa["red"] = mapa.distribuidora_id.map(NOMBRE)
trafos = mapa[["trafo_elemento_id", "red"]].drop_duplicates().reset_index(drop=True)
trafos["i"] = np.arange(len(trafos))
elems = mapa[["elemento_corte_id"]].drop_duplicates().reset_index(drop=True)
elems["j"] = np.arange(len(elems))
m = mapa.merge(trafos, on=["trafo_elemento_id", "red"]).merge(elems, on="elemento_corte_id")
B = np.zeros((len(trafos), len(elems)), dtype=np.int32)
B[m.i.values, m.j.values] = 1
S = B @ B.T  # elementos compartidos por cada par de trafos
red = trafos.red.values
iu = np.triu_indices(len(trafos), k=1)
comp = S[iu]
cruzado = red[iu[0]] != red[iu[1]]
d1d2 = ((red[iu[0]] == "D1") & (red[iu[1]] == "D2")) | ((red[iu[0]] == "D2") & (red[iu[1]] == "D1"))
misma = ~cruzado
out(f"trafos {len(trafos)} (D1 {int((red == 'D1').sum())}, D2 {int((red == 'D2').sum())}, "
    f"D3 {int((red == 'D3').sum())}), elementos {len(elems)}, aristas {len(m)}")
out("| m (elementos compartidos, al menos) | pares D1 - D2 | trafos D1 | trafos D2 | parte de los pares D1 - D2 | "
    "parte de los pares de la misma red |")
out("| --- | --- | --- | --- | --- | --- |")
for k in [1, 2, 3, 5, 10]:
    sel = d1d2 & (comp >= k)
    a, b = iu[0][sel], iu[1][sel]
    t_d1 = len(set(a[red[a] == "D1"]) | set(b[red[b] == "D1"]))
    t_d2 = len(set(a[red[a] == "D2"]) | set(b[red[b] == "D2"]))
    out(f"| {k} | {int(sel.sum()):,} | {t_d1} | {t_d2} | {sel.sum() / d1d2.sum():.2%} | "
        f"{(misma & (comp >= k)).sum() / misma.sum():.2%} |")

# elementos que cortan trafos de las dos redes, con todas las aristas y con las fuertes (n_cortes de 2 o más y el 20 % de los cortes del trafo)
tot = mapa.groupby("trafo_elemento_id").n_cortes.transform("sum")
mapa["fuerte"] = (mapa.n_cortes >= 2) & (mapa.n_cortes / tot >= 0.2)
for etiqueta, sub in [("todas las aristas", mapa), ("aristas fuertes", mapa[mapa.fuerte])]:
    redes_por_el = sub.groupby("elemento_corte_id").red.nunique()
    el_cruce = redes_por_el[redes_por_el >= 2].index
    s2 = sub[sub.elemento_corte_id.isin(el_cruce)]
    out(f"elementos que cortan trafos de dos redes ({etiqueta}): {len(el_cruce)}; trafos D1 {s2[s2.red == 'D1'].trafo_elemento_id.nunique()}, "
        f"D2 {s2[s2.red == 'D2'].trafo_elemento_id.nunique()}")

# componente mixta con aristas fuertes
fu = mapa[mapa.fuerte]
red_trafo = dict(zip("t" + trafos.trafo_elemento_id.astype(str), trafos.red))
mixta_trafos = set()
for c in componentes_conexas("t" + fu.trafo_elemento_id.astype(str), "e" + fu.elemento_corte_id.astype(str)):
    rr = pd.Series([red_trafo[n] for n in c if n.startswith("t")]).value_counts()
    if len(rr) >= 2:
        out(f"componente fuerte mixta: {rr.to_dict()}, elementos {sum(1 for n in c if n.startswith('e'))}")
        mixta_trafos |= {int(n[1:]) for n in c if n.startswith("t")}
out()

# ---------------------------------------------------------------- B. el evento como nodo
out("## B. Eventos de corte del SCADA que dejan en Off trafos de dos redes")
cs["red"] = cs.distribuidora_id.map(NOMBRE)
cs["anio"] = cs.inicio_ts.dt.year
cs["largo"] = cs.duracion_s.fillna(0) > 180
ev = cs.groupby("evento_off_id").agg(
    inicio=("inicio_ts", "min"), n_red=("red", "nunique"), redes=("red", lambda s: "+".join(sorted(set(s)))),
    n_trafos=("ct_id", "nunique"), n_largos=("largo", "sum"), anio=("anio", "min"))
# En las salidas los eventos van numerados por orden de inicio (1 es el primero desde 2021), sin el identificador
# interno del SCADA.
num = pd.Series(np.arange(1, len(ev) + 1), index=ev.sort_values("inicio", kind="mergesort").index)
cruz = ev[ev.n_red >= 2].copy()
out(f"eventos {len(ev):,}; con trafos de dos redes {len(cruz)} ({len(cruz) / len(ev):.1%}); "
    f"pares de redes {cruz.redes.value_counts().to_dict()}")
por_anio = pd.DataFrame({"eventos": ev.groupby("anio").size(), "cruzados": cruz.groupby("anio").size()}).fillna(0).astype(int)
por_anio["parte"] = (por_anio.cruzados / por_anio.eventos).map(lambda x: f"{x:.1%}")
out(por_anio.to_string())
filas_cruz = cs[cs.evento_off_id.isin(cruz.index)]
tam = filas_cruz.groupby(["evento_off_id", "red"]).ct_id.nunique().unstack(fill_value=0)
out(f"trafos por evento cruzado: D1 mediana {tam['D1'].median():.0f} (max {tam['D1'].max()}), "
    f"D2 mediana {tam['D2'].median():.0f} (max {tam['D2'].max()})")
largos_dos = filas_cruz[filas_cruz.largo].groupby("evento_off_id").red.nunique()
out(f"eventos cruzados con algun Off de mas de 180 s: {filas_cruz[filas_cruz.largo].evento_off_id.nunique()}; "
    f"con Offs de mas de 180 s en las dos redes: {int((largos_dos >= 2).sum())}")
trafos_cruz = filas_cruz.elemento_id.unique()
tr_cruz_red = filas_cruz.groupby("red").elemento_id.nunique().to_dict()
out(f"trafos distintos que aparecen en eventos cruzados: {len(trafos_cruz)} {tr_cruz_red}")
en_mixta = np.isin(trafos_cruz, list(mixta_trafos)).mean() if mixta_trafos else float("nan")
out(f"de esos trafos, parte dentro de la componente fuerte mixta de A: {en_mixta:.1%}")
# los trafos de D2 en eventos cruzados frente a los 36 de la componente
d2_cruz = set(filas_cruz[filas_cruz.red == "D2"].elemento_id)
d2_mixta = {t for t in mixta_trafos if red_trafo.get("t" + str(t)) == "D2"}
out(f"trafos D2 en eventos cruzados {len(d2_cruz)}; de los {len(d2_mixta)} D2 de la componente mixta estan {len(d2_cruz & d2_mixta)}")
cruz.reset_index().assign(evento_off_id=lambda d: d.evento_off_id.map(num)).rename(
    columns={"evento_off_id": "evento"}).to_csv(SALIDA / "eventos_scada_dos_redes.csv", index=False)
out()

# ---------------------------------------------------------------- C. Calser en el mismo segundo
out("## C. Incidencias de Calser que empiezan en el mismo segundo en dos redes")
inc["red"] = inc.distribuidora_id.map(NOMBRE)
imp = inc[inc.clasificacion_id == "CL_IMPRE"].copy()
g = imp.groupby("inicio_ts").agg(n_red=("red", "nunique"), n_inc=("id", "size"),
                                 redes=("red", lambda s: "+".join(sorted(set(s)))))
inst = g[g.n_red >= 2]
n_inc_sim = int(inst.n_inc.sum())
out(f"instantes con incidencias imprevistas en dos redes: {len(inst)}; incidencias en ellos: {n_inc_sim}")
inst_anio = inst.groupby(inst.index.year).agg(instantes=("n_inc", "size"), incidencias=("n_inc", "sum"))
out(inst_anio.to_string())
desde21 = inst[inst.index >= "2021-01-01"]
out(f"desde 2021 (ventana del SCADA): {len(desde21)} instantes: " + ", ".join(
    f"{t:%d.%m.%Y %H:%M:%S} ({r.redes})" for t, r in desde21.iterrows()))

# para cada instante desde 2021: eventos del SCADA a menos de 5 minutos y si alguno corta las dos redes
filas = []
for t, r in desde21.iterrows():
    cerca = ev[(ev.inicio >= t - pd.Timedelta("5min")) & (ev.inicio <= t + pd.Timedelta("5min"))]
    filas.append({"instante": f"{t:%d.%m.%Y %H:%M:%S}", "redes_calser": r.redes, "incidencias": int(r.n_inc),
                  "eventos_scada_5min": len(cerca), "eventos_dos_redes_5min": int((cerca.n_red >= 2).sum()),
                  "trafos_en_esos_eventos": int(cerca.n_trafos.sum()),
                  "desfase_s_evento_mas_cercano": (None if cerca.empty else
                                                   round(float((cerca.inicio - t).abs().dt.total_seconds().min()), 1))})
tabla_c = pd.DataFrame(filas)
out(tabla_c.to_string(index=False))
tabla_c.to_csv(SALIDA / "simultaneas_frente_a_scada.csv", index=False)
# el evento del SCADA del mismo segundo (menos de 2 s) en cada instante: trafos por red y Offs largos
for t, r in desde21.iterrows():
    cerca = ev[(ev.inicio - t).abs() <= pd.Timedelta(seconds=2)]
    for e in cerca.index:
        f = cs[cs.evento_off_id == e]
        por_red = f.groupby("red").agg(trafos=("ct_id", "nunique"), largos=("largo", "sum")).to_dict("index")
        out(f"  {t:%d.%m.%Y %H:%M:%S}: evento {num[e]} a {float((ev.loc[e, 'inicio'] - t).total_seconds()):+.1f} s, {por_red}")

# al revés: los eventos cruzados con Offs largos en las dos redes, que dice Calser de cada red a menos de 10 min
out()
out("Eventos del SCADA con Offs de mas de 180 s (sin emparejamiento dudoso) en las dos redes y lo que tiene Calser "
    "a menos de 10 minutos (imprevistas y programadas):")
fiables = filas_cruz[filas_cruz.largo & ~filas_cruz.emparejamiento_dudoso]
largos_fiables = fiables.groupby("evento_off_id").red.nunique()
ev_largos = largos_fiables[largos_fiables >= 2].index
out(f"eventos con Offs largos fiables en las dos redes: {len(ev_largos)} (con dudosos: {int((largos_dos >= 2).sum())})")
filas = []
for e in ev_largos:
    t = ev.loc[e, "inicio"]
    ventana = inc[(inc.inicio_ts >= t - pd.Timedelta("10min")) & (inc.inicio_ts <= t + pd.Timedelta("10min"))]
    mismas = ventana[ventana.clasificacion_id == "CL_IMPRE"].groupby("inicio_ts").red.nunique()
    filas.append({"evento": int(num[e]), "inicio": f"{t:%d.%m.%Y %H:%M:%S}", "trafos": int(ev.loc[e, 'n_trafos']),
                  "redes_con_incidencia_calser": "+".join(sorted(set(ventana.red))) or "ninguna",
                  "programadas": int((ventana.clasificacion_id == "CL_PROGR").sum()),
                  "mismo_segundo_en_dos_redes": bool((mismas >= 2).any()),
                  "dur_mediana_min": round(float(fiables[fiables.evento_off_id == e].duracion_s.median()) / 60, 1)})
tabla_l = pd.DataFrame(filas)
if not tabla_l.empty:
    out(tabla_l.redes_con_incidencia_calser.value_counts().to_string())
    out(f"con alguna programada a menos de 10 min: {int((tabla_l.programadas > 0).sum())}")
    out(f"con incidencias del mismo segundo en dos redes: {int(tabla_l.mismo_segundo_en_dos_redes.sum())} de {len(tabla_l)}")
    out(f"duracion mediana de los Offs largos por evento (min): mediana {tabla_l.dur_mediana_min.median()}, "
        f"p25 {tabla_l.dur_mediana_min.quantile(.25)}, p75 {tabla_l.dur_mediana_min.quantile(.75)}")
    out(tabla_l.to_string(index=False))
tabla_l.to_csv(SALIDA / "eventos_largos_dos_redes_frente_a_calser.csv", index=False)
out()

# ---------------------------------------------------------------- D. el anillo ya se conocía antes del evento?
# Las aristas de map_aguas_arriba se aprenden de los propios cortes: dos trafos cortados por un mismo evento
# comparten elemento por construcción. Para no hacer trampa, solo cuentan las aristas conocidas antes del evento
# (primer_conocido_ts anterior a su inicio), como haría una alerta en tiempo real.
out("## D. Con las aristas conocidas antes de cada evento cruzado")
m["conocido"] = m.primer_conocido_ts
filas = []
for e, r in cruz.sort_values("inicio").iterrows():
    t = r.inicio
    k = m[m.conocido < t]
    Bt = np.zeros((len(trafos), len(elems)), dtype=np.float32)
    Bt[k.i.values, k.j.values] = 1
    f = cs[cs.evento_off_id == e]
    tr = f.elemento_id.map(idx_tr := dict(zip(trafos.trafo_elemento_id, trafos.i))).dropna().astype(int)
    a = np.unique(tr[f.red.values == "D1"]) if len(tr) else np.array([], int)
    b = np.unique(tr[f.red.values == "D2"]) if len(tr) else np.array([], int)
    if len(a) == 0 or len(b) == 0:
        continue
    sub = Bt[a] @ Bt[b].T
    base = Bt[red == "D1"] @ Bt[red == "D2"].T
    filas.append({"evento": int(num[e]), "inicio": t, "anio": t.year, "pares": sub.size,
                  "pares_previos_1": float((sub >= 1).mean()), "pares_previos_2": float((sub >= 2).mean()),
                  "algun_par_previo": bool((sub >= 1).any()), "base_1": float((base >= 1).mean()),
                  "base_2": float((base >= 2).mean())})
tabla_d = pd.DataFrame(filas)
tabla_d.to_csv(SALIDA / "eventos_cruzados_conocimiento_previo.csv", index=False)
res = tabla_d.groupby("anio").agg(eventos=("evento", "size"), con_anillo_previo=("algun_par_previo", "mean"),
                                  pares_previos_1=("pares_previos_1", "mean"), base_1=("base_1", "mean"))
out(res.round(3).to_string())
out(f"todos: {len(tabla_d)} eventos cruzados; con algun par D1 - D2 ya unido por un elemento conocido antes: "
    f"{tabla_d.algun_par_previo.mean():.1%}; parte media de pares unidos antes {tabla_d.pares_previos_1.mean():.1%} "
    f"(>= 2 elementos {tabla_d.pares_previos_2.mean():.1%}) frente a la tasa base del momento {tabla_d.base_1.mean():.1%} "
    f"(>= 2 {tabla_d.base_2.mean():.1%})")
# los eventos cruzados de los tres instantes con incidencias de Calser en el mismo segundo en las dos redes
for instante in ["2022-04-17 14:40:46", "2024-03-27 19:21:20", "2026-01-28 08:35:11"]:
    for e in num[cruz.index[(cruz.inicio - pd.Timestamp(instante)).abs() <= pd.Timedelta(seconds=2)]]:
        if e not in set(tabla_d.evento):
            continue
        r = tabla_d[tabla_d.evento == e].iloc[0]
        out(f"  evento {e} ({r.inicio:%d.%m.%Y}): pares {r.pares:,}; unidos antes por >= 1 elemento {r.pares_previos_1:.1%}, "
            f">= 2 {r.pares_previos_2:.1%}; tasa base {r.base_1:.1%}")
out()
out(f"tiempo total {time.time() - T0:.1f} s")
(SALIDA / "anillos_red.txt").write_text("\n".join(lineas) + "\n", encoding="utf-8")
