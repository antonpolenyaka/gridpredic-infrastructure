"""M1. Dos CT de redes distintas cortados por el mismo evento del SCADA con un elemento aguas arriba
comun, conocido antes del evento. Es el motivo de los "Wikipedia collaborators" de la d. 5 del
laboratorio (dos editores, dos artículos, "same day") con eventos, trafos y elementos.

GraphFrames (sin ejecutar):
  m1 = (g.find("(ev)-[d1]->(t1); (ev)-[d2]->(t2); (t1)-[a1]->(x); (t2)-[a2]->(x)")
         .filter("d1.tipo = 'DEJA_SIN_TENSION' and d2.tipo = 'DEJA_SIN_TENSION'")
         .filter("a1.tipo = 'AGUAS_ARRIBA' and a2.tipo = 'AGUAS_ARRIBA'")
         .filter("t1.red = 'D1' and t2.red = 'D2'")
         .filter("a1.ts < ev.ts and a2.ts < ev.ts"))

find() traduce el motivo a una cadena de joins en el orden en que se escriben los términos y no
quita repetidos. Aquí se emula esa cadena en pandas, con los filtros de un solo elemento empujados a
su lado (lo que hace Catalyst) y la condición entre términos en el join en que ya están los dos.
Se comparan tres formulaciones con el mismo resultado:
  A. el motivo sobre el grafo entero (filas de cada join contadas sin materializar);
  B. el motivo después de filtrar el subgrafo con dos aggregateMessages y filterVertices (la buena
     práctica del laboratorio: filtrar antes de iterar o de buscar patrones);
  C. por conjuntos, como lo escribiría el repositorio: join de los trafos de cada evento con sus
     elementos conocidos y groupBy(evento, elemento) con las redes distintas.

Uso: python m1_dos_redes.py <carpeta zonas_20261013_1041> <carpeta de salida>
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


import grafo_gf as gg  # noqa: E402

BASE, SALIDA = Path(sys.argv[1]), Path(sys.argv[2])
SALIDA.mkdir(parents=True, exist_ok=True)
lineas = []


def out(txt=""):
    print(txt)
    lineas.append(str(txt))


t0 = time.time()
V, E = gg.construir(gg.cargar(BASE))
out(f"grafo: {len(V):,} vertices {V.tipo.value_counts().to_dict()}, {len(E):,} aristas {E.tipo.value_counts().to_dict()} "
    f"({time.time() - t0:.1f} s)")

ev_v = V[V.tipo == "evento"][["id", "ts"]]
tr_v = V[V.tipo == "trafo"][["id", "red"]]
DEJA = E[E.tipo == "DEJA_SIN_TENSION"][["src", "dst"]]
AA = E[E.tipo == "AGUAS_ARRIBA"][["src", "dst", "ts"]]

# --------------------------------------------------------------------- A. filas de la cadena de joins
t1 = time.time()
d = DEJA.merge(tr_v, left_on="dst", right_on="id")[["src", "dst", "red"]].rename(columns={"src": "ev", "dst": "t"})
n = d.groupby(["ev", "red"]).size().unstack(fill_value=0)
for r in ("D1", "D2", "D3"):
    if r not in n:
        n[r] = 0
n["all"] = n[["D1", "D2", "D3"]].sum(axis=1)
deg = AA.groupby("src").size()
d["deg"] = d.t.map(deg).fillna(0)
d = d.merge(ev_v.rename(columns={"id": "ev", "ts": "ev_ts"}), on="ev")
# solo los eventos con trafos de D1 y de D2 aportan filas a partir del join con t2 (el resto multiplica por 0)
con_dos = n[(n["D1"] > 0) & (n["D2"] > 0)].index
dk = d[d.ev.isin(con_dos)].merge(AA.rename(columns={"src": "t", "dst": "x", "ts": "conocido"}), on="t")
dk["previo"] = dk.conocido < dk.ev_ts
deg_prev = dk[dk.previo].groupby(["ev", "t"]).size().rename("deg_prev").reset_index()
d = d.merge(deg_prev, on=["ev", "t"], how="left")
d["deg_prev"] = d.deg_prev.fillna(0)
s1 = d[d.red == "D1"].groupby("ev").agg(sdeg=("deg", "sum"), sdegp=("deg_prev", "sum"))
c = dk.groupby(["ev", "x", "red"]).size().unstack(fill_value=0)
cp = dk[dk.previo].groupby(["ev", "x", "red"]).size().unstack(fill_value=0)
R1 = int(n["D1"].sum())
R2a = int((n["D1"] * n["all"]).sum())
R2 = int((n["D1"] * n["D2"]).sum())
R3 = int((n["D2"].reindex(s1.index, fill_value=0) * s1.sdeg).sum())
R3p = int((n["D2"].reindex(s1.index, fill_value=0) * s1.sdegp).sum())
R4 = int((c.get("D1", 0) * c.get("D2", 0)).sum())
R5 = int((cp.get("D1", 0) * cp.get("D2", 0)).sum())
tA = time.time() - t1
out("A. Motivo sobre el grafo entero, filas tras cada join (orden en que se escribe el motivo):")
out(f"  (ev)-[d1]->(t1) con t1 en D1: {R1:,}")
out(f"  + (ev)-[d2]->(t2), antes de unir t2: {R2a:,}; con t2 en D2: {R2:,}")
out(f"  + (t1)-[a1]->(x): {R3:,}; con a1.ts < ev.ts como condicion del join: {R3p:,}")
out(f"  + (t2)-[a2]->(x): {R4:,} sin la condicion temporal; con a1.ts y a2.ts < ev.ts: {R5:,} instancias")
out(f"  (recuento sin materializar, {tA:.2f} s)")
# el mismo motivo sin filtro de red en t1 y t2 (pares de trafos de cualquier red), solo para dar la escala
out(f"  sin filtros de red, el segundo join daria {int((n['all'] ** 2).sum()):,} filas (pares ordenados por evento)")
# el otro orden: "(t1)-[a1]->(x); (t2)-[a2]->(x); (ev)-[d1]->(t1); (ev)-[d2]->(t2)"
aa_t = AA.merge(tr_v, left_on="src", right_on="id")
c1x = aa_t[aa_t.red == "D1"].groupby("dst").size()
c2x = aa_t[aa_t.red == "D2"].groupby("dst").size()
O1 = int(c1x.sum())
O2 = int((c1x * c2x.reindex(c1x.index, fill_value=0)).sum())
n_ev_t = d.groupby("t").ev.nunique()
pares_x = aa_t[aa_t.red == "D1"][["src", "dst"]].merge(aa_t[aa_t.red == "D2"][["src", "dst"]], on="dst",
                                                         suffixes=("_1", "_2"))
O3 = int(pares_x.src_1.map(n_ev_t).fillna(0).sum())
out(f"  en el otro orden, empezando por (t1)-[a1]->(x): {O1:,}; + (t2)-[a2]->(x): {O2:,}; "
    f"+ (ev)-[d1]->(t1): {O3:,}; + (ev)-[d2]->(t2): {R4:,}")

# --------------------------------------------------------------------- B. subgrafo filtrado antes
t1 = time.time()
# aggregateMessages 1: cada arista DEJA_SIN_TENSION manda la red del trafo a su evento
redes_ev = d.groupby("ev").red.agg(lambda s: frozenset(s))
ev_cruz = redes_ev[redes_ev.map(lambda s: {"D1", "D2"} <= s)].index
# aggregateMessages 2: cada arista AGUAS_ARRIBA manda la red del trafo a su elemento
aa_red = AA.merge(tr_v, left_on="src", right_on="id")
redes_x = aa_red.groupby("dst").red.agg(lambda s: frozenset(s))
x_cruz = redes_x[redes_x.map(lambda s: {"D1", "D2"} <= s)].index
# filterVertices: eventos y elementos que pueden aparecer en una instancia; quita sus aristas
Vb = V[(V.tipo == "trafo") | V.id.isin(ev_cruz) | V.id.isin(x_cruz)]
keep = set(Vb.id)
Eb = E[E.src.isin(keep) & E.dst.isin(keep)]
t_filtro = time.time() - t1
ev_b = Vb[Vb.tipo == "evento"]
tr_b = Vb[Vb.tipo == "trafo"]
x_b = Vb[Vb.tipo == "elemento"]
d_b = Eb[Eb.tipo == "DEJA_SIN_TENSION"]
a_b = Eb[Eb.tipo == "AGUAS_ARRIBA"]
# Misma cadena de joins que find(), conservando solo los ids y las horas que usa la consulta (la poda de
# columnas de Spark tras un select); con todas las columnas de cada struct no cabe en 8 GB de memoria.
pasos = []
r = (gg.nest(d_b[["src", "dst"]], "d1").merge(gg.nest(ev_b[["id", "ts"]], "ev"), left_on="d1.src", right_on="ev.id")
     .merge(gg.nest(tr_b[tr_b.red == "D1"][["id"]], "t1"), left_on="d1.dst", right_on="t1.id"))[["ev.id", "ev.ts", "t1.id"]]
pasos.append(len(r))
r = r.merge(gg.nest(d_b[["src", "dst"]], "d2"), left_on="ev.id", right_on="d2.src")[["ev.id", "ev.ts", "t1.id", "d2.dst"]]
pasos.append(len(r))
r = r.merge(gg.nest(tr_b[tr_b.red == "D2"][["id"]], "t2"), left_on="d2.dst", right_on="t2.id")[
    ["ev.id", "ev.ts", "t1.id", "t2.id"]]
pasos.append(len(r))
r = r.merge(gg.nest(a_b[["src", "dst", "ts"]], "a1"), left_on="t1.id", right_on="a1.src")
r = r[r["a1.ts"] < r["ev.ts"]][["ev.id", "ev.ts", "t1.id", "t2.id", "a1.dst"]]
r = r.merge(gg.nest(x_b[["id"]], "x"), left_on="a1.dst", right_on="x.id")[["ev.id", "ev.ts", "t1.id", "t2.id", "x.id"]]
pasos.append(len(r))
r = r.merge(gg.nest(a_b[["src", "dst", "ts"]], "a2"), left_on=["t2.id", "x.id"], right_on=["a2.src", "a2.dst"])
r = r[r["a2.ts"] < r["ev.ts"]][["ev.id", "t1.id", "t2.id", "x.id"]]
pasos.append(len(r))
tB = time.time() - t1
out()
out(f"B. Subgrafo filtrado antes: {len(ev_cruz)} eventos con trafos de D1 y D2 y {len(x_cruz)} elementos con trafos "
    f"de las dos redes; {len(Eb):,} aristas")
out(f"  filas tras cada join: {', '.join(f'{p:,}' for p in pasos)}; instancias {len(r):,} "
    f"(filtrado {t_filtro:.2f} s, total {tB:.2f} s)")
inst_b = r

# --------------------------------------------------------------------- C. por conjuntos
t1 = time.time()
dc = d[d.ev.isin(ev_cruz) & d.red.isin(["D1", "D2"])][["ev", "t", "red", "ev_ts"]]
k = dc.merge(AA.rename(columns={"src": "t", "dst": "x", "ts": "conocido"}), on="t")
k = k[k.conocido < k.ev_ts]
comun = k.groupby(["ev", "x"]).red.nunique()
comun = comun[comun == 2].reset_index()
por_ev = comun.groupby("ev").x.nunique()
tC = time.time() - t1
out()
out(f"C. Por conjuntos (join y groupBy): {len(k):,} filas intermedias; {time.time() - t1:.3f} s")

# --------------------------------------------------------------------- comprobaciones y resultado
ev_inst = set(inst_b["ev.id"])
assert ev_inst == set(por_ev.index), "B y C no dan los mismos eventos"
assert len(inst_b) == R5, "B no reproduce el recuento de A"
pares_b = inst_b[["ev.id", "t1.id", "t2.id"]].drop_duplicates()
tot_pares = int((n.loc[ev_cruz, "D1"] * n.loc[ev_cruz, "D2"]).sum())
out()
out(f"Resultado (igual en A, B y C): de {len(ev_cruz)} eventos con trafos de las dos redes, {len(ev_inst)} tienen "
    f"algun par D1 - D2 con un elemento comun conocido antes del evento ({len(ev_inst) / len(ev_cruz):.1%})")
out(f"  pares (evento, t1, t2) con elemento comun: {len(pares_b):,} de {tot_pares:,} ({len(pares_b) / tot_pares:.1%})")
out(f"  instancias por par: mediana {inst_b.groupby(['ev.id', 't1.id', 't2.id']).size().median():.0f}; "
    f"elementos comunes por evento: mediana {por_ev.median():.0f}, maximo {por_ev.max()}")
out(f"  instancias del motivo frente a eventos de la respuesta: {len(inst_b):,} filas para {len(ev_inst)} eventos")
out(f"tiempo total {time.time() - t0:.1f} s")
(SALIDA / "m1_dos_redes.txt").write_text("\n".join(lineas) + "\n", encoding="utf-8")
