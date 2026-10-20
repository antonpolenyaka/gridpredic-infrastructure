"""M2. CT cuyo elemento aguas arriba actuó el día anterior: la cabecera que, según Calser, despejó una
avería ayer, y que hoy alimenta a un CT al que esa avería no corto. Lleva una arista negada.

GraphFrames (sin ejecutar), para el día d:
  m2 = (g.find("(i)-[c]->(x); (t)-[a]->(x); !(i)-[]->(t)")
         .filter("c.tipo = 'DESPEJADA_POR' and a.tipo = 'AGUAS_ARRIBA' and t.tipo = 'trafo'")
         .filter(F.to_date("i.ts") == F.date_sub(F.lit(d), 1))
         .filter(F.col("a.ts") < F.lit(d)))          # la arista ya se conocía al empezar el día
  candidatos = m2.select("t.id").distinct()

La negación !(i)-[]->(t) es un anti-join contra todas las aristas entre i y t (aquí, AFECTA).
Se emula la cadena de joins de find() para todos los días a la vez, se compara con un merge y un
anti-join de pandas (como lo haría el repositorio) y se mide si estar en el conjunto anticipa una
interrupción principal ese día, con la misma evaluación por CT-dia de e2_fallo_ayer.py.

Uso: python m2_cabecera_ayer.py <carpeta zonas_20261013_1041> <carpeta de salida>
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402

import grafo_gf as gg  # noqa: E402

BASE, SALIDA = Path(sys.argv[1]), Path(sys.argv[2])
SALIDA.mkdir(parents=True, exist_ok=True)
DESDE, HASTA = pd.Timestamp("2021-01-07"), pd.Timestamp("2026-08-01")
EXCLUIDOS = {gg.APAGON, gg.APAGON + pd.Timedelta(days=1)}
lineas = []


def out(txt=""):
    print(txt)
    lineas.append(str(txt))


t0 = time.time()
t = gg.cargar(BASE)
V, E = gg.construir(t)
inc_v = V[V.tipo == "incidencia"][["id", "ts"]]
tr_v = V[V.tipo == "trafo"][["id"]]
x_v = V[V.tipo == "elemento"][["id"]]
C = E[E.tipo == "DESPEJADA_POR"][["src", "dst"]]
A = E[E.tipo == "AGUAS_ARRIBA"][["src", "dst", "ts"]]
AF = E[E.tipo == "AFECTA"][["src", "dst", "ts"]]

# --------------------------------------------------------------------- motivo como cadena de joins
t1 = time.time()
r = (gg.nest(C, "c").merge(gg.nest(inc_v, "i"), left_on="c.src", right_on="i.id")
     .merge(gg.nest(x_v, "x"), left_on="c.dst", right_on="x.id"))
p1 = len(r)
r = r.merge(gg.nest(A, "a"), left_on="x.id", right_on="a.dst").merge(gg.nest(tr_v, "t"), left_on="a.src", right_on="t.id")
p2 = len(r)
neg = E[["src", "dst"]].drop_duplicates().assign(_hay=True)
r = r.merge(neg, left_on=["i.id", "t.id"], right_on=["src", "dst"], how="left")
r = r[r._hay.isna()].drop(columns=["src", "dst", "_hay"])
p3 = len(r)
r["dia"] = r["i.ts"].dt.normalize() + pd.Timedelta(days=1)
r = r[r["a.ts"] < r["dia"]]
p4 = len(r)
cand_gf = r[["dia", "t.id"]].drop_duplicates()
tm = time.time() - t1
out(f"Motivo con negacion: filas tras cada paso {p1:,} (cabeceras de Calser casadas), {p2:,} (CT bajo esa cabecera), "
    f"{p3:,} (sin los que corto la averia), {p4:,} (arista conocida al empezar el dia); "
    f"{len(cand_gf):,} pares (dia, CT) distintos ({tm:.3f} s)")

# --------------------------------------------------------------------- merge y anti-join al estilo del repo
t1 = time.time()
cab = C.merge(inc_v, left_on="src", right_on="id")[["src", "dst", "ts"]].rename(columns={"src": "inc", "dst": "x"})
cab["dia"] = cab.ts.dt.normalize() + pd.Timedelta(days=1)
bajo = cab.merge(A.rename(columns={"src": "t", "dst": "x", "ts": "conocido"}), on="x")
bajo = bajo[bajo.conocido < bajo.dia]
cortados = AF[["src", "dst"]].drop_duplicates().rename(columns={"src": "inc", "dst": "t"})
bajo = bajo.merge(cortados, on=["inc", "t"], how="left", indicator=True)
cand_pd = bajo[bajo._merge == "left_only"][["dia", "t"]].drop_duplicates()
tp = time.time() - t1
assert set(map(tuple, cand_pd.values)) == set(map(tuple, cand_gf.values)), "el motivo y el anti-join no coinciden"
out(f"Merge y anti-join: el mismo conjunto ({tp:.3f} s)")

# --------------------------------------------------------------------- evaluación por CT-dia
dim = t["dim_ct"]
estudio = set(dim[dim.en_estudio & dim.trafo_elemento_id.notna()].trafo_elemento_id.astype("int64"))
af = AF[AF.dst.isin(estudio)].copy()
af["dia"] = af.ts.dt.normalize()
dias_inc = af.groupby("src").dia.apply(set).to_dict()
af["sigue"] = [(dd - pd.Timedelta(days=1)) in dias_inc[i] for dd, i in zip(af.dia, af.src)]
pos = af[~af.sigue][["dia", "dst"]].drop_duplicates()
pos = pos[(pos.dia >= DESDE) & (pos.dia <= HASTA) & ~pos.dia.isin(EXCLUIDOS)]
dias = [dd for dd in pd.date_range(DESDE, HASTA, freq="D") if dd not in EXCLUIDOS]
n_ctdia = len(dias) * len(estudio)
cand = cand_gf[cand_gf["t.id"].isin(estudio) & cand_gf.dia.isin(set(dias))]
cand_set = set(zip(cand.dia, cand["t.id"]))
pos_set = set(zip(pos.dia, pos.dst))
dentro = len(cand_set)
pos_dentro = len(cand_set & pos_set)
fuera = n_ctdia - dentro
pos_fuera = len(pos_set) - pos_dentro
base = len(pos_set) / n_ctdia
out(f"CT-dia evaluados {n_ctdia:,} ({len(dias)} dias, {len(estudio)} CT), con interrupcion nueva {len(pos_set):,} "
    f"({base:.2%})")
out(f"  dias con algun candidato: {cand.dia.nunique()}; candidatos por dia con alguno: mediana "
    f"{cand.groupby('dia').size().median():.0f}")
out(f"  positivos dentro: {pos_dentro} de {len(pos_set):,}")
out(f"  dentro del conjunto: {dentro:,} CT-dia ({dentro / n_ctdia:.2%}), tasa {pos_dentro / max(dentro, 1):.2%} "
    f"({pos_dentro / max(dentro, 1) / base:.1f} veces la base), recoge el {pos_dentro / len(pos_set):.1%} de los positivos")
out(f"  fuera: tasa {pos_fuera / fuera:.2%}")
out(f"tiempo total {time.time() - t0:.1f} s")
(SALIDA / "m2_cabecera_ayer.txt").write_text("\n".join(lineas) + "\n", encoding="utf-8")
