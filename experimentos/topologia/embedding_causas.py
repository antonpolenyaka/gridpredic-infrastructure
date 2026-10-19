"""
E2. Embedding de grafo de conocimiento sobre CT, grupo, cabecera, celda,
municipio y familia de causa: aporta algo al ranking anual por familia?

Grafo para cada año evaluado Y (2021 - 2025, los del modelo anual):
  entidades: CT del estudio, grupo de red, cabecera, celda, municipio, tipo
  de zona, red y las cuatro familias de causa de riesgo_ct (meteo, fauna,
  equipo, resto);
  relaciones: EN_GRUPO, CABECERA, EN_CELDA, EN_MUNICIPIO, EN_ZONA, DE_RED y
  SUFRE (CT -> familia si el CT tuvo alguna interrupción imprevista de esa
  familia entre 2016 y el 31.12.(Y-1)).
Tarea: para cada familia, ordenar los CT vigentes en Y por la plausibilidad
de (CT, SUFRE, familia); positivos, los CT con alguna interrupción de esa
familia en Y.

Modelos: TransE con los parámetros del cuaderno (256 dimensiones, 100 épocas,
un negativo sin filtrar; una semilla), TransE tipado y filtrado (32
dimensiones, 300 épocas, 3 negativos, norma 1) y DistMult tipado y filtrado
(32, 300, 3); dos semillas en los tipados.
Referencias sin grafo aprendido: popularidad del CT (interrupciones de
cualquier causa en dos años), lista naif (kVA.h de la familia en el CT en dos
años, como s_naif), historia de la familia en el CT desde 2016, kVA.h de la
familia por kVA de su grupo, cabecera, celda y municipio en tres años, y una
logística con todo eso entrenada con los años anteriores (la tarea 1 del
laboratorio: el modelo clásico antes que el grafo).
Métricas: PR-AUC, ROC-AUC, parte de los CT con interrupción y de los kVA.h de
la familia en el 10 % superior (empates rotos al azar, media de 20), y PR-AUC
de los CT sin historia de la familia (candidatos filtrados).

Cada año evaluado se añade a kge_causas_por_anio.csv en cuanto termina, de
modo que una ejecución cortada conserva lo hecho. E0 (TransE de una sola
relación) está en embedding_una_relacion.py.

Uso: python embedding_causas.py <carpeta de la exportación de Gold> [salida/]
"""
import os
import sys
import time

import numpy as np
import pandas as pd
from scipy.stats import rankdata
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import kge_lib  # noqa: E402
from comun import activos, cargar, salida  # noqa: E402
from kge_lib import entrenar_distmult, entrenar_transe, score_distmult, score_transe  # noqa: E402

YEARS = [2021, 2022, 2023, 2024, 2025]
FAMS = ["meteo", "fauna", "equipo", "resto"]
SEEDS = 2
F3 = lambda v: f"{v:.3f}"  # noqa: E731
pd.set_option("display.width", 230)


def captura(values, scores, frac=0.10, reps=20, seed=0):
    k = max(1, int(np.ceil(frac * len(values))))
    tot = values.sum()
    if tot <= 0:
        return np.nan
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(reps):
        order = np.lexsort((rng.random(len(scores)), -scores))
        out.append(values[order[:k]].sum() / tot)
    return float(np.mean(out))


def pct(x):
    return rankdata(x) / len(x)


def tablas(dim, it):
    """Por CT, año y familia: interrupciones y kVA.h."""
    g = it.groupby(["node", "anio", "familia"]).agg(n=("ct_id", "size"), kvah=("kva_h", "sum")).reset_index()
    return g


def rasgos(dim, agg, Y, f, n):
    """Rasgos de referencia de cada CT para predecir el año Y y la familia f, con datos anteriores a Y."""
    def suma(years, fam=None, col="kvah"):
        s = agg[agg.anio.isin(years) & ((agg.familia == fam) if fam else True)].groupby("node")[col].sum()
        v = np.zeros(n)
        v[s.index.values] = s.values
        return v
    pop2 = suma([Y - 1, Y - 2], None, "n")
    naif = suma([Y - 1, Y - 2], f, "kvah")
    hist = suma(list(range(2016, Y)), f, "n")
    kv3 = suma([Y - 1, Y - 2, Y - 3], f, "kvah")
    out = {"popularidad (todas las causas, 2 anios)": pop2, "naif (kVA.h de la familia en el CT, 2 anios)": naif,
           "historia de la familia en el CT desde 2016": hist}
    kva = dim.kva.values
    for col, name in (("grupo_red_id", "grupo"), ("cabecera", "cabecera"), ("celda_id", "celda"),
                      ("municipio_id", "municipio")):
        key = dim[col].astype(str).where(dim[col].notna(), None).values
        df = pd.DataFrame({"k": key, "kv3": kv3, "kva": kva})
        tot = df.groupby("k")[["kv3", "kva"]].transform("sum")
        r = np.where(pd.notna(key) & (tot.kva.values > 0), tot.kv3.values / np.maximum(tot.kva.values, 1e-9), 0.0)
        out[f"{name}: kVA.h de la familia por kVA, 3 anios"] = r
    return out


def construir_kg(dim, agg, Y, n):
    ents = {"ct": [f"ct{i}" for i in range(n)]}
    tipo = [0] * n
    names = list(ents["ct"])
    def add(prefix, values, t):
        idx = {}
        for v in values:
            if pd.isna(v):
                continue
            key = f"{prefix}:{v}"
            if key not in idx:
                idx[key] = len(names)
                names.append(key)
                tipo.append(t)
        return idx
    cols = [("grupo_red_id", "g", 1), ("cabecera", "c", 2), ("celda_id", "l", 3), ("municipio_id", "m", 4),
            ("tipo_zona_estat", "z", 5), ("distribuidora_id", "r", 6)]
    maps = {}
    for col, p, t in cols:
        maps[col] = add(p, dim[col].values, t)
    fam_idx = add("f", FAMS, 7)
    tr = []
    for rel, (col, p, t) in enumerate(cols):
        for i, v in enumerate(dim[col].values):
            if pd.notna(v):
                tr.append((i, rel, maps[col][f"{p}:{v}"]))
    past = agg[(agg.anio < Y)].groupby(["node", "familia"]).size().reset_index()
    for node, fam in zip(past.node.values, past.familia.values):
        tr.append((int(node), 6, fam_idx[f"f:{fam}"]))
    tr = np.array(tr, dtype=np.int64)
    tipo = np.array(tipo)
    cola_tipo = np.array([1, 2, 3, 4, 5, 6, 7])
    return tr, len(names), 7, tipo, cola_tipo, fam_idx


def evaluar(rows, Y, f, name, s, lab, kvah, hist, act):
    s, lab, kvah, hist = s[act], lab[act], kvah[act], hist[act]
    row = dict(anio=Y, familia=f, puntuacion=name, positivos=int(lab.sum()), ct=int(act.sum()))
    if lab.sum() == 0:
        return
    row["pr_auc"] = average_precision_score(lab, s)
    row["roc_auc"] = roc_auc_score(lab, s)
    row["captura_ct_10"] = captura(lab.astype(float), s)
    row["captura_kvah_10"] = captura(kvah, s)
    m = hist == 0
    if lab[m].sum() and (~lab[m]).sum():
        row["pr_auc_sin_historia"] = average_precision_score(lab[m], s[m])
        row["tasa_sin_historia"] = lab[m].mean()
        row["positivos_sin_historia"] = int(lab[m].sum())
    rows.append(row)


def main():
    t0 = time.time()
    dim, it, inc, aa = cargar()
    n = len(dim)
    agg = tablas(dim, it)
    rows = []
    for Y in YEARS:
        act = activos(dim, f"{Y}-01-01", f"{Y + 1}-01-01")
        kge_lib.POOLS.clear()
        tr, ne, nr, tipo, cola_tipo, fam_idx = construir_kg(dim, agg, Y, n)
        kge = {}
        E, R = entrenar_transe(tr, ne, nr, d=256, epochs=100, k=1, modo="cuaderno", seed=0)
        kge["TransE como el cuaderno"] = [("t", E, R)]
        for seed in range(SEEDS):
            E, R = entrenar_transe(tr, ne, nr, d=32, epochs=300, k=3, modo="tipado", seed=seed, tipo_ent=tipo,
                                   cola_tipo=cola_tipo, normalizar=True)
            kge.setdefault("TransE tipado y filtrado", []).append(("t", E, R))
            E, R = entrenar_distmult(tr, ne, nr, d=32, epochs=300, k=3, seed=seed, tipo_ent=tipo, cola_tipo=cola_tipo)
            kge.setdefault("DistMult tipado y filtrado", []).append(("d", E, R))
        print(f"  {Y}: grafo con {ne} entidades y {len(tr)} triples ({(tr[:, 1] == 6).sum()} SUFRE); "
              f"modelos entrenados en {time.time() - t0:.0f} s", flush=True)
        heads = np.arange(n)
        for f in FAMS:
            cur = agg[(agg.anio == Y) & (agg.familia == f)]
            lab = np.zeros(n, bool)
            lab[cur.node.values] = True
            kvah = np.zeros(n)
            kvah[cur.node.values] = cur.kvah.values
            feats = rasgos(dim, agg, Y, f, n)
            hist = feats["historia de la familia en el CT desde 2016"]
            scores = dict(feats)
            # logística con los años anteriores (2019 .. Y-1)
            Xs, ys = [], []
            for Yp in range(2019, Y):
                fp = rasgos(dim, agg, Yp, f, n)
                actp = activos(dim, f"{Yp}-01-01", f"{Yp + 1}-01-01")
                labp = np.zeros(n, bool)
                curp = agg[(agg.anio == Yp) & (agg.familia == f)]
                labp[curp.node.values] = True
                Xs.append(np.log1p(np.c_[tuple(fp.values())])[actp])
                ys.append(labp[actp])
            Xt, yt = np.vstack(Xs), np.concatenate(ys)
            mu, sd = Xt.mean(0), Xt.std(0) + 1e-9
            lr = LogisticRegression(C=1.0, max_iter=3000).fit((Xt - mu) / sd, yt)
            scores["logistica con esos rasgos (anios anteriores)"] = lr.predict_proba(
                (np.log1p(np.c_[tuple(feats.values())]) - mu) / sd)[:, 1]
            fi = fam_idx[f"f:{f}"]
            for name, models in kge.items():
                ss = []
                for kind, E, R in models:
                    sc = score_transe if kind == "t" else score_distmult
                    ss.append(pct(sc(E, R, heads, 6, np.full(n, fi))))
                scores[name] = np.mean(ss, axis=0)
            naif = scores["naif (kVA.h de la familia en el CT, 2 anios)"]
            logi = scores["logistica con esos rasgos (anios anteriores)"]
            celda = scores["celda: kVA.h de la familia por kVA, 3 anios"]
            scores["naif + TransE tipado (percentiles)"] = (pct(naif) + scores["TransE tipado y filtrado"]) / 2
            scores["naif + DistMult (percentiles)"] = (pct(naif) + scores["DistMult tipado y filtrado"]) / 2
            scores["naif + celda (percentiles, control sin grafo aprendido)"] = (pct(naif) + pct(celda)) / 2
            scores["logistica + TransE tipado (percentiles)"] = (pct(logi) + scores["TransE tipado y filtrado"]) / 2
            scores["logistica + DistMult (percentiles)"] = (pct(logi) + scores["DistMult tipado y filtrado"]) / 2
            scores["azar"] = np.random.default_rng(Y).random(n)
            for name, s in scores.items():
                evaluar(rows, Y, f, name, np.asarray(s, dtype=float), lab, kvah, hist, act)
        pd.DataFrame(rows).to_csv(salida("kge_causas_por_anio.csv"), index=False)
        print(f"  {Y}: evaluado y guardado ({time.time() - t0:.0f} s)", flush=True)
    r = pd.DataFrame(rows)
    cols = ["pr_auc", "roc_auc", "captura_ct_10", "captura_kvah_10", "pr_auc_sin_historia"]
    print("\n== Positivos por familia y anio (CT con alguna interrupcion de la familia; sin historia de la familia)")
    print(r[r.puntuacion == "azar"].pivot_table(index="familia", columns="anio",
                                                 values=["positivos", "positivos_sin_historia"]).to_string())
    for f in FAMS:
        print(f"\n== Familia {f}: media 2021 - 2025")
        print(r[r.familia == f].groupby("puntuacion")[cols].mean().sort_values("captura_kvah_10", ascending=False)
              .to_string(float_format=F3))
    print("\n== Media de las cuatro familias y los cinco anios")
    print(r.groupby("puntuacion")[cols].mean().sort_values("pr_auc", ascending=False).to_string(float_format=F3))
    print(f"\nTiempo E2: {time.time() - t0:.0f} s")

    print(f"Tiempo total: {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main()
