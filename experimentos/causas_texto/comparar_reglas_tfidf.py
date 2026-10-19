"""
Las reglas de causas_incidencia.py frente a un clasificador clásico de texto (memoria, 5.8.1;
docs/causas-incidencias.md 2), sobre la tabla de incidencias que deja job_causas_incidencias.py
(incidencias_causa.csv de cau_20261012T025409_4681c9), sin Spark:

    python comparar_reglas_tfidf.py <incidencias_causa.csv> <salida/> [--rapido]

Los 563 partes de CT desde 2016, con la causa de las reglas como etiqueta (las clases con menos de cinco casos van
juntas en "otras"):

1. Cinco combinaciones de representación y modelo (recuento con Naive Bayes, como en clase; TF-IDF de palabras con
   Naive Bayes y con logística; TF-IDF de palabras y caracteres con logística y con SVM lineal), 5 particiones x 5
   repeticiones estratificadas: acierto y macro-F1.
2. Los partes repetidos: 45 partes repiten el texto de otro tras normalizar. La logística de palabras y caracteres con
   particiones estratificadas y con particiones que agrupan los textos iguales (StratifiedGroupKFold), cinco semillas.
3. El parte más parecido (k = 1, coseno sobre el mismo TF-IDF), el voto de los cinco más parecidos, el centroide de
   cada causa y la logística, en las mismas particiones (sin agrupar y agrupando), y la logística sin el nombre de la
   instalación que classify_frame añade al final del texto.
4. El acuerdo con el código oficial de Extremadura con el criterio de job_causas_incidencias.py (CODIGO_ESPERADO: la
   causa cuadra si es de la familia compatible con el código), en las 421 incidencias con código y causa leída del
   texto: las reglas (dentro de muestra, porque se escribieron leyendo estos partes) y cada modelo fuera de
   partición; y el acierto exacto de una logística entrenada para dar el código.
5. La curva de aprendizaje de la logística (test fijo del 30 %).

Deja comparativa.csv (una fila por método y partición), desacuerdos.csv (los desacuerdos entre las reglas y la
logística de la semilla 0 con la clave completa de cada incidencia: red como Distribuidora N, periodo_id e id) y
comparativa.md. Con --rapido solo hace 1, 2 y 4 y los desacuerdos (las cifras que cita la memoria, en un par de
minutos); sin él, también 3 y 5 (unos diez minutos con la máquina libre). No imprime ni guarda ningún texto ni término de los partes, ni nombres de instalaciones o de bases de
datos: la red se numera por su número de incidencias, como en la memoria.
"""

import os
import sys
import time
import warnings

import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import (RepeatedStratifiedKFold, StratifiedGroupKFold, StratifiedKFold,
                                     cross_val_predict, train_test_split)
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.preprocessing import normalize as l2
from sklearn.svm import LinearSVC

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.environ.get("GRIDPREDIC_REPO") or os.path.join(HERE, "..", "..")
sys.path.insert(0, os.path.join(REPO, "etl", "jobs", "02_silver"))
from causas_incidencia import normalize  # noqa: E402  (función pura: minúsculas, sin tildes, letras y cifras)

warnings.filterwarnings("ignore")

# Palabras vacías mínimas (NLTK no es una dependencia del repositorio)
STOP = "de la por del en el un una y a al las los que con se su para lo o e es ha fue".split()
# Copia de job_causas_incidencias.py (CODIGO_ESPERADO): las causas compatibles con cada código oficial
EXPECTED = {
    "atmosferica": {"meteorologia"},
    "fuerza_mayor": {"meteorologia", "incendio", "apagon"},
    "agente_externo": {"fauna", "vegetacion", "objeto_ajeno", "agente_externo", "terceros"},
    "accion_terceros": {"terceros", "vegetacion", "incendio", "objeto_ajeno"},
    "interna": {"equipo", "programada", "maniobra", "sobrecarga", "red_bt"},
    "instalacion_particular": {"equipo", "fauna", "vegetacion", "terceros", "meteorologia", "objeto_ajeno"},
    "otra_distribuidora": {"aguas_arriba"},
    "transporte": {"aguas_arriba", "apagon"},
    "desconocida": {"desconocida", "sin_causa", "apagon"},
}


def fits_code(cause, code, origin) -> bool:
    return cause in EXPECTED.get(code, set()) or (code == "instalacion_particular" and origin == "instalacion_particular")


def word_vectorizer():
    return TfidfVectorizer(ngram_range=(1, 2), min_df=2, stop_words=STOP, sublinear_tf=True)


def word_char():
    return FeatureUnion([("palabras", word_vectorizer()),
                         ("caracteres", TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=2,
                                                        sublinear_tf=True))])


def logistic():
    return LogisticRegression(C=10, max_iter=2000, class_weight="balanced")


COMBINATIONS = {
    "recuento + Naive Bayes": lambda: Pipeline([("v", CountVectorizer(min_df=2, stop_words=STOP)),
                                                ("m", MultinomialNB())]),
    "TF-IDF palabras + Naive Bayes": lambda: Pipeline([("v", word_vectorizer()), ("m", MultinomialNB())]),
    "TF-IDF palabras + logística": lambda: Pipeline([("v", word_vectorizer()), ("m", logistic())]),
    "TF-IDF palabras y caracteres + logística": lambda: Pipeline([("v", word_char()), ("m", logistic())]),
    "TF-IDF palabras y caracteres + SVM lineal": lambda: Pipeline([("v", word_char()),
                                                                   ("m", LinearSVC(C=1.0, class_weight="balanced"))]),
}


def load(path):
    df = pd.read_csv(path, low_memory=False)
    counts = df["source_database"].value_counts()
    df["red"] = df["source_database"].map({db: f"Distribuidora {i + 1}" for i, db in enumerate(counts.index)})
    df["anio"] = pd.to_datetime(df["inicio_ts"], errors="coerce").dt.year
    df["texto"] = df["texto_causa"].fillna("").str.replace("|", " ", regex=False)
    # classify_frame añade el nombre de la instalación como último tramo de texto_causa: la variante sin él lo quita
    place = df["instalacion_origen"].map(normalize)
    pieces = df["texto_causa"].fillna("").str.split(r" \| ")
    df["texto_sin_instalacion"] = [" ".join(s[:-1] if (p and s and s[-1].strip() == p) else s)
                                   for s, p in zip(pieces, place)]
    rec = df[(df["anio"] >= 2016) & (df["nivel_afectacion"] == "ct")].reset_index(drop=True)
    return rec.drop(columns=["source_database", "instalacion_origen", "instalacion_cabecera", "texto_causa"])


def grouped_labels(y):
    counts = pd.Series(y).value_counts()
    small = set(counts[counts < 5].index)
    return np.array([c if c not in small else "otras" for c in y]), small


def nearest(sim, labels, k):
    order = np.argsort(-sim, axis=1)
    first = labels[order[:, 0]]
    vote = [pd.Series(sim[i, order[i, :k]], index=labels[order[i, :k]]).groupby(level=0).sum().idxmax()
            for i in range(sim.shape[0])]
    return first, np.array(vote)


def centroids(x_train, labels, x_test):
    names = np.unique(labels)
    centres = l2(np.vstack([np.asarray(x_train[labels == c].mean(axis=0)).ravel() for c in names]))
    return names[np.argmax(np.asarray(x_test @ centres.T), axis=1)]


def similarity_methods(rec, y, splits, with_code):
    """Más parecido, cinco más parecidos, centroide, logística y logística sin la instalación, en las mismas particiones."""
    methods = ("parecido_k1", "parecidos_k5", "centroide", "logistica", "logistica_sin_instalacion")
    scores = {m: {"acierto": [], "macro_f1": []} for m in methods}
    fits = {m: np.zeros(len(rec)) for m in methods}
    seen = np.zeros(len(rec))
    for train, test in splits:
        vec = word_char().fit(rec["texto"].iloc[train])
        x_train = l2(sparse.csr_matrix(vec.transform(rec["texto"].iloc[train])))
        x_test = l2(sparse.csr_matrix(vec.transform(rec["texto"].iloc[test])))
        k1, k5 = nearest(np.asarray((x_test @ x_train.T).todense()), y[train], 5)
        found = {"parecido_k1": k1, "parecidos_k5": k5, "centroide": centroids(x_train, y[train], x_test)}
        model = logistic().fit(vec.transform(rec["texto"].iloc[train]), y[train])
        found["logistica"] = model.predict(vec.transform(rec["texto"].iloc[test]))
        plain = Pipeline([("v", word_char()), ("m", logistic())]).fit(rec["texto_sin_instalacion"].iloc[train], y[train])
        found["logistica_sin_instalacion"] = plain.predict(rec["texto_sin_instalacion"].iloc[test])
        for m, p in found.items():
            scores[m]["acierto"].append(accuracy_score(y[test], p))
            scores[m]["macro_f1"].append(f1_score(y[test], p, average="macro"))
            fits[m][test] += [fits_code(c, k, o) for c, k, o in zip(p, rec["casuistica_nombre"].iloc[test],
                                                                     rec["origen"].iloc[test])]
        seen[test] += 1
    return {m: {"acierto": np.mean(v["acierto"]), "macro_f1": np.mean(v["macro_f1"]),
                "cuadra_codigo": float(np.mean(fits[m][with_code] / seen[with_code]))} for m, v in scores.items()}


def md(frame):
    shown = frame.copy()
    for c in shown.columns:
        if shown[c].dtype.kind == "f":
            shown[c] = shown[c].map(lambda v: "" if pd.isna(v) else f"{v:.3f}")
    return "\n".join(["| " + " | ".join(shown.columns) + " |", "| " + " | ".join("---" for _ in shown.columns) + " |"]
                     + ["| " + " | ".join(str(v) for v in r) + " |" for r in shown.itertuples(index=False)])


def main(argv) -> int:
    quick = "--rapido" in argv
    path, out = [a for a in argv if a != "--rapido"][:2]
    os.makedirs(out, exist_ok=True)
    t0 = time.time()
    rec = load(path)
    y, small = grouped_labels(rec["causa"].to_numpy())
    groups = pd.factorize(rec["texto"].map(normalize))[0]
    with_code = (rec["casuistica_nombre"].notna() & (rec["fuente_causa"] == "texto")).to_numpy()
    rules_fit = float(np.mean([fits_code(c, k, o) for c, k, o in zip(rec["causa"][with_code],
                                                                     rec["casuistica_nombre"][with_code],
                                                                     rec["origen"][with_code])]))
    rows = []

    # 1. Cinco combinaciones, 5 x 5 estratificadas
    repeated = list(RepeatedStratifiedKFold(n_splits=5, n_repeats=5, random_state=7).split(rec, y))
    for name, build in COMBINATIONS.items():
        acc, f1 = [], []
        for train, test in repeated:
            p = build().fit(rec["texto"].iloc[train], y[train]).predict(rec["texto"].iloc[test])
            acc.append(accuracy_score(y[test], p))
            f1.append(f1_score(y[test], p, average="macro"))
        rows.append({"bloque": "1. combinaciones", "metodo": name, "particion": "5 x 5 estratificada",
                     "acierto": np.mean(acc), "acierto_sd": np.std(acc), "macro_f1": np.mean(f1),
                     "macro_f1_sd": np.std(f1)})

    # 2. Partes repetidos y 4. acuerdo con el código: la logística fuera de partición, cinco semillas
    best = COMBINATIONS["TF-IDF palabras y caracteres + logística"]
    disagreements = None
    for scheme in ("estratificada", "agrupando textos iguales"):
        acc, f1, fit = [], [], []
        for seed in range(5):
            if scheme == "estratificada":
                cv = StratifiedKFold(5, shuffle=True, random_state=seed)
                pred = cross_val_predict(best(), rec["texto"], y, cv=cv)
            else:
                cv = StratifiedGroupKFold(5, shuffle=True, random_state=seed)
                pred = cross_val_predict(best(), rec["texto"], y, cv=cv, groups=groups)
            acc.append(accuracy_score(y, pred))
            f1.append(f1_score(y, pred, average="macro"))
            fit.append(np.mean([fits_code(c, k, o) for c, k, o in zip(pred[with_code],
                                                                      rec["casuistica_nombre"][with_code],
                                                                      rec["origen"][with_code])]))
            if scheme == "estratificada" and seed == 0:
                disagreements = rec.assign(causa_logistica=pred)
        rows.append({"bloque": "2. partes repetidos", "metodo": "TF-IDF palabras y caracteres + logística",
                     "particion": f"{scheme}, 5 semillas, cada repetición entera", "acierto": np.mean(acc),
                     "acierto_min": np.min(acc), "acierto_max": np.max(acc), "macro_f1": np.mean(f1),
                     "cuadra_codigo": np.mean(fit), "cuadra_codigo_min": np.min(fit), "cuadra_codigo_max": np.max(fit)})

    # 3. Parecidos, centroide y logística en las mismas particiones (media de los pliegues)
    schemes = {"5 x 5 estratificada": repeated,
               "5 x 5 agrupando textos iguales": [s for seed in range(5) for s in
                                                  StratifiedGroupKFold(5, shuffle=True, random_state=seed)
                                                  .split(rec, y, groups)]}
    for scheme, splits in ({} if quick else schemes).items():
        for method, found in similarity_methods(rec, y, splits, with_code).items():
            rows.append({"bloque": "3. parecidos", "metodo": method, "particion": scheme, **found})
    rows.append({"bloque": "4. código oficial", "metodo": "reglas", "particion": "dentro de muestra",
                 "cuadra_codigo": rules_fit})

    # 4. El código como etiqueta: acierto exacto de una logística entrenada para darlo
    ext = rec[with_code]
    code, _ = grouped_labels(ext["casuistica_nombre"].to_numpy())
    pred = cross_val_predict(best(), ext["texto"], code, cv=StratifiedKFold(5, shuffle=True, random_state=7))
    rows.append({"bloque": "4. código oficial", "metodo": "logística entrenada con el código",
                 "particion": "5 estratificada", "acierto": accuracy_score(code, pred),
                 "macro_f1": f1_score(code, pred, average="macro")})

    # 5. Curva de aprendizaje
    x_all, x_test, y_all, y_test = train_test_split(rec["texto"], y, test_size=0.3, stratify=y, random_state=11)
    for n in (() if quick else (50, 100, 200, 300, len(x_all))):
        acc, f1 = [], []
        for seed in range(10):
            idx = np.random.RandomState(seed).choice(len(x_all), size=min(n, len(x_all)), replace=False)
            p = best().fit(x_all.iloc[idx], y_all[idx]).predict(x_test)
            acc.append(accuracy_score(y_test, p))
            f1.append(f1_score(y_test, p, average="macro"))
        rows.append({"bloque": "5. curva de aprendizaje", "metodo": "TF-IDF palabras y caracteres + logística",
                     "particion": f"{min(n, len(x_all))} partes de entrenamiento, test fijo del 30 %",
                     "acierto": np.mean(acc), "macro_f1": np.mean(f1), "macro_f1_sd": np.std(f1)})
    table = pd.DataFrame(rows)
    table.to_csv(os.path.join(out, "comparativa.csv"), index=False)

    # Desacuerdos entre las reglas y la logística (semilla 0), con la clave completa y sin texto
    d = disagreements
    d = d[(d["fuente_causa"] == "texto") & (d["causa_logistica"] != np.where(d["causa"].isin(small), "otras",
                                                                             d["causa"]))].copy()
    d["regla_cuadra"] = [fits_code(c, k, o) if isinstance(k, str) else None
                         for c, k, o in zip(d["causa"], d["casuistica_nombre"], d["origen"])]
    d["logistica_cuadra"] = [fits_code(c, k, o) if isinstance(k, str) else None
                             for c, k, o in zip(d["causa_logistica"], d["casuistica_nombre"], d["origen"])]
    d = d[["red", "periodo_id", "id", "periodo", "regla", "causa", "causa_logistica", "casuistica_nombre",
           "regla_cuadra", "logistica_cuadra"]].rename(columns={"casuistica_nombre": "codigo_oficial"})
    if d.duplicated(["red", "periodo_id", "id"]).any():
        raise ValueError("desacuerdos.csv repetiría claves")
    d.sort_values(["red", "periodo_id", "id"]).to_csv(os.path.join(out, "desacuerdos.csv"), index=False)
    coded = d[d["codigo_oficial"].notna()]
    cross = pd.crosstab(coded["regla_cuadra"], coded["logistica_cuadra"])

    lines = ["# Reglas frente a TF-IDF en las causas de los partes", "",
             f"{len(rec)} partes de CT desde 2016 ({groups.max() + 1} textos distintos tras normalizar); "
             f"{int(with_code.sum())} con código oficial y causa leída del texto; clases agrupadas en 'otras': "
             f"{len(small)}. Las reglas cuadran con el código en {rules_fit:.3f} (dentro de muestra).", "",
             md(table[["bloque", "metodo", "particion", "acierto", "macro_f1", "cuadra_codigo"]]), "",
             f"Desacuerdos entre las reglas y la logística de la semilla 0 (causa del texto): {len(d)}, sin claves "
             f"repetidas; con código oficial {len(coded)}: la regla cuadra y la logística no "
             f"{int(cross.loc[True, False]) if True in cross.index and False in cross.columns else 0}; la logística "
             f"cuadra y la regla no {int(cross.loc[False, True]) if False in cross.index and True in cross.columns else 0}; "
             f"ninguna {int(cross.loc[False, False]) if False in cross.index and False in cross.columns else 0}.", "",
             f"Tiempo: {time.time() - t0:.0f} s."]
    text = "\n".join(lines) + "\n"
    with open(os.path.join(out, "comparativa.md"), "w", encoding="utf-8") as handle:
        handle.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
