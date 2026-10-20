"""
Funciones comunes para los embeddings de CT del grafo de co-corte
map_aguas_arriba (trafo - elemento cuyo cambio de estado le produjo un Off).

No hay gensim, node2vec, torch ni PyTorch Geometric en el entorno, así que:
  - incidencia_tfidf: la fila de cada trafo como "bolsa de elementos" con
    pesos TF-IDF (el elemento que corta a muchos trafos informa menos, como
    una palabra frecuente). Es el embedding hecho a mano, disperso.
  - lsa: SVD truncada de esa matriz (como LSA en texto), denso.
  - espectral: Laplacian eigenmaps (sklearn SpectralEmbedding) sobre la
    similitud coseno entre las filas de incidencia.
  - netmf: DeepWalk en su forma cerrada de factorización de matrices (Qiu et
    al., WSDM 2018): log(max(vol/(bT) * sum_r (D^-1 A)^r D^-1, 1)) y SVD.
    Equivale a paseos aleatorios con ventana T y b muestras negativas sin
    entrenar nada; con 1.666 nodos es una matriz densa pequeña.
"""
import os
import sys

import numpy as np
import pandas as pd
from sklearn.decomposition import TruncatedSVD
from sklearn.manifold import SpectralEmbedding

# La exportación de Gold (zonas_20261013_1041) y las puntuaciones del riesgo anual (rct_todas_20261012_0744/
# puntuaciones.csv), las dos en _runlogs/ml_resultados del portátil: primer y segundo argumento de los guiones.
DIR = sys.argv[1] if len(sys.argv) > 1 else ""
PUNT = sys.argv[2] if len(sys.argv) > 2 else ""
OUT = sys.argv[3] if len(sys.argv) > 3 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "salida")


def load():
    dim = pd.read_parquet(f"{DIR}/dim_ct.parquet")
    aa = pd.read_parquet(f"{DIR}/map_aguas_arriba.parquet")
    it = pd.read_parquet(f"{DIR}/interrupciones.parquet")
    return dim, aa, it


def incidence(aa, cutoff=None, weight="n_cortes"):
    """Rows: trafos; columns: cutting elements. weight 'n_cortes' (all the history) or 'binary'."""
    e = aa if cutoff is None else aa[aa.primer_conocido_ts < pd.Timestamp(cutoff)]
    trafos = np.sort(e.trafo_elemento_id.unique())
    elems = np.sort(e.elemento_corte_id.unique())
    ti = {t: i for i, t in enumerate(trafos)}
    ei = {x: j for j, x in enumerate(elems)}
    B = np.zeros((len(trafos), len(elems)))
    vals = e.n_cortes.values.astype(float) if weight == "n_cortes" else np.ones(len(e))
    for t, x, v in zip(e.trafo_elemento_id.values, e.elemento_corte_id.values, vals):
        B[ti[t], ei[x]] += v
    return B, trafos, elems


def l2(x):
    return x / np.maximum(np.linalg.norm(x, axis=1, keepdims=True), 1e-12)


def incidencia_tfidf(B):
    tf = B / np.maximum(B.sum(1, keepdims=True), 1e-12)
    df = (B > 0).sum(0)
    idf = np.log(B.shape[0] / np.maximum(df, 1)) + 1.0
    return l2(tf * idf)


def lsa(X, d=16, seed=0):
    return l2(TruncatedSVD(n_components=d, random_state=seed).fit_transform(X))


def espectral(X, d=16, seed=0):
    W = X @ X.T
    np.fill_diagonal(W, 0.0)
    W = np.maximum(W, 0.0)
    return SpectralEmbedding(n_components=d, affinity="precomputed", random_state=seed).fit_transform(W)


def netmf(B, d=16, window=10, neg=1.0):
    """DeepWalk as matrix factorisation on the bipartite graph (trafos then elements). Returns the trafo rows."""
    nt, ne = B.shape
    A = np.zeros((nt + ne, nt + ne))
    A[:nt, nt:] = B
    A[nt:, :nt] = B.T
    deg = A.sum(1)
    vol = deg.sum()
    Dinv = 1.0 / np.maximum(deg, 1e-12)
    P = A * Dinv[:, None]
    S = np.zeros_like(A)
    Pr = np.eye(len(A))
    for _ in range(window):
        Pr = Pr @ P
        S += Pr
    M = (vol / (neg * window)) * S * Dinv[None, :]
    M = np.log(np.maximum(M, 1.0))
    M = (M + M.T) / 2
    w, U = np.linalg.eigh(M)
    order = np.argsort(-np.abs(w))[:d]
    E = U[:, order] * np.sqrt(np.abs(w[order]))
    return E[:nt]


def embeddings(B, d=16, seed=0):
    X = incidencia_tfidf(B)
    return {
        "incidencia_tfidf": X,
        "lsa": lsa(X, d, seed),
        "espectral": espectral(X, d, seed),
        "netmf": netmf(B, d),
    }


def knn_cosine(E, k, mask_same=None):
    """Indices of the k nearest rows by cosine (self excluded); mask_same restricts to rows with the same value."""
    En = l2(E)
    S = En @ En.T
    np.fill_diagonal(S, -np.inf)
    if mask_same is not None:
        S = np.where(mask_same[:, None] == mask_same[None, :], S, -np.inf)
    return np.argsort(-S, axis=1)[:, :k], S
