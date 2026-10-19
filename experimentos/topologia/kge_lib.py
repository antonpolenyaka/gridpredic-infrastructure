"""
TransE y DistMult en numpy (no hay torch en el entorno), con el entrenamiento
del cuaderno 283581_UPCSchool_Graphs.ipynb (celda 29) como opción:

- modo "cuaderno": TransE con distancia L2, pérdida de margen 1, un negativo por
  positivo que cambia la cabeza o la cola (p = 0,5) por una entidad cualquiera
  al azar, sin comprobar que el triple no exista, sin normalizar las
  entidades, Adam lr 0,01, lote completo, inicialización de Xavier uniforme.
- modo "tipado": los negativos cambian la cabeza por otro CT y la cola por otra
  entidad del mismo tipo, y se descartan los que ya están en el grafo
  (diapositiva 79 de la teoría: "such that the negative triple does not
  exist in the original KG"); TransE con entidades de norma 1 (Bordes y otros,
  2013) y DistMult con pérdida logística y regularización L2.
"""
import numpy as np

POOLS = {}


class Adam:
    def __init__(self, params, lr=0.01, b1=0.9, b2=0.999, eps=1e-8):
        self.p, self.lr, self.b1, self.b2, self.eps = params, lr, b1, b2, eps
        self.m = [np.zeros_like(x) for x in params]
        self.v = [np.zeros_like(x) for x in params]
        self.t = 0

    def step(self, grads):
        self.t += 1
        for i, (x, g) in enumerate(zip(self.p, grads)):
            self.m[i] = self.b1 * self.m[i] + (1 - self.b1) * g
            self.v[i] = self.b2 * self.v[i] + (1 - self.b2) * g * g
            mh = self.m[i] / (1 - self.b1 ** self.t)
            vh = self.v[i] / (1 - self.b2 ** self.t)
            x -= self.lr * mh / (np.sqrt(vh) + self.eps)


def xavier(rng, n, d):
    b = np.sqrt(6.0 / (n + d))
    return rng.uniform(-b, b, (n, d)).astype(np.float32)


def scatter(idx, g, n):
    """Suma las filas de g en las posiciones idx (como np.add.at, pero con bincount)."""
    d = g.shape[1]
    flat = (idx.astype(np.int64)[:, None] * d + np.arange(d)[None, :]).ravel()
    return np.bincount(flat, weights=g.ravel().astype(np.float64), minlength=n * d).reshape(n, d).astype(np.float32)


def _keys(tr, ne, nr):
    return (tr[:, 0].astype(np.int64) * nr + tr[:, 1]) * ne + tr[:, 2]


def negativos(tr, ne, nr, rng, k, modo, tipo_ent=None, cola_tipo=None, cabeza_tipo=0, conocidos=None):
    """k negativos por positivo. modo 'cuaderno': entidad cualquiera, sin filtrar.
    modo 'tipado': entidad del tipo correcto; devuelve también la máscara de los que no están en el grafo."""
    rep = np.repeat(tr, k, axis=0)
    m = len(rep)
    head = rng.random(m) > 0.5
    if modo == "cuaderno":
        r = rng.integers(0, ne, m)
        rep[head, 0] = r[head]
        rep[~head, 2] = r[~head]
        return rep, np.ones(m, bool)
    pools = POOLS.setdefault(id(tipo_ent), {t: np.where(tipo_ent == t)[0] for t in np.unique(tipo_ent)})
    hp = pools[cabeza_tipo]
    rep[head, 0] = hp[rng.integers(0, len(hp), head.sum())]
    tt = cola_tipo[rep[~head, 1]]
    newt = np.empty(len(tt), dtype=rep.dtype)
    for t in np.unique(tt):
        sel = tt == t
        pool = pools[t]
        newt[sel] = pool[rng.integers(0, len(pool), sel.sum())]
    rep[~head, 2] = newt
    kk = _keys(rep, ne, nr)
    pos = np.searchsorted(conocidos, kk)
    pos = np.minimum(pos, len(conocidos) - 1)
    ok = conocidos[pos] != kk
    return rep, ok


def entrenar_transe(tr, ne, nr, d=64, epochs=300, lr=0.01, margin=1.0, k=1, modo="cuaderno", seed=0,
                    tipo_ent=None, cola_tipo=None, normalizar=False):
    rng = np.random.default_rng(seed)
    E = xavier(rng, ne, d)
    R = xavier(rng, nr, d)
    opt = Adam([E, R], lr=lr)
    conocidos = np.sort(_keys(tr, ne, nr)) if modo != "cuaderno" else None
    for _ in range(epochs):
        neg, ok = negativos(tr, ne, nr, rng, k, modo, tipo_ent, cola_tipo, 0, conocidos)
        pos = np.repeat(tr, k, axis=0)
        dp = E[pos[:, 0]] + R[pos[:, 1]] - E[pos[:, 2]]
        dn = E[neg[:, 0]] + R[neg[:, 1]] - E[neg[:, 2]]
        fp = np.linalg.norm(dp, axis=1) + 1e-9
        fn = np.linalg.norm(dn, axis=1) + 1e-9
        act = ((margin + fp - fn) > 0) & ok
        c = max(ok.sum(), 1)
        gp = (act / c / fp)[:, None] * dp
        gn = -(act / c / fn)[:, None] * dn
        allt = np.r_[pos, neg]
        allg = np.r_[gp, gn]
        gE = scatter(allt[:, 0], allg, ne) - scatter(allt[:, 2], allg, ne)
        gR = scatter(allt[:, 1], allg, nr)
        opt.step([gE, gR])
        if normalizar:
            E /= np.maximum(np.linalg.norm(E, axis=1, keepdims=True), 1e-9)
    return E, R


def entrenar_distmult(tr, ne, nr, d=64, epochs=300, lr=0.01, k=5, seed=0, tipo_ent=None, cola_tipo=None, reg=1e-4):
    rng = np.random.default_rng(seed)
    E = xavier(rng, ne, d)
    R = xavier(rng, nr, d)
    opt = Adam([E, R], lr=lr)
    conocidos = np.sort(_keys(tr, ne, nr))
    sig = lambda x: 1.0 / (1.0 + np.exp(-x))  # noqa: E731
    for _ in range(epochs):
        neg, ok = negativos(tr, ne, nr, rng, k, "tipado", tipo_ent, cola_tipo, 0, conocidos)
        gE = np.zeros_like(E)
        gR = np.zeros_like(R)
        sp = np.sum(E[tr[:, 0]] * R[tr[:, 1]] * E[tr[:, 2]], axis=1)
        sn = np.sum(E[neg[:, 0]] * R[neg[:, 1]] * E[neg[:, 2]], axis=1)
        wp = (-sig(-sp) / len(tr))[:, None]
        wn = ((sig(sn) * ok) / max(ok.sum(), 1))[:, None]
        for t, w in ((tr, wp), (neg, wn)):
            h, r, tt = E[t[:, 0]], R[t[:, 1]], E[t[:, 2]]
            gE += scatter(t[:, 0], w * r * tt, ne) + scatter(t[:, 2], w * h * r, ne)
            gR += scatter(t[:, 1], w * h * tt, nr)
        gE += 2 * reg * E
        gR += 2 * reg * R
        opt.step([gE, gR])
    return E, R


def score_transe(E, R, heads, r, tail):
    return -np.linalg.norm(E[heads] + R[r] - E[tail], axis=1)


def score_distmult(E, R, heads, r, tail):
    return np.sum(E[heads] * R[r] * E[tail], axis=1)
