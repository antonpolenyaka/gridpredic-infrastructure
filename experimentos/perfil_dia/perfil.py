"""
Perfil de una respuesta binaria al estilo de catdes de FactoMineR (Husson, Lê y
Pagès), como se hace en el curso de Data Analytics: para cada variable continua,
el valor test de la media de la clase frente a la media global; para cada
categórica, el chi cuadrado de la tabla y el valor test hipergeométrico de cada
modalidad. Las fórmulas son las del código de catdes (versión 2.18) y
reproducen los valores test del ejemplo de clase (scoring de créditos).
"""

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency, hypergeom, mannwhitneyu, norm


def vtest_numeric(x: pd.Series, y: pd.Series, cls=1) -> dict:
    """v.test = (media de la clase - media global) / sqrt((s2 / nk) (N - nk) / (N - 1)), s con denominador N."""
    ok = x.notna() & y.notna()
    x, y = x[ok].astype(float), y[ok]
    n, nk = len(x), int((y == cls).sum())
    mean, sd = x.mean(), x.std(ddof=0)
    mean_k = x[y == cls].mean()
    v = (mean_k - mean) / np.sqrt((sd ** 2 / nk) * (n - nk) / (n - 1)) if sd > 0 and 0 < nk < n else np.nan
    u, _ = mannwhitneyu(x[y == cls], x[y != cls], alternative="two-sided")
    return {"n": n, "n_clase": nk, "media_clase": mean_k, "media_resto": x[y != cls].mean(), "media_global": mean,
            "sd_global": sd, "v_test": v, "p_valor": 2 * norm.sf(abs(v)), "d": (mean_k - mean) / sd if sd else np.nan,
            "auc": u / (nk * (n - nk))}


def vtest_category(nkj: int, nk: int, nj: int, n: int) -> tuple:
    """v.test de la modalidad j en la clase k: prueba hipergeométrica de dos colas pasada a la normal."""
    rv = hypergeom(M=n, n=nk, N=nj)
    p = min(2 * rv.cdf(nkj - 1) + rv.pmf(nkj), 2 * rv.sf(nkj) + rv.pmf(nkj), 1.0)
    sign = 1 if nkj / nk > nj / n else -1
    return (-sign * norm.ppf(p / 2) if p < 1 else 0.0), p


def catdes(df: pd.DataFrame, y: str, numeric: list, categorical: list, cls=1) -> tuple:
    """(continuas ordenadas por v.test, chi cuadrado por variable, modalidades ordenadas por v.test)."""
    rows = []
    for column in numeric:
        x = df[column]
        if x.nunique(dropna=True) < 2:
            continue
        rows.append({"variable": column, **vtest_numeric(x, df[y], cls)})
    quanti = pd.DataFrame(rows).sort_values("v_test", ascending=False).reset_index(drop=True)

    chi, mods = [], []
    for column in categorical:
        table = pd.crosstab(df[column], df[y])
        if cls not in table.columns or table.shape[0] < 2:
            continue
        stat, p, dof, expected = chi2_contingency(table.to_numpy(), correction=False)
        n, nk = int(table.to_numpy().sum()), int(table[cls].sum())
        chi.append({"variable": column, "chi2": stat, "gl": dof, "p_valor": p,
                    "v_cramer": np.sqrt(stat / (n * (min(table.shape) - 1))), "esperado_min": expected.min()})
        for mod, row in table.iterrows():
            nj, nkj = int(row.sum()), int(row[cls])
            v, pv = vtest_category(nkj, nk, nj, n)
            mods.append({"variable": column, "modalidad": mod, "dias": nj, "dias_averia": nkj, "cla_mod": 100 * nkj / nj,
                         "mod_cla": 100 * nkj / nk, "global": 100 * nj / n, "lift": (nkj / nj) / (nk / n),
                         "v_test": v, "p_valor": pv})
    chi = pd.DataFrame(chi, columns=["variable", "chi2", "gl", "p_valor", "v_cramer", "esperado_min"])
    mods = pd.DataFrame(mods, columns=["variable", "modalidad", "dias", "dias_averia", "cla_mod", "mod_cla", "global",
                                       "lift", "v_test", "p_valor"])
    chi = chi.sort_values("p_valor").reset_index(drop=True)
    mods = mods.sort_values("v_test", ascending=False).reset_index(drop=True)
    return quanti, chi, mods


def stability(df: pd.DataFrame, y: str, numeric: list, cut: pd.Timestamp) -> tuple:
    """El v.test antes y después de cut: con una relación real, el signo y el orden se repiten."""
    before = catdes(df[df["fecha"] < cut], y, numeric, [])[0].set_index("variable")["v_test"]
    after = catdes(df[df["fecha"] >= cut], y, numeric, [])[0].set_index("variable")["v_test"]
    both = pd.concat({"antes": before, "despues": after}, axis=1).dropna()
    same_sign = float((np.sign(both["antes"]) == np.sign(both["despues"])).mean())
    return both, float(both.corr(method="spearman").iloc[0, 1]), same_sign


def wilson(k: int, n: int, z: float = 1.96) -> tuple:
    """Intervalo de Wilson del 95 % de una proporción k / n."""
    if n == 0:
        return np.nan, np.nan
    p = k / n
    centre = (p + z * z / (2 * n)) / (1 + z * z / n)
    half = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return centre - half, centre + half
