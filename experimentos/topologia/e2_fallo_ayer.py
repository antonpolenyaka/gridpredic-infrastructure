"""
E2. Patrón temporal (memoria, 5.7.4): los CT que comparten elemento aguas arriba con un CT que falló ayer.

  MATCH (f:CT)<-[a:AFECTA]-(:Incidencia) WHERE date(a.inicio_ts) = $ayer
  MATCH (f)-[r1:AGUAS_ARRIBA]->(e:ElementoCorte)<-[r2:AGUAS_ARRIBA]-(v:CT)
  WHERE r1.primer_conocido_ts < $hoy AND r2.primer_conocido_ts < $hoy AND NOT v IN fallados_ayer

Se resuelve con joins de pandas para cada día del 07.01.2021 (la primera arista se conoce el 06.01.2021) al
01.08.2026, sin el apagón del 28.04.2025 ni el día siguiente, y se mide si estar en ese conjunto anticipa una
interrupción principal ese día, frente a claves que el repositorio ya usa (mismo grupo de red, misma cabecera, misma
distribuidora). Como los CT-día positivos se agrupan en pocas incidencias, también se cuentan por incidencia: cuántas
incidencias distintas hay entre los positivos y cuántas tocan el conjunto.

    python e2_fallo_ayer.py <carpeta de la exportación> [salida/]
"""

import sys
import time
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402

import grafo_gp as gg  # noqa: E402

warnings.filterwarnings("ignore", category=DeprecationWarning)

DESDE = pd.Timestamp("2021-01-07")
HASTA = pd.Timestamp("2026-08-01")
EXCLUIDOS = {gg.APAGON, gg.APAGON + pd.Timedelta(days=1)}
LINES = []


def say(text=""):
    print(text)
    LINES.append(text)


def main():
    t = gg.cargar(sys.argv[1])
    dim, aa, it = t["dim_ct"], t["map_aguas_arriba"], t["interrupciones"]
    universo = set(dim[dim.en_estudio].ct)
    grupo = dict(zip(dim.ct, dim.grupo_red_id))
    cabecera = dict(zip(dim.ct, dim.cabecera_id))
    red = dict(zip(dim.ct, dim.distribuidora_id))
    tot = aa.groupby("ct").n_cortes.transform("sum")
    aa["fuerte"] = (aa.n_cortes >= 2) & (aa.n_cortes / tot >= 0.2)

    it = it[it.ct.isin(universo)].copy()
    it["dia"] = it.inicio_ts.dt.normalize()
    por_dia = it.groupby("dia").ct.apply(set).to_dict()
    inc_ct_dia = it.groupby(["dia", "ct"]).inc.first().to_dict()
    # incidencias que ya tenían inicios el día anterior (la misma incidencia que sigue)
    inc_dias = it.groupby("inc").dia.apply(set).to_dict()
    it["sigue"] = [(d - pd.Timedelta(days=1)) in inc_dias[i] for d, i in zip(it.dia, it.inc)]
    nuevas_por_dia = it[~it.sigue].groupby("dia").ct.apply(set).to_dict()
    dias = [d for d in pd.date_range(DESDE, HASTA, freq="D") if d not in EXCLUIDOS]

    def vecinos(F, hoy, solo_fuertes=False):
        a = aa[aa.primer_conocido_ts < hoy]
        if solo_fuertes:
            a = a[a.fuerte]
        els = a.loc[a.ct.isin(F), "el"].unique()
        return set(a.loc[a.el.isin(els), "ct"].unique()) - set(F)

    claves = ["co-corte (aristas conocidas)", "co-corte fuerte (recuentos de todo el histórico)", "mismo grupo de red",
              "misma cabecera (argmax de todo el histórico)", "misma distribuidora"]
    acum = {k: {"S": 0, "SY": 0, "SYn": 0} for k in claves}
    tot_U = tot_Y = tot_Yn = 0
    tam_F, tam_N, positivos = [], [], []
    segundos = 0.0
    for hoy in dias:
        ayer = hoy - pd.Timedelta(days=1)
        F = por_dia.get(ayer, set())
        if not F:
            continue
        tam_F.append(len(F))
        t0 = time.perf_counter()
        N = vecinos(F, hoy) & universo
        segundos += time.perf_counter() - t0
        tam_N.append(len(N))
        U = universo - F
        Y = por_dia.get(hoy, set()) - F
        Yn = nuevas_por_dia.get(hoy, set()) - F  # positivos de incidencias sin inicios el día anterior
        tot_U, tot_Y, tot_Yn = tot_U + len(U), tot_Y + len(Y), tot_Yn + len(Yn)
        gF = {grupo.get(f) for f in F if pd.notna(grupo.get(f))}
        cF = {cabecera.get(f) for f in F if pd.notna(cabecera.get(f))}
        rF = {red.get(f) for f in F}
        conjuntos = {
            claves[0]: N,
            claves[1]: vecinos(F, hoy, solo_fuertes=True) & universo,
            claves[2]: {c for c in U if grupo.get(c) in gF},
            claves[3]: {c for c in U if cabecera.get(c) in cF},
            claves[4]: {c for c in U if red.get(c) in rF},
        }
        for k, S in conjuntos.items():
            S = S & U
            acum[k]["S"] += len(S)
            acum[k]["SY"] += len(S & Y)
            acum[k]["SYn"] += len(S & Yn)
        positivos += [{"dia": hoy, "inc": inc_ct_dia[(hoy, c)], "dentro": c in N} for c in Y]

    n_dias = len(tam_F)
    say(f"días evaluados: {len(dias)}; con algún CT fallado el día anterior: {n_dias}")
    say(f"CT fallados el día anterior (mediana, máximo): {int(pd.Series(tam_F).median())}, {max(tam_F)}")
    say(f"CT en el patrón (mediana, p90, máximo): {int(pd.Series(tam_N).median())}, "
        f"{int(pd.Series(tam_N).quantile(0.9))}, {max(tam_N)}")
    say(f"tiempo del patrón con pandas: {segundos:.2f} s ({1000 * segundos / n_dias:.1f} ms por día)")
    base, base_n = tot_Y / tot_U, tot_Yn / tot_U
    say(f"\nCT-día evaluados: {tot_U:,}; con interrupción principal ese día: {tot_Y:,} (tasa {base:.4%}); "
        f"de incidencias nuevas: {tot_Yn:,} ({base_n:.4%})")
    say("conjunto | CT-día en el conjunto | tasa dentro | tasa fuera | veces la tasa base | parte de los positivos "
        "| ídem con incidencias nuevas: tasa dentro, parte")
    for k, v in acum.items():
        dentro = v["SY"] / v["S"] if v["S"] else float("nan")
        fuera = (tot_Y - v["SY"]) / (tot_U - v["S"])
        dn = v["SYn"] / v["S"] if v["S"] else float("nan")
        say(f"{k} | {v['S']:,} ({v['S'] / tot_U:.1%}) | {dentro:.3%} | {fuera:.3%} | {dentro / base:.1f} | "
            f"{v['SY'] / tot_Y:.1%} | {dn:.3%}, {v['SYn'] / tot_Yn:.1%}")

    pos = pd.DataFrame(positivos)
    g = pos.groupby("inc").agg(n=("dentro", "size"), dentro=("dentro", "mean"))
    say(f"\npor incidencia: {len(pos):,} CT-día positivos de {len(g)} incidencias distintas en {pos.dia.nunique()} días; "
        f"las tres mayores reúnen el {g.n.nlargest(3).sum() / len(pos):.0%}; tocan el conjunto del co-corte "
        f"{int((g.dentro > 0).sum())} de {len(g)} (el conjunto reúne el {acum[claves[0]]['S'] / tot_U:.0%} de los CT-día)")

    if len(sys.argv) > 2:
        Path(sys.argv[2]).mkdir(parents=True, exist_ok=True)
        (Path(sys.argv[2]) / "e2_fallo_ayer.txt").write_text("\n".join(LINES) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
