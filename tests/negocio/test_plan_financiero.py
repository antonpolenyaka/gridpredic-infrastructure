"""Pruebas del plan financiero (experimentos/plan_financiero/plan_financiero.py).

Sin Spark: solo la biblioteca estándar. Comprueban que el modelo reproduce al euro las
cifras de la v10, que el balance cuadra en todos los cierres, que la prueba de cuadre
falla cuando algo no cuadra y que la v11 cumple las tres decisiones del 19.10.2026.
"""

import copy
import importlib.util
import os

import pytest

RUTA = os.path.join(os.path.dirname(__file__), "..", "..", "experimentos", "plan_financiero", "plan_financiero.py")
spec = importlib.util.spec_from_file_location("plan_financiero", RUTA)
pf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pf)


@pytest.fixture(scope="module")
def calculo():
    return pf.calcular()


def test_reproduce_la_v10(calculo):
    r = calculo["r10"]
    assert [pf.r0(pf.pyg_anual(r, a)["resultado"]) for a in (1, 2, 3)] == [-75330, -19173, 39147]
    assert [pf.r0(r["caja"][a * 12]) for a in (1, 2, 3)] == [288313, 286906, 374377]
    cm, mm = pf.caja_minima(r)
    assert (pf.r0(cm), mm) == (30625, 5)
    assert pf.r0(r["balances"][11]["pn_mercantil"]) == 10182


def test_balance_cuadra_en_los_37_cierres(calculo):
    for clave in ("r10", "r11"):
        r = calculo[clave]
        assert len(r["balances"]) == 37
        for b in r["balances"].values():
            assert abs(b["total_activo"] - b["total_pn_pasivo"]) < 0.01


def test_la_prueba_de_cuadre_falla_si_no_cuadra(calculo):
    r = copy.deepcopy(calculo["r11"])
    r["caja"][24] += 1.0
    with pytest.raises(AssertionError):
        pf.comprobar_encadenado(r)


def test_v11_cumple_art_363_y_caja_positiva(calculo):
    for clave in ("r11", "r11_60"):
        h, _ = pf.holgura_363(calculo[clave])
        cm, _ = pf.caja_minima(calculo[clave])
        assert h >= 0 and cm >= 0
    # sin ENISA en el momento 0, los fondos propios de las cuentas de 2026 cubren el préstamo
    assert calculo["r11"]["balances"][0]["total_pn"] >= pf.V11["enisa"]["importe"]
    assert calculo["r11"]["P"]["entrada_prestamo"][pf.MES_ENISA_V11] == pf.V11["enisa"]["importe"]
    assert sum(calculo["r11"]["P"]["entrada_prestamo"][:pf.MES_ENISA_V11]) == 0


def test_v11_igual_o_mejor_que_v10(calculo):
    c10, c11 = calculo["c10"], calculo["c11"]
    for a in range(1, 6):
        assert c11[f"ingresos_a{a}"] >= c10[f"ingresos_a{a}"]
        assert c11[f"resultado_a{a}"] >= c10[f"resultado_a{a}"]
    assert c11["van5"] >= c10["van5"]
    assert c11["punto_muerto_licencias"] <= c10["punto_muerto_licencias"]


def test_tarifa_minimo_y_sin_techo():
    assert pf.licencia(0, 0) == pf.TARIFA["minimo"]
    assert pf.licencia(10, 600) == pf.TARIFA["minimo"]
    grande = pf.licencia(5000, 300000)
    assert grande == pf.TARIFA["fijo"] + 5000 * pf.TARIFA["por_ct"] + 300000 * pf.TARIFA["por_senal"]
    assert pf.V11["precio_licencia"] == pf.licencia(**pf.RED_TIPO_PLAN)
