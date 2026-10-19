"""Pruebas del plan financiero (experimentos/plan_financiero/plan_financiero.py).

Sin Spark: solo la biblioteca estándar. Comprueban que el modelo reproduce al euro las
cifras de la v10, que el balance cuadra en todos los cierres, que la prueba de cuadre
falla cuando algo no cuadra y que la v11 cumple las decisiones del 19 y del 20.10.2026:
caja y patrimonio del art. 363 en positivo todos los meses, ENISA por debajo de los
fondos propios de 2026 y un plan igual o mejor que el de la v10.
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


def test_v11_cumple_todos_los_meses(calculo):
    S = pf.V11
    pf.comprobar_v11(calculo["r11"], calculo["r11_60"], S)
    r = calculo["r11"]
    assert r["balances"][0]["total_pn"] >= S["enisa"]["importe"]
    assert r["P"]["entrada_prestamo"][pf.MES_ENISA_V11] == pf.IMPORTE_ENISA_V11
    assert sum(r["P"]["entrada_prestamo"][:pf.MES_ENISA_V11]) == 0
    assert pf.sueldo_renunciado(S) == pytest.approx(90000 / 12 * 0.5 * (pf.MES_ENISA_V11 - 1))


def test_sin_medio_sueldo_algun_mes_incumple():
    S = copy.deepcopy(pf.V11)
    S["recortes_fundadores"] = []
    r36, r60 = pf.simular(S), pf.simular(pf.a_60_meses(S))
    with pytest.raises(AssertionError):
        pf.comprobar_v11(r36, r60, S)


def test_un_sueldo_aplazado_no_arregla_el_patrimonio():
    # el patrimonio no depende de cuándo se paga el sueldo: sin rebaja, el mes 4 queda por debajo del umbral
    S = copy.deepcopy(pf.V11)
    S["recortes_fundadores"] = []
    b = pf.simular(S)["balances"][4]
    assert b["pn_mercantil"] < b["umbral_363"]


def test_v11_igual_o_mejor_que_v10(calculo):
    c10, c11 = calculo["c10"], calculo["c11"]
    for a in range(1, 6):
        assert c11[f"ingresos_a{a}"] >= c10[f"ingresos_a{a}"]
        assert c11[f"resultado_a{a}"] >= c10[f"resultado_a{a}"]
    assert c11["van5"] >= c10["van5"]
    assert c11["punto_muerto_licencias"] <= c10["punto_muerto_licencias"]
    peor = dict(c11, resultado_a3=c10["resultado_a3"] - 1)
    with pytest.raises(AssertionError):
        pf.comprobar_igual_o_mejor(c10, peor)
    peor = dict(c11, punto_muerto_licencias=c10["punto_muerto_licencias"] + 0.1)
    with pytest.raises(AssertionError):
        pf.comprobar_igual_o_mejor(c10, peor)


def test_tarifa_por_tramos_minimo_y_sin_techo():
    t = pf.TARIFA
    assert pf.licencia(0, 0) == t["minimo"]
    assert pf.licencia(10, 600) == t["minimo"]
    assert pf.licencia(100, 0) == 3000 + 100 * 150
    assert pf.licencia(101, 0) == 3000 + 100 * 150 + 15
    assert pf.licencia(301, 0) == 3000 + 100 * 150 + 200 * 15 + 10
    grande = pf.licencia(5000, 300000)
    assert grande == 3000 + 100 * 150 + 200 * 15 + 4700 * 10 + 0.10 * 300000
    # cada CT paga el precio de su tramo: la licencia crece sin saltos
    previo = pf.licencia(1, 0)
    for ct in range(2, 1000):
        actual = pf.licencia(ct, 0)
        assert 0 <= actual - previo <= 150
        previo = actual
    assert pf.V11["precio_licencia"] == pf.licencia(**pf.RED_TIPO_PLAN)
    assert pf.licencia(**pf.REDES_CALIBRACION["Distribuidora 3"]) == t["minimo"]
