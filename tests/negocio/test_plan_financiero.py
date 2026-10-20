"""Pruebas del plan financiero (experimentos/plan_financiero/plan_financiero.py).

Sin Spark: solo la biblioteca estándar. Comprueban que el modelo reproduce al euro las
cifras de la v10, que el balance cuadra en todos los cierres, que la prueba de cuadre
falla cuando algo no cuadra y que la v11 cumple las decisiones del 19 y del 20.10.2026:
caja y patrimonio del art. 363 en positivo todos los meses, ENISA por debajo de los
fondos propios de 2026 y un plan igual o mejor que el de la v10.
"""

import copy
import importlib.util
import json
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


def test_salta_si_activo_no_es_patrimonio_neto_mas_pasivo(calculo):
    balances = copy.deepcopy(calculo["r11"]["balances"])
    pf.comprobar_cuadre(balances)
    balances[7]["tesoreria"] += 1.0
    with pytest.raises(AssertionError):
        pf.comprobar_cuadre(balances)
    balances = copy.deepcopy(calculo["r11"]["balances"])
    balances[30]["periodificacion"] -= 1.0
    with pytest.raises(AssertionError):
        pf.comprobar_cuadre(balances)


def test_salidas_de_texto_al_dia_con_el_codigo(calculo):
    # solo las tablas y la comparación: el Excel, el JSON y el CSV cambian en el cuarto decimal entre
    # versiones de Python
    _, _, _, textos = pf.generar(calculo)
    for nombre in ("tablas_v11.txt", "comparacion.txt"):
        with open(os.path.join(pf.SALIDA, nombre), encoding="utf-8") as f:
            assert f.read() == textos[nombre], f"salida/{nombre} no está al día: vuelve a ejecutar plan_financiero.py"


def test_enisa_denegado_sigue_cumpliendo():
    r = pf.simular(pf.escenario_enisa_denegado(pf.V11))
    h, _ = pf.holgura_363(r)
    c, _ = pf.caja_minima(r)
    assert h >= 0 and c >= 0
    assert sum(r["P"]["entrada_prestamo"]) == 0


def test_v11_cumple_todos_los_meses(calculo):
    S = pf.V11
    pf.comprobar_v11(calculo["r11"], calculo["r11_60"], S)
    r = calculo["r11"]
    assert r["balances"][0]["total_pn"] >= S["enisa"]["importe"]
    assert r["P"]["entrada_prestamo"][pf.MES_ENISA_V11] == pf.IMPORTE_ENISA_V11
    assert sum(r["P"]["entrada_prestamo"][:pf.MES_ENISA_V11]) == 0
    desde, hasta, parte = pf.RECORTE_FUNDADORES_V11
    bruto_fundadores = sum(b for (_, b, mes) in S["plantilla"] if mes == 1)
    assert hasta == pf.MES_ENISA_V11 - 1
    assert pf.sueldo_renunciado(S) == pytest.approx(bruto_fundadores / 12 * parte * (hasta - desde + 1))


def test_sin_medio_sueldo_algun_mes_incumple():
    S = copy.deepcopy(pf.V11)
    S["recortes_fundadores"] = []
    r36, r60 = pf.simular(S), pf.simular(pf.a_60_meses(S))
    with pytest.raises(AssertionError):
        pf.comprobar_v11(r36, r60, S)


def test_sin_rebaja_el_mes_4_queda_por_debajo_del_umbral():
    # sin la rebaja, el mes 4 queda por debajo de la mitad del capital; un sueldo que solo se aplaza se
    # devenga igual, así que su patrimonio es este mismo
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


def test_contrataciones_ligadas_a_las_puestas_en_marcha():
    # en el plan la regla no cambia nada: el ingeniero entra en el 13 y el comercial en el 25
    assert pf.meses_contratacion(pf.V11) == {13: 13, 25: 25}
    S = copy.deepcopy(pf.V11)
    S["contrataciones_ligadas"] = {}
    assert pf.r0(pf.pyg_anual(pf.simular(S), 3)["resultado"]) == pf.r0(pf.pyg_anual(pf.simular(pf.V11), 3)["resultado"])
    # con los retrasos entran con la tercera y la novena puesta en marcha
    assert pf.meses_contratacion(pf.escenario_retraso(pf.V11, 6)) == {13: 15, 25: 30}
    assert pf.meses_contratacion(pf.escenario_retraso(pf.V11, 12)) == {13: 21, 25: 36}
    assert pf.meses_contratacion(pf.escenario_menos_clientes(pf.V11, 2)) == {13: 13, 25: 27}
    r = pf.simular(pf.escenario_retraso(pf.V11, 6))
    ingeniero = [d for (n, _, d) in r["S"]["plantilla"] if n.startswith("Ingeniero")]
    assert ingeniero == [15]


def test_ipc_cero_no_cambia_el_plan():
    S = copy.deepcopy(pf.V11)
    S["ipc_licencia"] = 0.0
    assert pf.simular(S)["caja"][36] == pytest.approx(pf.simular(pf.V11)["caja"][36])
    extra = pf.valor_ipc(pf.V11)
    assert extra[1] == 0 and 0 < extra[2] < extra[3]


def test_analisis_cuadran_con_la_cuenta(calculo):
    r = calculo["r11"]
    for a in (1, 2, 3):
        c = pf.cascada(r, a)
        assert c["coste_ventas"] + c["gastos_operativos"] == pytest.approx(pf.pyg_anual(r, a)["gastos_expl"])
    p3 = pf.punto_muerto_detalle(r, 3)
    assert p3["licencias"] == pytest.approx(pf.punto_muerto(r, 3)[0])
    f = pf.fondo_maniobra(r, 12)
    b = r["balances"][12]
    assert f["fondo_maniobra"] == pytest.approx(b["clientes"] + b["hp_deudora_iva"] + b["tesoreria"] - b["total_pc"])


# --- mercado.py: la retribución del BOE se lee de un fichero privado que no se versiona
spec_m = importlib.util.spec_from_file_location(
    "mercado", os.path.join(os.path.dirname(__file__), "..", "..", "experimentos", "plan_financiero", "mercado.py"))
mercado = importlib.util.module_from_spec(spec_m)
spec_m.loader.exec_module(mercado)

BOE_FICTICIO = """ANEXO I
  R1-001   Empresa uno          100.000.000
  R1-002   Empresa dos           3.000.000
ANEXO II
  R1-003   Empresa tres a cuenta        900.000
ANEXO III
"""


def test_lee_la_resolucion_del_boe():
    assert mercado.leer_boe(BOE_FICTICIO) == {"R1-001": 100000000, "R1-002": 3000000, "R1-003": 900000}


def test_mercado_solo_agregados_y_al_dia():
    ruta = os.path.join(os.path.dirname(mercado.__file__), "salida", "mercado.json")
    with open(ruta, encoding="utf-8") as f:
        guardado = json.load(f)
    assert set(guardado) == {
        "distribuidoras_sin_las_grandes", "con_mas_de_un_millon", "tarifa_320", "tarifa_mas_de_un_millon",
        "tarifa_89_mayores", "tarifa_52_mayores", "regla_320", "regla_89_mayores", "de_las_89_con_150_ct_o_mas",
        "de_las_89_con_100_ct_o_mas", "mediana_ct_89", "incentivo_4_pct_mediana_mas_de_un_millon"}
    if not os.path.exists(mercado.PRIVADO):
        pytest.skip("sin el fichero privado con la retribución del BOE")
    with open(mercado.PRIVADO, encoding="utf-8") as f:
        conf = json.load(f)
    with open(conf["boe_txt"], encoding="utf-8") as f:
        nuevo = mercado.calcular(mercado.leer_boe(f.read()), conf["codigos"])
    assert nuevo == guardado, "salida/mercado.json no está al día: vuelve a ejecutar mercado.py"
