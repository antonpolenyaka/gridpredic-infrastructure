# -*- coding: utf-8 -*-
"""Plan financiero de GridPredic a tres años (capítulo 3 de la memoria, 3.9).

Modelo mensual con partida doble: cada movimiento de caja tiene su contrapartida y el
balance se calcula desde las cuentas, sin partida de ajuste. En cada ejecución
comprobamos que el activo es igual al patrimonio neto más el pasivo en los 37 cierres
(momento 0 y meses 1 a 36), que la tesorería final de un año es la inicial del
siguiente y que el resultado de cada año pasa a los fondos propios. Si algo no
cuadra, el guion se para con un AssertionError.

Hay dos juegos de supuestos:

- V10: el plan de la memoria v10 (licencia de 18.000 euros, ENISA cobrada en el
  momento 0, 40.000 euros de los fundadores). Lo reproducimos al euro como referencia.
- V11: las tres decisiones del 19.10.2026: tarifa por tamaño de red (fijo + CT +
  señales, con un mínimo y sin máximo), ENISA cobrada en el mes 6 y el capital y las
  aportaciones que hacen falta para que el patrimonio neto no baje de la mitad del
  capital (art. 363.1.e LSC) en ningún mes.

Para comparar los dos planes más allá del año 3 alargamos los dos a 60 meses con los
mismos supuestos de los años 4 y 5 (ANIOS_4_5). Esa extensión es solo de contraste:
las tablas de 3.9 son las de 36 meses.

Uso:
    python plan_financiero.py              # calcula, comprueba y escribe salida/
    python plan_financiero.py --sin-salida # solo calcula, comprueba y enseña las cifras

Salidas (salida/):
    tablas_v11.txt       tablas de 3.9 en el formato de la memoria (T, H, W, R)
    tablas_v10.txt       las mismas tablas con los supuestos de la v10
    cifras_v11.json      cifras sueltas que cita el texto
    comparacion.txt      antes y después de las cifras clave, con el cálculo de la tarifa
    mensual_v11.csv      tesorería, resultado y patrimonio de cada mes
    detalle_v11.xlsx     el detalle mes a mes y los balances (si está openpyxl)

Todas las cifras van sin IVA salvo los cobros y pagos de la tesorería. Solo usa la
biblioteca estándar (openpyxl es opcional).
"""

import copy
import csv
import json
import os
import sys

AQUI = os.path.dirname(os.path.abspath(__file__))
SALIDA = os.path.join(AQUI, "salida")

# ---------------------------------------------------------------------------
# Tarifa por tamaño de red (decisión 9 del 19.10.2026)
# ---------------------------------------------------------------------------
# Como un SCADA: una parte fija, una por CT y otra por señal, con un mínimo y sin
# máximo. Señales = tags del SCADA que cuelgan de la distribuidora (medidas, estados
# y alarmas). La calibración está en calibrar_tarifa().
TARIFA = {"fijo": 3000.0, "por_ct": 50.0, "por_senal": 1.0, "minimo": 6000.0}

# Las tres redes del TFM. Los CT son los de la red actual (docs/cifras.md, apartado 1).
# Las señales son PROVISIONALES: los 78.301 tags de TedisNet repartidos por CT
# (unos 58 por CT), a la espera del recuento por distribuidora (consulta en el README).
REDES_CALIBRACION = {
    "Distribuidora 1": {"ct": 760, "senales": 44400},
    "Distribuidora 2": {"ct": 570, "senales": 33300},
    "Distribuidora 3": {"ct": 10, "senales": 600},
}
SENALES_PROVISIONALES = True

# Red tipo de los clientes del plan: una red pequeña con telecontrol, más pequeña que
# la mitad de las 52 mayores del mercado. Es la que fija la licencia media del plan.
RED_TIPO_PLAN = {"ct": 150, "senales": 9000}


def licencia(ct, senales, t=TARIFA):
    """Licencia anual en euros: fijo + CT + señales, con el mínimo y sin techo."""
    return max(t["minimo"], t["fijo"] + t["por_ct"] * ct + t["por_senal"] * senales)


# ---------------------------------------------------------------------------
# Supuestos
# ---------------------------------------------------------------------------
V10 = {
    "nombre": "v10",
    "meses": 36,
    # mes de puesta en marcha (1 = enero de 2027); la firma es el mes anterior
    "puestas_en_marcha": [5, 7, 9,
                          14, 16, 18, 20, 22, 24,
                          26, 27, 29, 30, 32, 33, 35, 36],
    # (mes de puesta en marcha del cliente, aniversario en que no renueva)
    "bajas": [(9, 21), (14, 26)],
    "precio_implantacion": 20000.0,
    "precio_licencia": 18000.0,
    "parte_implantacion_firma": 0.5,
    "cobro_meses": 1,
    "plantilla": [("Fundador (dirección y negocio)", 50000.0, 1),
                  ("Fundador (datos y plataforma)", 40000.0, 1),
                  ("Ingeniero de implantación y soporte", 30000.0, 13),
                  ("Responsable comercial del sector", 36000.0, 25)],
    "cargas_sociales": 0.30,
    "ss_trabajador": 0.065,
    "retencion_irpf": 0.18,
    # euros al año; el último valor se repite si el horizonte es más largo
    "gastos_anuales": {
        "Nube (cómputo, almacenamiento, demos)": [7200.0, 9600.0, 12000.0],
        "Asesoría jurídica": [4000.0, 8000.0, 6000.0],
        "Gestoría": [2400.0, 3600.0, 4800.0],
        "Actividad comercial y ferias": [5000.0, 15000.0, 25000.0],
        "Equipos menores, viajes y otros": [3000.0, 6000.0, 9000.0],
        "Seguros (responsabilidad civil)": [1200.0, 2000.0, 2000.0],
    },
    "herramientas_persona_mes": 100.0,
    "coworking_persona_mes": 250.0,
    "comision_socio": 0.10,
    "comision_desde_mes": 13,
    "imprevistos": 0.05,
    "inversiones": [(13, "Equipo del ingeniero", 2500.0, "equipo"),
                    (14, "Marca en la EUIPO", 1000.0, "marca"),
                    (25, "Equipo del responsable comercial", 2500.0, "equipo")],
    "amortizacion": {"marca": 0.10, "aplicaciones": 0.25, "mobiliario": 0.10, "equipo": 0.25},
    "reparto_equipo": {"equipo": 2000.0 / 2500.0, "mobiliario": 500.0 / 2500.0},
    "activo_inicial": {"marca": 500.0, "aplicaciones": 5000.0, "mobiliario": 1500.0, "equipo": 6000.0},
    "fianza": 1000.0,
    "capital_fundadores": 3000.0,
    "prima_fundadores": 37000.0,
    "gastos_constitucion": 1500.0,
    # mes_cobro 0 = en el momento 0, como la v10
    "enisa": {"importe": 38000.0, "interes": 0.06, "carencia_meses": 24, "plazo_meses": 84,
              "comision_apertura": 0.005, "mes_cobro": 0},
    "banco": {"importe": 20000.0, "interes": 0.065, "carencia_meses": 12, "plazo_meses": 60},
    "ronda": {"mes": 12, "importe": 250000.0, "participacion": 0.25, "gastos": 3000.0},
    "iva": 0.21,
    "tipo_is": 0.15,
    "mes_pago_is": 7,
}

# Mes de cobro de ENISA en la v11 y por qué (decisión 10): constituimos en diciembre de
# 2026; el primer ejercicio cierra el 31.12.2026; formulamos, aprobamos en junta universal
# y depositamos las cuentas de 2026 en enero (arts. 253.1, 164.1, 178 y 279.1 LSC); la
# solicitud va en febrero (mes 2), cuando el Registro ha despachado el depósito; ENISA
# abre el estudio con el expediente completo (30 días para subir los documentos) y la
# resolución suele llegar de uno a tres meses después; con la póliza ante notario y el
# desembolso, el dinero está en el mes 6.
MES_ENISA_V11 = 6

V11 = copy.deepcopy(V10)
V11.update({
    "nombre": "v11",
    "precio_licencia": licencia(**RED_TIPO_PLAN),
})
V11["enisa"]["mes_cobro"] = MES_ENISA_V11
# Capital social y aportaciones (decisión 11): el capital se queda en 3.000 euros, el
# mínimo con el que no se aplica el régimen de las sociedades de menos de 3.000 euros
# (art. 4.2 y 4.3 LSC, redacción de la Ley 18/2022), y la mitad del capital, el umbral
# del art. 363.1.e, en 1.500 euros. Lo que hace falta de más entra como prima de
# asunción en la constitución. La cifra sale de aportacion_minima() y la redondeamos a
# miles por fundador; main() comprueba que sigue siendo suficiente.
APORTACION_FUNDADORES_V11 = 60000.0
V11["prima_fundadores"] = APORTACION_FUNDADORES_V11 - V11["capital_fundadores"]

# Años 4 y 5, solo para comparar los dos planes (los mismos para los dos): ocho clientes
# nuevos al año al ritmo del año 3, dos bajas al año, la misma plantilla y los gastos del
# año 3.
ANIOS_4_5 = {
    "puestas_extra": [g + 12 for g in (26, 27, 29, 30, 32, 33, 35, 36)]
    + [g + 24 for g in (26, 27, 29, 30, 32, 33, 35, 36)],
    "bajas_extra": [(16, 40), (18, 42), (27, 51), (29, 53)],
}
TASA_VAN = 0.10   # tasa de contraste; la misma para los dos planes

MULTIPLO_VENTA = 4.0
ANIOS_HASTA_VENTA = 4
SIN_IVA = {"Seguros (responsabilidad civil)"}


def a_60_meses(S):
    S2 = copy.deepcopy(S)
    S2["meses"] = 60
    S2["puestas_en_marcha"] = S["puestas_en_marcha"] + ANIOS_4_5["puestas_extra"]
    S2["bajas"] = S["bajas"] + ANIOS_4_5["bajas_extra"]
    return S2


# ---------------------------------------------------------------------------
# Formato
# ---------------------------------------------------------------------------
def r0(x):
    return int(round(x + (1e-9 if x >= 0 else -1e-9)))


def fmt(x, cero="0"):
    v = r0(x)
    if v == 0:
        return cero
    s = f"{abs(v):,}".replace(",", ".")
    return ("-" if v < 0 else "") + s


def fmt1(x):
    """Un decimal con coma: 18,6."""
    return f"{x:.1f}".replace(".", ",")


def fmt_pct(x, dec=1):
    return f"{x * 100:.{dec}f}".replace(".", ",") + " %"


def anio_de(m):
    return (m - 1) // 12 + 1


# ---------------------------------------------------------------------------
# Simulación
# ---------------------------------------------------------------------------
def simular(S):
    S = copy.deepcopy(S)
    N = S["meses"]
    iva = S["iva"]
    P = {k: [0.0] * (N + 2) for k in [
        "ing_impl", "ing_lic", "sueldos", "cargas", "externos", "comision", "imprevistos",
        "amort", "financieros", "is",
        "fact_base", "fact_iva", "cobros", "pago_nomina", "pago_ss", "pago_irpf",
        "pago_externos", "pago_iva", "pago_is", "pago_inversion", "pago_intereses",
        "pago_principal", "entrada_capital", "pago_gastos_ronda", "entrada_prestamo",
    ]}
    detalle_ext = {}

    # --- clientes -------------------------------------------------------------
    bajas = dict(S["bajas"])
    clientes = [{"gm": gm, "fin": bajas.get(gm)} for gm in S["puestas_en_marcha"]]
    lic_mes = S["precio_licencia"] / 12.0
    for c in clientes:
        gm = c["gm"]
        firma = gm - 1
        f1 = S["precio_implantacion"] * S["parte_implantacion_firma"]
        f2 = S["precio_implantacion"] - f1
        if 1 <= firma <= N:
            P["fact_base"][firma] += f1
        if gm <= N:
            P["fact_base"][gm] += f2
            P["ing_impl"][gm] += S["precio_implantacion"]
        m = gm
        while m <= N:
            if c["fin"] is not None and m >= c["fin"]:
                break
            P["fact_base"][m] += S["precio_licencia"]
            m += 12
        for m in range(gm, N + 1):
            if c["fin"] is not None and m >= c["fin"]:
                break
            P["ing_lic"][m] += lic_mes
    for m in range(1, N + 1):
        P["fact_iva"][m] = P["fact_base"][m] * iva

    # --- plantilla --------------------------------------------------------------
    personas_mes = [0] * (N + 2)
    for (_, bruto, desde) in S["plantilla"]:
        for m in range(desde, N + 1):
            P["sueldos"][m] += bruto / 12.0
            personas_mes[m] += 1
    recorte = S.get("recorte_fundadores")
    if recorte:
        d, h, parte = recorte
        bruto_fund = sum(b for (_, b, desde) in S["plantilla"] if desde == 1)
        for m in range(d, h + 1):
            P["sueldos"][m] -= bruto_fund / 12.0 * parte
    for m in range(1, N + 1):
        P["cargas"][m] = P["sueldos"][m] * S["cargas_sociales"]

    # --- gastos externos ----------------------------------------------------------
    for concepto, anual in S["gastos_anuales"].items():
        detalle_ext[concepto] = [0.0] * (N + 2)
        for m in range(1, N + 1):
            detalle_ext[concepto][m] = anual[min(anio_de(m), len(anual)) - 1] / 12.0
    detalle_ext["Herramientas de desarrollo"] = [0.0] * (N + 2)
    detalle_ext["Coworking"] = [0.0] * (N + 2)
    for m in range(1, N + 1):
        detalle_ext["Herramientas de desarrollo"][m] = S["herramientas_persona_mes"] * personas_mes[m]
        detalle_ext["Coworking"][m] = S["coworking_persona_mes"] * personas_mes[m]
    for m in range(1, N + 1):
        P["externos"][m] = sum(v[m] for v in detalle_ext.values())
        if m >= S["comision_desde_mes"]:
            P["comision"][m] = P["ing_lic"][m] * S["comision_socio"]
        base_imp = P["sueldos"][m] + P["cargas"][m] + P["externos"][m] + P["comision"][m]
        P["imprevistos"][m] = base_imp * S["imprevistos"]

    iva_sop = [0.0] * (N + 2)
    for m in range(1, N + 1):
        con_iva = sum(v[m] for k, v in detalle_ext.items() if k not in SIN_IVA)
        con_iva += P["comision"][m] + P["imprevistos"][m]
        iva_sop[m] = con_iva * iva
        P["pago_externos"][m] = P["externos"][m] + P["comision"][m] + P["imprevistos"][m] + iva_sop[m]

    # --- inmovilizado -------------------------------------------------------------
    activos = [(1, tipo, coste) for tipo, coste in S["activo_inicial"].items()]
    for (mes, _, importe, tipo) in S["inversiones"]:
        if mes > N:
            continue
        if tipo == "equipo":
            for t2, parte in S["reparto_equipo"].items():
                activos.append((mes, t2, importe * parte))
        else:
            activos.append((mes, tipo, importe))
        P["pago_inversion"][mes] += importe * (1 + iva)
        iva_sop[mes] += importe * iva
    amort_acum = {"intangible": [0.0] * (N + 2), "material": [0.0] * (N + 2)}
    coste_bruto = {"intangible": [0.0] * (N + 2), "material": [0.0] * (N + 2)}
    for m in range(1, N + 1):
        for (alta, tipo, coste) in activos:
            clase = "intangible" if tipo in ("marca", "aplicaciones") else "material"
            if alta <= m:
                coste_bruto[clase][m] += coste
                cuota = coste * S["amortizacion"][tipo] / 12.0
                acum_hoy = min(cuota * (m - alta + 1), coste)
                acum_antes = min(cuota * (m - alta), coste)
                amort_acum[clase][m] += acum_hoy
                P["amort"][m] += acum_hoy - acum_antes

    # --- financiación -------------------------------------------------------------
    E, B, R = S["enisa"], S["banco"], S["ronda"]
    mc = E["mes_cobro"]
    enisa_pend = [0.0] * (N + 2)
    banco_pend = [0.0] * (N + 2)
    banco_pend[0] = B["importe"]
    if mc == 0:
        enisa_pend[0] = E["importe"]
    cuota_enisa = E["importe"] / ((E["plazo_meses"] - E["carencia_meses"]) / 3.0)   # trimestral
    cuota_banco = B["importe"] / (B["plazo_meses"] - B["carencia_meses"])           # mensual
    comision = E["importe"] * E["comision_apertura"]
    P["financieros"][max(mc, 1)] += comision
    P["pago_intereses"][max(mc, 1)] += comision
    intereses_pend = [0.0] * (N + 2)
    for m in range(1, N + 1):
        e_prev = enisa_pend[m - 1]
        b_prev = banco_pend[m - 1]
        if mc > 0 and m == mc:
            P["entrada_prestamo"][m] += E["importe"]
        P["financieros"][m] += e_prev * E["interes"] / 12.0
        if m > mc and (m - mc) % 3 == 0:
            P["pago_intereses"][m] += sum(enisa_pend[k] * E["interes"] / 12.0 for k in range(m - 3, m))
            if m > mc + E["carencia_meses"]:
                pago = min(cuota_enisa, e_prev)
                P["pago_principal"][m] += pago
                e_prev -= pago
        if mc > 0 and m == mc:
            e_prev += E["importe"]
        enisa_pend[m] = e_prev
        P["financieros"][m] += b_prev * B["interes"] / 12.0
        P["pago_intereses"][m] += b_prev * B["interes"] / 12.0
        if m > B["carencia_meses"]:
            pago = min(cuota_banco, b_prev)
            P["pago_principal"][m] += pago
            b_prev -= pago
        banco_pend[m] = b_prev
        intereses_pend[m] = intereses_pend[m - 1] + P["financieros"][m] - P["pago_intereses"][m]
    # la comisión se paga el mismo mes que se devenga: no queda pendiente
    capital_nominal_fund = S["capital_fundadores"]
    nuevas = 0
    if R["mes"] and R["mes"] <= N:
        nuevas = int(-(-capital_nominal_fund * R["participacion"] // (1 - R["participacion"])))
        P["entrada_capital"][R["mes"]] += R["importe"]
        P["pago_gastos_ronda"][R["mes"]] += R["gastos"] * (1 + iva)
        iva_sop[R["mes"]] += R["gastos"] * iva

    # --- impuestos y tesorería ------------------------------------------------------
    caja = [0.0] * (N + 2)
    caja[0] = (S["capital_fundadores"] + S["prima_fundadores"] + enisa_pend[0] + B["importe"]
               - S["gastos_constitucion"] - sum(S["activo_inicial"].values()) - S["fianza"])
    iva_saldo = [0.0] * (N + 2)
    irpf_pend = [0.0] * (N + 2)
    ss_pend = [0.0] * (N + 2)
    is_pend = [0.0] * (N + 2)
    bin_disponible = 0.0
    resultados_anuales, bai_anual, is_anual, iva_fin_trimestre = {}, {}, {}, {}
    for m in range(1, N + 1):
        k = m - S["cobro_meses"]
        if k >= 1:
            P["cobros"][m] = P["fact_base"][k] + P["fact_iva"][k]
        bruto = P["sueldos"][m]
        P["pago_nomina"][m] = bruto * (1 - S["retencion_irpf"] - S["ss_trabajador"])
        P["pago_ss"][m] = ss_pend[m - 1]
        ss_pend[m] = bruto * (S["cargas_sociales"] + S["ss_trabajador"])
        irpf_pend[m] = irpf_pend[m - 1] + bruto * S["retencion_irpf"]
        if m % 3 == 1 and m > 1:
            P["pago_irpf"][m] = irpf_pend[m - 1]
            irpf_pend[m] -= irpf_pend[m - 1]
        iva_saldo[m] = iva_saldo[m - 1] + P["fact_iva"][m] - iva_sop[m]
        if m % 3 == 0:
            iva_fin_trimestre[m] = iva_saldo[m]
        if m % 3 == 1 and m > 1:
            a_pagar = iva_fin_trimestre[m - 1]
            if a_pagar > 0:
                P["pago_iva"][m] = a_pagar
                iva_saldo[m] -= a_pagar
        if (m - 1) % 12 + 1 == S["mes_pago_is"] and m > 12:
            P["pago_is"][m] = is_pend[m - 1]
            is_pend[m] = is_pend[m - 1] - P["pago_is"][m]
        else:
            is_pend[m] = is_pend[m - 1]
        if m % 12 == 0:
            a = m // 12
            meses_a = range(m - 11, m + 1)
            ingresos = sum(P["ing_impl"][x] + P["ing_lic"][x] for x in meses_a)
            gastos = sum(P["sueldos"][x] + P["cargas"][x] + P["externos"][x] + P["comision"][x]
                         + P["imprevistos"][x] + P["amort"][x] + P["financieros"][x] for x in meses_a)
            bai = ingresos - gastos
            if bai < 0:
                bin_disponible += -bai
                cuota = 0.0
            else:
                compensa = min(bin_disponible, bai)
                bin_disponible -= compensa
                cuota = (bai - compensa) * S["tipo_is"]
            P["is"][m] = cuota
            is_pend[m] += cuota
            bai_anual[a], is_anual[a], resultados_anuales[a] = bai, cuota, bai - cuota
        salidas = (P["pago_nomina"][m] + P["pago_ss"][m] + P["pago_irpf"][m] + P["pago_externos"][m]
                   + P["pago_iva"][m] + P["pago_is"][m] + P["pago_inversion"][m] + P["pago_intereses"][m]
                   + P["pago_principal"][m] + P["pago_gastos_ronda"][m])
        entradas = P["cobros"][m] + P["entrada_capital"][m] + P["entrada_prestamo"][m]
        caja[m] = caja[m - 1] + entradas - salidas

    # --- balances -------------------------------------------------------------------
    def corto_plazo(m):
        e_corto = sum(cuota_enisa for k in range(m + 1, m + 13)
                      if k > mc and (k - mc) % 3 == 0 and k > mc + E["carencia_meses"])
        e_corto = min(e_corto, enisa_pend[m])
        b_corto = min(sum(cuota_banco for k in range(m + 1, m + 13) if k > B["carencia_meses"]), banco_pend[m])
        return e_corto, b_corto

    def balance(m):
        b = {}
        if m == 0:
            b["intangible"] = S["activo_inicial"]["marca"] + S["activo_inicial"]["aplicaciones"]
            b["material"] = S["activo_inicial"]["mobiliario"] + S["activo_inicial"]["equipo"]
        else:
            b["intangible"] = coste_bruto["intangible"][m] - amort_acum["intangible"][m]
            b["material"] = coste_bruto["material"][m] - amort_acum["material"][m]
        b["fianza"] = S["fianza"]
        b["clientes"] = (P["fact_base"][m] + P["fact_iva"][m]) if m >= 1 else 0.0
        b["hp_deudora_iva"] = -iva_saldo[m] if iva_saldo[m] < 0 else 0.0
        b["tesoreria"] = caja[m]
        b["total_activo"] = (b["intangible"] + b["material"] + b["fianza"] + b["clientes"]
                             + b["hp_deudora_iva"] + b["tesoreria"])
        ronda_hecha = bool(R["mes"]) and R["mes"] <= m
        b["capital"] = S["capital_fundadores"] + (nuevas if ronda_hecha else 0)
        b["prima"] = S["prima_fundadores"] + ((R["importe"] - nuevas) if ronda_hecha else 0.0)
        b["reservas"] = -S["gastos_constitucion"] - (R["gastos"] if ronda_hecha else 0.0)
        a = anio_de(m) if m >= 1 else 0
        b["anteriores"] = sum(resultados_anuales.get(x, 0.0) for x in range(1, a)) if m >= 1 else 0.0
        if m >= 1:
            meses_a = range((a - 1) * 12 + 1, m + 1)
            b["resultado"] = sum(P["ing_impl"][x] + P["ing_lic"][x] - P["sueldos"][x] - P["cargas"][x]
                                 - P["externos"][x] - P["comision"][x] - P["imprevistos"][x] - P["amort"][x]
                                 - P["financieros"][x] - P["is"][x] for x in meses_a)
        else:
            b["resultado"] = 0.0
        b["total_pn"] = b["capital"] + b["prima"] + b["reservas"] + b["anteriores"] + b["resultado"]
        # patrimonio neto a efectos del art. 363 LSC: con el préstamo participativo
        # (art. 20.1.d del Real Decreto-ley 7/1996)
        b["pn_mercantil"] = b["total_pn"] + enisa_pend[m]
        b["umbral_363"] = b["capital"] / 2.0
        e_corto, b_corto = corto_plazo(m)
        b["enisa_lp"] = enisa_pend[m] - e_corto
        b["banco_lp"] = banco_pend[m] - b_corto
        b["total_pnc"] = b["enisa_lp"] + b["banco_lp"]
        b["deuda_cp"] = e_corto + b_corto + intereses_pend[m]
        anticipos = diferidos = 0.0
        for c in clientes:
            firma = c["gm"] - 1
            if 1 <= firma <= m < c["gm"]:
                anticipos += S["precio_implantacion"] * S["parte_implantacion_firma"]
            fact = 0.0
            mm = c["gm"]
            while mm <= m:
                if c["fin"] is not None and mm >= c["fin"]:
                    break
                fact += S["precio_licencia"]
                mm += 12
            rec = 0.0
            for x in range(c["gm"], m + 1):
                if c["fin"] is not None and x >= c["fin"]:
                    break
                rec += lic_mes
            diferidos += fact - rec
        b["anticipos"] = anticipos
        b["periodificacion"] = diferidos
        b["hp_acreedora"] = (iva_saldo[m] if iva_saldo[m] > 0 else 0.0) + irpf_pend[m] + is_pend[m]
        b["ss_acreedora"] = ss_pend[m]
        b["hp_ss"] = b["hp_acreedora"] + b["ss_acreedora"]
        b["total_pc"] = b["deuda_cp"] + b["anticipos"] + b["periodificacion"] + b["hp_ss"]
        b["total_pn_pasivo"] = b["total_pn"] + b["total_pnc"] + b["total_pc"]
        return b

    balances = {m: balance(m) for m in range(0, N + 1)}

    # ENISA presta como mucho los fondos propios; la solicitud se estudia con las cuentas
    # de 2026, es decir, con el balance del momento 0
    assert balances[0]["total_pn"] >= E["importe"], "ENISA: fondos propios por debajo del préstamo"
    # prueba de cuadre: activo = patrimonio neto + pasivo en todos los cierres
    for m in range(0, N + 1):
        dif = balances[m]["total_activo"] - balances[m]["total_pn_pasivo"]
        assert abs(dif) < 0.01, f"El balance no cuadra en el mes {m}: diferencia {dif:.4f}"

    res = {
        "S": S, "N": N, "P": P, "caja": caja, "balances": balances, "clientes": clientes,
        "detalle_ext": detalle_ext, "personas_mes": personas_mes,
        "resultados_anuales": resultados_anuales, "bai_anual": bai_anual, "is_anual": is_anual,
        "bin_final": bin_disponible, "nuevas_participaciones": nuevas,
        "enisa_pend": enisa_pend, "banco_pend": banco_pend,
    }
    comprobar_encadenado(res)
    return res


def comprobar_encadenado(res):
    """Tesorería y resultados encadenados entre años; falla si algo no cuadra."""
    B = res["balances"]
    anios = res["N"] // 12
    for a in range(1, anios + 1):
        t = tesoreria_anual(res, a)
        y = pyg_anual(res, a)
        b_fin = B[a * 12]
        assert abs(b_fin["resultado"] - y["resultado"]) < 0.01, f"resultado del año {a}"
        assert abs(b_fin["tesoreria"] - t["final"]) < 0.01, f"tesorería del año {a}"
        if a > 1:
            b_ant = B[(a - 1) * 12]
            assert abs(b_fin["anteriores"] - (b_ant["anteriores"] + b_ant["resultado"])) < 0.01, \
                f"resultados anteriores del año {a}"
            assert abs(t["inicial"] - tesoreria_anual(res, a - 1)["final"]) < 0.01, f"tesorería inicial del año {a}"
    assert abs(B[0]["tesoreria"] - tesoreria_anual(res, 1)["inicial"]) < 0.01


# ---------------------------------------------------------------------------
# Agregados
# ---------------------------------------------------------------------------
def suma(lista, a):
    return sum(lista[m] for m in range((a - 1) * 12 + 1, a * 12 + 1))


def pyg_anual(res, a):
    P = res["P"]
    d = {"impl": suma(P["ing_impl"], a), "lic": suma(P["ing_lic"], a)}
    d["ingresos"] = d["impl"] + d["lic"]
    d["sueldos"] = suma(P["sueldos"], a)
    d["cargas"] = suma(P["cargas"], a)
    d["ext"] = {k: suma(v, a) for k, v in res["detalle_ext"].items()}
    d["comision"] = suma(P["comision"], a)
    d["imprevistos"] = suma(P["imprevistos"], a)
    d["gastos_expl"] = d["sueldos"] + d["cargas"] + sum(d["ext"].values()) + d["comision"] + d["imprevistos"]
    d["ebitda"] = d["ingresos"] - d["gastos_expl"]
    d["amort"] = suma(P["amort"], a)
    d["ebit"] = d["ebitda"] - d["amort"]
    d["financieros"] = suma(P["financieros"], a)
    d["bai"] = d["ebit"] - d["financieros"]
    d["is"] = suma(P["is"], a)
    d["resultado"] = d["bai"] - d["is"]
    return d


def tesoreria_anual(res, a):
    P = res["P"]
    t = {"inicial": res["caja"][(a - 1) * 12], "cobros": suma(P["cobros"], a)}
    t["personal"] = suma(P["pago_nomina"], a) + suma(P["pago_ss"], a) + suma(P["pago_irpf"], a)
    t["proveedores"] = suma(P["pago_externos"], a)
    t["iva_is"] = suma(P["pago_iva"], a) + suma(P["pago_is"], a)
    t["explotacion"] = t["cobros"] - t["personal"] - t["proveedores"] - t["iva_is"]
    t["inversion"] = -suma(P["pago_inversion"], a)
    t["capital"] = suma(P["entrada_capital"], a) - suma(P["pago_gastos_ronda"], a)
    t["prestamos"] = suma(P["entrada_prestamo"], a)
    t["intereses"] = -suma(P["pago_intereses"], a)
    t["principal"] = -suma(P["pago_principal"], a)
    t["financiacion"] = t["capital"] + t["prestamos"] + t["intereses"] + t["principal"]
    t["final"] = res["caja"][a * 12]
    assert abs(t["inicial"] + t["explotacion"] + t["inversion"] + t["financiacion"] - t["final"]) < 0.01
    return t


def activos_al_final(res, m):
    return sum(1 for c in res["clientes"] if c["gm"] <= m and (c["fin"] is None or c["fin"] > m))


def caja_minima(res, desde=1, hasta=None):
    hasta = hasta or res["N"]
    m = min(range(desde, hasta + 1), key=lambda x: res["caja"][x])
    return res["caja"][m], m


def holgura_363(res, hasta=None):
    """Mes con menos margen sobre la mitad del capital y ese margen (puede ser negativo)."""
    hasta = hasta if hasta is not None else res["N"]
    B = res["balances"]
    m = min(range(0, hasta + 1), key=lambda x: B[x]["pn_mercantil"] - B[x]["umbral_363"])
    return B[m]["pn_mercantil"] - B[m]["umbral_363"], m


def pn_minimo(res, hasta=None):
    hasta = hasta if hasta is not None else res["N"]
    B = res["balances"]
    m = min(range(0, hasta + 1), key=lambda x: B[x]["pn_mercantil"])
    return B[m]["pn_mercantil"], m


def punto_muerto(res, a=3):
    """Licencias cuyo margen paga la estructura del año a (como clientes_equilibrio de la v10)."""
    S = res["S"]
    y = pyg_anual(res, a)
    fijos = y["sueldos"] + y["cargas"] + sum(y["ext"].values())
    k, imp = S["comision_socio"], S["imprevistos"]
    lic_eq = (fijos * (1 + imp) + y["amort"] + y["financieros"]) / (1 - k * (1 + imp))
    return lic_eq / S["precio_licencia"], lic_eq


def van(res, tasa=TASA_VAN):
    """VAN de los flujos libres (explotación más inversiones) de todos los años, sin valor residual."""
    anios = res["N"] // 12
    flujos = []
    for a in range(1, anios + 1):
        t = tesoreria_anual(res, a)
        flujos.append(t["explotacion"] + t["inversion"])
    return sum(f / (1 + tasa) ** (i + 1) for i, f in enumerate(flujos)), flujos


def aportacion_minima(S):
    """Aportación extra de los fundadores (prima) que deja la caja y el patrimonio del art. 363 en
    positivo todos los meses. Las dos crecen euro a euro con la aportación, que no cambia el resultado."""
    r = simular(S)
    h, _ = holgura_363(r)
    c, _ = caja_minima(r, 1)
    return max(0.0, -h, -c)


# ---------------------------------------------------------------------------
# Escenarios de sensibilidad
# ---------------------------------------------------------------------------
def escenario_retraso(S, meses=6, recorte=None):
    S2 = copy.deepcopy(S)
    S2["puestas_en_marcha"] = [g + meses for g in S["puestas_en_marcha"]]
    S2["bajas"] = [(g + meses, f + meses) for (g, f) in S["bajas"]]
    if recorte:
        S2["recorte_fundadores"] = recorte
    return S2


def escenario_menos_clientes(S, menos=2):
    S2 = copy.deepcopy(S)
    a1 = [g for g in S["puestas_en_marcha"] if g <= 12]
    a2 = [g for g in S["puestas_en_marcha"] if 12 < g <= 24]
    a3 = [g for g in S["puestas_en_marcha"] if g > 24]
    S2["puestas_en_marcha"] = a1 + a2[:len(a2) - menos] + a3[:len(a3) - menos]
    S2["bajas"] = [(g, f) for (g, f) in S["bajas"] if g in S2["puestas_en_marcha"]]
    return S2


def escenario_sin_ronda(S):
    S2 = copy.deepcopy(S)
    S2["ronda"] = {"mes": None, "importe": 0.0, "participacion": 0.25, "gastos": 0.0}
    return S2


def escenario_enisa(S, mes):
    S2 = copy.deepcopy(S)
    S2["enisa"]["mes_cobro"] = mes
    return S2


def escenario_enisa_importe(S, importe):
    S2 = copy.deepcopy(S)
    S2["enisa"]["importe"] = importe
    return S2


def escenario_red(S, ct, senales):
    S2 = copy.deepcopy(S)
    S2["precio_licencia"] = licencia(ct, senales)
    return S2


def escenarios(S):
    lista = [
        ("Base: el plan", S),
        ("Los contratos llegan seis meses más tarde", escenario_retraso(S, 6)),
        ("Lo mismo, con los sueldos de los fundadores a la mitad de los meses 4 a 11",
         escenario_retraso(S, 6, recorte=(4, 11, 0.5))),
        ("Los contratos llegan un año más tarde", escenario_retraso(S, 12)),
        ("Dos clientes nuevos menos en los años 2 y 3", escenario_menos_clientes(S, 2)),
        ("Sin la ronda", escenario_sin_ronda(S)),
    ]
    if S["nombre"] == "v11":
        lista += [
            ("ENISA llega dos meses más tarde (mes 8)", escenario_enisa(S, MES_ENISA_V11 + 2)),
            ("ENISA presta solo el mínimo de la línea (25.000 €) en el mes 6", escenario_enisa_importe(S, 25000.0)),
            ("Clientes más pequeños: red media de 100 CT y 6.000 señales", escenario_red(S, 100, 6000)),
        ]
    # en los escenarios el art. 363 y la caja se miden, no se exigen
    return [(nombre, simular(s)) for nombre, s in lista]


# ---------------------------------------------------------------------------
# Tablas en el formato de la memoria
# ---------------------------------------------------------------------------
def tabla(titulo, cabecera, filas, anchos=None):
    out = [f"T {titulo}", "H " + " | ".join(cabecera)]
    if anchos:
        out.append("W " + " | ".join(str(w) for w in anchos))
    for f in filas:
        assert len(f) == len(cabecera), (titulo, f)
        for celda in f:
            assert "|" not in str(celda)
        out.append("R " + " | ".join(str(c) for c in f))
    return "\n".join(out)


def construir_tablas(res, escen):
    S = res["S"]
    tablas = {}
    y = {a: pyg_anual(res, a) for a in (1, 2, 3)}
    t = {a: tesoreria_anual(res, a) for a in (1, 2, 3)}
    B = res["balances"]

    def fila(nombre, clave=None, f=None, signo=1):
        return [nombre] + [fmt(signo * (f(y[a]) if f else y[a][clave])) for a in (1, 2, 3)]

    ext_orden = ["Nube (cómputo, almacenamiento, demos)", "Herramientas de desarrollo", "Asesoría jurídica",
                 "Gestoría", "Actividad comercial y ferias", "Equipos menores, viajes y otros", "Coworking",
                 "Seguros (responsabilidad civil)"]
    nombres_ext = {"Nube (cómputo, almacenamiento, demos)": "Nube (cómputo, almacenamiento y demos)",
                   "Seguros (responsabilidad civil)": "Seguro de responsabilidad civil"}
    nuevos = [sum(1 for g in S["puestas_en_marcha"] if (a - 1) * 12 < g <= a * 12) for a in (1, 2, 3)]
    filas = [
        ["Clientes nuevos en el año"] + [str(n) for n in nuevos],
        ["Clientes activos al final del año"] + [str(activos_al_final(res, a * 12)) for a in (1, 2, 3)],
        ["Licencias anuales en cartera al final del año"]
        + [fmt(activos_al_final(res, a * 12) * S["precio_licencia"]) for a in (1, 2, 3)],
        fila("Ingresos por implantación", "impl"),
        fila("Ingresos por licencias", "lic"),
        fila("Total ingresos", "ingresos"),
        fila("Sueldos y salarios", "sueldos", signo=-1),
        fila("Cargas sociales", "cargas", signo=-1),
    ]
    for k in ext_orden:
        filas.append(fila(nombres_ext.get(k, k), f=lambda d, k=k: d["ext"][k], signo=-1))
    filas += [
        fila("Comisión del socio (10 % de las licencias)", "comision", signo=-1),
        fila("Imprevistos (5 %)", "imprevistos", signo=-1),
        fila("Total gastos de explotación", "gastos_expl", signo=-1),
        fila("Resultado bruto de explotación", "ebitda"),
        fila("Amortizaciones", "amort", signo=-1),
        fila("Resultado de explotación", "ebit"),
        fila("Gastos financieros", "financieros", signo=-1),
        fila("Resultado antes de impuestos", "bai"),
        fila("Impuesto sobre Sociedades", "is", signo=-1),
        fila("Resultado del ejercicio", "resultado"),
        fila("Cash flow (resultado más amortizaciones)", f=lambda d: d["resultado"] + d["amort"]),
    ]
    tablas["pyg"] = tabla("Cuenta de explotación a tres años, en euros",
                          ["Concepto", "Año 1", "Año 2", "Año 3"], filas, [6, 2, 2, 2])

    def bfila(nombre, clave, momentos=(0, 12, 24, 36)):
        return [nombre] + [fmt(B[m][clave]) for m in momentos]
    filas = [
        bfila("Inmovilizado intangible (marca y aplicaciones)", "intangible"),
        bfila("Inmovilizado material (equipos y mobiliario)", "material"),
        ["Total activo no corriente"] + [fmt(B[m]["intangible"] + B[m]["material"]) for m in (0, 12, 24, 36)],
        bfila("Fianza del coworking", "fianza"),
        bfila("Clientes", "clientes"),
        bfila("Hacienda Pública deudora por IVA", "hp_deudora_iva"),
        bfila("Tesorería", "tesoreria"),
        ["Total activo corriente"] + [fmt(B[m]["fianza"] + B[m]["clientes"] + B[m]["hp_deudora_iva"]
                                          + B[m]["tesoreria"]) for m in (0, 12, 24, 36)],
        bfila("Total activo", "total_activo"),
        bfila("Capital social", "capital"),
        bfila("Prima de asunción", "prima"),
        bfila("Reservas (gastos de constitución y de la ronda)", "reservas"),
        bfila("Resultados de ejercicios anteriores", "anteriores"),
        bfila("Resultado del ejercicio", "resultado"),
        bfila("Total patrimonio neto", "total_pn"),
        bfila("Préstamo de ENISA a largo plazo", "enisa_lp"),
        bfila("Préstamo bancario a largo plazo", "banco_lp"),
        bfila("Total pasivo no corriente", "total_pnc"),
        bfila("Cuotas de los préstamos que vencen en el año siguiente", "deuda_cp"),
        ["Ingresos anticipados (licencias y anticipos facturados)"]
        + [fmt(B[m]["periodificacion"] + B[m]["anticipos"]) for m in (0, 12, 24, 36)],
        bfila("Hacienda Pública y Seguridad Social acreedoras", "hp_ss"),
        bfila("Total pasivo corriente", "total_pc"),
        bfila("Total patrimonio neto y pasivo", "total_pn_pasivo"),
    ]
    tablas["balances"] = tabla("Balances de situación de los momentos 0 a 3, en euros",
                               ["Partida", "Momento 0", "Momento 1", "Momento 2", "Momento 3"],
                               filas, [6, 2, 2, 2, 2])

    P = res["P"]
    filas = []
    for m in range(1, 13):
        cobros = P["cobros"][m]
        personal = P["pago_nomina"][m] + P["pago_ss"][m] + P["pago_irpf"][m]
        prov = P["pago_externos"][m] + P["pago_inversion"][m]
        iva_is = P["pago_iva"][m] + P["pago_is"][m]
        finan = (P["entrada_capital"][m] + P["entrada_prestamo"][m] - P["pago_gastos_ronda"][m]
                 - P["pago_intereses"][m] - P["pago_principal"][m])
        neto = cobros - personal - prov - iva_is + finan
        assert abs(res["caja"][m - 1] + neto - res["caja"][m]) < 0.01
        b = B[m]
        filas.append([str(m), fmt(cobros), fmt(-personal), fmt(-prov), fmt(-iva_is), fmt(finan), fmt(neto),
                      fmt(res["caja"][m]), fmt(b["pn_mercantil"])])
    tablas["tesoreria_mensual"] = tabla(
        "Tesorería del año 1 mes a mes, en euros (cobros y pagos con IVA), y patrimonio neto con el préstamo "
        "participativo (art. 363 LSC)",
        ["Mes", "Cobros", "Personal y Seguridad Social", "Proveedores", "IVA", "Financiación",
         "Saldo del mes", "Tesorería final", "Patrimonio neto (art. 363)"], filas, [1, 2, 2, 2, 2, 2, 2, 2, 2])

    def tfila(nombre, clave, signo=1):
        return [nombre] + [fmt(signo * t[a][clave]) for a in (1, 2, 3)]
    filas = [
        tfila("Tesorería inicial", "inicial"),
        tfila("Cobros de clientes", "cobros"),
        tfila("Pagos de personal y Seguridad Social", "personal", -1),
        tfila("Pagos a proveedores", "proveedores", -1),
        tfila("IVA e Impuesto sobre Sociedades", "iva_is", -1),
        tfila("Flujo de caja de la explotación", "explotacion"),
        tfila("Inversiones", "inversion"),
        tfila("Ronda de capital, neta de gastos", "capital"),
        tfila("Préstamo de ENISA cobrado", "prestamos"),
        tfila("Intereses pagados", "intereses"),
        tfila("Devolución de préstamos", "principal"),
        tfila("Tesorería final", "final"),
    ]
    tablas["tesoreria_anual"] = tabla("Tesorería de los tres años, en euros",
                                      ["Concepto", "Año 1", "Año 2", "Año 3"], filas, [6, 2, 2, 2])

    filas = []
    mc = S["enisa"]["mes_cobro"]
    momentos = [("Momento 0", 0)]
    if mc > 0:
        momentos.append((f"Mes {mc - 1}, antes de ENISA", mc - 1))
    momentos += [("Mes 11, antes de la ronda", 11), ("Momento 1", 12), ("Momento 2", 24), ("Momento 3", 36)]
    for etiqueta, m in momentos:
        pr = B[m]["total_pn"]
        aj = res["enisa_pend"][m] + res["banco_pend"][m]
        reparto = "Patrimonio neto negativo" if pr < 0 else fmt_pct(pr / (pr + aj), 0) + " / " + fmt_pct(aj / (pr + aj), 0)
        filas.append([etiqueta, fmt(pr), fmt(aj), reparto, fmt(B[m]["pn_mercantil"])])
    tablas["estructura"] = tabla("Fondos propios frente a deuda financiera, en euros",
                                 ["Momento", "Patrimonio neto", "Préstamos (ENISA y banco)", "Propio / ajeno",
                                  "Patrimonio neto con el participativo (art. 363)"], filas, [3, 2, 2, 2, 3])

    filas = []
    for nombre, r in escen:
        cm, mm = caja_minima(r, 1, 36)
        h, mh = holgura_363(r, 36)
        filas.append([nombre, fmt(cm) + f" (mes {mm})", fmt(h + r["balances"][mh]["umbral_363"]) + f" (mes {mh})",
                      fmt(pyg_anual(r, 2)["resultado"]), fmt(pyg_anual(r, 3)["ingresos"]),
                      fmt(pyg_anual(r, 3)["resultado"]), fmt(r["caja"][36]), str(activos_al_final(r, 36))])
    tablas["sensibilidad"] = tabla(
        "Sensibilidad del plan, en euros",
        ["Escenario", "Caja más baja (mes)", "Patrimonio neto más bajo con el participativo (mes)",
         "Resultado del año 2", "Ingresos del año 3", "Resultado del año 3", "Tesorería al final del año 3",
         "Clientes al final"], filas, [5, 2, 2, 2, 2, 2, 2, 1])

    filas = []
    for nombre, red in REDES_CALIBRACION.items():
        bruto = TARIFA["fijo"] + TARIFA["por_ct"] * red["ct"] + TARIFA["por_senal"] * red["senales"]
        lic = licencia(**red)
        nota = "Mínimo" if lic > bruto else "Fijo + CT + señales"
        filas.append([nombre, str(red["ct"]), fmt(red["senales"]) + (" (provisional)" if SENALES_PROVISIONALES else ""),
                      fmt(bruto), fmt(lic), nota])
    filas.append(["Red tipo del plan", str(RED_TIPO_PLAN["ct"]), fmt(RED_TIPO_PLAN["senales"]),
                  fmt(TARIFA["fijo"] + TARIFA["por_ct"] * RED_TIPO_PLAN["ct"] + TARIFA["por_senal"] * RED_TIPO_PLAN["senales"]),
                  fmt(licencia(**RED_TIPO_PLAN)), "La licencia de cada cliente del plan"])
    tablas["tarifa"] = tabla(
        f"Licencia anual con la tarifa por tamaño de red ({fmt(TARIFA['fijo'])} € fijos, {fmt(TARIFA['por_ct'])} € por CT "
        f"y {fmt1(TARIFA['por_senal'])} € por señal, con un mínimo de {fmt(TARIFA['minimo'])} € y sin máximo), en euros",
        ["Red", "CT", "Señales", "Fijo + CT + señales", "Licencia", "Qué se aplica"], filas, [3, 1, 2, 2, 2, 3])
    return tablas, y, t


# ---------------------------------------------------------------------------
# Cifras clave y comparación
# ---------------------------------------------------------------------------
def cifras_clave(r36, r60):
    c = {}
    for a in (1, 2, 3):
        y = pyg_anual(r36, a)
        c[f"ingresos_a{a}"] = y["ingresos"]
        c[f"resultado_a{a}"] = y["resultado"]
        c[f"caja_fin_a{a}"] = r36["caja"][a * 12]
        c[f"pn_a{a}"] = r36["balances"][a * 12]["total_pn"]
    for a in (4, 5):
        y = pyg_anual(r60, a)
        c[f"ingresos_a{a}"] = y["ingresos"]
        c[f"resultado_a{a}"] = y["resultado"]
        c[f"caja_fin_a{a}"] = r60["caja"][a * 12]
    pm, lic_eq = punto_muerto(r36, 3)
    c["punto_muerto_licencias"] = pm
    c["punto_muerto_euros"] = lic_eq
    c["licencias_cartera_a3"] = activos_al_final(r36, 36) * r36["S"]["precio_licencia"]
    cm, mm = caja_minima(r36)
    c["caja_min"], c["mes_caja_min"] = cm, mm
    pn, mp = pn_minimo(r36)
    c["pn363_min"], c["mes_pn363_min"] = pn, mp
    h, mh = holgura_363(r36)
    c["holgura_363"], c["mes_holgura_363"] = h, mh
    c["umbral_363"] = r36["balances"][0]["umbral_363"]
    c["van5"], c["flujos5"] = van(r60)
    c["caja_m0"] = r36["caja"][0]
    c["pn_m0"] = r36["balances"][0]["total_pn"]
    c["aportacion_fundadores"] = r36["S"]["capital_fundadores"] + r36["S"]["prima_fundadores"]
    c["capital"] = r36["S"]["capital_fundadores"]
    c["mes_enisa"] = r36["S"]["enisa"]["mes_cobro"]
    c["licencia"] = r36["S"]["precio_licencia"]
    return c


def texto_comparacion(c10, c11, ap_min):
    lin = []
    lin.append("Antes (v10) y después (v11) de las cifras clave del plan financiero, en euros")
    lin.append("")
    filas = [
        ("Licencia anual por cliente del plan", "licencia"),
        ("Aportación de los fundadores en la constitución", "aportacion_fundadores"),
        ("Capital social", "capital"),
        ("Mes de cobro de ENISA (0 = momento 0)", "mes_enisa"),
        ("Tesorería del momento 0", "caja_m0"),
        ("Ingresos del año 1", "ingresos_a1"), ("Ingresos del año 2", "ingresos_a2"),
        ("Ingresos del año 3", "ingresos_a3"), ("Ingresos del año 4 (contraste)", "ingresos_a4"),
        ("Ingresos del año 5 (contraste)", "ingresos_a5"),
        ("Resultado del año 1", "resultado_a1"), ("Resultado del año 2", "resultado_a2"),
        ("Resultado del año 3", "resultado_a3"), ("Resultado del año 4 (contraste)", "resultado_a4"),
        ("Resultado del año 5 (contraste)", "resultado_a5"),
        ("Licencias en cartera al final del año 3", "licencias_cartera_a3"),
        ("Punto muerto: euros de licencias que pagan la estructura del año 3", "punto_muerto_euros"),
        ("Tesorería al final del año 3", "caja_fin_a3"),
        ("Tesorería al final del año 5 (contraste)", "caja_fin_a5"),
        ("VAN de los flujos libres de los años 1 a 5 al 10 %, sin valor residual", "van5"),
        ("Patrimonio neto al final del año 3", "pn_a3"),
        ("Umbral del art. 363 LSC (mitad del capital)", "umbral_363"),
    ]
    lin.append("| Cifra | v10 | v11 |")
    lin.append("| --- | --- | --- |")
    for nombre, k in filas:
        lin.append(f"| {nombre} | {fmt(c10[k])} | {fmt(c11[k])} |")
    lin.append(f"| Punto muerto en licencias | {fmt1(c10['punto_muerto_licencias'])} | {fmt1(c11['punto_muerto_licencias'])} |")
    lin.append(f"| Caja más baja (mes) | {fmt(c10['caja_min'])} (mes {c10['mes_caja_min']}) | "
               f"{fmt(c11['caja_min'])} (mes {c11['mes_caja_min']}) |")
    lin.append(f"| Patrimonio neto más bajo con el participativo (mes) | {fmt(c10['pn363_min'])} (mes {c10['mes_pn363_min']}) | "
               f"{fmt(c11['pn363_min'])} (mes {c11['mes_pn363_min']}) |")
    lin.append(f"| Margen sobre la mitad del capital en el peor mes | {fmt(c10['holgura_363'])} (mes {c10['mes_holgura_363']}) | "
               f"{fmt(c11['holgura_363'])} (mes {c11['mes_holgura_363']}) |")
    lin.append("")
    lin.append(f"Flujos libres de los años 1 a 5, v10: {', '.join(fmt(f) for f in c10['flujos5'])}")
    lin.append(f"Flujos libres de los años 1 a 5, v11: {', '.join(fmt(f) for f in c11['flujos5'])}")
    lin.append("")
    lin.append(f"Aportación mínima de los fundadores sobre los 40.000 euros de la v10 para que caja y patrimonio "
               f"(art. 363) queden en positivo todos los meses con ENISA en el mes {MES_ENISA_V11}: {fmt(ap_min)} euros, "
               f"es decir, {fmt(40000 + ap_min)} en total; el plan pone {fmt(APORTACION_FUNDADORES_V11)}.")
    lin.append("")
    lin.append("Tarifa: " + ", ".join(f"{k} {v}" for k, v in TARIFA.items()))
    for nombre, red in list(REDES_CALIBRACION.items()) + [("Red tipo del plan", RED_TIPO_PLAN)]:
        lin.append(f"  {nombre}: {red['ct']} CT, {fmt(red['senales'])} señales -> {fmt(licencia(**red))} euros al año")
    if SENALES_PROVISIONALES:
        lin.append("  Las señales de las tres distribuidoras son provisionales (78.301 tags de TedisNet repartidos por CT).")
    return "\n".join(lin) + "\n"


def mensual_csv(res, ruta):
    P, B = res["P"], res["balances"]
    with open(ruta, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["mes", "ingresos", "gastos_explotacion", "resultado_mes", "cobros", "pagos",
                    "entrada_prestamo", "entrada_capital", "tesoreria_final", "patrimonio_neto",
                    "patrimonio_neto_363", "umbral_363"])
        for m in range(0, res["N"] + 1):
            if m == 0:
                w.writerow([0, 0, 0, 0, 0, 0, 0, 0, round(res["caja"][0], 2), round(B[0]["total_pn"], 2),
                            round(B[0]["pn_mercantil"], 2), round(B[0]["umbral_363"], 2)])
                continue
            ing = P["ing_impl"][m] + P["ing_lic"][m]
            gas = P["sueldos"][m] + P["cargas"][m] + P["externos"][m] + P["comision"][m] + P["imprevistos"][m]
            resm = ing - gas - P["amort"][m] - P["financieros"][m] - P["is"][m]
            pagos = (P["pago_nomina"][m] + P["pago_ss"][m] + P["pago_irpf"][m] + P["pago_externos"][m]
                     + P["pago_iva"][m] + P["pago_is"][m] + P["pago_inversion"][m] + P["pago_intereses"][m]
                     + P["pago_principal"][m] + P["pago_gastos_ronda"][m])
            w.writerow([m, round(ing, 2), round(gas, 2), round(resm, 2), round(P["cobros"][m], 2), round(pagos, 2),
                        round(P["entrada_prestamo"][m], 2), round(P["entrada_capital"][m], 2),
                        round(res["caja"][m], 2), round(B[m]["total_pn"], 2), round(B[m]["pn_mercantil"], 2),
                        round(B[m]["umbral_363"], 2)])


def escribir_xlsx(res, ruta):
    try:
        import openpyxl
    except ImportError:
        return False
    P = res["P"]
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Mensual"
    cols = [k for k in P.keys()]
    ws.append(["mes"] + cols + ["tesoreria_final"])
    for m in range(1, res["N"] + 1):
        ws.append([m] + [round(P[k][m], 2) for k in cols] + [round(res["caja"][m], 2)])
    ws2 = wb.create_sheet("Balances")
    claves = ["intangible", "material", "fianza", "clientes", "hp_deudora_iva", "tesoreria", "total_activo",
              "capital", "prima", "reservas", "anteriores", "resultado", "total_pn", "pn_mercantil", "umbral_363",
              "enisa_lp", "banco_lp", "deuda_cp", "anticipos", "periodificacion", "hp_acreedora", "ss_acreedora",
              "total_pn_pasivo"]
    ws2.append(["mes"] + claves)
    for m in range(0, res["N"] + 1):
        ws2.append([m] + [round(res["balances"][m][k], 2) for k in claves])
    ws3 = wb.create_sheet("Supuestos")
    for k, v in res["S"].items():
        ws3.append([k, json.dumps(v, ensure_ascii=False)])
    wb.save(ruta)
    return True


def calcular():
    """Todo el cálculo, con sus comprobaciones. Lo usan main() y las pruebas."""
    r10 = simular(V10)
    r11 = simular(V11)
    r10_60 = simular(a_60_meses(V10))
    r11_60 = simular(a_60_meses(V11))
    # la v11 cumple el art. 363 y tiene caja positiva todos los meses de los 36 y de los 60
    for r in (r11, r11_60):
        h, mh = holgura_363(r)
        assert h >= 0, f"v11: patrimonio por debajo de la mitad del capital en el mes {mh} ({h:.2f})"
        cm, mm = caja_minima(r)
        assert cm >= 0, f"v11: caja negativa en el mes {mm} ({cm:.2f})"
    # aportación mínima con el resto de supuestos de la v11, partiendo de los 40.000 euros de la v10
    s_min = copy.deepcopy(V11)
    s_min["prima_fundadores"] = V10["prima_fundadores"]
    ap_min = aportacion_minima(s_min)
    assert APORTACION_FUNDADORES_V11 >= 40000 + ap_min, "la aportación de la v11 ya no basta"
    c10 = cifras_clave(r10, r10_60)
    c11 = cifras_clave(r11, r11_60)
    # la v11 queda igual o mejor que la v10 en ingresos, resultados, punto muerto y VAN
    for k in ["ingresos_a1", "ingresos_a2", "ingresos_a3", "ingresos_a4", "ingresos_a5",
              "resultado_a1", "resultado_a2", "resultado_a3", "resultado_a4", "resultado_a5", "van5",
              "caja_fin_a3", "caja_fin_a5"]:
        assert c11[k] >= c10[k] - 0.01, f"la v11 empeora {k}: {c11[k]:.0f} frente a {c10[k]:.0f}"
    assert c11["punto_muerto_licencias"] <= c10["punto_muerto_licencias"] + 1e-9, "la v11 necesita más clientes para el punto muerto"
    return {"r10": r10, "r11": r11, "r10_60": r10_60, "r11_60": r11_60, "c10": c10, "c11": c11, "ap_min": ap_min}


def main():
    R = calcular()
    e11 = escenarios(V11)
    e10 = escenarios(V10)
    t11, _, _ = construir_tablas(R["r11"], e11)
    t10, _, _ = construir_tablas(R["r10"], e10)
    comp = texto_comparacion(R["c10"], R["c11"], R["ap_min"])
    if "--sin-salida" in sys.argv:
        print(comp)
        for v in t11.values():
            print()
            print(v)
        return
    os.makedirs(SALIDA, exist_ok=True)
    with open(os.path.join(SALIDA, "tablas_v11.txt"), "w", encoding="utf-8") as f:
        f.write("\n\n".join(t11.values()) + "\n")
    with open(os.path.join(SALIDA, "tablas_v10.txt"), "w", encoding="utf-8") as f:
        f.write("\n\n".join(t10.values()) + "\n")
    with open(os.path.join(SALIDA, "comparacion.txt"), "w", encoding="utf-8") as f:
        f.write(comp)
    cif = {k: (round(v, 2) if isinstance(v, float) else v) for k, v in R["c11"].items() if k != "flujos5"}
    cif["flujos5"] = [round(x, 2) for x in R["c11"]["flujos5"]]
    cif["aportacion_minima_extra"] = round(R["ap_min"], 2)
    with open(os.path.join(SALIDA, "cifras_v11.json"), "w", encoding="utf-8") as f:
        json.dump(cif, f, ensure_ascii=False, indent=1, sort_keys=True)
    mensual_csv(R["r11"], os.path.join(SALIDA, "mensual_v11.csv"))
    escribir_xlsx(R["r11"], os.path.join(SALIDA, "detalle_v11.xlsx"))
    print(comp)
    print("Comprobaciones superadas: balances cuadrados en los 37 cierres (y en los 61 de la extensión), "
          "tesorería y resultados encadenados, art. 363 LSC y caja en positivo todos los meses en la v11.")


if __name__ == "__main__":
    main()
