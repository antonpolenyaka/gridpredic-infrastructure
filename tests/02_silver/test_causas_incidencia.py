"""
Cause rules of the Calser incidents (etl/jobs/02_silver/causas_incidencia.py)
on texts taken from the real databases. Pure Python, no Spark.

    python -m pytest tests/02_silver/test_causas_incidencia.py -q
"""

import os
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "etl", "jobs", "02_silver"))

import causas_incidencia as ci  # noqa: E402


def incident(descripcion, **kw):
    record = {
        "clasificacion_id": "CL_IMPRE", "factor_id": "FA_PROPI", "tipo_equipo_id": "LAERE", "casuistica": None,
        "nivel": "ct", "fecha": "2022-05-10", "descripcion": descripcion,
    }
    record.update(kw)
    return ci.classify(record)


@pytest.mark.parametrize("text, causa, subcausa", [
    ("Avería provocada por palos de nido de cigüeñas en apoyo de la línea Sierra de Fuentes Sur",
     "fauna", "ciguena_nido"),
    ("Actuación de las protecciones del transformador de potencia de la STR Torremocha provocado por un animal.",
     "fauna", "animal_subestacion"),
    ("DISPARO TRAFO STR CORIA POR UN GATO EN CELDAS", "fauna", "animal_subestacion"),
    ("Avería originada por un roedor que provocó la fusión de dos fusibles del CT 2218", "fauna", "roedor"),
    ("Avería provocada por un palo en el IAAT Dominguín de la línea Zarza", "fauna", "palos_restos_nido"),
    ("Avería provocada por la caída de un pino sobre la parte aérea de la línea Hervas II.",
     "vegetacion", "caida_arbol_rama"),
    ("Avería provocada por la caída de un árbol procedente de una poda en la circunvalación de Baños.",
     "vegetacion", "poda_tala_terceros"),
    ("Conductor de línea subterránea dañado por máquina escavadora", "terceros", "maquinaria_obras"),
    ("Avería en la derivación de la charca del Cura motivado por la rotura de los platos de un aislador por disparos.",
     "terceros", "disparos_caza"),
    ("Sabotaje cables de mando IAAT Dominguín", "terceros", "vandalismo_robo"),
    ("Bloqueo del IAAT Antonio 1 motivado por el contacto de cables de teléfonica que se enredaron en la línea",
     "terceros", "cables_terceros"),
    ("Corte del servicio realizado a la localidad de Cabezabellosa, solicitado por el Servicio de Emergencias 112 "
     "de Extremadura, motivado por el incendio de Jarilla", "incendio", "corte_preventivo_emergencias"),
    ("Bloqueo del interruptor automático de la línea Zarza por tormenta eléctrica.", "meteorologia", "rayo_tormenta"),
    ("Bloqueo del interruptor de la línea Alcántara motivado por las adversas condiciones metereológicas provocadas "
     "por la borrasca Kristin.", "meteorologia", "viento_temporal"),
    ("Avería provocada por la rotura de un conductor por manguito de hielo en la derivación al CT1603",
     "meteorologia", "hielo_nieve"),
    ("Bloqueo del interruptor automático de la línea Sierra de Fuentes Sur por causas desconocidas. Fuerte tormenta "
     "en la zona.", "meteorologia", "sin_localizar_rayo_tormenta"),
    ("Bloqueo del IAAT Zarza 1 por causas desconocidas.", "desconocida", "declarada_desconocida"),
    ("Avería provocada por la rotura de un puente en el apoyo 1890.68 que provocó el suministro a dos fases.",
     "equipo", "puente"),
    ("Bloqueo del reconectador Guijo motivado por la rotura de un XS en el CT 0307 de Ahigal",
     "equipo", "seccionador_xs_fusible_mt"),
    ("Interrupción del servicio provocada por una avería en una botella terminal en el CT6602 de Torremocha",
     "equipo", "cable_subterraneo_terminal"),
    ("Disparo protección diferencial trafo 2 STR Torre de Santa Maria", "equipo", "transformador_potencia_str"),
    ("Avería del transformador del CT5701 de Salvatierra de Santiago", "equipo", "transformador_ct"),
])
def test_agent_from_text(text, causa, subcausa):
    out = incident(text)
    assert (out["causa"], out["subcausa"]) == (causa, subcausa), out


def test_tenido_is_not_a_nest():
    out = incident("Interrupción del servicio que se ha tenido que realizar para quitar pasto que se había quedado "
                   "enganchado en la línea Aldea del Cano")
    assert out["causa"] == "objeto_ajeno"
    assert out["maniobra_reparacion"]


def test_switch_that_tripped_is_not_a_broken_transformer():
    out = incident("Bloqueo del interruptor de salida del trafo nº 2 de la STR Sierra de Fuentes.", casuistica=35)
    assert out["causa"] == "desconocida"
    assert out["origen"] == "subestacion"


def test_weather_named_with_an_agent_keeps_the_agent_and_the_flag():
    out = incident("Avería provocada por la caída de una rama sobre el CT7427 originado por fuerte temporal de viento "
                   "y lluvia.")
    assert out["causa"] == "vegetacion"
    assert out["meteo_en_texto"]


def test_equipment_failure_caused_by_weather_is_weather():
    out = incident("Interrupción del servicio originada por una avería en un puente de la línea circunvalación Aldea "
                   "del Cano provocada por las fuertes condiciones metereológicas que se produjeron en la zona.")
    assert out["causa"] == "meteorologia"
    assert out["componente"] == "puente"


def test_upstream_network():
    out = incident("Ausencia de tensión en interconexión con Endesa", factor_id="FA_TERCE", casuistica=28)
    assert (out["causa"], out["subcausa"], out["origen"]) == ("aguas_arriba", "interconexion_endesa", "aguas_arriba")
    out = incident("Interrupción provocada por la ausencia de tensión en la línea a 45 kV Cáceres - Trujillo propiedad "
                   "de Iberdrola Distribución Eléctrica", factor_id="FA_TERCE")
    assert out["subcausa"] == "interconexion_iberdrola"


def test_blackout_of_28_04_2025():
    out = incident("Interrupción causada por el cero energético", factor_id="FA_TERCE", fecha="2025-04-28",
                   instalacion_origen="RED DE TRANSPORTE")
    assert (out["causa"], out["origen"]) == ("apagon", "sistema")


def test_planned_works_from_the_reasons_of_its_interruptions():
    out = incident("Corte programado Navas", clasificacion_id="CL_PROGR", factor_id="FA_DISTR",
                   motivos="Instalación medidas disuasorias para nidificación de cigüeñas")
    assert (out["causa"], out["subcausa"]) == ("programada", "proteccion_avifauna")
    out = incident("CCTT6501 - 6506", clasificacion_id="CL_PROGR", factor_id="FA_DISTR",
                   motivos="Digitalización de Centro de Transformación")
    assert out["subcausa"] == "digitalizacion_telegestion"


def test_low_voltage_network_and_customer():
    out = incident("Fusible fundido en caja de acometida", nivel="abonado", tipo_equipo_id="BAJAT")
    assert (out["causa"], out["subcausa"], out["origen"]) == ("red_bt", "fusible_portafusible", "red_bt")
    out = incident("ICP averiado", nivel="abonado", factor_id="FA_CLIEN", tipo_equipo_id="OTROS")
    assert (out["causa"], out["origen"]) == ("cliente_bt", "cliente_bt")


def test_private_installation_is_the_origin():
    out = incident("Avería provocada por un nido de cigüeñas en la derivación particular al CT4337 de Navas del Madroño.",
                   factor_id="FA_TERCE", casuistica=29)
    assert (out["causa"], out["origen"]) == ("fauna", "instalacion_particular")


def test_official_code_when_the_text_names_no_agent():
    out = incident("Bloqueo del interruptor de la posición J52-1.", casuistica=32)
    assert (out["causa"], out["fuente_causa"]) == ("meteorologia", "codigo_oficial")


def test_old_default_codes_are_not_a_cause():
    out = incident("DISPARO LINEA VALDEFUENTES", casuistica=35, fecha="2012-03-01")
    assert (out["causa"], out["subcausa"]) == ("sin_causa", "solo_actuacion_proteccion")
    out = incident("DISPARO LINEA VALDEFUENTES", casuistica=35, fecha="2021-03-01")
    assert out["causa"] == "desconocida"


def test_normalize():
    assert ci.normalize("  Bloqueo   del IAAT «Zarza 1»  ") == "bloqueo del iaat zarza 1"
    assert ci.normalize(None) == "" and ci.normalize(float("nan")) == ""


def test_the_same_second_in_two_networks_is_only_flagged():
    """job_silver_f_incidencia_causa.shared_start: needs every distribuidora at once, so it runs after classify."""
    import pandas as pd

    import job_silver_f_incidencia_causa as job

    pdf = pd.DataFrame({
        "distribuidora_id": [3, 2, 3, 2, 3, 2, 1366],
        "inicio_ts": pd.to_datetime(["2026-01-28 08:35:11", "2026-01-28 08:35:11", "2026-03-15 08:00:00",
                                     "2026-03-15 08:00:00", "2025-04-28 12:33:14", "2025-04-28 12:33:14",
                                     "2026-01-28 08:35:11"]),
        "clasificacion_id": ["CL_IMPRE", "CL_IMPRE", "CL_PROGR", "CL_PROGR", "CL_IMPRE", "CL_IMPRE", "CL_PROGR"],
    })
    out = pd.DataFrame({
        "causa": ["meteorologia", "sin_causa", "programada", "programada", "apagon", "apagon", "programada"],
        "subcausa": ["viento", None, "obras", "obras", None, None, "obras"],
        "origen": ["linea_mt", "linea_mt", "programada", "programada", "sistema", "sistema", "programada"],
        "regla": ["texto", "sin_texto", "clasificacion", "clasificacion", "texto", "texto", "clasificacion"],
        "fuente_causa": ["texto", "ninguna", "clasificacion", "clasificacion", "texto", "texto", "clasificacion"],
    })
    result = job.shared_start(pdf, out)
    # The storm of 28.01.2026 started in the same second in the Distribuidora 1 and the 2, and so did the blackout:
    # both are flagged. Planned works at the same hour are not, nor a planned one of a third network.
    assert result["simultanea_otra_red"].tolist() == [True, True, False, False, True, True, False]
    # The cause of Calser stands: the flag is information, not a verdict (the common-provider reading of 17.10.2026
    # was withdrawn on 18.10.2026).
    assert result["causa"].tolist() == out["causa"].tolist() and result["subcausa"].tolist() == out["subcausa"].tolist()
    assert "simultanea_otra_red" not in out.columns


# Errores de las reglas que encontró la lectura de los desacuerdos con el clasificador de texto en los que el código
# oficial no respalda a la regla (experimentos/causas_texto/salida/desacuerdos.csv, 19.10.2026): de los 8, 2 son
# errores y 6 decisiones de prioridad (tormenta con causa desconocida, corte aguas arriba por un incendio, maniobra
# para localizar una avería, maniobra para retirar un ave, caída de árbol por un temporal y disparo de un
# transformador con la reparación del apoyo). Los textos son sintéticos con el mismo patrón que el parte real. Se
# marcan como fallo esperado estricto: la prueba avisa cuando se corrija la regla, sin cambiar hoy las etiquetas.
@pytest.mark.xfail(strict=True, reason="VEGETACION busca 'alamo' sin límite de palabra y lo encuentra dentro del "
                                       "nombre de una línea")
def test_nombre_de_linea_con_alamo_no_es_vegetacion():
    out = incident("Bloqueo de la línea Calamoxa por las fuertes rachas de viento de la borrasca.")
    assert out["causa"] == "meteorologia"


@pytest.mark.xfail(strict=True, reason="un corte para poner sistemas antinidificación sale como fauna; es una "
                                       "maniobra para hacer trabajos, como cualquier otro corte de ese tipo")
def test_corte_para_poner_antinidificacion_no_es_fauna():
    out = incident("Corte del servicio realizado para poder poner sistemas antinidificación en una torre.")
    assert out["causa"] == incident("Corte del servicio realizado para poder sustituir un apoyo de la línea.")["causa"]
