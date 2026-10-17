"""
Cause of every Calser incident, read from everything the operator wrote and
coded about it.

Calser keeps the cause in three places and none of them is enough on its
own:

- The factor (FA_*) and the classification (CL_*) say who is responsible
  (own network, third parties, force majeure, upstream, customer) and if the
  cut was planned, not what happened.
- The cause code of the Extremadura module (incidencias_ex, casuistica
  21 - 36: atmospheric, external agents, internal, unknown, private
  installations, other distribuidora...) is only filled with care from
  2016: from 2011 to 2015, 85 - 93 % of the incidents have 35 (unknown),
  the default value, and 93 - 97 % have 35 or 34 (internal).
- The free text (description, observations, resolution, the descriptions
  of its interruptions, the reasons and works of the planned ones, and the
  installation where it started) is the richest source, but about half of
  the older texts only say which switch tripped ("DISPARO LINEA X").

This module combines them in two axes:

- causa / subcausa: the physical agent (weather, fauna, vegetation, fire,
  third parties, equipment failure by component, low voltage network,
  customer installation, overload, upstream network, planned works, unknown
  or not stated).
- origen: where it started (upstream network, substation, 45 kV line,
  medium voltage line, CT, low voltage network, private installation of a
  customer, customer installation in low voltage, system).

The text decides when it names an agent; the official code fills the cases
whose text names none; the factor is the last resort. Every result carries
the rule that produced it (regla) and the source (fuente_causa), so a wrong
pattern can be found and fixed. meteo_en_texto marks the incidents whose
text mentions the weather even when the agent is another one (a tree that
the wind threw on the line), and maniobra_reparacion the cuts made on
purpose to repair or find a fault.

Pure Python on purpose: the Silver job applies it to a pandas frame of a few
thousand incidents and the tests run without Spark.
"""

import re
import unicodedata

# Cause codes of the Extremadura module (IncidenciaEx.TypeCodigosCausas).
CASUISTICA = {
    21: "fuerza_mayor",
    23: "generacion",
    25: "transporte",
    28: "otra_distribuidora",
    29: "instalacion_particular",
    30: "accion_terceros",
    31: "huelga",
    32: "atmosferica",
    33: "agente_externo",
    34: "interna",
    35: "desconocida",
    36: "no_subsumible",
}

# Voltage level of incidencias_ex and interrupciones_ex.
NIVEL_TENSION = {0: "AT", 1: "MT", 2: "BT", 3: "AT+MT+BT", 4: "AT+MT", 5: "AT+BT", 6: "MT+BT"}

# Collection methods of incidencias_metodos_recogida_ex.
METODO_RECOGIDA = {4: "telecontrol", 5: "telesenalizacion", 6: "actuacion_manual", 7: "atencion_cliente"}

TIPO_DIA = {0: "laborable", 1: "festivo_domingo"}

# Families shown in the reports, in this order.
CAUSAS = [
    "programada", "aguas_arriba", "apagon", "meteorologia", "fauna", "vegetacion", "incendio", "terceros",
    "objeto_ajeno", "agente_externo", "equipo", "red_bt", "cliente_bt", "sobrecarga", "maniobra", "desconocida",
    "sin_causa",
]


def normalize(text) -> str:
    """Lower case, no accents, only letters and digits separated by one space."""
    if text is None:
        return ""

    value = str(text)

    if value.strip().lower() in ("", "nan", "none", "null"):
        return ""

    value = unicodedata.normalize("NFKD", value.lower())
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def _rx(pattern: str):
    return re.compile(pattern)


# ---------------------------------------------------------------------------
# Patterns (on normalized text)
# ---------------------------------------------------------------------------

APAGON = _rx(r"cero energetico|cero general|cero nacional|cero de tension nacional|apagon|28 04 2025")

PROGRAMADA = _rx(r"corte programado|cortes programados|interrupcion programada|\bprogramad[oa]s?\b")
PROLONGACION = _rx(r"prolongacion del corte")
PARA_PROGRAMADO = _rx(r"(efectuar|realizar|hacer) un corte programado|deshacer (el )?corte programado")

PROGRAMADA_SUB = [
    ("proteccion_avifauna", _rx(r"avifauna|antinid|anti nid|\bnid(o|os|ificacion|ifica)\b|cigu|estornino|"
                                r"forrado (de )?(apoyos|conductores)|paragu")),
    ("digitalizacion_telegestion", _rx(r"digitaliz|telegestion|concentrador|telemedida")),
    ("automatizacion_linea", _rx(r"reconectador|\biaat\b|interruptor aereo|interruptor automatico aereo|"
                                 r"telemand|motorizad")),
    ("cambio_tension", _rx(r"cambio (de )?tension|nueva tension|bitension")),
    ("otra_empresa", _rx(r"(realizado por|de) (endesa|iberdrola)|^corte endesa|endesa$|iberdrola$")),
    ("reforma_nuevas_instalaciones", _rx(
        r"traslado|nuevo (ct|centro|apoyo|emplazamiento)|nueva linea|enganche|conexion (de )?(nueva|lmt)|desvio|"
        r"elevacion|puesta en servicio|torres? metalica|izado|by pass|construccion del ave|poligono|desmontar|"
        r"desmontaje de tramo|nuevo seccionador|instalacion (de )?(nuevo|seccionadores)")),
    ("sustitucion_equipos", _rx(r"sustitu|cuadros de baja|cambio de trafo|cambio trafo|nuevas celdas|nueva celda|"
                                r"montaje de celdas")),
    ("reparacion_averias", _rx(r"averiad|reparacion|repara|rotos|rotura")),
    ("mantenimiento", _rx(r"mantenimiento|manteniento|revision|limpieza|engrase")),
    ("poda", _rx(r"\bpoda|\btala\b")),
    ("maniobras_programado", _rx(r"deshacer|cierre de puentes|cerrar puentes|cierre puentes|apertura de puentes")),
]

AGUAS_ARRIBA_TEXTO = _rx(
    r"ausencia de tension|asencia de tension|asuencia de tension|falta (de )?alimentacion|"
    r"distribuidor aguas arriba|distrubuidor aguas arriba|aguas arriba|propiedad de (iberdrola|endesa)|"
    r"titularidad de iberdrola|disparos? (en )?(linea de )?endesa|^disparo endesa|corte (programado )?(realizado por )?"
    r"(de )?endesa|^corte endesa|pruebas iberdrola|red de transporte|averia de (iberdrola|endesa)"
)
AGUAS_ARRIBA_SUB = [
    ("interconexion_endesa", _rx(r"endesa|sevillana|e distribucion|edistribucion|edesa")),
    ("interconexion_iberdrola", _rx(r"iberdrola|i de\b|epdslu|caceres trujillo")),
    ("red_transporte", _rx(r"red de transporte|transporte|220")),
]

INCENDIO = _rx(r"incendio|infoex|extincion|\bfuego\b")
INCENDIO_NO = _rx(r"incendio del transformador|contenedor|modulo|contador|coche|vehiculo|camion|caja")
INCENDIO_PREVENTIVO = _rx(r"112|infoex|emergencia|extincion|solicit|peticion|indicaciones")

FAUNA = _rx(
    r"cigu|cigena|\bnid(o|os|ificacion|ifica)\b|nidifica|estornino|\baves?\b|pajar|animal|roedor|\bratas?\b|"
    r"\braton|serpiente|culebra|\bgatos?\b|gardun|buitre|aguila|cuervo|paloma|lagart"
)
SUBESTACION_LUGAR = r"(\bstr\b|trafo|transformador|subestacion|reactancia|\bceldas\b|barras|centro de seccionamiento)"
FAUNA_SUB = [
    ("ciguena_nido", _rx(r"cigu|cigena|\bnid(o|os|ificacion|ifica)\b|nidifica")),
    ("estorninos", _rx(r"estornino")),
    ("animal_subestacion", _rx(r"(animal|\bgatos?\b|gardun|culebra|serpiente|lagart|pajar|\baves?\b).*" + SUBESTACION_LUGAR
                               + "|" + SUBESTACION_LUGAR + r".*(animal|\bgatos?\b|gardun|culebra|serpiente|lagart|pajar|\baves?\b)")),
    ("roedor", _rx(r"roedor|\bratas?\b|\braton")),
    ("ave", _rx(r"\baves?\b|pajar|buitre|aguila|cuervo|paloma")),
    ("otro_animal", _rx(r".")),
]
PALOS = _rx(r"\bpalos?\b|alambres? caid")

VEGETACION = _rx(
    r"arbol|\bramas?\b|chopo|\bpinos?\b|encina|eucalipt|\bolmo|\bpoda|\btala\b|vegetac|maleza|arbust|alcornoque|"
    r"\bolivo|alamo"
)
VEGETACION_TERCEROS = _rx(r"\bpoda|\btala\b|trabajos de tala")

TERCEROS = [
    ("maquinaria_obras", _rx(r"excavador|escavador|retroexcav|maquina|\bobras?\b|planta fotovoltaica|"
                             r"construccion del ave")),
    ("vehiculo", _rx(r"camion|vehiculo|tractor|\bcoche|accidente de trafico|choque|grua")),
    ("aeronave", _rx(r"helicoptero|avioneta|aeronave")),
    ("vandalismo_robo", _rx(r"vandal|\brobo|robar|robos|hurto|sabotaje|sustrac|intencionad")),
    ("disparos_caza", _rx(r"por disparos|perdigon|escopeta|cazador")),
    ("cables_terceros", _rx(r"telefonica|cables de telefon|cable de telefon|fibra optica")),
    ("fuego_urbano", _rx(r"contenedor")),
]

OBJETO_AJENO = _rx(r"pasto|plastico|techo de una vivienda|chapa|lona|globo|cometa")

METEO = _rx(
    r"tormenta|descarga atmosf|\brayos?\b|aparato electrico|temporal|vendaval|viento|\brachas?\b|borrasca|"
    r"lluvia|llover|\bllueve|inundac|desbord|riada|hielo|nieve|nevada|helada|granizo|meteorolog|metereolog|"
    r"climatolog|condiciones adversas|condiciones [a-z ]{0,30}adversas|\bdana\b|\bgloria\b|kristin|barbara|"
    r"\bfilomena"
)
METEO_SUB = [
    ("rayo_tormenta", _rx(r"tormenta|descarga atmosf|\brayos?\b|aparato electrico")),
    ("hielo_nieve", _rx(r"hielo|nieve|nevada|helada|granizo|filomena")),
    ("lluvia_inundacion", _rx(r"inundac|desbord|riada|lluvia|llover|\bllueve|\bdana\b")),
    ("viento_temporal", _rx(r"temporal|vendaval|viento|\brachas?\b|borrasca|gloria|kristin|barbara")),
    ("adversas_sin_detalle", _rx(r".")),
]

SOBRECARGA = _rx(r"sobrecarga|sobreintensidad|exceso de carga|por carga\b|calentamiento")

# Equipment of the medium and high voltage network, from the most specific.
# A component only counts as the cause when the text also says that it
# failed (FALLO): "bloqueo del interruptor de salida del trafo" names a
# switch that tripped, not a broken transformer.
EQUIPO = [
    ("transformador_potencia_str", _rx(
        r"transformador(es)? de potencia|trafo de potencia|proteccion(es)? diferencial|diferencia del trafo|"
        r"diferencial del (trafo|transformador)|trafo n[o ]*\d+ (de la )?str|transformador n[o ]*\d+ \d+ ?\d* ?kv|"
        r"132 ?45|220 ?45|reactancia|autotransformador|incendio del transformador")),
    ("aparamenta_subestacion", _rx(
        r"seccionador (tripolar )?de barras|acoplamiento de barras|averia (del|en el) interruptor|fallo de interruptor|"
        r"botella del cr|pieza americana|pasa ?muros de la str|posicion de la linea")),
    ("puente", _rx(r"\bpuentes?\b|\bpuerte\b|suministro a dos fases|suministro en dos fases")),
    ("seccionador_xs_fusible_mt", _rx(r"\bxs\b|cuchilla|fusible xs|ballesta|fusibles? del ct|fusion de dos fusibles")),
    ("seccionador", _rx(r"seccionador|tripolar|seccionamiento|seccioanador")),
    ("aislador_cruceta", _rx(r"aislador|cadena de (aisladores|suspension|amarre)|cruceta|platos")),
    ("autovalvula", _rx(r"autovalvula|pararrayo")),
    ("cable_subterraneo_terminal", _rx(r"botella|terminal|subterrane|\blsmt\b|pasamuros|pasa muros|cable seco|"
                                       r"perforad|manguera")),
    ("conductor", _rx(r"conductor|cruce de conductores|cruzamiento|cruzad")),
    ("conector_empalme", _rx(r"grimpee|crimpee|crimpit|crimpis|empalme|manguito|grapa|conector")),
    ("transformador_ct", _rx(r"(averia|defecto|rotura) (del |de |en el |en )?(trafo|transformador)\b|"
                             r"transformador (del|de) (ct|centro|c t)|trafo del ct|averia del ct|averia en el ct|"
                             r"transformador n[o ]*\d+ del ct|averia del transformador|transformador del centro|"
                             r"transformador n[o ]*\d{4}")),
    ("celda_ct", _rx(r"\bceldas?\b")),
    ("equipo_corte_linea", _rx(r"averia del iaat|iaat .*averi|reconectador .*averi|averia del reconectador")),
    ("apoyo_poste", _rx(r"\bapoyos?\b .*(caido|caida|derrib|partid|rotura)|\bpostes?\b|derribo")),
]
FALLO = _rx(
    r"averi|avera\b|rotura|\brot[oa]s?\b|partid|quemad|fundid|perforad|preforad|fogue|suelt[oa]|danad|defect|"
    r"fallo|sulfatad|mal estado|desgaste|chispazo|cortocircuito|\bcorto\b|recalentad|reparar|\brepara\b|reparacion|"
    r"sustitu|incendio del transformador|diferencial|derivacion en|suministro a dos fases|suministro en dos fases|"
    r"descolg|caida|cruce|cruzamiento|cruzad|desprend|rompi|cortad|explosion|mal contacto|falso contacto|en corto|"
    r"deterior|vencid|desconect"
)

# Low voltage: the customer installation first (meter, ICP, indoor wiring).
CLIENTE_BT = _rx(
    r"\bicp\b|contador|limitador|diferencial (disparado|en mal estado|averiado)|neutro suelto en el diferencial|"
    r"magnetotermico|"
    r"cuadro del abonado|vivienda del abonado|instalacion interior|lampara|interruptor general disparado|"
    r"rearme|rearmar|modulo del abonado|derivacion individual|electrodomestic|ruido raro"
)
RED_BT = [
    ("fusible_portafusible", _rx(r"fusible|portafusible|fuslibel|\blira\b")),
    ("acometida", _rx(r"acometida|paso de calle|paso de trenzado")),
    ("conexion_empalme_bt", _rx(r"falso contacto|sulfatad|empalme|boquilla|nilex|niled|nilet|borna|petaca|"
                                r"mal contacto|conexion|conector")),
    ("neutro", _rx(r"\bneutros?\b")),
    ("conductor_bt", _rx(r"trenzad|alimentador|alimentacion trifasica|cable|subterraneo|linea de \d+ mm|fase quemada")),
    ("caja_cuadro_bt", _rx(r"caja|cuadro|centralizacion|armario")),
]

DESCONOCIDA = _rx(
    r"desconoc|desonoc|deconoc|desonoc|no se localiz|sin localiz|no localiz|origen desconocido|sin determinar|"
    r"no se encontr|no se encuentra"
)

MANIOBRA_REPARACION = _rx(
    r"^maniobras? |^disparo realizado|maniobra (realizada|que se tuvo que realizar|para)|corte (del (servicio|suministro) )?"
    r"(realizado|que se (tuvo|ha tenido) que realizar)|para (poder )?(reparar|repara|reponer|cerrar puentes|quitar|"
    r"bajar|localizar|intentar|poner|efectuar)|reparacion de|apertura de interruptor|interrupcion que se tuvo que realizar|"
    r"corte de suministro realizado"
)

PROTECCION = _rx(r"disparo|bloqueo|bleoqueo|bloquo|reconectador|actuacion de (la|las) protecc|apertura|"
                 r"iaat|interruptor|proteccion")

# Where it started.
ORIGEN_PARTICULAR = _rx(
    r"particular|propiedad del (excmo )?ayuntamiento|instalacion(es)? particular|cooperativa|balneario|"
    r"fotovoltaica solaria|fotovoltacia|camping|bombeo|finca"
)
ORIGEN_SUBESTACION = _rx(
    r"transformador(es)? de potencia|trafo de potencia|proteccion(es)? diferencial|trafo n[o ]*\d+ (de la )?str|"
    r"transformador n[o ]*\d+ \d+|reactancia|barras|interruptor de (entrada|salida)|salida (del )?trafo|"
    r"entrada (del )?trafo|entrada trafo|132|220 ?45|botella del cr|subestacion|centro de reparto|"
    r"protecciones del sistema"
)
ORIGEN_AT = _rx(r"45 ?kv|145 ?kv|linea .*str [a-z ]+ str|alimentacion a la str|line(a|as) a 45")
ORIGEN_CT = _rx(r"\bct\b|\bcctt\b|centro de transformacion|centro de tranformacion|cc tt")


def _first(rules, text: str, default=None):
    for name, pattern in rules:
        if pattern.search(text):
            return name
    return default


def _code(value):
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return None
    return number


def classify(record: dict) -> dict:
    """
    Cause of one incident. record has the keys clasificacion_id, factor_id,
    tipo_equipo_id, casuistica, nivel (ct, salida, acometida, abonado or
    sin_interrupciones), fecha (YYYY-MM-DD) and the texts descripcion,
    observaciones, resolucion, instalacion_origen, instalacion_cabecera,
    int_descripciones, motivos and trabajos (any of them can be missing).
    """
    desc = " ".join(normalize(record.get(k)) for k in ("descripcion", "observaciones", "resolucion")).strip()
    extra = " ".join(normalize(record.get(k)) for k in ("int_descripciones", "motivos", "trabajos")).strip()
    place = " ".join(normalize(record.get(k)) for k in ("instalacion_origen", "instalacion_cabecera")).strip()
    text = " ".join(t for t in (desc, extra) if t)
    full = " ".join(t for t in (text, place) if t)

    clasificacion = record.get("clasificacion_id")
    factor = record.get("factor_id")
    nivel = record.get("nivel") or ""
    tipo_equipo = record.get("tipo_equipo_id")
    code = _code(record.get("casuistica"))
    fecha = str(record.get("fecha") or "")[:10]

    out = {
        "causa": None, "subcausa": None, "componente": None, "origen": None, "regla": None,
        "fuente_causa": "texto", "meteo_en_texto": bool(METEO.search(text)),
        "maniobra_reparacion": bool(MANIOBRA_REPARACION.search(desc)),
        "casuistica_nombre": CASUISTICA.get(code) if code is not None else None,
    }

    def done(causa, subcausa, regla, fuente="texto", componente=None):
        out.update(causa=causa, subcausa=subcausa, regla=regla, fuente_causa=fuente, componente=componente)

    low_voltage = nivel in ("abonado", "acometida", "salida") or tipo_equipo == "BAJAT"
    component = _first(EQUIPO, text) if FALLO.search(text) else None
    wildfire = INCENDIO.search(text) and not INCENDIO_NO.search(text)

    if (fecha == "2025-04-28" and APAGON.search(full)) or APAGON.search(text):
        done("apagon", "cero_peninsular", "apagon")
    elif clasificacion == "CL_PROGR":
        done("programada", _first(PROGRAMADA_SUB, text, "corte_programado"), "clasificacion_programada", "clasificacion")
    elif PARA_PROGRAMADO.search(desc):
        done("programada", "maniobras_programado", "maniobra_para_programado")
    elif PROLONGACION.search(text):
        upstream = AGUAS_ARRIBA_TEXTO.search(text) or code == 28
        done("aguas_arriba" if upstream else "programada", "prolongacion_programada", "prolongacion")
    elif factor in ("FA_TRANS", "FA_GENER") or code in (25, 23, 28) or (
            AGUAS_ARRIBA_TEXTO.search(text) and factor != "FA_PROPI"):
        done("aguas_arriba", _first(AGUAS_ARRIBA_SUB, full, "otra"), "aguas_arriba",
             "texto" if AGUAS_ARRIBA_TEXTO.search(text) else "codigo_oficial")
    elif wildfire and INCENDIO_PREVENTIVO.search(text):
        done("incendio", "corte_preventivo_emergencias", "incendio")
    elif FAUNA.search(text):
        done("fauna", _first(FAUNA_SUB, text), "fauna")
    elif VEGETACION.search(text):
        done("vegetacion", "poda_tala_terceros" if VEGETACION_TERCEROS.search(text) else "caida_arbol_rama",
             "vegetacion")
    elif _first(TERCEROS, text):
        done("terceros", _first(TERCEROS, text), "terceros")
    elif wildfire:
        done("incendio", "dano_por_incendio", "incendio")
    elif PALOS.search(text):
        done("fauna", "palos_restos_nido", "palos")
    elif OBJETO_AJENO.search(text):
        done("objeto_ajeno", "objeto_enganchado", "objeto_ajeno")
    elif SOBRECARGA.search(text):
        done("sobrecarga", "sobrecarga", "sobrecarga")
    elif low_voltage and CLIENTE_BT.search(text):
        done("cliente_bt", "instalacion_cliente", "cliente_bt")
    elif low_voltage and _first(RED_BT, text):
        bt = _first(RED_BT, text)
        done("red_bt", bt, "red_bt", componente=bt)
    elif component:
        done("equipo", component, "equipo", componente=component)
    elif METEO.search(text):
        unknown = DESCONOCIDA.search(text)
        done("meteorologia", ("sin_localizar_" if unknown else "") + _first(METEO_SUB, text),
             "desconocida_con_meteo" if unknown else "meteo")
    elif CLIENTE_BT.search(text):
        done("cliente_bt", "instalacion_cliente", "cliente_bt")
    elif _first(RED_BT, text) and FALLO.search(text):
        bt = _first(RED_BT, text)
        done("red_bt", bt, "red_bt", componente=bt)
    elif DESCONOCIDA.search(text):
        done("desconocida", "declarada_desconocida", "desconocida")
    elif out["maniobra_reparacion"]:
        done("maniobra", "maniobra_derivada", "maniobra")

    # Weather named as the cause of an agent that would otherwise be "unknown"
    # or an equipment failure: "bloqueo ... por causas desconocidas, fuerte
    # tormenta en la zona", "puente ... provocada por las fuertes condiciones
    # meteorologicas". An unknown cause with the atmospheric code (32) is
    # also taken as weather.
    if out["causa"] in ("desconocida", "equipo") and out["meteo_en_texto"]:
        part = out["componente"]
        subcause = _first(METEO_SUB, text)
        if out["causa"] == "desconocida":
            done("meteorologia", "sin_localizar_" + subcause, "desconocida_con_meteo")
        else:
            done("meteorologia", subcause, "equipo_con_meteo", componente=part)
    elif out["causa"] == "desconocida" and code == 32:
        done("meteorologia", "sin_localizar_codigo_atmosferica", "desconocida_codigo_atmosferica", "codigo_oficial")

    # The official code when the text names no agent.
    if out["causa"] is None:
        by_code = {
            32: ("meteorologia", "codigo_atmosferica"),
            33: ("agente_externo", "codigo_agente_externo"),
            34: ("equipo", "codigo_interna"),
            29: ("equipo", "codigo_instalacion_particular"),
            30: ("terceros", "codigo_accion_terceros"),
            21: ("meteorologia", "codigo_fuerza_mayor"),
            35: ("desconocida", "codigo_desconocida"),
        }
        if code in by_code:
            causa, subcausa = by_code[code]
            done(causa, subcausa, "codigo_oficial", "codigo_oficial")
        elif factor == "FA_CLIEN":
            done("cliente_bt", "factor_cliente", "factor_cliente", "factor")
        elif factor == "FA_TERCE":
            done("terceros", "factor_terceros", "factor_terceros", "factor")
        elif factor == "FA_FUMAY":
            done("meteorologia", "factor_fuerza_mayor", "factor_fuerza_mayor", "factor")
        elif desc:
            done("sin_causa", "solo_actuacion_proteccion" if PROTECCION.search(desc) else "solo_nombre_instalacion",
                 "sin_causa", "ninguna")
        else:
            done("sin_causa", "sin_texto", "sin_texto", "ninguna")

    # Before 2016 the codes were filled by default (35 unknown, 34 internal
    # on almost every trip): an incident whose only cause is one of those
    # codes is "not stated".
    if out["regla"] == "codigo_oficial" and code in (34, 35) and fecha[:4] and fecha[:4] < "2016":
        done("sin_causa", "solo_actuacion_proteccion" if PROTECCION.search(desc) else "solo_nombre_instalacion",
             "sin_causa_codigo_por_defecto", "ninguna")

    out["origen"] = origin(out, full, text, code, factor, low_voltage)
    return out


def origin(out: dict, full: str, text: str, code, factor, low_voltage: bool) -> str:
    causa = out["causa"]

    if causa == "apagon":
        return "sistema"
    if causa == "aguas_arriba":
        return "aguas_arriba"
    if causa == "cliente_bt":
        return "cliente_bt"
    if code == 29 or ORIGEN_PARTICULAR.search(full):
        return "instalacion_particular"
    if causa == "red_bt" or low_voltage:
        return "red_bt"
    if causa == "programada":
        return "programada"
    if ORIGEN_SUBESTACION.search(text) or out["componente"] in ("transformador_potencia_str", "aparamenta_subestacion"):
        return "subestacion"
    if ORIGEN_AT.search(full):
        return "linea_at"
    if out["componente"] in ("transformador_ct", "celda_ct") or ORIGEN_CT.search(text):
        return "ct"
    return "linea_mt"

