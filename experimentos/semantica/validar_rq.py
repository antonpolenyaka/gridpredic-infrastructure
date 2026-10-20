"""
Comprueba con el analizador de SPARQL de rdflib que las consultas propuestas son SPARQL 1.1 autocontenido.
No ejecuta nada ni contacta ningún servicio: solo analiza el texto.

Uso: python validar_rq.py -> salida/validar_rq_salida.txt
"""

import os

from rdflib.plugins.sparql import prepareQuery

HERE = os.path.dirname(os.path.abspath(__file__))

for nombre in ["wikidata_municipios_repo.rq", "wikidata_municipios_estandar.rq", "federada_municipios.rq"]:
    texto = open(os.path.join(HERE, nombre), encoding="utf-8").read()
    try:
        q = prepareQuery(texto)
        print(f"{nombre}: analiza bien ({q.algebra.name})")
    except Exception as error:  # noqa: BLE001
        print(f"{nombre}: no analiza: {type(error).__name__}: {str(error)[:160]}")
