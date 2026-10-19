"""
The groups of causes are written in several places: the list of families
(CAUSAS) of the classification in Silver and of the analysis of causes, the
causes that are not weather (causas_no_meteo) of the day model, of the
hourly model and of config_ml.json, the local causes left out of the risk
by zones, and the weather families of the yearly risk by CT. Nothing checks
that they agree, and a new family would fall in "the rest" by complement
without anybody noticing. The test reads the sources, without Spark, and
does not change any behaviour.

    python -m pytest tests/04_ml/test_grupos_causa.py -q
"""

import ast
import json
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Every family, classified by hand: a new one in CAUSAS fails here until it is placed.
WEATHER_PAIR = {"meteorologia", "vegetacion"}                     # the weather causes of riesgo_zonas
YEARLY_WEATHER = WEATHER_PAIR | {"incendio", "objeto_ajeno", "desconocida"}   # familias_causa.meteo of riesgo_ct
NOT_WEATHER = {"aguas_arriba", "apagon", "terceros", "programada", "maniobra", "cliente_bt", "red_bt"}
LOCAL_EXCLUDED = NOT_WEATHER - {"terceros"}
OTHERS = {"fauna", "equipo", "agente_externo", "sobrecarga", "sin_causa"}


def source(*parts) -> ast.Module:
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as file:
        return ast.parse(file.read())


def constant(tree: ast.Module, name: str):
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return node.value
    raise AssertionError(f"{name} not found")


def key_of(tree: ast.Module, dict_name: str, key: str):
    node = constant(tree, dict_name)
    for k, v in zip(node.keys, node.values):
        if isinstance(k, ast.Constant) and k.value == key:
            return ast.literal_eval(v)
    raise AssertionError(f"{dict_name}[{key}] not found")


def weather_pairs(tree: ast.Module) -> list:
    """The literal lists passed to .isin([...]) that hold "meteorologia" (the weather causes of a function)."""
    out = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == "isin"
                and node.args and isinstance(node.args[0], ast.List)):
            values = ast.literal_eval(node.args[0])
            if "meteorologia" in values:
                out.append(set(values))
    return out


def test_the_groups_of_causes_agree():
    silver = ast.literal_eval(constant(source("etl", "jobs", "02_silver", "causas_incidencia.py"), "CAUSAS"))
    analysis = ast.literal_eval(constant(source("etl", "jobs", "04_ml", "job_causas_incidencias.py"), "CAUSAS"))
    assert silver == analysis
    families = set(silver)
    assert len(families) == len(silver) == 17
    # Every family is classified explicitly, not by complement.
    assert families == YEARLY_WEATHER | NOT_WEATHER | OTHERS
    assert not (YEARLY_WEATHER & NOT_WEATHER) and not (OTHERS & (YEARLY_WEATHER | NOT_WEATHER))

    day = key_of(source("etl", "jobs", "04_ml", "job_ml_riesgo_dia.py"), "DEFAULTS", "causas_no_meteo")
    hourly = key_of(source("etl", "jobs", "04_ml", "job_ml_meteo_historia.py"), "DEFAULTS", "causas_no_meteo")
    with open(os.path.join(ROOT, "etl", "config", "04_ml", "config_ml.json"), encoding="utf-8") as file:
        config = json.load(file)["meteo_historia"]["causas_no_meteo"]
    assert set(day) == set(hourly) == set(config) == NOT_WEATHER

    zones = source("etl", "jobs", "04_ml", "riesgo_zonas.py")
    assert set(ast.literal_eval(constant(zones, "LOCAL_EXCLUDED"))) == LOCAL_EXCLUDED
    pairs = weather_pairs(zones)
    assert len(pairs) == 2 and all(p == WEATHER_PAIR for p in pairs)
    families_ct = key_of(source("etl", "jobs", "04_ml", "job_ml_riesgo_ct.py"), "DEFAULTS", "familias_causa")
    assert set(families_ct["meteo"]) == YEARLY_WEATHER

    # The chain: 2 in 5 in 10 in 11 families.
    weather_like = families - NOT_WEATHER
    local = families - LOCAL_EXCLUDED
    assert WEATHER_PAIR < set(families_ct["meteo"]) < weather_like < local
    assert [len(WEATHER_PAIR), len(families_ct["meteo"]), len(weather_like), len(local)] == [2, 5, 10, 11]
