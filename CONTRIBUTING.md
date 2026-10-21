# Cómo contribuimos: Git y GitHub Flow

El repositorio lo empezamos entre dos personas (Josep y Anton), desde máquinas distintas y con un stack que tarda más de una hora en arrancar, y desde octubre de 2026 lo lleva Anton solo. La forma de trabajar tiene que evitar dos cosas, sea una persona o sean varias: que los cambios de una rama pisen los de otra y que se rompa `main`, que es lo que se clona para levantar el entorno. Seguimos [GitHub Flow](https://docs.github.com/en/get-started/using-github/github-flow), con estas reglas concretas.

## 1. `main` siempre funciona

Lo que hay en `main` tiene que poder levantarse con `docker compose up -d --build --wait` y pasar los tests. Nadie hace commit directamente en `main`: todo entra por pull request. Al principio del repositorio, del 11.09 al 20.09.2026, los dos hicimos commits directos en `main`; desde el primer pull request (25.09.2026), ninguno.

## 2. Una rama por cambio

Cada tarea (una capa nueva, una corrección, documentación) sale de `main` en su propia rama. Nombres cortos y descriptivos, con un prefijo que diga de qué tipo es:

```text
feat/silver-layer            nueva funcionalidad
feat2609/improve-mlops       nueva funcionalidad, con la fecha ddmm en la que se abrió
fix/minio-image              corrección
fix1410/dashboard-data-gitignore  corrección, con la fecha ddmm en la que se abrió
docs/dataset-card            solo documentación
exp/guiones-estudio-2010     experimentos y sus salidas (experimentos/), con la fecha ddmm al final
pagina/franja-linea-2010     lo que se publica en gridpredic.com (web/); el commit y el PR empiezan por "PÁGINA PÚBLICA:"
rel1710/version-0.15.0       cierre de una versión (CHANGELOG y número de versión)
```

Si la tarea tiene issue, el número va en el nombre (`feat/12-gold-labels`) y el PR lo cierra con `Closes #12`.

```bash
git checkout main
git pull
git checkout -b feat/12-gold-labels
```

## 3. Commits pequeños y con mensaje útil

- Un commit por cambio relacionado. No mezclar una corrección de Compose con un job nuevo.
- Asunto en imperativo, sin punto final y de menos de 50 caracteres; si hace falta explicar el porqué, una línea en blanco y un cuerpo de hasta 72 caracteres por línea. Ejemplo real del historial:

```text
anade la capa Gold y el dataset de entrenamiento
```

- Probar antes de hacer commit: `make lint` y `make test` (o `ruff check .` y `python -m pytest tests -q`).
- `git push` al menos una vez al día, aunque el trabajo no esté acabado. La rama remota es la copia de seguridad y deja ver por dónde va el cambio.

## 4. Pull request

Cuando el cambio está listo (o antes, como borrador, si se quiere opinión sobre el enfoque), se abre un PR contra `main` con la plantilla de `.github/PULL_REQUEST_TEMPLATE.md`: qué cambia, por qué, cómo se ha probado y qué hay que hacer después de mezclarlo (por ejemplo, reconstruir una imagen o volver a ejecutar un DAG). `CODEOWNERS` dice quién responde del repositorio: GitHub pide la revisión a los que figuran ahí salvo al autor del PR, así que mientras solo está Anton no pide ninguna.

## 5. Revisión

Mientras fuimos dos, `CODEOWNERS` pedía en cada PR la revisión del otro miembro, pero en GitHub no quedó ninguna registrada: la condición que aplicamos siempre para mezclar fue la integración continua. Con una sola persona, antes de mezclar se exigen dos comprobaciones:

- La integración continua (`.github/workflows/ci.yml`) ejecuta el linter, valida `compose.yaml`, corre con Python 3.10 los tests de la Landing y Bronze, de Silver, de Gold y de los modelos en un Spark local, entre ellos los que comprueban que cada job de Silver y de Gold espera en `depends_on` a los que escriben lo que lee (`test_dependencias_dag.py`), y con Python 3.11 los del plan financiero. Un PR con la CI en rojo no se mezcla.
- La prueba contra el stack real del paso 6, cuando el cambio lo toca, para lo que la CI no cubre: ningún test carga los DAG en Airflow, ni ejecuta los cuadernos o el cuadro de mando, ni toca el stack real.

Cuando el equipo vuelva a ser de más de una persona, la revisión de otro miembro vuelve a ser condición para mezclar: se añade a cada miembro en `CODEOWNERS` y se activa en GitHub la protección de `main` que exige una aprobación. El revisor prueba lo que pueda y deja los comentarios en el propio PR; las respuestas van como commits nuevos en la misma rama y el PR se actualiza solo.

## 6. Desplegar antes de mezclar

Nuestro "despliegue" es levantar el stack con la rama y ejecutar lo que toque: un DAG, un job de streaming o `_runlogs/run_silver_test.ps1` contra los datos reales. Si algo falla, se arregla en la rama; `main` no se ha tocado.

## 7. Mezclar y borrar la rama

Con la CI en verde, la prueba del paso 6 cuando hace falta y, si hay equipo, la revisión aprobada, el autor mezcla desde GitHub (merge commit, para conservar el historial de la rama) y borra la rama. El PR y sus comentarios se quedan como registro de por qué se hizo el cambio.

```bash
git checkout main
git pull
git branch -d feat/12-gold-labels
```

## Lo que no hacemos

- No reescribimos historial publicado (`push --force` sobre ramas compartidas o sobre `main`).
- No versionamos ficheros que se pueden regenerar ni que pesan: backups `.bak`, logs, cachés de Ivy, volúmenes de Docker. Está en `.gitignore`.
- No versionamos credenciales. Van en `.env`; `.env.example` documenta qué variables hacen falta.

## Versionado de datos y de modelos

Git versiona el código, la configuración y el fichero de referencia de municipios. Los datos se versionan de otra manera:

- El dato crudo es el backup fechado de SQL Server. Cambiar de backup es cambiar de versión.
- Bronze, Silver y Gold son tablas Delta con transaction log y time travel de una semana: `dag_mantenimiento_delta` hace cada domingo un VACUUM con 168 horas de retención. Cada ejecución de los DAG de Silver y de Gold deja su `run_id` en `dq_metrics`.
- El dataset de entrenamiento se congela por versiones en Gold (`l3_gold.dataset_versions`) y cada entrenamiento guarda en `l3_gold.ml_runs` la versión que usó. El registro en MLflow sigue pendiente.
- Valoramos DVC y no lo adoptamos: los datos viven en tablas del lakehouse y Delta ya cubre lo que DVC nos daría.

## Herramientas

```bash
pip install -r requirements.txt   # pyspark, delta-spark, librerías del modelo, pytest, ruff, pre-commit
pre-commit install                # ruff, YAML, JSON y claves privadas antes de cada commit (opcional)
make lint                         # ruff check .
make test                         # python -m pytest tests -q (de Bronze a los modelos)
```
