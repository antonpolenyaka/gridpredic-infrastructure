# Estructura del repositorio y relación con Cookiecutter Data Science

Este documento explica dónde está cada cosa en `gridpredic-infrastructure`, por qué la estructura es la que es y cómo se corresponde con la plantilla [Cookiecutter Data Science v2](https://cookiecutter-data-science.drivendata.org/) (CCDS) que se propuso en el seminario de MLOps del máster.

## 1. De dónde venimos

En junio de 2026 creamos el repositorio [GridPredict_TFM_UPC](https://github.com/antonpolenyaka/GridPredict_TFM_UPC) con la plantilla CCDS tal cual (`ccds`), con la primera versión de la dataset card y la model card y un primer ciclo de GitHub Flow. En aquel momento el proyecto se planteaba como un paquete Python clásico de ciencia de datos: `data/raw` con ficheros, un módulo con `dataset.py`, `features.py` y `modeling/`, y un `Makefile`.

Durante el verano el proyecto cambió de forma. Los datos reales son cuatro bases de datos de SQL Server de 61 GB en backups (una de ellas de 220 GB restaurada), y para trabajar con ellos hace falta una plataforma: SQL Server, Kafka con Debezium, Spark con Delta Lake, MinIO, Hive Metastore, Trino y Airflow, todo en Docker Compose. El código ya no es un paquete que se importa, sino jobs de Spark que se lanzan con `spark-submit` desde DAGs de Airflow. Por eso el trabajo se movió a este repositorio, que Josep creó el 11.09.2026, y la plantilla CCDS se adaptó en lugar de copiarse. La idea de la plantilla se mantiene: cualquiera que llegue sabe dónde buscar los datos, el código, los tests y la documentación, y el pipeline se puede reproducir desde los datos crudos con lo que hay en el repositorio.

## 2. Estructura actual

```text
gridpredic-infrastructure/
├── compose.yaml                 # Servicios, dependencias, healthchecks y volúmenes
├── .env.example                 # Plantilla de credenciales (el .env real no se versiona)
├── requirements.txt             # Entorno local para los tests y las herramientas de desarrollo
├── pyproject.toml               # Metadatos del proyecto y configuración de ruff y pytest
├── Makefile                     # Atajos: make up, make test, make lint, make silver...
├── README.md                    # Guía de instalación y operación
├── CONTRIBUTING.md              # Cómo trabajamos con Git y GitHub Flow
├── CHANGELOG.md                 # Historial de versiones
├── CITATION.cff                 # Cómo citar el repositorio
├── .github/                     # Plantillas de PR e issues, CODEOWNERS y CI
├── infra/                       # Un directorio por servicio: Dockerfile, conf, healthcheck, post_start
│   ├── airflow/  hive/  kafka/  kafka-connect/  minio/  postgres/  spark/  trino/
│   └── sqlserver/
│       ├── backups/             # Los .bak (ignorados por Git)
│       └── scripts/             # restore_databases.sql, enable_cdc.sql
├── etl/                         # El pipeline, agrupado por capa de destino
│   ├── dags/                    # Orquestación con Airflow (01_bronze, 02_silver, 03_gold)
│   ├── config/                  # Parámetros de los procesos en JSON (qué tablas, qué orden, qué recursos, reglas de Gold)
│   └── jobs/                    # Transformaciones con Spark (00_landing ... 03_gold) y entrenamiento (04_ml)
├── data/
│   └── reference_data/          # Datos externos pequeños que sí van en Git (municipios.xlsx)
├── docs/                        # Documentación: cards, capas Silver y Gold, flujo de datos, este documento
├── notebooks/                   # Exploración (convención de nombres en su README)
├── tests/                       # Pruebas locales de los jobs, sin Docker
└── _runlogs/                    # Scripts de pruebas manuales; los logs que generan se ignoran
```

Dentro de `dags/`, `config/` y `jobs/` los ficheros se agrupan por capa (`00_landing`, `01_bronze`, `02_silver` y `03_gold`) y el nombre lleva el tipo, la capa y el origen o la entidad: `dag_bronze_sqlserver.py`, `config_silver.json`, `job_silver_f_interrupcion.py`. El nombre del job coincide con el de la tabla que escribe.

## 3. Correspondencia con Cookiecutter Data Science

| CCDS v2 | En este repositorio | Motivo de la adaptación |
| --- | --- | --- |
| `data/raw` | `infra/sqlserver/backups/*.bak` (fuera de Git) y la zona Bronze del lakehouse (`l1_bronze`) | Los datos crudos son 61 GB de backups fechados. El dato inmutable es el backup; Bronze es su copia fiel en Delta. Ninguno cabe ni debe ir en Git |
| `data/interim` | Zona Silver (`l2_silver`) en MinIO | Los intermedios son tablas Delta, no ficheros del repositorio |
| `data/processed` | Zona Gold (`l3_gold`) | El dataset final de entrenamiento se congela por versiones en Gold (`dataset_train` y `dataset_versions`) |
| `data/external` | `data/reference_data/` | Único dato que sí se versiona: el fichero de municipios del INE (28 KB) |
| `<modulo>/dataset.py`, `features.py`, `modeling/` | `etl/jobs/00_landing`, `01_bronze`, `02_silver`, `03_gold` y `04_ml` | El código se ejecuta con `spark-submit` desde Airflow, así que se organiza por capa del pipeline en lugar de como paquete importable. `silver_common.py` y `gold_common.py` hacen de librería compartida de su capa; en `04_ml`, `ml_models.py` y `ml_metrics.py` no dependen de Spark y se prueban solos |
| `Makefile` | `Makefile` (y `_runlogs/run_silver_test.ps1` para Windows) | Mismos atajos: levantar el stack, lanzar tests, lint |
| `requirements.txt` | `requirements.txt` en la raíz para el entorno local; `infra/spark/requirements.txt` y los `Dockerfile` para el runtime | Las versiones de Spark, Delta, Airflow, Hive y Trino están fijadas en las imágenes. El `requirements.txt` de la raíz reproduce el entorno de pruebas sin Docker |
| `pyproject.toml` | `pyproject.toml` | Configuración de ruff y pytest y metadatos del proyecto |
| `docs/` | `docs/` | Dataset card y model card en formato Hugging Face, diseño de las capas Silver y Gold, diagrama de flujo, este documento |
| `notebooks/` | `notebooks/` | Convención de nombres de CCDS (`número-iniciales-tema`). Lo que sirva se refactoriza a `etl/jobs` |
| `models/` | Registro de modelos en MLflow y bucket de MinIO (hito M7) | Los artefactos de modelo no van en Git; se versionan junto al dataset congelado |
| `references/` | Documentación de SITEL sobre Calser y TedisNet, fuera del repositorio | Son manuales confidenciales del fabricante. Lo que necesita el pipeline está resumido en `docs/silver-layer.md` |
| `reports/` | Memoria del TFM, fuera del repositorio | El informe se entrega en el campus; las figuras se generan con scripts en el mismo repositorio de la memoria |
| `.env`, `.gitignore` | `.env.example`, `.gitignore` | Credenciales fuera de Git; backups, logs y cachés también |
| `tests/` | `tests/02_silver/test_silver_smoke.py`, `tests/03_gold/test_gold_smoke.py`, `tests/04_ml/` | Tests de extremo a extremo de Silver y de Gold en un Spark local, con los problemas reales de los datos sembrados a propósito y una prueba de leakage de las features; tests de las métricas y los modelos y del job de entrenamiento sobre un dataset sintético |
| (no existe en CCDS) | `compose.yaml`, `infra/` | La plataforma se levanta con Docker Compose; cada servicio tiene su carpeta con su Dockerfile y sus scripts |
| (no existe en CCDS) | `etl/dags/`, `etl/config/` | Separar orquestación (Airflow), parámetros (JSON) y transformación (Spark) permite añadir una tabla o cambiar el orden sin tocar código |

## 4. Cómo se reproduce el pipeline

La regla de CCDS que más nos importa es que cualquiera pueda reproducir el resultado final con el código del repositorio y los datos crudos. En nuestro caso:

1. Los datos crudos son los cuatro backups fechados (14.08.2026), que SITEL entrega fuera de Git. Se copian en `infra/sqlserver/backups/` con los nombres que espera `restore_databases.sql`.
2. `cp .env.example .env` y rellenar las credenciales.
3. `docker compose up -d --build --wait` construye las imágenes, restaura las bases, activa CDC, crea los buckets y los esquemas y deja el stack listo. Todas las versiones (Spark 4.2.0, Delta 4.4.0, Airflow 3.3.2, Hive 4.1.0, Trino 483, Debezium 3.6, imagen de MinIO con release fijo) están fijadas en `compose.yaml` y en los `Dockerfile`.
4. En Airflow: `ingest_sqlserver_batch_bronze`, después los dos jobs de streaming (Landing y Bronze) y `dag_bronze_reference_municipios`, luego `dag_silver` y por último `dag_gold`. Qué tablas se cargan, en qué orden y con qué reglas está en `etl/config/`.
5. El resultado se consulta desde Trino (`lakehouse.l2_silver`, `lakehouse.l3_gold`) y la calidad de cada ejecución en `dq_metrics` de cada capa, con el `run_id` de Airflow. Cada dataset de entrenamiento queda registrado en `l3_gold.dataset_versions` con sus parámetros y las versiones Delta de sus tablas de entrada.

Sin Docker, las reglas de Silver y de Gold se pueden reproducir en cualquier máquina con `pip install -r requirements.txt` y `python -m pytest tests -q`. Son los mismos tests que ejecuta la integración continua en cada pull request.

## 5. Lo que queda por añadir

- Registro de experimentos y modelos con MLflow, y un servicio más en `compose.yaml` para ello. Mientras tanto, `l3_gold.ml_runs` guarda cada ejecución con su versión de dataset.
- Ingesta de meteorología (API externa) en Landing.
- Notebooks de exploración de Gold y del modelo, siguiendo la convención de `notebooks/README.md`.
