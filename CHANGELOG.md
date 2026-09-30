# Historial de cambios

Formato basado en [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/). Las fechas son las de la mezcla en `main`.

## [0.4.0] - 30.09.2026

Entrega del seminario de MLOps (SE4ML) del máster. No cambia el pipeline; documenta y equipa el repositorio.

### Añadido

- `docs/dataset_card.md` y `docs/model_card.md` en formato Hugging Face, con los datos reales de Calser y TedisNet, la definición del target, los sesgos y los criterios de aceptación del modelo.
- `docs/project-structure.md`: correspondencia con Cookiecutter Data Science y cómo reproducir el pipeline.
- `CONTRIBUTING.md`: GitHub Flow tal como lo aplicamos, reglas de commits y versionado de datos.
- `.github/`: plantilla de pull request, plantillas de issues, `CODEOWNERS` y el workflow de integración continua (ruff, validación de `compose.yaml` y test de Silver en Spark local).
- `requirements.txt`, `pyproject.toml`, `Makefile`, `.pre-commit-config.yaml` y `CITATION.cff`.
- `notebooks/README.md` con la convención de nombres.
- Este `CHANGELOG.md`.

### Cambiado

- `README.md`: estructura actualizada, apartado de documentación y apartado de desarrollo y contribución.
- `.gitignore`: cachés de Python, de pytest y de ruff, y salidas CSV de las pruebas manuales.

## [0.3.1] - 30.09.2026

### Cambiado

- Imagen de MinIO: de `quay.io/minio/minio` a `pgsty/minio` con release fijo, porque Quay pasó a pedir autenticación (PR #2).

### Añadido

- `_runlogs/run_silver_test.ps1`: prueba de Silver contra los datos reales desde PowerShell.

## [0.3.0] - 25.09.2026

### Añadido

- Capa Silver completa para Calser y TedisNet (PR #1): `silver_common.py`, dimensiones `d_*`, hechos `f_*`, tablas `_rejected` con motivo, `dq_metrics` y `job_silver_dq_checks.py` (integridad, cobertura de la clave Calser - TedisNet, alineación horaria). DAG `dag_silver` con dependencias leídas de `config_silver.json`.
- `docs/silver-layer.md` con las reglas de cada tabla y su motivo.
- `tests/02_silver/test_silver_smoke.py`: prueba de extremo a extremo en Spark local, sin Docker.

## [0.2.0] - 20.09.2026

### Añadido

- Ingesta del fichero de referencia de municipios (`dag_bronze_reference_municipios`).
- `parametros_configuracion` de Calser en Bronze y dimensión `d_elemento`.
- Documentación de requisitos de la máquina, ubicación del disco de Docker y puertos ocupados.

### Cambiado

- Spark 4.2.0 y Delta Lake 4.4.0, con las dependencias alineadas.
- Límite de memoria de SQL Server a 2 GB; sobreescritura de esquema permitida en el Hive Metastore.
- Healthchecks y `start_period` ajustados al primer arranque; puerto de la UI de Spark configurable por `.env`.

## [0.1.0] - 14.09.2026

### Añadido

- Stack de Docker Compose: SQL Server 2022 con restauración automática de backups y CDC, Kafka y Kafka Connect con Debezium, Spark master, worker e history server, Hive Metastore sobre PostgreSQL, MinIO, Trino, Airflow y Kafka UI.
- Jobs de Landing y Bronze en streaming (CDC) y Bronze en batch (JDBC), con el DAG `ingest_sqlserver_batch_bronze` y la configuración de tablas en JSON.
- README, `.env.example`, `.gitignore` y convención de nombres por capa.
