# Cómo contribuimos: Git y GitHub Flow

Somos dos personas (Josep y Anton) trabajando en el mismo repositorio desde máquinas distintas, con un stack que tarda más de una hora en arrancar. La forma de trabajar tiene que evitar dos cosas: pisarnos los cambios y romper `main`, que es lo que cualquiera de los dos clona para levantar el entorno. Seguimos [GitHub Flow](https://docs.github.com/en/get-started/using-github/github-flow), con estas reglas concretas.

## 1. `main` siempre funciona

Lo que hay en `main` tiene que poder levantarse con `docker compose up -d --build --wait` y pasar los tests. Nadie hace commit directamente en `main`: todo entra por pull request. Al principio del repositorio (septiembre de 2026) sí hubo commits directos mientras solo trabajaba una persona; desde que somos dos, no.

## 2. Una rama por cambio

Cada tarea (una capa nueva, una corrección, documentación) sale de `main` en su propia rama. Nombres cortos y descriptivos, con un prefijo que diga de qué tipo es:

```text
feat/silver-layer            nueva funcionalidad
feat2609/improve-mlops       nueva funcionalidad, con la fecha ddmm en la que se abrió
fix/minio-image              corrección
docs/dataset-card            solo documentación
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
cambia la imagen de MinIO a pgsty/minio porque Quay ahora pide autenticacion
```

- Probar antes de hacer commit: `make lint` y `make test` (o `ruff check .` y `python -m pytest tests -q`).
- `git push` al menos una vez al día, aunque el trabajo no esté acabado. La rama remota es la copia de seguridad y permite que el otro vea por dónde va el cambio.

## 4. Pull request

Cuando el cambio está listo (o antes, como borrador, si se quiere opinión sobre el enfoque), se abre un PR contra `main` con la plantilla de `.github/PULL_REQUEST_TEMPLATE.md`: qué cambia, por qué, cómo se ha probado y qué hay que hacer después de mezclarlo (por ejemplo, reconstruir una imagen o volver a ejecutar un DAG). `CODEOWNERS` pide la revisión al otro miembro automáticamente.

## 5. Revisión

El otro miembro revisa el código, prueba lo que pueda y deja comentarios en el propio PR. Las respuestas van como commits nuevos en la misma rama; el PR se actualiza solo. Mientras tanto, la integración continua (`.github/workflows/ci.yml`) ejecuta el linter, valida `compose.yaml` y corre los tests de Silver y de Gold en un Spark local. Un PR con la CI en rojo no se mezcla.

## 6. Desplegar antes de mezclar

Nuestro "despliegue" es levantar el stack con la rama y ejecutar lo que toque: un DAG, un job de streaming o `_runlogs/run_silver_test.ps1` contra los datos reales. Si algo falla, se arregla en la rama; `main` no se ha tocado.

## 7. Mezclar y borrar la rama

Con la revisión aprobada y la CI en verde, se mezcla desde GitHub (merge commit, para conservar el historial de la rama) y se borra la rama. El PR y sus comentarios se quedan como registro de por qué se hizo el cambio.

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
- Bronze, Silver y Gold son tablas Delta con transaction log y time travel. Cada ejecución del DAG de Silver deja su `run_id` en `dq_metrics`.
- El dataset de entrenamiento se congelará por versiones en Gold y se registrará con el modelo en MLflow (hito M7).
- Valoramos DVC y no lo adoptamos: los datos viven en el lakehouse, no en ficheros, y Delta ya cubre lo que DVC nos daría.

## Herramientas

```bash
pip install -r requirements.txt   # pyspark, delta-spark, pytest, ruff, pre-commit
pre-commit install                # ruff antes de cada commit (opcional)
make lint                         # ruff check .
make test                         # python -m pytest tests -q (Silver y Gold)
```
