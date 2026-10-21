# Licencias de lo que se instalaría en una distribuidora

Inventario de los componentes que irían en la máquina virtual de plantilla del apartado 6 de [operacion-diaria.md](operacion-diaria.md): las imágenes de `compose.yaml`, lo que añaden los `Dockerfile` de `infra/`, las librerías de Python de las imágenes y lo que sirve el cuadro de mando. Las versiones son las fijadas en el repositorio a fecha 21.10.2026. Lo que solo usan los experimentos (`experimentos/`) o el desarrollo en local no se instala en el cliente y no está aquí, salvo en la última tabla.

La columna "Obligación" resume lo que nos toca si entregamos la imagen a un cliente. No es una opinión jurídica: antes del primer contrato la revisaríamos con un abogado.

## Imágenes de `compose.yaml`

| Componente | Versión | Licencia | Obligación |
| --- | --- | --- | --- |
| MinIO, fork de Pigsty (`pgsty/minio`) | RELEASE.2026-08-04T00-00-00Z | AGPL-3.0 | Si se modifica y se ofrece por red, publicar el código modificado. Lo usamos sin modificar y solo dentro de la máquina del cliente |
| SQL Server (`mcr.microsoft.com/mssql/server`, `MSSQL_PID` Developer) | 2022-latest | Microsoft, edición Developer: solo desarrollo y pruebas | No vale para producción. En el producto la base es la réplica de Calser y TedisNet de la distribuidora, con su licencia; el contenedor del Compose solo restaura las copias en desarrollo y no debería ir al cliente con la edición Developer |
| PostgreSQL (catálogo de Hive) | 17 | PostgreSQL License | Conservar el aviso de copyright |
| Apache Spark, con Delta Lake 4.4.0 | 4.2.0 | Apache-2.0 | Conservar la licencia y el fichero NOTICE |
| Apache Hive Metastore | 4.1.0 | Apache-2.0 | Igual |
| Apache Airflow | 3.3.2 | Apache-2.0 | Igual |
| Trino | 483 | Apache-2.0 | Igual |
| Kafka y Kafka Connect de Debezium (`quay.io/debezium/kafka` y `connect`) | 3.6 | Apache-2.0 | Igual. Solo con el perfil `cdc` |
| Kafka UI de Kafbat | v1.5.0 | Apache-2.0 | Igual. Solo con el perfil `cdc` |
| nginx (cuadro de mando) | 1.27-alpine | BSD-2-Clause | Conservar el aviso de copyright |

Las imágenes llevan además su sistema base (Debian, Ubuntu o Alpine) con paquetes bajo sus propias licencias, algunas GPL (BusyBox en Alpine, la JVM de OpenJDK con la excepción de Classpath). Entregar la imagen es redistribuir esos paquetes: hay que poder dar su código fuente, lo que con imágenes públicas se cubre señalando la imagen de origen y su versión.

## Lo que añaden los `Dockerfile` de `infra/`

| Componente | Dónde | Licencia |
| --- | --- | --- |
| hadoop-aws 3.5.0 y 3.4.1, AWS SDK for Java (bundle 2.35.4 y 2.24.6), Analytics Accelerator for S3 1.3.1 | `infra/spark/Dockerfile` y `infra/hive/Dockerfile` | Apache-2.0 |
| Controlador JDBC de PostgreSQL 42.7.13 | `infra/hive/Dockerfile` | BSD-2-Clause |
| Controlador JDBC de SQL Server 13.4.0 | `spark.jars.packages` de `infra/spark/conf/spark-defaults.conf` | MIT |
| Conector de Kafka para Spark 4.2.0 | `spark.jars.packages` | Apache-2.0 |
| OpenJDK 17 (imagen de Airflow) | `infra/airflow/Dockerfile` | GPL-2.0 con la excepción de Classpath |

## Librerías de Python de las imágenes

De `infra/spark/requirements.txt` y del `pip install` de `infra/airflow/Dockerfile`:

| Librería | Versión | Licencia |
| --- | --- | --- |
| pyspark y delta-spark | 4.2.0 y 4.4.0 | Apache-2.0 |
| apache-airflow-providers-apache-spark | 6.3.2 | Apache-2.0 |
| pandas | 2.3.3 | BSD-3-Clause |
| pyarrow | 21.0.0 | Apache-2.0 |
| scikit-learn | 1.7.2 | BSD-3-Clause |
| xgboost-cpu | 3.0.5 | Apache-2.0 |
| s3fs | 2026.7.0 | BSD-3-Clause |
| openpyxl | 3.1.5 | MIT |
| h5py | 3.14.0 | BSD-3-Clause |
| boto3 | 1.43.75 | Apache-2.0 |

Ninguna de estas es de licencia recíproca. Las dependencias que arrastran no las hemos revisado una a una, y es lo siguiente que hay que hacer antes del primer piloto.

## Lo que sirve el cuadro de mando

| Componente | Versión | Licencia | Obligación |
| --- | --- | --- | --- |
| GSAP y ScrollTrigger (`dashboard/vendor/gsap`, también en `web/`) | 3.13.0 | Licencia estándar de GSAP, sin cargo | Se puede usar en proyectos comerciales; no permite usarlo en una herramienta visual de animación que compita con Webflow. Hay que volver a leer los términos con cada versión |
| Leaflet (`dashboard/vendor/leaflet`) | 1.9.4 | BSD-2-Clause | Conservar el aviso de copyright |
| IBM Plex Sans (`dashboard/vendor/fonts`) | - | SIL Open Font License 1.1 | Conservar `LICENSE-IBM-Plex-Sans.txt` junto a las fuentes |

Los datos de las fuentes públicas y su atribución están en la [dataset card](dataset_card.md) y al pie de las páginas del cuadro de mando.

## Solo en desarrollo

pytest (MIT), ruff (MIT) y pre-commit (MIT), de `requirements.txt` de la raíz. No van en las imágenes.

## La cadena de suministro

El punto débil del inventario es MinIO. MinIO dejó de publicar binarios de la edición comunitaria en octubre de 2025 y archivó su repositorio el 12.02.2026, y desde entonces usamos el fork de Pigsty, que mantiene una sola persona (nota sobre la imagen de MinIO del [README](../README.md)). Si ese fork se para, la salida es otro almacenamiento compatible con S3, sin tocar los trabajos, porque todos leen por `s3a://`. Para un cliente sujeto a NIS2 esto es un riesgo de cadena de suministro que tiene que estar en su análisis de proveedores, igual que la versión fija de cada imagen.
