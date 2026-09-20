# GridPredic Infra

Entorno local de desarrollo para ejecutar la plataforma de datos de GridPredic con Docker Compose.

## Estructura del repositorio

```text
├── compose.yaml      # Servicios, dependencias y montajes
├── infra/            # Dockerfiles, configuración y scripts por componente
├── etl/
│   ├── dags/         # Orquestación con Airflow
│   ├── config/       # Parámetros de los procesos
│   └── jobs/         # Ingesta y transformación con Spark
└── docs/             # Documentación y arquitectura
```

Dentro de `dags/`, `config/` y `jobs/`, los archivos se agrupan por **capa de destino**: `00_landing/`, `01_bronze/` y `02_silver/`. Solo se crean las carpetas que contienen archivos.

Los nombres identifican el **tipo**, la **capa** y el **origen** (Landing/Bronze) o la **entidad** (Silver/Gold). Solo los jobs de Landing y Bronze añaden `batch` o `streaming`:

```text
dag_bronze_sqlserver.py
config_bronze_sqlserver.json
job_bronze_sqlserver_batch.py
job_silver_f_salidas.py
```

## Flujo de datos

![Flujo de datos de GridPredic](docs/data-flow.jpeg)

> El diagrama representa la arquitectura objetivo. Actualmente este repositorio implementa la ingesta de SQL Server en batch y streaming hasta Bronze, además de un ejemplo de transformación Silver.

## Stack tecnológico

| Área | Tecnología | Función |
| --- | --- | --- |
| Fuentes | **SQL Server 2022** | Bases de datos operacionales y fuentes batch |
| CDC y mensajería | **Debezium, Kafka, Kafka Connect** | Captura y transporte de cambios en tiempo real |
| Orquestación | **Apache Airflow** | Orquestación de las cargas batch |
| Procesamiento | **Apache Spark 4 + Delta Lake** | Ingesta streaming/batch y transformaciones Bronze → Silver → Gold |
| Almacenamiento | **MinIO** | Object Storage compatible con S3 para Landing y Delta Lake |
| Catálogo | **Hive Metastore + PostgreSQL** | Catálogo compartido de las tablas Delta |
| Consulta | **Trino** | Motor SQL sobre el lakehouse |
| Operación | **Kafka UI + Spark History Server** | Inspección de Kafka y ejecuciones Spark |

> **Nota sobre la imagen de MinIO.** MinIO dejó de publicar sus imágenes community en Docker Hub en octubre de 2025, por lo que `minio/minio` ya no puede descargarse de allí. El síntoma al levantar el stack es un error engañoso, porque habla de permisos cuando en realidad el repositorio no existe:
>
> ```text
> Error response from daemon: pull access denied for minio/minio,
> repository does not exist or may require 'docker login'
> ```
>
> El `compose.yaml` apunta por ese motivo a Quay, donde las imágenes siguen publicadas, y fija un release concreto en lugar de `latest` para que el entorno sea reproducible:
>
> ```yaml
> image: quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z
> ```
>
> La imagen es equivalente a la de Docker Hub: mismo entrypoint, mismas variables `MINIO_ROOT_USER` y `MINIO_ROOT_PASSWORD`, y sigue incluyendo el cliente `mc` del que dependen los scripts de `infra/minio/`.

---

## 1. Requisitos mínimos del ordenador

Para ejecutar todo el stack localmente:

- **CPU:** 4 cores.
- **RAM:** 16 GB.
- **Disco libre:** al menos **350 GB**, repartidos según el desglose siguiente.

Se recomienda asignar a Docker Desktop al menos **4 CPU y 12 GB de RAM**.

### 1.1 Desglose del espacio en disco

| Concepto | Dónde se almacena | Tamaño aproximado |
| --- | --- | --- |
| Los cuatro backups `.bak` | Carpeta del repositorio | 61,4 GB |
| `sqlserver-data` (las cuatro bases restauradas) | Disco virtual de Docker | 200 - 250 GB |
| `minio-data` (Landing y Delta Lake, crece con el uso) | Disco virtual de Docker | 50 GB en adelante |
| Imágenes y caché de construcción (Spark, Hive, Airflow, Trino, Kafka) | Disco virtual de Docker | 25 - 35 GB |
| `kafka-data`, `airflow-data`, `hive-db` | Disco virtual de Docker | 5 - 10 GB |

La mayor parte del consumo no cae en la carpeta del repositorio, sino en el **disco virtual de Docker**, que por defecto se crea en la unidad del sistema. Si esa unidad va justa de espacio, hay que moverlo antes del primer arranque: ver el apartado 3.

---

## 2. Software necesario

Instalar únicamente:

- **Git**
- **Docker Desktop**
- **Docker Compose >= 2.30.0** (`docker compose`)

Comprobación rápida:

```bash
git --version
docker --version
docker compose version
```

---

## 3. Ubicar los datos de Docker fuera del disco del sistema

**Motivo.** Los volúmenes declarados en `compose.yaml` (`sqlserver-data`, `minio-data`, `kafka-data`, `airflow-data`, `hive-db`) no son carpetas del sistema de archivos del anfitrión: viven dentro del **disco virtual de Docker**, junto con las imágenes y la caché de construcción. Ese disco se crea por defecto en la unidad del sistema, con independencia de dónde esté clonado el repositorio. Con `TedisNet_EOSA` restaurada se superan holgadamente los 250 GB, de modo que si la unidad del sistema no dispone de ese margen el arranque se interrumpe a media restauración y hay que empezar de cero.

**Windows (Docker Desktop).** Antes del primer arranque:

1. Detener el stack, si estuviera levantado:

   ```bash
   docker compose down
   ```

2. Crear la carpeta destino en la unidad con espacio, por ejemplo `D:\DockerData`.
3. Abrir Docker Desktop -> Settings -> Resources -> Advanced -> "Disk image location" y seleccionar esa carpeta.
4. Pulsar Apply & restart. Docker mueve el disco virtual y reinicia su máquina virtual.

Comprobación desde PowerShell:

```powershell
Get-ChildItem D:\DockerData -Recurse -Filter *.vhdx |
    Select-Object FullName, @{n='GB';e={[math]::Round($_.Length/1GB,2)}}
```

Debe aparecer `docker_data.vhdx` en la ruta elegida.

**macOS (Docker Desktop).** Misma opción: Settings -> Resources -> Advanced -> "Disk image location".

**Linux.** Ajustar `data-root` en `/etc/docker/daemon.json` y reiniciar el servicio:

```json
{
  "data-root": "/mnt/datos/docker"
}
```

El cambio es global de Docker, no específico de este proyecto: afecta a todas las imágenes y volúmenes de la máquina.

---

## 4. Clonar el repositorio

```bash
git clone https://github.com/antonpolenyaka/gridpredic-infrastructure.git
cd gridpredic-infrastructure
```

Todos los comandos siguientes se ejecutan desde la raíz del repositorio.

---

## 5. Añadir los backups de SQL Server

Crear, si no existe, la carpeta:

```text
infra/sqlserver/backups/
```

Copiar dentro los cuatro backups con estos nombres exactos:

```text
Calser_DIST_1_14082026.bak
Calser_DIST_2_14082026.bak
Calser_DIST_3_14082026.bak
TedisNet_EOSA_backup_2026_08_14_020001_2902307.bak
```

Durante el arranque se restauran como:

| Backup | Base de datos |
| --- | --- |
| `Calser_DIST_1_14082026.bak` | `Calser_EOSA` |
| `Calser_DIST_2_14082026.bak` | `Calser_Pitarch` |
| `Calser_DIST_3_14082026.bak` | `Calser_ValleSantaAna` |
| `TedisNet_EOSA_backup_2026_08_14_020001_2902307.bak` | `TedisNet_EOSA` |

---

## 6. Crear el archivo `.env`

Duplicar `.env.example` como `.env`:

```bash
cp .env.example .env
```

Y configurar las credenciales locales:

```dotenv
HIVE_METASTORE_DB_PASSWORD=<PASSWORD>
MINIO_ROOT_USER=<USER>
MINIO_ROOT_PASSWORD=<PASSWORD>
MSSQL_SA_PASSWORD=<PASSWORD>
```

`MSSQL_SA_PASSWORD` debe cumplir la política de complejidad de SQL Server.

> **No utilizar el carácter `$` en las contraseñas.** Compose interpola variables también dentro del propio `.env`, de manera que una contraseña como `hM7#kL9$vE2@wQ4x` se resuelve como `hM7#kL9@wQ4x`: el servicio se inicializa con una contraseña distinta de la escrita y a partir de ahí nada cuadra. El síntoma es un aviso al ejecutar cualquier comando de Compose:
>
> ```text
> WARN[0000] The "vE2" variable is not set. Defaulting to a blank string.
> ```
>
> Si se necesita un `$` literal hay que duplicarlo (`$$`).

Antes de arrancar conviene verificar que las contraseñas se resuelven enteras y que no aparece ningún aviso:

```bash
docker compose config
```

---

## 7. Levantar la infraestructura

```bash
docker compose up -d --build --wait
```

Los `healthchecks` y las dependencias del Compose controlan el orden de inicialización. Cuando el comando termina correctamente, el entorno está listo para utilizarse.

El primer arranque es largo. Como referencia, en un portátil con 4 cores y 12 GB asignados a Docker:

| Fase | Duración aproximada |
| --- | --- |
| Descarga de imágenes y construcción de Spark, Hive y Airflow | 10 - 15 min |
| Restauración de las cuatro bases en SQL Server | 60 - 70 min |
| Resolución de dependencias de Spark en `spark-master` | 10 min |

La restauración manda: el backup de `TedisNet_EOSA` son 63,5 GB y se lee desde una ruta del anfitrión montada en la máquina virtual de Docker, a unos 60 MB/s. Por eso el healthcheck de `sqlserver` tiene un `start_period` de 30 minutos.

Los `start_period` de los healthchecks están dimensionados para ese primer arranque. Si se reducen, Compose da por fallidos servicios que en realidad están inicializándose y aborta la cadena de dependencias, aunque los contenedores acaben levantando solos.

La resolución de dependencias de Spark merece una nota aparte. Entre `spark.jars.packages` y `spark.sql.hive.metastore.jars maven` se descargan unos 320 jars de Maven Central. Esa caché se guarda en los volúmenes `ivy-spark-master` e `ivy-airflow`, de forma que solo se paga una vez y sobrevive a la recreación de los contenedores. Son dos volúmenes separados a propósito: Ivy no usa bloqueo de ficheros por defecto y los dos servicios pueden resolver a la vez.

Durante el arranque se realiza automáticamente:

- construcción de las imágenes locales de **Spark**, **Hive** y **Airflow**;
- creación de los buckets `datalake` y `spark-events` en **MinIO**;
- arranque de **PostgreSQL** y **Hive Metastore**;
- creación de los schemas `l1_bronze`, `l2_silver` y `l3_gold`;
- restauración de las cuatro bases de datos en **SQL Server**;
- activación de **CDC** sobre las tablas configuradas de `TedisNet_EOSA`;
- arranque de **Kafka** y registro del conector `tedisnet-eosa-connector` en **Kafka Connect**;
- arranque del clúster **Spark**, **Airflow**, **Trino**, **Kafka UI** y **Spark History Server**.

---

## 8. Puertos y herramientas de acceso

| Servicio | Dirección | Uso |
| --- | --- | --- |
| SQL Server | `localhost:1433` | Acceso a las cuatro bases de datos |
| Spark Master | http://localhost:8080 | Estado del clúster Spark |
| Spark Worker | http://localhost:8081 | Estado del worker Spark |
| Kafka UI | http://localhost:8082 | Topics, mensajes y estado de Kafka Connect |
| Airflow | http://localhost:8084 | Gestión y ejecución de DAGs |
| Trino | http://localhost:8085 | Endpoint/UI del motor SQL |
| MinIO Console | http://localhost:9001 | Navegación por buckets y objetos |
| Spark History Server | http://localhost:18080 | Histórico de aplicaciones Spark |

> **Si algún puerto está ocupado en tu máquina.** El puerto 8080 de la UI de Spark Master es configurable desde el `.env` con `SPARK_MASTER_UI_PORT`, porque en Windows suele estar tomado por IIS u otro servicio sobre `http.sys`. El síntoma al levantar el stack es:
>
> ```text
> Error response from daemon: ports are not available: exposing port TCP
> 0.0.0.0:8080 -> 127.0.0.1:0: listen tcp 0.0.0.0:8080: bind: An attempt was
> made to access a socket in a way forbidden by its access permissions
> ```
>
> Para comprobar quién lo tiene, en PowerShell como administrador:
>
> ```powershell
> Get-NetTCPConnection -LocalPort 8080 | Select-Object State, OwningProcess
> netsh interface ipv4 show excludedportrange protocol=tcp
> ```
>
> Un `OwningProcess` igual a 4 significa que lo retiene el kernel por una reserva de `http.sys`. Lo más rápido es dejarlo estar y asignar otro puerto en el `.env`:
>
> ```dotenv
> SPARK_MASTER_UI_PORT=8090
> ```

### SQL Server — SSMS

Conectar con **SQL Server Management Studio (SSMS)**:

```text
Server: localhost,1433
Authentication: SQL Server Authentication
Login: sa
Password: <valor de MSSQL_SA_PASSWORD>
```

### Trino — DBeaver

Crear una conexión **Trino** en DBeaver:

```text
Host: localhost
Port: 8085
Database/Schema: lakehouse
Username <cualquier valor>
```

No hay autenticación por contraseña configurada para Trino en este entorno local.

### MinIO

Acceder a `http://localhost:9001` con `MINIO_ROOT_USER` y `MINIO_ROOT_PASSWORD`.

### Airflow

Acceder a `http://localhost:8084` con:

```text
User: admin
Password: generada automáticamente por Airflow en el primer arranque
```

Para consultar la contraseña:

```bash
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

---

## 9. Ejecutar los jobs de streaming

Los dos jobs son procesos continuos, por lo que conviene ejecutarlos en **dos terminales diferentes**.

### 9.1 Landing

En la primera terminal:

```bash
docker compose exec spark-master \
  spark-submit \
  /app/jobs/00_landing/job_landing_sqlserver_streaming.py
```

El job consume los eventos CDC de Kafka y los persiste como Parquet en:

```text
s3a://datalake/00_landing/tedisnet-eosa
```

### 9.2 Bronze

**No arrancar Bronze inmediatamente.** Su esquema se obtiene leyendo los ficheros ya existentes en Landing.

Cuando Landing haya escrito al menos los primeros ficheros Parquet —puede comprobarse desde la consola de MinIO—, abrir una segunda terminal y ejecutar:

```bash
docker compose exec spark-master \
  spark-submit \
  /app/jobs/01_bronze/job_bronze_sqlserver_streaming.py
```

Este job procesa los eventos CDC de Landing y mantiene las tablas Delta correspondientes en `l1_bronze`.

---

## 10. Ejecutar los DAGs batch de Airflow

Abrir:

```text
http://localhost:8084
```

### 10.1 SQL Server

Ejecutar manualmente el DAG:

```text
ingest_sqlserver_batch_bronze
```

El DAG lanza los jobs Spark que extraen las tablas batch configuradas en `etl/config/01_bronze/config_bronze_sqlserver.json` y las cargan como tablas Delta en `l1_bronze`.

### 10.2 Municipios

Ejecutar manualmente el DAG:

```text
dag_bronze_reference_municipios
```

El DAG sube `data/reference_data/municipios.xlsx` a MinIO y carga la hoja `Municipios` con Spark, reemplazando la tabla Delta `l1_bronze.reference_municipios`.

---

## 11. Ejemplo de transformación Silver: `salidas`

Una vez completado correctamente el DAG batch, puede ejecutarse la transformación de ejemplo `job_silver_f_salidas.py`:

```bash
docker compose exec spark-master \
  spark-submit \
  /app/jobs/02_silver/job_silver_f_salidas.py
```

El job integra las tablas `salidas` de las tres bases Calser y genera:

```text
l2_silver.salidas
```

Puede comprobarse desde Trino/DBeaver, por ejemplo:

```sql
SELECT *
FROM lakehouse.l2_silver.salidas
LIMIT 100;
```
