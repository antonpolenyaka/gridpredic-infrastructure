# Prueba de la capa Silver con datos reales, sin la serie grande
# (HistoricTagIntervalValuesBig). Ejecutar desde la raiz del repo, con el
# stack ya levantado:  .\_runlogs\run_silver_test.ps1
# Todo queda en _runlogs\*.log

$ErrorActionPreference = "Continue"
$L = "_runlogs"
$RunId = "test_silver_" + (Get-Date -Format "yyyyMMdd_HHmm")
$Skip = @("HistoricTagIntervalValuesBig")

function Log($msg) {
    $line = (Get-Date -Format "HH:mm:ss") + " " + $msg
    $line | Tee-Object -FilePath "$L\progress.log" -Append
}

Log "=== Inicio prueba Silver, run_id=$RunId ==="

# ---------------------------------------------------------------- 1. Bronze batch
$config = Get-Content etl\config\01_bronze\config_bronze_sqlserver.json -Raw | ConvertFrom-Json
foreach ($src in $config.sources) {
    foreach ($db in $src.databases) {
        foreach ($t in $src.tables) {
            if ($Skip -contains $t.name) { Log "SKIP bronze $db.$($t.name)"; continue }
            docker compose exec -T airflow spark-submit `
                --conf spark.cores.max=2 --conf spark.executor.memory=2g `
                /app/jobs/01_bronze/job_bronze_sqlserver_batch.py `
                --database $db --table $t.name `
                --extract-mode $t.extract_mode --write-mode $t.write_mode `
                *>> "$L\bronze_batch.log"
            Log "bronze $db.$($t.name) exit=$LASTEXITCODE"
        }
    }
}

# ---------------------------------------------------------------- 2. Streaming CDC
Log "Arrancando landing streaming"
docker compose exec -d spark-master sh -c "spark-submit /app/jobs/00_landing/job_landing_sqlserver_streaming.py > /tmp/landing.log 2>&1"
Start-Sleep -Seconds 240

Log "Arrancando bronze streaming"
docker compose exec -d spark-master sh -c "spark-submit /app/jobs/01_bronze/job_bronze_sqlserver_streaming.py > /tmp/bronze_stream.log 2>&1"

$needed = @("tedis_net_eosa_system_elements", "tedis_net_eosa_system_tags",
            "tedis_net_eosa_system_nodes", "tedis_net_eosa_system_devices")
for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep -Seconds 60
    $ls = docker compose exec -T minio mc ls local/datalake/01_bronze/ 2>$null
    $missing = $needed | Where-Object { -not ($ls -match $_) }
    Log "CDC en Bronze, faltan: $($missing -join ', ')"
    if (-not $missing) { break }
}
# margen para que terminen los microbatches del snapshot
Start-Sleep -Seconds 300
docker compose exec -T minio mc ls local/datalake/01_bronze/ *> "$L\bronze_tables.log"
Log "Parando jobs de streaming"
docker compose exec -T spark-master sh -c 'for p in /proc/[0-9]*; do [ "${p#/proc/}" = "$$" ] && continue; if grep -qa -e job_landing_sqlserver_streaming -e job_bronze_sqlserver_streaming $p/cmdline 2>/dev/null; then kill ${p#/proc/}; fi; done'
docker compose exec -T spark-master sh -c "tail -n 60 /tmp/landing.log; tail -n 60 /tmp/bronze_stream.log" *> "$L\streaming_tail.log"

# ---------------------------------------------------------------- 3. Silver
$silver = @("d_elemento", "d_periodo", "d_municipio", "d_tipo_generico", "d_ct",
            "d_salida", "f_incidencia", "f_interrupcion", "d_tag",
            "f_tag_value_change", "f_evento", "f_command_execution", "f_corte",
            "dq_checks")
foreach ($job in $silver) {
    Log "silver $job ..."
    docker compose exec -T airflow spark-submit `
        --conf spark.cores.max=2 --conf spark.executor.memory=3g `
        --py-files /app/jobs/02_silver/silver_common.py,/app/jobs/02_silver/job_silver_f_tag_value_change.py `
        /app/jobs/02_silver/job_silver_$job.py --run-id $RunId `
        *> "$L\silver_$job.log"
    Log "silver $job exit=$LASTEXITCODE"
}

# ---------------------------------------------------------------- 4. Informe
$py = @"
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('silver-test-report').getOrCreate()
m = spark.table('l2_silver.dq_metrics').where("run_id = '$RunId'")
m.orderBy('job','entidad','ambito','metrica').toPandas().to_csv('/tmp/dq_metrics.csv', index=False)
rows = []
for t in spark.catalog.listTables('l2_silver'):
    try:
        rows.append((t.name, spark.table('l2_silver.' + t.name).count()))
    except Exception as e:
        rows.append((t.name, str(e)[:80]))
open('/tmp/silver_counts.csv','w').write('\n'.join(f'{a},{b}' for a,b in rows))
"@
$py | docker compose exec -T airflow sh -c "cat > /tmp/report.py && spark-submit /tmp/report.py" *> "$L\report.log"
docker compose exec -T airflow cat /tmp/dq_metrics.csv > "$L\dq_metrics.csv"
docker compose exec -T airflow cat /tmp/silver_counts.csv > "$L\silver_counts.csv"
Log "=== Fin prueba Silver, run_id=$RunId ==="
