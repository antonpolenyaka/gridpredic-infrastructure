# M102: por que HistoricEvents no llego a Bronze. Busca en los registros de Airflow las tareas de dag_bronze_sqlserver
# de esa tabla y ensena su estado y las ultimas lineas de error. Solo lee; un minuto.
$Repo = "D:\GitHub\gridpredic-infrastructure"
$Compose = "$Repo\compose.yaml"
$L = "$Repo\_runlogs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
$Fallos = 0
$Out = "$L\m102_historic_events_airflow_$Stamp.log"
function Say($msg) {
    $line = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $msg
    try { $line | Tee-Object -FilePath $Out -Append | Write-Host } catch { Write-Host $line }
}
if (-not (Test-Path "D:\")) { Write-Host "ERROR: no esta el disco D:"; exit 1 }
docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Docker Desktop no esta arrancado"; exit 1 }
Say "=== M102: HistoricEvents en los registros de Airflow === Log: $Out"
# bash con pipefail: si falla find, ls o airflow, el fallo no se pierde en el sort, el tail o el sed que vienen detras.
# sed -n en vez de head: head cierra la tuberia antes y daria un falso fallo (SIGPIPE) con pipefail.
$sh = 'set -o pipefail; d=/opt/airflow/logs; { ls -d $d/dag_id=dag_bronze_sqlserver* 2>/dev/null || true; } | sed -n 1,3p; ' +
      'find $d -path "*dag_bronze_sqlserver*" -ipath "*historicevents*" -name "*.log" 2>/dev/null | sort | tail -20'
$sh2 = 'set -o pipefail; d=/opt/airflow/logs; files=$(find $d -path "*dag_bronze_sqlserver*" -ipath "*historicevents*" -name "*.log" 2>/dev/null | sort | tail -3) || exit 1; ' +
       'for f in $files; do echo "=== $f"; { grep -i -E "error|exception|traceback|failed|no data|success|marking task" "$f" || true; } | tail -15; done'
$sh3 = 'set -o pipefail; airflow dags list-runs dag_bronze_sqlserver 2>/dev/null | sed -n 1,15p'
docker compose -f $Compose exec -T airflow bash -c $sh 2>&1 | ForEach-Object { Say ("  " + $_) }
if ($LASTEXITCODE -ne 0) { $Fallos++; Say "  FALLO: la consulta de los registros devolvio $LASTEXITCODE" }
docker compose -f $Compose exec -T airflow bash -c $sh2 2>&1 | ForEach-Object { Say ("  " + $_.Substring(0, [Math]::Min(300, $_.Length))) }
if ($LASTEXITCODE -ne 0) { $Fallos++; Say "  FALLO: la lectura de los errores devolvio $LASTEXITCODE" }
docker compose -f $Compose exec -T airflow bash -c $sh3 2>&1 | ForEach-Object { Say ("  " + $_.Substring(0, [Math]::Min(300, $_.Length))) }
if ($LASTEXITCODE -ne 0) { $Fallos++; Say "  FALLO: la lista de ejecuciones devolvio $LASTEXITCODE" }
if ($Fallos -gt 0) { Say "Terminado con $Fallos consultas fallidas: el resultado no es completo."; exit 1 }
Say "Terminado sin errores."
exit 0
