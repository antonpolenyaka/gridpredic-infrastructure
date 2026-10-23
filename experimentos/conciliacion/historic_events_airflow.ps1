# M102: por que HistoricEvents no llego a Bronze. Busca en los registros de Airflow las tareas de dag_bronze_sqlserver
# de esa tabla y ensena su estado y las ultimas lineas de error. Solo lee; un minuto.
$Repo = "D:\GitHub\gridpredic-infrastructure"
$Compose = "$Repo\compose.yaml"
$L = "$Repo\_runlogs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
$Out = "$L\m102_historic_events_airflow_$Stamp.log"
function Say($msg) {
    $line = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $msg
    try { $line | Tee-Object -FilePath $Out -Append | Write-Host } catch { Write-Host $line }
}
if (-not (Test-Path "D:\")) { Write-Host "ERROR: no esta el disco D:"; exit 1 }
docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Docker Desktop no esta arrancado"; exit 1 }
Say "=== M102: HistoricEvents en los registros de Airflow === Log: $Out"
$sh = 'd=/opt/airflow/logs; ls -d $d/dag_id=dag_bronze_sqlserver* 2>/dev/null | head -3; ' +
      'find $d -path "*dag_bronze_sqlserver*" -ipath "*historicevents*" -name "*.log" 2>/dev/null | sort | tail -20'
docker compose -f $Compose exec -T airflow sh -c $sh 2>&1 | ForEach-Object { Say ("  " + $_) }
$sh2 = 'd=/opt/airflow/logs; for f in $(find $d -path "*dag_bronze_sqlserver*" -ipath "*historicevents*" -name "*.log" 2>/dev/null | sort | tail -3); do ' +
       'echo "=== $f"; grep -i -E "error|exception|traceback|failed|no data|success|marking task" "$f" | tail -15; done'
docker compose -f $Compose exec -T airflow sh -c $sh2 2>&1 | ForEach-Object { Say ("  " + $_.Substring(0, [Math]::Min(300, $_.Length))) }
$sh3 = 'airflow tasks states-for-dag-run --help >/dev/null 2>&1; airflow dags list-runs dag_bronze_sqlserver 2>/dev/null | head -15'
docker compose -f $Compose exec -T airflow sh -c $sh3 2>&1 | ForEach-Object { Say ("  " + $_.Substring(0, [Math]::Min(300, $_.Length))) }
Say "Terminado sin errores."
exit 0
