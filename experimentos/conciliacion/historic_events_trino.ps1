# M102: HistoricEvents (2.757.447 filas en SQL Server) no esta en Bronze. Mira que tablas de eventos hay en
# l1_bronze, que tiene l2_silver.f_evento (filas, fechas, origen) y que apunto Silver en dq_metrics la ultima vez.
# Solo lee con Trino; un minuto.
$Repo = "D:\GitHub\gridpredic-infrastructure"
$Compose = "$Repo\compose.yaml"
$L = "$Repo\_runlogs"
$Stamp = Get-Date -Format "yyyyMMdd_HHmm"
$Fallos = 0
$Out = "$L\m102_historic_events_$Stamp.log"
function Say($msg) {
    $line = (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + " " + $msg
    try { $line | Tee-Object -FilePath $Out -Append | Write-Host } catch { Write-Host $line }
}
function Trino($titulo, $sql) {
    Say "--- $titulo"
    docker compose -f $Compose exec -T trino trino --output-format ALIGNED --execute $sql 2>&1 | ForEach-Object { Say ("    " + $_) }
    if ($LASTEXITCODE -ne 0) { $script:Fallos++; Say "    FALLO: Trino devolvio $LASTEXITCODE" }
}
if (-not (Test-Path "D:\")) { Write-Host "ERROR: no esta el disco D:"; exit 1 }
docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "ERROR: Docker Desktop no esta arrancado"; exit 1 }
Say "=== M102: HistoricEvents en Bronze y f_evento en Silver === Log: $Out"
Trino "Tablas de eventos en l1_bronze" "SHOW TABLES FROM lakehouse.l1_bronze LIKE '%event%'"
Trino "f_evento en Silver: filas y fechas" "SELECT count(*) AS filas, min(ts) AS desde, max(ts) AS hasta, count(DISTINCT fecha) AS dias FROM lakehouse.l2_silver.f_evento"
Trino "f_evento por anio" "SELECT year(ts) AS anio, count(*) AS filas FROM lakehouse.l2_silver.f_evento GROUP BY 1 ORDER BY 1"
Trino "dq_metrics de f_evento (ultimas 15 filas)" "SELECT metrica, valor, run_id, ts FROM lakehouse.l2_silver.dq_metrics WHERE entidad = 'f_evento' ORDER BY ts DESC LIMIT 15"
Trino "SystemEvents en Bronze: filas y fechas" "SELECT count(*) AS filas FROM lakehouse.l1_bronze.tedis_net_eosa_system_events"
if ($Fallos -gt 0) { Say "Terminado con $Fallos consultas fallidas: el resultado no es completo."; exit 1 }
Say "Terminado sin errores."
exit 0
